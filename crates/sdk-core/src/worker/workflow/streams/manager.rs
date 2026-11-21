//! Runs the Stream calls of a Worker's runs, one task for each call.

use super::{StreamRuntimes, StreamSettings, marker::ReadDone};
use crate::telemetry::metrics::{FailureReason, MetricsContext, failure_reason};
use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use futures_util::FutureExt;
use parking_lot::Mutex;
use std::{
    collections::HashMap, fmt, future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration,
};
use temporalio_common::protos::{
    coresdk::external_data::{StreamAppendOffsets, StreamFailure, stream_append_result},
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    },
};
use temporalio_streams::{
    AnyReason, AppendError, CloseError, ReadError, StreamError, StreamRuntime,
    proto::{
        AppendRecordsRequest, CloseStreamRequest, ReadRecordsRequest, ReadRecordsResponse,
        StreamErrorReason, StreamRef,
    },
};
use tokio::{runtime::Handle, task::AbortHandle, time::Instant};

/// The maximum length of a failure message that Workflow code gets and a marker records, in bytes.
/// A Store can return error text of any size, so the message needs a bound.
const MAX_FAILURE_MESSAGE_BYTES: usize = 4096;

/// The maximum random change of the wait before the next try, as a fraction of that wait. A retry
/// of a client call has the same value.
const RETRY_RANDOMIZATION: f64 = 0.2;

/// A call of a run to start or to stop.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Most requests start a call
pub(crate) enum Request {
    /// Starts a call.
    Run(Call),
    /// Stops the call with this id, which then gives no result.
    Stop(u64),
}

/// A call to start, with the request that each of its tries sends.
#[derive(Debug)]
pub(crate) struct Call {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    /// The Store name of the call's ref, which picks the runtime of the call.
    pub(crate) store: String,
    pub(crate) op: Op,
}

#[derive(Debug)]
pub(crate) enum Op {
    Append(AppendRecordsRequest),
    Close(CloseStreamRequest),
    Read(ReadCall),
}

/// A read to run.
#[derive(Debug)]
pub(crate) struct ReadCall {
    /// The request of the first try, with the `from_offset` and the page size of the read.
    pub(crate) request: ReadRecordsRequest,
    /// How long the read waits for a Record.
    pub(crate) wait: Duration,
    pub(crate) records_in_history: bool,
}

/// The result of a call, for its run.
#[derive(Debug)]
pub(crate) struct StreamResolution {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) outcome: Outcome,
}

#[derive(Debug)]
pub(crate) enum Outcome {
    Append(stream_append_result::Result),
    /// `None` if the Stream is closed.
    Close(Option<StreamFailure>),
    Read(ReadDone),
}

type Sink = Arc<dyn Fn(String, StreamResolution) + Send + Sync>;

/// The running calls, keyed by run id and call id.
type Tasks = Arc<Mutex<HashMap<String, HashMap<u64, Task>>>>;

struct Task {
    abort: AbortHandle,
    /// The failure of the last try, while the call waits to try again.
    outage: Option<String>,
}

/// The runner of a Worker's Stream calls. It runs them on the Worker's runtime, so Store work never
/// holds up the thread of the runs, and dropping it stops every call.
pub(crate) struct StreamManager {
    runtimes: StreamRuntimes,
    handle: Handle,
    sink: Sink,
    tasks: Tasks,
}

impl StreamManager {
    /// Creates a manager that runs calls on `handle`, and gives each result to `sink` with its run
    /// id.
    pub(crate) fn new(
        runtimes: StreamRuntimes,
        handle: Handle,
        sink: impl Fn(String, StreamResolution) + Send + Sync + 'static,
    ) -> Self {
        Self {
            runtimes,
            handle,
            sink: Arc::new(sink),
            tasks: Default::default(),
        }
    }

    /// Starts and stops calls of the run `run_id`. `metrics` gives the metrics of a Store.
    pub(crate) fn dispatch(
        &self,
        run_id: &str,
        requests: Vec<Request>,
        mut metrics: impl FnMut(&str) -> MetricsContext,
    ) {
        // The lock is held across each spawn, so a call that ends immediately finds its task.
        let mut tasks = self.tasks.lock();
        for request in requests {
            match request {
                Request::Run(call) => {
                    let call_id = call.call_id;
                    let metrics = metrics(&call.store);
                    let abort = self.spawn(run_id, call, metrics);
                    let run = tasks.entry(run_id.to_string()).or_default();
                    run.insert(
                        call_id,
                        Task {
                            abort,
                            outage: None,
                        },
                    );
                }
                Request::Stop(call_id) => {
                    if let Some(run) = tasks.get_mut(run_id)
                        && let Some(task) = run.remove(&call_id)
                    {
                        task.abort.abort();
                        if run.is_empty() {
                            tasks.remove(run_id);
                        }
                    }
                }
            }
        }
    }

    /// Stops every call of the run. A result that it already sent finds no call.
    pub(crate) fn cancel_run(&self, run_id: &str) {
        if let Some(run) = self.tasks.lock().remove(run_id) {
            for task in run.into_values() {
                task.abort.abort();
            }
        }
    }

    /// Returns the failure of the run's call that waits to try again after an outage. If several
    /// do, it's the one with the lowest call id.
    pub(crate) fn outage(&self, run_id: &str) -> Option<String> {
        let tasks = self.tasks.lock();
        let run = tasks.get(run_id)?;
        run.iter()
            .filter_map(|(call_id, task)| Some((*call_id, task.outage.as_ref()?)))
            .min_by_key(|(call_id, _)| *call_id)
            .map(|(_, outage)| outage.clone())
    }

    fn spawn(&self, run_id: &str, call: Call, metrics: MetricsContext) -> AbortHandle {
        let Call {
            call_id,
            seq,
            store,
            op,
        } = call;
        let stream_id = match &op {
            Op::Append(request) => &request.stream_id,
            Op::Close(request) => &request.stream_id,
            Op::Read(read) => &read.request.stream_id,
        };
        let runtime = (self.runtimes.get(&store).cloned())
            .expect("The run starts no call on a Store with no runtime");
        let settings = self.runtimes.settings;
        let note = Note {
            tasks: self.tasks.clone(),
            run_id: run_id.to_string(),
            call_id,
            what: format!(
                "{} of Stream \"{}\" on Store \"{}\"",
                match op {
                    Op::Append(_) => "append",
                    Op::Close(_) => "close",
                    Op::Read(_) => "read",
                },
                stream_id,
                store
            ),
        };
        let sink = self.sink.clone();
        let task = self.handle.spawn(async move {
            let outcome = run(&runtime, seq, &store, op, settings, &note, &metrics).await;
            if note.end() {
                sink(
                    note.run_id,
                    StreamResolution {
                        call_id,
                        seq,
                        outcome,
                    },
                );
            }
        });
        task.abort_handle()
    }
}

impl Drop for StreamManager {
    fn drop(&mut self) {
        for run in self.tasks.lock().drain() {
            for task in run.1.into_values() {
                task.abort.abort();
            }
        }
    }
}

/// Runs the call `seq` on the Store `store`, and returns its outcome. The metrics count each try
/// when it starts, as those of a Local Activity count each attempt. They time a try of an append
/// or a close once it ends, so a try that is stopped has only its count, and a read has no latency.
async fn run(
    runtime: &StreamRuntime,
    seq: u32,
    store: &str,
    op: Op,
    settings: StreamSettings,
    note: &Note,
    metrics: &MetricsContext,
) -> Outcome {
    match op {
        Op::Append(request) => {
            let send = || {
                metrics.stream_append_executed();
                runtime.append_records(request.clone())
            };
            let tried = |took: Duration, error: Option<&AppendError>| {
                metrics.stream_append_exec_latency(took);
                if let Some(error) = error {
                    failed(metrics, error).stream_append_execution_failed();
                }
            };
            let answer = tries(settings, note, send, tried).await;
            Outcome::Append(match answer {
                Ok(appended) => stream_append_result::Result::Appended(StreamAppendOffsets {
                    first_offset: appended.first_offset,
                    last_offset: appended.last_offset,
                }),
                Err(error) => stream_append_result::Result::Failure(recorded(&error)),
            })
        }
        Op::Close(request) => {
            let send = || {
                metrics.stream_close_executed();
                runtime.close_stream(request.clone())
            };
            let tried = |took: Duration, error: Option<&CloseError>| {
                metrics.stream_close_exec_latency(took);
                if let Some(error) = error {
                    failed(metrics, error).stream_close_execution_failed();
                }
            };
            let answer = tries(settings, note, send, tried).await;
            Outcome::Close(answer.err().map(|error| recorded(&error)))
        }
        Op::Read(read) => {
            let stream = StreamRef {
                id: read.request.stream_id.clone(),
                store: store.to_string(),
            };
            let from_offset = read.request.from_offset.clone();
            let page = read_page(runtime, read.request, read.wait, settings, note, metrics).await;
            if let Ok(page) = &page {
                metrics.stream_read_records(page.records.len() as u64);
            }
            let page = page.map_err(|error| recorded(&error));
            let in_history = read.records_in_history;
            Outcome::Read(ReadDone::new(&stream, seq, from_offset, page, in_history))
        }
    }
}

/// Reads the page of Records after the `from_offset` of `request`, and waits up to `wait` for its
/// first Record. A try that fails with `StoreUnavailable`, or gets no answer by its deadline, notes
/// an outage, and the read asks again from where it is after a backoff.
async fn read_page(
    runtime: &StreamRuntime,
    mut request: ReadRecordsRequest,
    wait: Duration,
    settings: StreamSettings,
    note: &Note,
    metrics: &MetricsContext,
) -> Result<ReadRecordsResponse, ReadError> {
    // A time past the end of the clock never ends the wait.
    let wait_until = Instant::now().checked_add(wait);
    let mut waits = retry_waits(&settings);
    let longest = prost_types::Duration {
        seconds: i64::MAX,
        nanos: 0,
    };
    loop {
        let max_wait = settings.read_max_wait;
        let left = wait_until.map_or(max_wait, |at| at.saturating_duration_since(Instant::now()));
        let wait = left.min(max_wait);
        request.wait = (!wait.is_zero()).then(|| wait.try_into().unwrap_or(longest));
        // The Store can take the whole wait to answer, so the deadline of the try adds it.
        let deadline = settings.try_deadline.saturating_add(wait);
        let asked = Instant::now();
        metrics.stream_read_executed();
        let read = caught(runtime.read_records(request.clone()));
        let answer = match tokio::time::timeout(deadline, read).await {
            Ok(Err(error)) if !is_outage(&error) => {
                failed(metrics, &error).stream_read_execution_failed();
                return Err(error);
            }
            Ok(answer) => answer,
            Err(_) => Err(StreamError::store_unavailable(format!(
                "The Store gave no answer in {deadline:?}"
            ))),
        };
        let page = match answer {
            Ok(page) => page,
            Err(error) => {
                failed(metrics, &error).stream_read_execution_failed();
                note.set(Some(&error));
                tokio::time::sleep(next_wait(&mut waits)).await;
                continue;
            }
        };
        note.set(None);
        waits = retry_waits(&settings);
        if !page.records.is_empty() || page.done {
            return Ok(page);
        }
        // A try that passed entries which aren't Records may have stopped short of a Record, so
        // the read tries again from where that try ended.
        if page.last_offset != request.from_offset {
            request.from_offset = page.last_offset;
            continue;
        }
        if wait_until.is_some_and(|at| at <= Instant::now()) {
            return Ok(page);
        }
        // A Store that answers immediately with nothing would otherwise get the next read
        // immediately, so the read waits out the rest of the wait first.
        tokio::time::sleep(wait.saturating_sub(asked.elapsed())).await;
    }
}

/// Sends the request again while the Store is in an outage, with no bound in time. A Store writes
/// nothing for the resend of a batch whose request id it still remembers, and a second close
/// succeeds. A try that reaches the Store after more batches than it remembers is written again.
/// `tried` gets the duration of each try, and its error if it failed.
async fn tries<T, R, F>(
    settings: StreamSettings,
    note: &Note,
    mut send: impl FnMut() -> F,
    tried: impl Fn(Duration, Option<&StreamError<R>>),
) -> Result<T, StreamError<R>>
where
    R: Copy + From<AnyReason> + Into<StreamErrorReason>,
    F: Future<Output = Result<T, StreamError<R>>>,
{
    let mut waits = retry_waits(&settings);
    loop {
        let started = Instant::now();
        let error = match tokio::time::timeout(settings.try_deadline, caught(send())).await {
            Ok(Err(error)) if is_outage(&error) => error,
            Ok(answer) => {
                tried(started.elapsed(), answer.as_ref().err());
                note.set(None);
                return answer;
            }
            Err(_) => StreamError::store_unavailable(format!(
                "The Store gave no answer in {:?}",
                settings.try_deadline
            )),
        };
        tried(started.elapsed(), Some(&error));
        note.set(Some(&error));
        tokio::time::sleep(next_wait(&mut waits)).await;
    }
}

/// Returns the answer of one try of a Store call. A panic of the Store's code counts as a failed
/// try in an outage. Without that, the call would hold its Workflow Task and never get a result.
pub(super) async fn caught<T, R: From<AnyReason>>(
    call: impl Future<Output = Result<T, StreamError<R>>>,
) -> Result<T, StreamError<R>> {
    let answer = AssertUnwindSafe(call).catch_unwind().await;
    answer.unwrap_or_else(|panic| {
        let message = (panic.downcast_ref::<&str>().copied())
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("Couldn't turn panic into a string");
        Err(StreamError::store_unavailable(format!(
            "The Store panicked: {message}"
        )))
    })
}

/// Returns the waits between the tries of a call, which double from `retry_initial_interval` up to
/// `retry_max_interval`.
pub(super) fn retry_waits(settings: &StreamSettings) -> ExponentialBackoff {
    ExponentialBuilder::new()
        .with_min_delay(settings.retry_initial_interval)
        .with_factor(2.0)
        .with_max_delay(settings.retry_max_interval)
        .without_max_times()
        .build()
}

/// Returns the next wait of `waits`, made longer or shorter at random by up to
/// [`RETRY_RANDOMIZATION`] of its length, so that the calls of many runs don't all try again at
/// the same moment.
pub(super) fn next_wait(waits: &mut ExponentialBackoff) -> Duration {
    let wait = waits.next().unwrap_or_default();
    let jitter = 1.0 + RETRY_RANDOMIZATION * (2.0 * rand::random::<f64>() - 1.0);
    Duration::try_from_secs_f64(wait.as_secs_f64() * jitter).unwrap_or(wait)
}

/// The handle of one call to its entry in the manager.
struct Note {
    tasks: Tasks,
    run_id: String,
    call_id: u64,
    what: String,
}

impl Note {
    /// Notes the failure of the last try, or clears it with `None`.
    fn set(&self, error: Option<&dyn fmt::Display>) {
        let mut tasks = self.tasks.lock();
        let task = tasks
            .get_mut(&self.run_id)
            .and_then(|run| run.get_mut(&self.call_id));
        if let Some(task) = task {
            task.outage = error.map(|error| {
                format!(
                    "StreamStoreUnavailable: {}: {}",
                    self.what,
                    cut(error.to_string())
                )
            });
        }
    }

    /// Removes the task. Returns false for a call that was stopped, which gives no result.
    fn end(&self) -> bool {
        let mut tasks = self.tasks.lock();
        let Some(run) = tasks.get_mut(&self.run_id) else {
            return false;
        };
        let ended = run.remove(&self.call_id).is_some();
        if run.is_empty() {
            tasks.remove(&self.run_id);
        }
        ended
    }
}

/// Returns `metrics` with the reason of `error` as the `failure_reason`, such as `stream_closed`.
fn failed<R: Copy + Into<StreamErrorReason>>(
    metrics: &MetricsContext,
    error: &StreamError<R>,
) -> MetricsContext {
    metrics.with_new_attrs([failure_reason(FailureReason::StreamError(error.token()))])
}

/// Returns true if `error` says that the Store is in an outage, so that another try of the call may
/// succeed.
pub(super) fn is_outage<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> bool {
    error.reason.into() == StreamErrorReason::StoreUnavailable
}

/// Returns the reason and the message of `error`, as a marker records a failed call.
pub(super) fn recorded<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> StreamFailure {
    StreamFailure {
        reason: error.reason.into().as_str_name().to_string(),
        message: cut(error.message.clone()),
    }
}

/// Returns the failure that Workflow code gets for a failed call that a marker records.
pub(super) fn failure_of(recorded: &StreamFailure) -> Failure {
    let reason = StreamErrorReason::from_str_name(&recorded.reason)
        .expect("The decode of a marker checks the reason of each failure");
    let message = recorded.message.clone();
    failure(&StreamError { reason, message })
}

/// Returns the failure that Workflow code gets for `error`. Its message is the error's message, its
/// type is the reason's token, and its details hold the encoded
/// [`StreamErrorDetails`](temporalio_streams::proto::StreamErrorDetails).
fn failure<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> Failure {
    Failure {
        message: cut(error.message.clone()),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: error.token(),
                non_retryable: !is_outage(error),
                details: Some(Payloads {
                    payloads: vec![Payload {
                        metadata: HashMap::from([
                            ("encoding".to_string(), b"binary/protobuf".to_vec()),
                            (
                                "messageType".to_string(),
                                b"temporal.api.stream.v1.StreamErrorDetails".to_vec(),
                            ),
                        ]),
                        data: error.details(),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

/// Cuts `text` on a character boundary to at most [`MAX_FAILURE_MESSAGE_BYTES`], with `...` at the
/// end.
pub(super) fn cut(text: String) -> String {
    if text.len() <= MAX_FAILURE_MESSAGE_BYTES {
        return text;
    }
    let mut end = MAX_FAILURE_MESSAGE_BYTES - "...".len();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::super::{
        marker::MarkerBuilder,
        test_store::{Entry, Fault, Model, ModelStore, client},
    };
    use super::*;
    use crate::test_help::stream_store::{MemoryStore, Op as StoreOp};
    use prost::Message;
    use prost_types::Timestamp;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use temporalio_common::protos::coresdk::workflow_activation::resolve_stream_read;
    use temporalio_streams::{
        AnyError, ReadReason, StreamStore,
        proto::{
            AppendRecord, CreateStreamRequest, Producer, StreamRecord, WorkflowProducer,
            WorkflowTaskAttempt, producer::Kind,
        },
        record::{StoreLimits, field_len},
    };
    use tokio::sync::mpsc;

    type Results = mpsc::UnboundedReceiver<(String, StreamResolution)>;

    /// Returns a manager of a Store that has the Stream `s`.
    async fn manager() -> (Arc<MemoryStore>, StreamManager, Results) {
        let store = Arc::new(MemoryStore::default());
        let create = CreateStreamRequest {
            stream_id: "s".to_string(),
            ..Default::default()
        };
        store.create_stream(create).await.unwrap();
        let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
        let runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
        let (tx, rx) = mpsc::unbounded_channel();
        let sink = move |run_id, resolution| tx.send((run_id, resolution)).unwrap();
        let manager = StreamManager::new(runtimes, Handle::current(), sink);
        (store, manager, rx)
    }

    fn no_metrics(_: &str) -> MetricsContext {
        MetricsContext::no_op()
    }

    fn producer() -> Option<Producer> {
        Some(Producer {
            kind: Some(Kind::Workflow(WorkflowProducer {
                namespace: "n".to_string(),
                workflow_id: "w".to_string(),
                run_id: "r".to_string(),
                attempt: Some(WorkflowTaskAttempt {
                    started_event_id: 3,
                    attempt: 1,
                    started_time: Some(Timestamp::default()),
                }),
            })),
        })
    }

    fn append(call_id: u64) -> Request {
        Request::Run(Call {
            call_id,
            seq: 1,
            store: "mem".to_string(),
            op: Op::Append(AppendRecordsRequest {
                stream_id: "s".to_string(),
                producer: producer(),
                records: vec![AppendRecord {
                    body: Some(Payload::default()),
                }],
                ..Default::default()
            }),
        })
    }

    fn close(call_id: u64) -> Request {
        Request::Run(Call {
            call_id,
            seq: 2,
            store: "mem".to_string(),
            op: Op::Close(CloseStreamRequest {
                stream_id: "s".to_string(),
                producer: producer(),
            }),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_try_with_no_answer_by_its_deadline_is_an_outage() {
        let (store, manager, mut results) = manager().await;
        store.hold(StoreOp::Append);
        manager.dispatch("r", vec![append(1)], no_metrics);
        tokio::time::sleep(Duration::from_millis(5010)).await;
        assert_eq!(
            manager.outage("r").as_deref(),
            Some(
                "StreamStoreUnavailable: append of Stream \"s\" on Store \"mem\": \
                 stream_store_unavailable: The Store gave no answer in 5s"
            )
        );
        store.release();
        let (run_id, resolution) = results.recv().await.unwrap();
        assert_eq!(run_id, "r");
        assert!(matches!(
            resolution.outcome,
            Outcome::Append(stream_append_result::Result::Appended(_))
        ));
        assert_eq!(manager.outage("r"), None);
        let tries = store.requests();
        assert!(tries.len() >= 2, "{tries:?}");
        assert!(tries.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancel_stops_the_calls_of_a_run_and_their_outage() {
        let (store, manager, mut results) = manager().await;
        let down = || AnyError::store_unavailable("The Store is down");
        store.fail(StoreOp::Append, down(), usize::MAX);
        store.fail(StoreOp::Close, down(), usize::MAX);
        manager.dispatch("a", vec![append(1), close(2), append(3)], no_metrics);
        manager.dispatch("b", vec![close(4)], no_metrics);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(manager.outage("a").is_some());
        manager.dispatch("a", vec![Request::Stop(3)], no_metrics);
        manager.cancel_run("a");
        assert_eq!(manager.outage("a"), None);
        assert!(manager.outage("b").is_some());
        store.fail(StoreOp::Close, down(), 0);
        let (run_id, resolution) = results.recv().await.unwrap();
        assert_eq!((run_id.as_str(), resolution.call_id), ("b", 4));
        assert!(matches!(resolution.outcome, Outcome::Close(None)));
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(results.try_recv().is_err());
        let after = store.requests().len();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(store.requests().len(), after);
    }

    // Workflow code gets the failure of a call from the reason and the message that a marker
    // records, in the first run and in a replay.
    #[test]
    fn a_failure_reads_back_as_the_failure_of_its_call() {
        let reasons: Vec<StreamErrorReason> = (0..)
            .map_while(|number| StreamErrorReason::try_from(number).ok())
            .collect();
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let reason = reasons[rng.random_range(0..reasons.len())];
            // Many messages are over the cut at 4096 bytes, which can fall inside a character.
            let message: String = (0..rng.random_range(0..2500))
                .map(|_| ['x', 'é', '€', '𝄞'][rng.random_range(0..4)])
                .collect();
            let error = StreamError { reason, message };
            let read_back = failure_of(&recorded(&error));
            assert_eq!(read_back, failure(&error), "seed {seed}: {reason:?}");
        }
    }

    /// The read that a test runs now. The Store checks each request against it.
    #[derive(Default)]
    struct Reading {
        from_offset: Vec<u8>,
        max_records: i32,
        max_bytes: i64,
        /// When the wait of the read is over, unless it never is.
        wait_until: Option<Instant>,
        violations: Vec<String>,
    }

    /// Returns a Stream of up to 40 entries, of which some aren't Records, that come over time.
    fn entries(rng: &mut StdRng) -> Vec<Entry> {
        let mut at = Duration::ZERO;
        (0..rng.random_range(0..40u16))
            .map(|index| {
                at += Duration::from_millis(rng.random_range(0..30));
                let offset = (index * 2 + 1).to_be_bytes().to_vec();
                let record = rng.random_bool(0.75).then(|| StreamRecord {
                    offset: offset.clone(),
                    body: Some(Payload {
                        data: vec![0; rng.random_range(0..100)],
                        ..Default::default()
                    }),
                    producer: Some(client()),
                });
                Entry { offset, at, record }
            })
            .collect()
    }

    // Workflow code reads a Stream with one read for each page, from the last Record that it got.
    // A read waits out its wait, so a Store that answers immediately gets no read in a loop.
    #[tokio::test(start_paused = true)]
    async fn a_read_gives_the_next_records_or_none_once_its_wait_is_over() {
        let (mut ends, mut outages, mut early, mut loops) = ([0; 3], 0, 0, 0);
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let entries = entries(&mut rng);
            let from_offset = match rng.random_range(0..=entries.len()) {
                0 => vec![],
                at => entries[at - 1].offset.clone(),
            };
            let wait = match rng.random_range(0..4) {
                0 => Duration::ZERO,
                1 => Duration::MAX,
                _ => Duration::from_millis(rng.random_range(1..300)),
            };
            // A read that never stops waiting ends once its Stream is closed and read.
            let closed = wait == Duration::MAX || rng.random_bool(0.5);
            let settings = StreamSettings {
                retry_initial_interval: Duration::from_millis(1),
                retry_max_interval: Duration::from_millis(5),
                try_deadline: Duration::from_millis(20),
                read_max_wait: Duration::from_millis(rng.random_range(1..200)),
                ..Default::default()
            };
            let fails = rng.random_bool(0.3);
            let reading = Arc::new(Mutex::new(Reading::default()));
            let checked = reading.clone();
            let fault = move |rng: &mut StdRng, request: &ReadRecordsRequest| {
                let mut reading = checked.lock();
                // The Store waits no longer than the read has left to wait.
                let max_wait = settings.read_max_wait;
                let left = (reading.wait_until).map_or(max_wait, |until| {
                    until.saturating_duration_since(Instant::now())
                });
                let wait = (request.wait).map_or(Duration::ZERO, |wait| wait.try_into().unwrap());
                let rules = [
                    request.max_records == reading.max_records,
                    request.max_bytes == reading.max_bytes,
                    request.from_offset >= reading.from_offset,
                    wait <= left.min(max_wait),
                ];
                if let Some(rule) = rules.iter().position(|kept| !kept) {
                    let violation = format!("seed {seed}: rule {rule} of a request");
                    reading.violations.push(violation);
                }
                match rng.random_range(0..100) {
                    0..=4 => Some(Fault::Fails(StreamError::store_unavailable("Down"))),
                    5..=7 => Some(Fault::Silent),
                    8 if fails => {
                        let gone = ReadError::operation(ReadReason::NotFound, "Gone");
                        Some(Fault::Fails(gone))
                    }
                    _ => None,
                }
            };
            let store = Arc::new(ModelStore::new(Model {
                rng: StdRng::seed_from_u64(seed),
                entries,
                closed,
                fault: Box::new(fault),
                ignores_wait: rng.random_bool(0.3),
                reading: (0, 0),
                moved_to: None,
                empty_until: None,
                answered_early: 0,
                violations: vec![],
            }));
            let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
            let mut runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
            runtimes.settings = settings;
            let (tx, mut results) = mpsc::unbounded_channel();
            let sink = move |_, resolution: StreamResolution| tx.send(resolution).unwrap();
            let manager = StreamManager::new(runtimes, Handle::current(), sink);
            let stream = StreamRef {
                id: "s".to_string(),
                store: "mem".to_string(),
            };
            let (mut cursor, mut got) = (from_offset.clone(), vec![]);
            for call_id in 1.. {
                let max_records = rng.random_range(1..=10);
                let max_bytes = rng.random_range(1..2000);
                let started = Instant::now();
                {
                    let mut reading = reading.lock();
                    reading.from_offset.clone_from(&cursor);
                    (reading.max_records, reading.max_bytes) = (max_records, max_bytes);
                    reading.wait_until = started.checked_add(wait);
                }
                let request = ReadRecordsRequest {
                    stream_id: stream.id.clone(),
                    from_offset: cursor.clone(),
                    max_records,
                    max_bytes,
                    wait: None,
                };
                let read = ReadCall {
                    request,
                    wait,
                    records_in_history: rng.random_bool(0.5),
                };
                let call = Call {
                    call_id,
                    seq: call_id as u32,
                    store: stream.store.clone(),
                    op: Op::Read(read),
                };
                manager.dispatch("r", vec![Request::Run(call)], no_metrics);
                let resolution = loop {
                    let look = Duration::from_millis(rng.random_range(1..40));
                    tokio::select! {
                        resolution = results.recv() => break resolution.unwrap(),
                        () = tokio::time::sleep(look) => {
                            outages += usize::from(manager.outage("r").is_some());
                        }
                    }
                };
                let Outcome::Read(done) = resolution.outcome else {
                    panic!("seed {seed}: a read gives the result of a read");
                };
                let job = MarkerBuilder::new(settings).read_result(done, 0);
                let page = match job.status.unwrap() {
                    resolve_stream_read::Status::Page(page) => page,
                    // Only a Store that can fail the read ends it with a failure.
                    resolve_stream_read::Status::Failed(failure) => {
                        assert!(fails, "seed {seed}: {failure:?}");
                        ends[0] += 1;
                        break;
                    }
                    resolve_stream_read::Status::Cancelled(()) => panic!("seed {seed}"),
                };
                // A page is within what the read asked for, or it's one Record.
                let bytes: usize = (page.records.iter())
                    .map(|record| field_len(record.encoded_len()))
                    .sum();
                assert!(page.records.len() <= max_records as usize, "seed {seed}");
                assert!(
                    bytes <= max_bytes as usize || page.records.len() == 1,
                    "seed {seed}"
                );
                if let Some(last) = page.records.last() {
                    cursor.clone_from(&last.offset);
                }
                let came = !page.records.is_empty();
                got.extend(page.records);
                let all = store.records_after(&from_offset);
                assert!(all.starts_with(&got), "seed {seed}");
                if page.done {
                    assert!(closed && got == all, "seed {seed}");
                    ends[1] += 1;
                    break;
                }
                // No Record came within the wait, and the last look found none.
                if !came {
                    let came = store.records_came_after(&from_offset);
                    assert_eq!(got, came, "seed {seed}");
                    assert!(started.elapsed() >= wait, "seed {seed}");
                    ends[2] += 1;
                    break;
                }
                loops += usize::from(call_id > 1);
            }
            let violations = [
                reading.lock().violations.clone(),
                store.model.lock().violations.clone(),
            ];
            assert_eq!(violations.concat(), Vec::<String>::new(), "seed {seed}");
            early += store.model.lock().answered_early;
        }
        // The scripts reach each kind of end, an outage, a Store that answers before the wait, and
        // a Stream that takes several reads.
        assert!(
            ends.iter().all(|count| *count > 0) && outages > 0 && early > 0 && loops > 0,
            "{ends:?} {outages} {early} {loops}"
        );
    }
}
