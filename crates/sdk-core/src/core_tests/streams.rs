//! The Stream calls of Workflow code, through a Worker on a mock server. The mock server writes the
//! History of each completion, so a replay Worker can replay it.

use crate::{
    StreamRuntimes, StreamSettings, Worker, WorkerConfig, WorkflowErrorType, init_replay_worker,
    internal_flags::CoreInternalFlags,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, MocksHolder, NAMESPACE, PollWFTRespExt, ResponseType, WorkerExt,
        build_mock_pollers, hist_to_poll_resp, mock_worker,
        stream_store::{MemoryStore, Op, Request},
        test_worker_cfg,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use futures_util::{StreamExt, stream};
use parking_lot::Mutex;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, StreamAppended, WorkflowActivation,
            remove_from_cache::EvictionReason, resolve_stream_append,
            workflow_activation_job::Variant as Job,
        },
        workflow_commands::{
            AppendToStream, CancelWorkflowExecution, CloseStream, CompleteWorkflowExecution,
            ContinueAsNewWorkflowExecution, FailWorkflowExecution, QueryResult, QuerySuccess,
            ScheduleLocalActivity, StartTimer, UpdateResponse, query_result, update_response,
            workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::Payload,
        enums::v1::{CommandType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::MarkerRecordedEventAttributes,
        query::v1::WorkflowQuery,
        sdk::v1::{
            WorkflowTaskStreamCalls,
            workflow_task_stream_calls::{AppendOffsets, AppendResult, CloseIntent, append_result},
        },
        workflowservice::v1::{
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_streams::{
    StreamError, StreamRuntime, StreamStore,
    attempt::producer_key,
    proto::{
        AppendRecord, AppendRecordsRequest, CreateStreamRequest, Producer, StreamRef,
        WorkflowProducer, WorkflowTaskAttempt, producer::Kind as ProducerKind,
    },
    record::StoreLimits,
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot::{self, error::TryRecvError},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

const STORE: &str = "mem";

fn stream(id: &str) -> StreamRef {
    StreamRef {
        id: id.to_string(),
        store: STORE.to_string(),
    }
}

/// Creates a Store with the Streams `ids`.
async fn store(ids: &[&str]) -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::default());
    for id in ids {
        let create = CreateStreamRequest {
            stream_ref: Some(stream(id)),
            ..Default::default()
        };
        store.create_stream(create, id.to_string()).await.unwrap();
    }
    store
}

fn runtimes(store: &Arc<MemoryStore>) -> StreamRuntimes {
    let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
    StreamRuntimes::new([(STORE.to_string(), Arc::new(runtime))])
}

/// Prints the seed of a generated test if the test panics while this lives. A panic on a thread of
/// Core reaches the test with no seed in its message.
struct SeedOnPanic(u64);

impl Drop for SeedOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("The test failed at seed {}", self.0);
        }
    }
}

/// Returns a History up to its first Workflow Task, with a task timeout of `task_timeout`.
fn history(task_timeout: Duration) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(task_timeout);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The task that the mock server gives after completion `number`, if any.
type NextTask =
    Box<dyn FnMut(usize, &mut TestHistoryBuilder) -> Option<PollWorkflowTaskQueueResponse> + Send>;

/// The task that the mock server gives in its answer to a completion, if any.
type Eager = Box<
    dyn FnMut(
            &WorkflowTaskCompletion,
            &mut TestHistoryBuilder,
        ) -> Option<PollWorkflowTaskQueueResponse>
        + Send,
>;

struct StartOpts {
    /// The tasks of the History that the poller gives.
    responses: Vec<ResponseType>,
    /// When set, the poller gives the tasks that the test sends here instead of `responses`, as
    /// soon as they arrive. Core can then get a task while an activation is outstanding.
    tasks: Option<UnboundedReceiver<PollWorkflowTaskQueueResponse>>,
    /// While the mock server answers completion `number`, it sends the task that this returns to
    /// `tasks` and closes it. It answers once Core took the task.
    during_report: Option<(UnboundedSender<PollWorkflowTaskQueueResponse>, NextTask)>,
    store: Arc<MemoryStore>,
    cache: usize,
    /// The mock server answers a forced completion with the next task, inline.
    served: Option<NextTask>,
    /// Runs a replay Worker on the History.
    replay: bool,
    /// The completion that the server refuses for an event that the Worker didn't see.
    refuse: Option<usize>,
    /// When set, the mock server answers each completion with the task that this returns, inline,
    /// in place of `served`.
    eager: Option<Eager>,
    /// The failed Workflow Tasks that the server gets.
    fails: usize,
    configure: fn(&mut WorkerConfig),
}

impl StartOpts {
    fn new(store: &Arc<MemoryStore>) -> Self {
        Self {
            responses: vec![ResponseType::AllHistory],
            tasks: None,
            during_report: None,
            store: store.clone(),
            cache: 1,
            served: Some(Box::new(|_, t| Some(next_task(t)))),
            replay: false,
            refuse: None,
            eager: None,
            fails: 0,
            configure: |_| {},
        }
    }
}

struct Run {
    worker: Worker,
    store: Arc<MemoryStore>,
    sent: Arc<Mutex<Vec<WorkflowTaskCompletion>>>,
    /// The cause and the message of each failed Workflow Task.
    failed: Arc<Mutex<Vec<(WorkflowTaskFailedCause, String)>>>,
    /// The History that the completions wrote.
    history: Arc<Mutex<TestHistoryBuilder>>,
    /// Resolves once Core has taken each task of `StartOpts::tasks`, after the test dropped its
    /// sender.
    all_taken: Option<oneshot::Receiver<()>>,
}

fn start(t: TestHistoryBuilder, opts: StartOpts) -> Run {
    let StartOpts {
        responses,
        tasks,
        during_report,
        store,
        cache,
        mut served,
        replay,
        refuse,
        mut eager,
        fails,
        configure,
    } = opts;
    let sent = Arc::new(Mutex::new(vec![]));
    let failed = Arc::new(Mutex::new(vec![]));
    let history = Arc::new(Mutex::new(t.clone()));
    if replay {
        let mut config = test_worker_cfg().build().unwrap();
        config.stream_runtimes = runtimes(&store);
        let t = HistoryForReplay::new(t.get_full_history_info().unwrap(), "fake_wf_id");
        let input = ReplayWorkerInput::new(config, stream::once(async move { t }));
        let worker = init_replay_worker(input).unwrap();
        return Run {
            worker,
            store,
            sent,
            failed,
            history,
            all_taken: None,
        };
    }
    // Resolves once Core polled past the last task of `tasks`.
    let (taken, all_taken) = oneshot::channel();
    let (mut during_report, all_taken) = match during_report {
        Some((sender, next_task)) => (Some((Some(sender), next_task, all_taken)), None),
        None => (None, Some(all_taken)),
    };
    let (kept, written) = (sent.clone(), history.clone());
    let complete = move |completion: &WorkflowTaskCompletion| {
        kept.lock().push(completion.clone());
        let number = kept.lock().len();
        if refuse == Some(number) {
            return Err(tonic::Status::invalid_argument("UnhandledCommand"));
        }
        let mut history = written.lock();
        write(&mut history, completion);
        let forced = completion.force_create_new_workflow_task;
        let workflow_task = match eager.as_mut() {
            Some(eager) => eager(completion, &mut history),
            None => served
                .as_mut()
                .filter(|_| forced && completion.return_new_workflow_task)
                .and_then(|next_task| next_task(number, &mut history)),
        };
        if let Some((sender, next_task, taken)) = during_report.as_mut()
            && let Some(task) = next_task(number, &mut history)
        {
            let sender = sender.take().expect("One task comes during a report");
            sender.send(task).unwrap();
            drop(sender);
            // Core takes the task on its own thread while this one waits.
            let deadline = Instant::now() + Duration::from_secs(30);
            while matches!(taken.try_recv(), Err(TryRecvError::Empty)) {
                assert!(Instant::now() < deadline, "Core never took the task");
                std::thread::yield_now();
            }
        }
        Ok(RespondWorkflowTaskCompletedResponse {
            workflow_task,
            ..Default::default()
        })
    };
    let kept = failed.clone();
    let fail = move |cause: &WorkflowTaskFailedCause, failure: &Option<Failure>| {
        let message = failure.as_ref().map(|f| f.message.clone());
        let mut failed = kept.lock();
        failed.push((*cause, message.unwrap_or_default()));
        assert!(failed.len() <= fails, "A task failed: {failed:?}");
    };
    let from_channel = tasks.is_some();
    let mut mock = match tasks {
        None => {
            let mut cfg = MockPollCfg::from_resps(t, responses);
            cfg.make_poll_stream_interminable = true;
            cfg.num_expected_completions = Some((0..).into());
            cfg.num_expected_fails = fails;
            cfg.completion_mock_fn = Some(Box::new(complete));
            cfg.expect_fail_wft_matcher = Box::new(move |_, cause, failure| {
                fail(cause, failure);
                true
            });
            build_mock_pollers(cfg)
        }
        Some(tasks) => {
            let input = UnboundedReceiverStream::new(tasks).chain(stream::once(async move {
                let _ = taken.send(());
                std::future::pending().await
            }));
            let mut client = mock_worker_client();
            let mut complete = complete;
            (client.expect_complete_workflow_task())
                .returning(move |completion, _| complete(&completion));
            (client.expect_fail_workflow_task()).times(fails).returning(
                move |_, cause, failure| {
                    fail(&cause, &failure);
                    Ok(Default::default())
                },
            );
            MocksHolder::from_wft_stream(client, input)
        }
    };
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = cache;
        wc.stream_runtimes = runtimes(&store);
        // A hold that waits for a call with no answer ends at the Stream time of its task, so a
        // short one keeps the tests fast.
        wc.stream_runtimes.settings.task_time = Some(Duration::from_millis(250));
        if from_channel {
            // With one extractor, Core polls past a task of the channel once it processed it.
            wc.fetching_concurrency = 1;
        }
        configure(wc);
    });
    Run {
        worker: mock_worker(mock),
        store,
        sent,
        failed,
        history,
        all_taken: all_taken.filter(|_| from_channel),
    }
}

/// Adds the next Workflow Task to `t`, and returns it with the whole History.
fn next_task(t: &mut TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
    t.add_workflow_task_scheduled_and_started();
    task_of(t)
}

/// Returns the Workflow Task at the end of `t`, with the whole History.
fn task_of(t: &TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
    let mut task = hist_to_poll_resp(t, "fake_wf_id", ResponseType::AllHistory).resp;
    task.attempt = 1;
    task
}

/// Answers the query of `activation`, if that's its first job.
fn answer(activation: &WorkflowActivation) -> Option<Command> {
    let Some(Job::QueryWorkflow(query)) = &activation.jobs[0].variant else {
        return None;
    };
    let answer = QueryResult {
        query_id: query.query_id.clone(),
        variant: Some(query_result::Variant::Succeeded(QuerySuccess::default())),
    };
    Some(answer.into())
}

/// Completes the last task of `t` with the ledger `data`.
fn complete_with_ledger(t: &mut TestHistoryBuilder, data: &WorkflowTaskStreamCalls) {
    t.add_workflow_task_completed();
    t.set_stream_calls_last_wft(data.clone());
}

/// Returns the ledger of a task that asked for close 1 of the Stream `stream_id`.
fn close_intent(stream_id: &str) -> WorkflowTaskStreamCalls {
    WorkflowTaskStreamCalls {
        close_intents: vec![CloseIntent {
            seq: 1,
            r#ref: Some(stream(stream_id)),
        }],
        ..Default::default()
    }
}

/// Adds the events of a completion that the server took to `history`.
fn write(history: &mut TestHistoryBuilder, completion: &WorkflowTaskCompletion) {
    history.add_workflow_task_completed();
    let completed = history.current_event_id();
    // A replay takes its chunking mode from this flag, so the mock server keeps it.
    let unchunked = CoreInternalFlags::DisableWorkflowTaskChunking;
    if (completion.sdk_metadata.core_used_flags).contains(&(unchunked as u32)) {
        history.set_flags_last_wft(&[unchunked], &[]);
    }
    if let Some(calls) = &completion.sdk_metadata.stream_calls {
        history.set_stream_calls_last_wft(calls.as_ref().clone());
    }
    for command in &completion.commands {
        match command.attributes.as_ref() {
            Some(Attributes::RecordMarkerCommandAttributes(marker)) => {
                history.add(MarkerRecordedEventAttributes {
                    marker_name: marker.marker_name.clone(),
                    details: marker.details.clone(),
                    workflow_task_completed_event_id: completed,
                    ..Default::default()
                });
            }
            Some(Attributes::StartTimerCommandAttributes(timer)) => {
                history.add_timer_started(timer.timer_id.clone());
            }
            Some(Attributes::CompleteWorkflowExecutionCommandAttributes(_)) => {
                history.add_workflow_execution_completed();
            }
            Some(Attributes::FailWorkflowExecutionCommandAttributes(_)) => {
                history.add_workflow_execution_failed();
            }
            Some(Attributes::CancelWorkflowExecutionCommandAttributes(_)) => {
                history.add_cancelled();
            }
            Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(_)) => {
                history.add_continued_as_new();
            }
            // No test replays an Update.
            Some(Attributes::ProtocolMessageCommandAttributes(_)) => {}
            other => panic!("The mock server can't write {other:?}"),
        }
    }
}

type Workflow = fn(&WorkflowActivation) -> Vec<Command>;

/// Completes each activation of `worker` with the commands of `workflow` until the run is
/// evicted, and returns the jobs of each activation with the reason of the eviction.
async fn drive_until_evicted(
    worker: &Worker,
    mut workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
) -> (Vec<Vec<Job>>, Option<EvictionReason>) {
    let mut jobs = vec![];
    let mut evicted = None;
    while let Ok(activation) = worker.poll_workflow_activation().await {
        let run_id = activation.run_id.clone();
        if let [job] = &activation.jobs[..]
            && let Some(Job::RemoveFromCache(eviction)) = &job.variant
        {
            evicted = Some(eviction.reason());
            let _ = worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
                .await;
            break;
        }
        jobs.push(
            activation
                .jobs
                .iter()
                .map(|job| job.variant.clone().unwrap())
                .collect(),
        );
        let completion = WorkflowActivationCompletion::from_cmds(run_id, workflow(&activation));
        worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
    }
    (jobs, evicted)
}

impl Run {
    /// Drives the run until it's evicted, and returns the jobs of each activation.
    async fn drive(
        &self,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        drive_until_evicted(&self.worker, workflow).await.0
    }

    /// Replays the History that the completions wrote on a replay Worker with the same Store, and
    /// returns the jobs of each activation. The replay must reach the end of that History.
    async fn replay(
        &self,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        self.replay_with(false, workflow).await
    }

    /// Replays as [`Run::replay`] does. With `chunking`, the first completion has no flag, as if
    /// it came from a Worker with no Stream runtime, so the replay has chunking on.
    async fn replay_with(
        &self,
        chunking: bool,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        let mut t = self.history.lock().clone();
        if chunking {
            t.set_flags_first_wft(&[], &[]);
        }
        let mut opts = StartOpts::new(&self.store);
        opts.replay = true;
        let replay = start(t, opts);
        let (jobs, evicted) = drive_until_evicted(&replay.worker, workflow).await;
        assert_eq!(evicted, Some(EvictionReason::LangRequested), "{jobs:?}");
        replay.worker.drain_pollers_and_shutdown_ref().await;
        jobs
    }

    fn sent(&self) -> Vec<WorkflowTaskCompletion> {
        self.sent.lock().clone()
    }
}

fn append(seq: u32, id: &str) -> Command {
    AppendToStream {
        seq,
        stream: Some(stream(id)),
        bodies: vec![Payload {
            data: vec![seq as u8],
            ..Default::default()
        }],
    }
    .into()
}

fn close(seq: u32, id: &str) -> Command {
    CloseStream {
        seq,
        stream: Some(stream(id)),
    }
    .into()
}

fn complete() -> Command {
    CompleteWorkflowExecution { result: None }.into()
}

/// Returns the ledger of a completion, if it has one.
fn ledger(completion: &WorkflowTaskCompletion) -> Option<WorkflowTaskStreamCalls> {
    completion.sdk_metadata.stream_calls.as_deref().cloned()
}

fn command_types(completion: &WorkflowTaskCompletion) -> Vec<CommandType> {
    (completion.commands.iter())
        .map(|command| command.command_type())
        .collect()
}

fn producer(run_id: &str, attempt: WorkflowTaskAttempt) -> Producer {
    Producer {
        kind: Some(ProducerKind::Workflow(WorkflowProducer {
            namespace: NAMESPACE.to_string(),
            workflow_id: "fake_wf_id".to_string(),
            run_id: run_id.to_string(),
            attempt: Some(attempt),
        })),
    }
}

#[tokio::test]
async fn an_append_writes_as_its_workflow_task_and_replays_from_its_ledger() {
    let store = store(&["s"]).await;
    let t = history(Duration::from_secs(60));
    let run_id = t.get_orig_run_id().to_string();
    let started_time = t.get_full_history_info().unwrap().events()[2].event_time;
    let run = start(t, StartOpts::new(&store));
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        _ => vec![complete()],
    };
    let live = run.drive(workflow).await;

    let records = store.records("s");
    let attempt = WorkflowTaskAttempt {
        started_event_id: 3,
        attempt: 1,
        started_time,
    };
    assert_eq!(records[0].producer, Some(producer(&run_id, attempt)));
    let bodies: Vec<&[u8]> = (records.iter())
        .map(|record| &record.body.as_ref().unwrap().data[..])
        .collect();
    assert_eq!(bodies, [[1]]);
    let offset = records[0].offset.clone();
    let appended = ResolveStreamAppend {
        seq: 1,
        status: Some(resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: offset.clone(),
            last_offset: offset.clone(),
        })),
    };
    assert_eq!(live[1], [Job::ResolveStreamAppend(appended)]);
    let entry = AppendResult {
        seq: 1,
        r#ref: Some(stream("s")),
        result: Some(append_result::Result::Appended(AppendOffsets {
            first_offset: offset.clone(),
            last_offset: offset,
        })),
    };
    let expected = WorkflowTaskStreamCalls {
        append_results: vec![entry],
        ..Default::default()
    };
    assert_eq!(ledger(&run.sent()[0]), Some(expected));
    run.worker.drain_pollers_and_shutdown_ref().await;
    let writes = store.requests().len();
    assert_eq!(run.replay(workflow).await, live);
    assert_eq!(store.requests().len(), writes);
    // Workflow code that ends with the append never gets the result that History has for it.
    let mut opts = StartOpts::new(&store);
    opts.replay = true;
    let changed = start(run.history.lock().clone(), opts);
    let ends_early = |_: &WorkflowActivation| vec![append(1, "s"), complete()];
    let (jobs, evicted) = drive_until_evicted(&changed.worker, ends_early).await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert_eq!(evicted, Some(EvictionReason::Nondeterminism));
    changed.worker.drain_pollers_and_shutdown_ref().await;
}

#[rstest::rstest]
#[case::the_server_takes_it(None)]
#[case::a_failed_task(Some(1))]
#[case::a_refused_completion(Some(2))]
#[tokio::test]
async fn a_close_takes_effect_only_after_the_server_took_its_task(#[case] failure: Option<u8>) {
    let store = store(&["c", "s"]).await;
    let closes = |store: &MemoryStore| {
        let closes = (store.requests().into_iter()).filter(|r| matches!(r, Request::Close(_)));
        closes.count()
    };
    let mut opts = StartOpts::new(&store);
    let watched = store.clone();
    opts.served = Some(Box::new(move |_, t| {
        let ran = closes(&watched);
        assert_eq!(ran, 0, "The close ran before the server took its task");
        Some(next_task(t))
    }));
    opts.fails = usize::from(failure == Some(1));
    opts.refuse = (failure == Some(2)).then_some(1);
    let run = start(history(Duration::from_secs(60)), opts);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    // The result of the append gives a second activation of the task.
    let commands = vec![close(1, "c"), append(1, "s")];
    let completion = WorkflowActivationCompletion::from_cmds(first.run_id.clone(), commands);
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    let appended = run.worker.poll_workflow_activation().await.unwrap();
    assert!(resolves_append(&appended, 1), "{appended:?}");
    let completion = match failure {
        Some(1) => WorkflowActivationCompletion::fail(appended.run_id, "boom".into(), None),
        _ => WorkflowActivationCompletion::empty(appended.run_id),
    };
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    let rest = run.drive(|_| vec![complete()]).await;
    if failure.is_none() {
        let closed = ResolveStreamClose {
            seq: 1,
            failed: None,
        };
        assert_eq!(rest, [[Job::ResolveStreamClose(closed)]]);
        let ledger = ledger(&run.sent()[0]).unwrap();
        assert_eq!(ledger.close_intents, close_intent("c").close_intents);
    } else {
        assert_eq!(rest, Vec::<Vec<Job>>::new());
        assert_eq!(closes(&store), 0);
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(store.is_closed("c"), failure.is_none());
}

/// The state of a call when Workflow code sends its last command.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AtTheEnd {
    /// The Store answered the append at once, so the activation of its round took the result.
    Taken,
    /// The Store answered while the last activation was outstanding, so no activation got the
    /// result.
    Answered,
    /// The Store has the request, and gave no answer.
    Runs,
    /// The close waits behind a close of its Stream that runs.
    Waits,
    /// Workflow code asked for the close in the last task, so History has no intent of it.
    Asked,
    /// The call is in the completion that has the last command.
    WithTheCommand,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum End {
    Complete,
    Fail,
    Cancel,
    ContinueAsNew,
    /// Lang reports a non-determinism error, and Core fails the Workflow for it.
    CoreFails,
}

#[derive(Clone, Debug)]
struct EndingCall {
    op: Op,
    seq: u32,
    stream: String,
    state: AtTheEnd,
}

impl EndingCall {
    fn command(&self) -> Command {
        match self.op {
            Op::Append => append(self.seq, &self.stream),
            Op::Close => close(self.seq, &self.stream),
        }
    }

    fn reaches_the_store(&self) -> bool {
        use AtTheEnd::*;
        matches!(self.state, Taken | Answered | Runs)
    }
}

/// A Workflow that ends in task 2 while it has Stream calls. Task 1 asked for the closes whose
/// intents History has, and a Signal starts task 2. The Signal's activation makes the appends of a
/// round, and the activation of that round sends the last command. With no append to take, the
/// Signal's activation sends it.
#[derive(Debug)]
struct Ending {
    end: End,
    calls: Vec<EndingCall>,
    /// The number of the closes that task 1 asked for. They're the first closes.
    in_history: usize,
    /// The number of calls before the last command in its completion.
    ahead: usize,
}

impl Ending {
    fn generate(rng: &mut StdRng) -> Self {
        use AtTheEnd::*;
        let end = [
            End::Complete,
            End::Fail,
            End::Cancel,
            End::ContinueAsNew,
            End::CoreFails,
        ][rng.random_range(0..5)];
        let mut calls: Vec<EndingCall> = vec![];
        // Adds a call with the next seq of its kind, on a Stream that no other call has.
        let add = |calls: &mut Vec<EndingCall>, op: Op, state| {
            let seq = calls.iter().filter(|call| call.op == op).count() as u32 + 1;
            calls.push(EndingCall {
                op,
                seq,
                stream: format!("{op:?}{seq}"),
                state,
            });
        };
        // The result of a close is always late, so no activation of task 2 takes it.
        for _ in 0..rng.random_range(0..=3) {
            let state = [Answered, Runs][rng.random_range(0..2)];
            add(&mut calls, Op::Close, state);
            if state == Runs && rng.random_bool(0.5) {
                let ahead = calls.last().unwrap().stream.clone();
                add(&mut calls, Op::Close, Waits);
                calls.last_mut().unwrap().stream = ahead;
            }
        }
        let in_history = calls.len();
        for _ in 0..rng.random_range(0..=2) {
            add(&mut calls, Op::Close, Asked);
        }
        for _ in 0..rng.random_range(0..=3) {
            let state = [Taken, Answered, Runs][rng.random_range(0..3)];
            add(&mut calls, Op::Append, state);
        }
        if calls.iter().all(|call| call.state != Taken) {
            for call in &mut calls[in_history..] {
                call.state = WithTheCommand;
            }
        }
        for _ in 0..rng.random_range(0..=2) {
            let op = [Op::Append, Op::Close][rng.random_range(0..2)];
            add(&mut calls, op, WithTheCommand);
        }
        // A failed completion has no command.
        if end == End::CoreFails {
            calls.retain(|call| call.state != WithTheCommand);
        }
        let with_the_command = calls.iter().filter(|call| call.state == WithTheCommand);
        let ahead = rng.random_range(0..=with_the_command.count());
        Self {
            end,
            calls,
            in_history,
            ahead,
        }
    }

    /// Returns the number of the activation that sends the last command.
    fn last(&self) -> usize {
        1 + usize::from(self.with(AtTheEnd::Taken).next().is_some())
    }

    fn with(&self, state: AtTheEnd) -> impl Iterator<Item = &EndingCall> {
        self.calls.iter().filter(move |call| call.state == state)
    }

    /// Returns the History up to task 2.
    fn history(&self, task_timeout: Duration) -> TestHistoryBuilder {
        let mut t = history(task_timeout);
        let closes = &self.calls[..self.in_history];
        let mut intents = WorkflowTaskStreamCalls::default();
        for close in closes {
            intents.close_intents.push(CloseIntent {
                seq: close.seq as i32,
                r#ref: Some(stream(&close.stream)),
            });
        }
        if closes.is_empty() {
            t.add_workflow_task_completed();
        } else {
            complete_with_ledger(&mut t, &intents);
        }
        t.add_we_signaled("go", vec![]);
        if self.end == End::Cancel {
            t.add_cancel_requested();
        }
        t.add_workflow_task_scheduled_and_started();
        t
    }

    /// Returns the commands of activation `number` of the Workflow. A replay sends the command that
    /// fails the Workflow where the first run reported its error.
    fn commands(&self, number: usize) -> Vec<Command> {
        use AtTheEnd::*;
        let mut commands = vec![];
        if number == 0 {
            let closes = &self.calls[..self.in_history];
            return closes.iter().map(EndingCall::command).collect();
        }
        if number == 1 {
            let sent =
                (self.calls[self.in_history..].iter()).filter(|call| call.state != WithTheCommand);
            commands.extend(sent.map(EndingCall::command));
        }
        if number == self.last() {
            commands.extend(self.with(WithTheCommand).map(EndingCall::command));
            let last: Command = match self.end {
                End::Complete => complete(),
                End::Fail | End::CoreFails => FailWorkflowExecution::default().into(),
                End::Cancel => CancelWorkflowExecution::default().into(),
                End::ContinueAsNew => ContinueAsNewWorkflowExecution {
                    workflow_type: "wf".to_string(),
                    ..Default::default()
                }
                .into(),
            };
            commands.insert(
                commands.len() - self.with(WithTheCommand).count() + self.ahead,
                last,
            );
        }
        commands
    }
}

#[tokio::test]
async fn the_last_command_of_a_workflow_drops_each_call_with_no_result() {
    use AtTheEnd::*;
    let mut seen = HashSet::new();
    let mut had_outage = false;
    for seed in 0..40 {
        let _seed = SeedOnPanic(seed);
        let ending = Ending::generate(&mut StdRng::seed_from_u64(seed));
        seen.extend(ending.calls.iter().map(|call| (call.op, call.state as u8)));
        let ids: Vec<&str> = (ending.calls.iter())
            .map(|call| call.stream.as_str())
            .collect();
        let store = store(&ids).await;
        // In the first seed whose closes of History all run, their Store is down, and the heartbeat
        // of the task fires while its last activation is outstanding.
        let closes = &ending.calls[..ending.in_history];
        let run_only = closes.iter().all(|call| matches!(call.state, Runs | Waits));
        let down = !had_outage && !closes.is_empty() && run_only;
        had_outage |= down;
        let in_outage = |id: &&&str| down && closes.iter().any(|close| close.stream == **id);
        let taken = |id: &&&str| ending.with(Taken).any(|call| call.stream == **id);
        for id in ids.iter().filter(|id| !in_outage(id) && !taken(id)) {
            store.hold_stream(id);
        }
        if down {
            let error = StreamError::store_unavailable("The Store is down");
            store.fail(Op::Close, error, usize::MAX);
        }
        let mut opts = StartOpts::new(&store);
        opts.served = None;
        opts.configure = |wc| {
            wc.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
        };
        let task_timeout = Duration::from_secs(if down { 1 } else { 60 });
        let run = start(ending.history(task_timeout), opts);
        let worker = &run.worker;
        let mut live = vec![];
        let last = ending.last();
        let mut activation = worker.poll_workflow_activation().await.unwrap();
        // Core got the task before it gave this activation, so the heartbeat of the task fires
        // before `heartbeat`.
        let heartbeat = Instant::now() + task_timeout.mul_f32(0.9);
        for number in 0..=last {
            if number > 0 {
                activation = worker.poll_workflow_activation().await.unwrap();
            }
            let jobs = activation.jobs.iter();
            live.push(
                jobs.map(|job| job.variant.clone().unwrap())
                    .collect::<Vec<_>>(),
            );
            let run_id = activation.run_id.clone();
            let mut completion =
                WorkflowActivationCompletion::from_cmds(run_id.clone(), ending.commands(number));
            if number == last {
                // Each call that reaches the Store is there, and the Store answers some of them
                // while the activation is outstanding.
                let reached = ending.calls.iter().filter(|call| call.reaches_the_store());
                let reached = reached.count();
                wait_until(|| calls_at(&store).len() == reached).await;
                for call in ending.with(Answered) {
                    store.release_stream(&call.stream);
                    wait_until(|| written(&store, call)).await;
                }
                if down {
                    tokio::time::sleep(heartbeat.saturating_duration_since(Instant::now())).await;
                }
                if ending.end == End::CoreFails {
                    let cause = WorkflowTaskFailedCause::NonDeterministicError;
                    completion =
                        WorkflowActivationCompletion::fail(run_id, "changed".into(), Some(cause));
                }
            }
            worker
                .complete_workflow_activation(completion)
                .await
                .unwrap();
        }
        // Lang gets no job after the last command.
        let after = run.drive(|_| vec![]).await;
        assert_eq!(after, Vec::<Vec<Job>>::new(), "seed {seed}: {ending:?}");
        worker.drain_pollers_and_shutdown_ref().await;

        // The completion has the results that Workflow code took, and then the last command. It
        // has no intent of a close, and forces no task.
        let mut expected = WorkflowTaskStreamCalls::default();
        for call in ending.with(Taken) {
            let offset = store.records(&call.stream)[0].offset.clone();
            let offsets = AppendOffsets {
                first_offset: offset.clone(),
                last_offset: offset,
            };
            expected.append_results.push(AppendResult {
                seq: call.seq as i32,
                r#ref: Some(stream(&call.stream)),
                result: Some(append_result::Result::Appended(offsets)),
            });
        }
        let sent = run.sent();
        assert_eq!(sent.len(), 1, "seed {seed}: {ending:?}");
        let recorded = (last == 2).then_some(expected);
        assert_eq!(ledger(&sent[0]), recorded, "seed {seed}: {ending:?}");
        let last_command = match ending.end {
            End::Complete => CommandType::CompleteWorkflowExecution,
            End::Fail | End::CoreFails => CommandType::FailWorkflowExecution,
            End::Cancel => CommandType::CancelWorkflowExecution,
            End::ContinueAsNew => CommandType::ContinueAsNewWorkflowExecution,
        };
        let types = command_types(&sent[0]);
        assert_eq!(types, [last_command], "seed {seed}: {ending:?}");
        assert!(!sent[0].force_create_new_workflow_task, "seed {seed}");

        // No Store call starts after the last command, and a call with no answer writes nothing.
        // A call reaches the Store once, but for a close in an outage, which tries again.
        let mut reached: Vec<(Op, String)> = (ending.calls.iter())
            .filter(|call| call.reaches_the_store())
            .map(|call| (call.op, call.stream.clone()))
            .collect();
        reached.sort();
        assert_eq!(calls_at(&store), reached, "seed {seed}: {ending:?}");
        assert!(
            down || store.requests().len() == reached.len(),
            "seed {seed}"
        );
        for call in &ending.calls {
            let answered = matches!(call.state, Taken | Answered);
            assert_eq!(written(&store, call), answered, "seed {seed}: {call:?}");
        }

        let mut number = 0;
        let replayed = run
            .replay(|_| {
                number += 1;
                ending.commands(number - 1)
            })
            .await;
        assert_eq!(replayed, live, "seed {seed}: {ending:?}");
    }
    assert!(had_outage);
    // The seeds reach each state of a call, for an append and for a close.
    let states = [Answered, Runs, WithTheCommand];
    let mut expected: HashSet<(Op, u8)> = (states.iter())
        .flat_map(|state| [(Op::Append, *state as u8), (Op::Close, *state as u8)])
        .collect();
    expected.extend([(Op::Append, Taken as u8)]);
    expected.extend([(Op::Close, Waits as u8), (Op::Close, Asked as u8)]);
    assert_eq!(seen, expected);
}

/// Returns the kind and the Stream of each call that reached the Store, once for each call.
fn calls_at(store: &MemoryStore) -> Vec<(Op, String)> {
    let mut calls: Vec<(Op, String)> = (store.requests().into_iter())
        .map(|request| match request {
            Request::Append(append, _) => (Op::Append, append.r#ref.unwrap().id),
            Request::Close(close) => (Op::Close, close.r#ref.unwrap().id),
        })
        .collect();
    calls.sort();
    calls.dedup();
    calls
}

/// Returns true if the Store wrote the append or the close `call`.
fn written(store: &MemoryStore, call: &EndingCall) -> bool {
    match call.op {
        Op::Append => !store.records(&call.stream).is_empty(),
        Op::Close => store.is_closed(&call.stream),
    }
}

fn fast_tries(wc: &mut WorkerConfig) {
    wc.stream_runtimes.settings = StreamSettings {
        retry_initial_interval: Duration::from_millis(10),
        retry_max_interval: Duration::from_millis(20),
        try_deadline: Duration::from_millis(50),
        ..wc.stream_runtimes.settings
    };
}

#[derive(Clone, Copy, Debug)]
enum Outage {
    AppendFails,
    AppendGetsNoAnswer,
    CloseFails,
    AppendFailsWithNoLocalActivityManager,
    /// The task fails on two attempts. Core reports the failure of a task only on its first
    /// attempt, so the server learns of the second one by the task timeout.
    AppendFailsOnALaterAttempt,
}

#[rstest::rstest]
#[case(Outage::AppendFails)]
#[case(Outage::AppendGetsNoAnswer)]
#[case(Outage::CloseFails)]
#[case(Outage::AppendFailsWithNoLocalActivityManager)]
#[case(Outage::AppendFailsOnALaterAttempt)]
#[tokio::test]
async fn a_call_in_an_outage_fails_its_task_at_the_heartbeat(#[case] outage: Outage) {
    let store = store(&["s"]).await;
    let down = StreamError::store_unavailable("The Store is down");
    match outage {
        Outage::AppendGetsNoAnswer => store.hold(Op::Append),
        Outage::CloseFails => store.fail(Op::Close, down, usize::MAX),
        _ => store.fail(Op::Append, down, usize::MAX),
    }
    let mut opts = StartOpts::new(&store);
    opts.fails = 1;
    opts.configure = match outage {
        Outage::AppendFailsWithNoLocalActivityManager => |wc| {
            fast_tries(wc);
            wc.task_types = WorkerTaskTypes::workflow_only();
        },
        _ => fast_tries,
    };
    let t = history(Duration::from_secs(1));
    let mut attempts = 1;
    if let Outage::AppendFailsOnALaterAttempt = outage {
        // The mock poller numbers the attempts of a task from 1.
        attempts = 2;
        let task = task_of(&t);
        opts.responses = vec![ResponseType::Raw(task.clone()), ResponseType::Raw(task)];
    }
    let run = start(t, opts);
    let workflow: Workflow = match outage {
        Outage::CloseFails => |_| vec![close(1, "s")],
        _ => |_| vec![append(1, "s")],
    };
    for _ in 0..attempts {
        let driven = tokio::time::timeout(Duration::from_secs(30), run.drive(workflow)).await;
        assert_eq!(driven.expect("The call never failed its task").len(), 1);
    }
    // The try with no answer ends at the deadline of the Worker's settings.
    let (call, error) = match outage {
        Outage::CloseFails => ("close", "The Store is down"),
        Outage::AppendGetsNoAnswer => ("append", "The Store gave no answer in 50ms"),
        _ => ("append", "The Store is down"),
    };
    let message = format!(
        "StreamStoreUnavailable: {call} of Stream \"s\" on Store \"{STORE}\": \
         stream_store_unavailable: {error}"
    );
    let failed = run.failed.lock().clone();
    assert_eq!(
        failed,
        [(
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
            message
        )]
    );
    // The task that the outage failed sent no completion, so it forced no task.
    let closes = usize::from(matches!(outage, Outage::CloseFails));
    assert_eq!(run.sent().len(), closes);
    assert!(store.requests().len() > 1, "{:?}", store.requests());
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[rstest::rstest]
#[case::at_the_heartbeat(false)]
#[case::at_a_query(true)]
#[tokio::test]
async fn a_call_in_flight_goes_on_in_a_forced_task(#[case] query: bool) {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let task_timeout = Duration::from_millis(if query { 60_000 } else { 200 });
    let t = history(task_timeout);
    let mut opts = StartOpts::new(&store);
    // An append that waits at the hold must not count as a try with no answer.
    opts.configure = |wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
    if query {
        let mut task = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
        let question = WorkflowQuery {
            query_type: "q".to_string(),
            ..Default::default()
        };
        task.queries.insert("q1".to_string(), question);
        opts.responses = vec![ResponseType::Raw(task)];
    }
    let held = store.clone();
    // The append answers once the server took the task, so its result needs the next one.
    opts.served = Some(Box::new(move |_, t| {
        held.release();
        Some(next_task(t))
    }));
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
        _ => vec![complete()],
    };
    let jobs = run.drive(workflow).await;
    assert!(matches!(
        jobs.last().unwrap()[..],
        [Job::ResolveStreamAppend(_)]
    ));
    let sent = run.sent();
    assert!(sent[0].force_create_new_workflow_task && sent[0].commands.is_empty());
    assert_eq!(sent[0].query_responses.len(), usize::from(query));
    // A heartbeat of the forced task can complete it before the result arrives.
    let recorded: Vec<usize> = (sent[1..].iter().filter_map(ledger))
        .map(|ledger| ledger.append_results.len())
        .collect();
    assert_eq!(recorded, [1]);
    assert_eq!(store.requests().len(), 1);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[tokio::test]
async fn a_call_in_an_outage_goes_on_when_a_full_ledger_completes_its_task() {
    let store = store(&["c", "f"]).await;
    // The close is in an outage once it runs, and each append fails with a large message.
    store.fail(
        Op::Close,
        StreamError::store_unavailable("down"),
        usize::MAX,
    );
    store.fail(
        Op::Append,
        StreamError::invalid_argument("x".repeat(4000)),
        usize::MAX,
    );
    let mut opts = StartOpts::new(&store);
    opts.configure = fast_tries;
    // Task 2 has a Signal, so that Workflow code gets an activation in it.
    opts.served = Some(Box::new(|number, t| {
        if number == 1 {
            t.add_we_signaled("go", vec![]);
        }
        Some(next_task(t))
    }));
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![close(1, "c")],
        Some(Job::SignalWorkflow(_)) => (1..=70).map(|seq| append(seq, "f")).collect(),
        Some(Job::ResolveStreamClose(_)) => vec![complete()],
        _ => vec![],
    };
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, workflow).await;
    let signal = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &signal, workflow).await;
    // The activation of the round has the results that fit in the ledger. The rest are late, so
    // Core completes the task with a new one forced, and the first activation of each later task
    // has the late results that fit.
    let mut results = vec![];
    while results.len() < 70 {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        for job in &activation.jobs {
            let Some(Job::ResolveStreamAppend(result)) = &job.variant else {
                panic!("{activation:?}");
            };
            results.push(result.seq);
        }
        respond(&run.worker, &activation, workflow).await;
    }
    assert_eq!(results, (1..=70).collect::<Vec<_>>());
    assert_eq!(run.failed.lock().clone(), []);
    // The close continues in the next task, and ends when the Store is available again.
    store.fail(Op::Close, StreamError::store_unavailable("down"), 0);
    let closed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        closed.jobs[0].variant,
        Some(Job::ResolveStreamClose(_))
    ));
    respond(&run.worker, &closed, workflow).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
    // Task 2 completed with a new task forced when its ledger was full, and the results that
    // didn't fit went into the ledgers of the later tasks.
    let sent = run.sent();
    let appends = |sent: &WorkflowTaskCompletion| -> Vec<u32> {
        let ledger = ledger(sent).unwrap_or_default();
        (ledger.append_results.iter())
            .map(|append| append.seq as u32)
            .collect()
    };
    let second = appends(&sent[1]);
    assert!(sent[1].force_create_new_workflow_task && second.len() < 70);
    let recorded: Vec<u32> = sent.iter().flat_map(appends).collect();
    assert_eq!(recorded, results);
    assert_eq!(
        command_types(sent.last().unwrap()).last(),
        Some(&CommandType::CompleteWorkflowExecution)
    );
}

#[rstest::rstest]
#[case::with_a_ledger_to_send(true, 1)]
#[case::with_nothing_else(false, 1)]
#[case::with_no_cache(false, 0)]
#[tokio::test]
async fn an_eviction_completes_a_task_that_a_call_holds(
    #[case] with_a_ledger: bool,
    #[case] cache: usize,
) {
    let store = store(&["s"]).await;
    let mut opts = StartOpts::new(&store);
    opts.cache = cache;
    opts.served = None;
    let run = start(history(Duration::from_secs(60)), opts);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    if !with_a_ledger {
        store.hold(Op::Append);
    }
    let completion = WorkflowActivationCompletion::from_cmd(run_id.clone(), append(1, "s"));
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    if with_a_ledger {
        let result = run.worker.poll_workflow_activation().await.unwrap();
        assert!(matches!(
            result.jobs[0].variant,
            Some(Job::ResolveStreamAppend(_))
        ));
        store.hold(Op::Append);
        let completion = WorkflowActivationCompletion::from_cmd(run_id.clone(), append(2, "s"));
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
    }
    run.worker.request_workflow_eviction(&run_id);
    assert_eq!(run.drive(|_| vec![]).await.len(), 0);
    let sent = run.sent();
    let last = sent.last().unwrap();
    assert!(last.force_create_new_workflow_task);
    // The completion has the sticky queue of a Worker with a cache, as any completion has.
    assert_eq!(last.sticky_attributes.is_some(), cache > 0);
    let recorded = ledger(last).map(|ledger| ledger.append_results.len());
    assert_eq!(recorded, Some(usize::from(with_a_ledger)));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// The first activation of a task waits for its late calls only while the events of the task give
// no job, so a Signal doesn't wait for a close that runs.
#[tokio::test]
async fn a_job_of_its_events_ends_the_hold_of_the_first_activation_of_a_task() {
    let store = store(&["c"]).await;
    store.hold(Op::Close);
    let mut t = history(Duration::from_secs(60));
    complete_with_ledger(&mut t, &close_intent("c"));
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let mut opts = StartOpts::new(&store);
    opts.configure = |wc| wc.stream_runtimes.settings.task_time = None;
    let run = start(t, opts);
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, |_| vec![close(1, "c")]).await;
    let signal = tokio::time::timeout(
        Duration::from_secs(5),
        run.worker.poll_workflow_activation(),
    );
    let signal = signal
        .await
        .expect("The Signal waited for the close")
        .unwrap();
    assert!(matches!(
        signal.jobs[0].variant,
        Some(Job::SignalWorkflow(_))
    ));
    assert_eq!(store.requests().len(), 1);
    respond(&run.worker, &signal, |_| vec![complete()]).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A Local Activity result that arrives while a round waits comes with the results of the round, so
// no activation cuts the round short.
#[tokio::test]
async fn a_local_activity_result_waits_for_the_round_of_its_activation() {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let mut opts = StartOpts::new(&store);
    opts.configure = |wc| wc.stream_runtimes.settings.task_time = None;
    let run = start(history(Duration::from_secs(60)), opts);
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, |_| {
        vec![local_activity(1), append(1, "s")]
    })
    .await;
    let task = run.worker.poll_activity_task().await.unwrap();
    let completion = ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    };
    run.worker.complete_activity_task(completion).await.unwrap();
    // The result of the Local Activity reaches the run before the Store answers.
    tokio::time::sleep(Duration::from_millis(200)).await;
    store.release();
    let round = run.worker.poll_workflow_activation().await.unwrap();
    let jobs: Vec<String> = (round.jobs.iter())
        .map(|job| summary(job.variant.as_ref().unwrap()))
        .collect();
    assert_eq!(jobs, ["local 1", "append 1"]);
    respond(&run.worker, &round, |_| vec![complete()]).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// At the heartbeat point the heartbeat completes the task, also when the Stream time ends at the
// same instant. The ready results of the round then come in the next task.
#[tokio::test]
async fn a_round_that_ends_at_the_heartbeat_gives_its_results_in_the_next_task() {
    let store = store(&["held", "s"]).await;
    store.hold_stream("held");
    let mut opts = StartOpts::new(&store);
    opts.configure = |wc| wc.stream_runtimes.settings.task_time = Some(Duration::from_secs(10));
    let held = store.clone();
    opts.served = Some(Box::new(move |_, t| {
        held.release_stream("held");
        Some(next_task(t))
    }));
    let run = start(history(Duration::from_secs(1)), opts);
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, |_| {
        vec![append(1, "s"), append(2, "held")]
    })
    .await;
    let next = run.worker.poll_workflow_activation().await.unwrap();
    let jobs: Vec<String> = (next.jobs.iter())
        .map(|job| summary(job.variant.as_ref().unwrap()))
        .collect();
    assert_eq!(jobs, ["append 1", "append 2"]);
    let sent = run.sent();
    assert!(sent[0].force_create_new_workflow_task);
    assert_eq!(ledger(&sent[0]), Some(WorkflowTaskStreamCalls::default()));
    respond(&run.worker, &next, |_| vec![complete()]).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A graceful shutdown ends the Stream time of the task in hand, so a round that waits for an
// append ends at once, and the late append completes the task as at a heartbeat.
#[tokio::test]
async fn a_shutdown_ends_a_round_at_once() {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let mut opts = StartOpts::new(&store);
    opts.configure = |wc| wc.stream_runtimes.settings.task_time = None;
    opts.served = None;
    let run = start(history(Duration::from_secs(60)), opts);
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, |_| vec![append(1, "s")]).await;
    wait_until(|| store.requests().len() == 1).await;
    run.worker.initiate_shutdown();
    let drained = tokio::time::timeout(Duration::from_secs(5), run.drive(|_| vec![]));
    assert_eq!(
        drained
            .await
            .expect("The round outlived the shutdown")
            .len(),
        0
    );
    let sent = run.sent();
    assert!(sent[0].force_create_new_workflow_task);
    assert_eq!(ledger(&sent[0]), Some(WorkflowTaskStreamCalls::default()));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[tokio::test]
async fn a_replay_worker_makes_no_live_call() {
    let store = store(&["s", "c"]).await;
    let mut t = history(Duration::from_secs(60));
    complete_with_ledger(&mut t, &close_intent("c"));
    t.add_workflow_task_scheduled_and_started();
    let mut opts = StartOpts::new(&store);
    opts.replay = true;
    let run = start(t, opts);
    let jobs = run.drive(|_| vec![append(1, "s"), close(1, "c")]).await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(store.requests(), []);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// What happened to task 2, which wrote append 1 and gave no result to History.
#[derive(Clone, Copy, Debug)]
enum EarlierAttempt {
    FailedTask,
    LaterRetryOfAFailedTask,
    TaskTimeout,
    ForcedTaskOfAnotherWorker,
    Eviction,
    UpdateAccepted,
    UpdateRejected,
}

/// Returns true if `activation` has the result of append `seq`.
fn resolves_append(activation: &WorkflowActivation, seq: u32) -> bool {
    (activation.jobs.iter()).any(
        |job| matches!(&job.variant, Some(Job::ResolveStreamAppend(append)) if append.seq == seq),
    )
}

/// Returns the key, the started event id and the attempt of a Workflow's Producer.
fn writer(producer: Option<Producer>) -> (Vec<u8>, i64, i32) {
    let producer = producer.unwrap();
    let Some(ProducerKind::Workflow(WorkflowProducer {
        attempt: Some(task),
        ..
    })) = &producer.kind
    else {
        panic!("{producer:?}");
    };
    let key = producer_key(&producer).unwrap();
    (key, task.started_event_id, task.attempt)
}

/// Returns the writer of each Record of the Stream `id`.
fn writers(store: &MemoryStore, id: &str) -> Vec<(Vec<u8>, i64, i32)> {
    (store.records(id).into_iter())
        .map(|record| writer(record.producer))
        .collect()
}

#[rstest::rstest]
#[case(EarlierAttempt::FailedTask)]
#[case(EarlierAttempt::LaterRetryOfAFailedTask)]
#[case(EarlierAttempt::TaskTimeout)]
#[case(EarlierAttempt::ForcedTaskOfAnotherWorker)]
#[case(EarlierAttempt::Eviction)]
#[case(EarlierAttempt::UpdateAccepted)]
#[case(EarlierAttempt::UpdateRejected)]
#[tokio::test]
async fn a_run_on_a_new_attempt_or_worker_writes_above_each_earlier_attempt(
    #[case] earlier: EarlierAttempt,
) {
    let store = store(&["s", "c"]).await;
    // Task 1 recorded the intent of close 1.
    let mut t = history(Duration::from_secs(60));
    complete_with_ledger(&mut t, &close_intent("c"));
    let second = next_task(&mut t);
    let run_id = t.get_orig_run_id().to_string();
    match earlier {
        EarlierAttempt::TaskTimeout => t.add_workflow_task_timed_out(),
        EarlierAttempt::ForcedTaskOfAnotherWorker | EarlierAttempt::Eviction => {
            t.add_workflow_task_completed();
        }
        _ => {
            let cause = WorkflowTaskFailedCause::Unspecified;
            t.add_workflow_task_failed_with_failure(cause, Default::default());
        }
    }
    let mut task = next_task(&mut t);
    if let EarlierAttempt::UpdateAccepted | EarlierAttempt::UpdateRejected = earlier {
        task.add_update_request("u", 8);
    }
    let runs_the_earlier_attempt = matches!(
        earlier,
        EarlierAttempt::LaterRetryOfAFailedTask | EarlierAttempt::Eviction
    );
    if !runs_the_earlier_attempt {
        let attempt = WorkflowTaskAttempt {
            started_event_id: 6,
            attempt: 1,
            started_time: None,
        };
        let write = AppendRecordsRequest {
            r#ref: Some(stream("s")),
            producer: Some(producer(&run_id, attempt)),
            records: vec![AppendRecord::default()],
        };
        store.append_records(write, "x".to_string()).await.unwrap();
    }
    let mut opts = StartOpts::new(&store);
    // The mock poller numbers the attempts of a task from 1.
    opts.responses = match earlier {
        EarlierAttempt::LaterRetryOfAFailedTask => {
            vec![ResponseType::Raw(task.clone()), ResponseType::Raw(task)]
        }
        EarlierAttempt::Eviction => vec![ResponseType::Raw(second), ResponseType::Raw(task)],
        _ => vec![ResponseType::Raw(task)],
    };
    if let EarlierAttempt::Eviction = earlier {
        // The evicted run gets its next task from a poll, as a task of another Worker.
        opts.served = None;
    }
    opts.fails = usize::from(matches!(earlier, EarlierAttempt::LaterRetryOfAFailedTask));
    let run = start(t, opts);
    // The Workflow ends once its last append and its close have their results.
    let last_append = if let EarlierAttempt::UpdateAccepted = earlier {
        2
    } else {
        1
    };
    let left = Cell::new(2usize);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        let results = (activation.jobs.iter()).filter(|job| match &job.variant {
            Some(Job::ResolveStreamAppend(append)) => append.seq == last_append,
            Some(Job::ResolveStreamClose(_)) => true,
            _ => false,
        });
        left.set(left.get() - results.count());
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![append(1, "s"), close(1, "c")],
            Some(Job::DoUpdate(_)) if last_append == 2 => {
                let accepted = update_response::Response::Accepted(());
                vec![update_response(activation, accepted), append(2, "s")]
            }
            Some(Job::DoUpdate(_)) => {
                let rejected = update_response::Response::Rejected(Default::default());
                vec![update_response(activation, rejected)]
            }
            _ if left.get() == 0 => vec![complete()],
            _ => vec![],
        }
    };
    if runs_the_earlier_attempt {
        // The close of the earlier attempt never reaches the Store.
        store.hold(Op::Close);
        if let EarlierAttempt::Eviction = earlier {
            store.lose_answers(Op::Append, usize::MAX);
        }
        let init = run.worker.poll_workflow_activation().await.unwrap();
        let commands = workflow(&init);
        let completion = WorkflowActivationCompletion::from_cmds(init.run_id.clone(), commands);
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
        if let EarlierAttempt::Eviction = earlier {
            wait_until(|| !store.records("s").is_empty()).await;
            run.worker.request_workflow_eviction(&init.run_id);
        } else {
            let appended = run.worker.poll_workflow_activation().await.unwrap();
            assert!(resolves_append(&appended, 1), "{appended:?}");
            let failed = WorkflowActivationCompletion::fail(appended.run_id, "boom".into(), None);
            run.worker
                .complete_workflow_activation(failed)
                .await
                .unwrap();
        }
        assert_eq!(run.drive(workflow).await.len(), 0);
        store.lose_answers(Op::Append, 0);
        store.release();
    }
    let jobs = run.drive(workflow).await;
    let appended = jobs.iter().flatten().any(|job| {
        matches!(
            job,
            Job::ResolveStreamAppend(ResolveStreamAppend {
                seq: 1,
                status: Some(resolve_stream_append::Status::Appended(_)),
            })
        )
    });
    assert!(appended, "{jobs:?}");
    let id = producer_key(&producer(&run_id, Default::default())).unwrap();
    let (earlier_task, go_live) = match earlier {
        EarlierAttempt::LaterRetryOfAFailedTask => ((9, 1), (9, 2)),
        EarlierAttempt::UpdateRejected => ((6, 1), (12, 1)),
        _ => ((6, 1), (9, 1)),
    };
    let mut expected = vec![
        (id.clone(), earlier_task.0, earlier_task.1),
        (id.clone(), go_live.0, go_live.1),
    ];
    if let EarlierAttempt::UpdateAccepted = earlier {
        expected.push(expected[1].clone());
    }
    assert_eq!(writers(&store, "s"), expected);
    // A request id is unique to one append call, and the append of a later attempt is another call.
    let mut attempts = HashMap::new();
    for request in store.requests() {
        if let Request::Append(append, request_id) = request {
            let (_, task, attempt) = writer(append.producer);
            let first = *attempts.entry(request_id).or_insert((task, attempt));
            assert_eq!(first, (task, attempt), "{earlier:?}");
        }
    }
    assert!(store.is_closed("c"));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// How a cached run gets a task after another attempt or Worker took over its task.
#[derive(Clone, Copy, Debug, PartialEq)]
enum OtherTask {
    /// Task 1 completed with an append in flight, and another Worker completed task 2.
    WithNoActivationOut,
    /// Task 1 timed out while its first activation was outstanding, and another Worker completed
    /// the retry.
    WithAnActivationOut,
    /// Another Worker completed a later attempt of task 1, which has the same started event id,
    /// while the first activation of attempt 2 was outstanding.
    AtTheSameStartedEventId,
}

/// Waits until `done` returns true, for at most 30 seconds, so that a test fails rather than hangs.
async fn wait_until(mut done: impl FnMut() -> bool) {
    let wait = async {
        while !done() {
            tokio::task::yield_now().await;
        }
    };
    (tokio::time::timeout(Duration::from_secs(30), wait).await).expect("The wait timed out");
}

/// Completes `activation` with the commands of `workflow`.
async fn respond(worker: &Worker, activation: &WorkflowActivation, workflow: Workflow) {
    let commands = workflow(activation);
    let completion = WorkflowActivationCompletion::from_cmds(activation.run_id.clone(), commands);
    worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case(OtherTask::WithNoActivationOut)]
#[case(OtherTask::WithAnActivationOut)]
#[case(OtherTask::AtTheSameStartedEventId)]
#[tokio::test]
async fn a_cached_run_that_meets_another_workers_task_replays_from_history(
    #[case] other_task: OtherTask,
) {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let t = history(Duration::from_secs(60));
    let mut first = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
    first.attempt = if other_task == OtherTask::AtTheSameStartedEventId {
        2
    } else {
        1
    };
    // The other attempt or Worker completed its task, with the result of append 1 in its ledger.
    let mut other = t.clone();
    match other_task {
        OtherTask::WithNoActivationOut => other.add_workflow_task_completed(),
        OtherTask::WithAnActivationOut => other.add_workflow_task_timed_out(),
        _ => {}
    }
    if other_task != OtherTask::AtTheSameStartedEventId {
        other.add_workflow_task_scheduled_and_started();
    }
    let offsets = AppendOffsets {
        first_offset: vec![7],
        last_offset: vec![7],
    };
    let result = AppendResult {
        seq: 1,
        r#ref: Some(stream("s")),
        result: Some(append_result::Result::Appended(offsets)),
    };
    let ledger = WorkflowTaskStreamCalls {
        append_results: vec![result],
        ..Default::default()
    };
    complete_with_ledger(&mut other, &ledger);
    other.add_we_signaled("done", vec![]);
    let next = next_task(&mut other);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
        Some(Job::SignalWorkflow(_)) => vec![complete()],
        _ => vec![],
    };
    let mut opts = StartOpts::new(&store);
    // An append that waits at the hold must not count as a try with no answer.
    opts.configure = |wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
    opts.served = None;
    let run = if other_task == OtherTask::WithNoActivationOut {
        // The answer of a query completes task 1 while the append runs.
        let question = WorkflowQuery {
            query_type: "q".to_string(),
            ..Default::default()
        };
        first.queries.insert("q1".to_string(), question);
        opts.responses = vec![ResponseType::Raw(first), ResponseType::Raw(next)];
        let run = start(t, opts);
        assert_eq!(run.drive(workflow).await.len(), 2);
        run
    } else {
        let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
        opts.tasks = Some(receiver);
        let mut run = start(t, opts);
        tasks.send(first).unwrap();
        let init = run.worker.poll_workflow_activation().await.unwrap();
        tasks.send(next).unwrap();
        drop(tasks);
        run.all_taken.take().unwrap().await.unwrap();
        respond(&run.worker, &init, workflow).await;
        assert_eq!(run.drive(workflow).await.len(), 0);
        // Workflow code sent the append after the run stopped its calls, so it holds no task.
        let stale = run.sent()[0].clone();
        assert!(stale.commands.is_empty() && !stale.force_create_new_workflow_task);
        run
    };
    let replayed = run.drive(workflow).await;
    let appended = ResolveStreamAppend {
        seq: 1,
        status: Some(resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: vec![7],
            last_offset: vec![7],
        })),
    };
    assert_eq!(replayed[1], [Job::ResolveStreamAppend(appended)]);
    assert!(matches!(replayed[2][..], [Job::SignalWorkflow(_)]));
    // The append of the cached run was in flight, or never started.
    let in_flight = other_task == OtherTask::WithNoActivationOut;
    assert_eq!(store.requests().len(), usize::from(in_flight));
    store.release();
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(store.records("s"), []);
}

#[tokio::test]
async fn a_task_that_comes_while_core_reports_a_completion_keeps_the_run() {
    let store = store(&["s"]).await;
    let t = history(Duration::from_secs(60));
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    tasks.send(task_of(&t)).unwrap();
    let mut opts = StartOpts::new(&store);
    opts.tasks = Some(receiver);
    opts.served = None;
    // Core takes task 2, which has a Signal, before the server answers the completion of task 1.
    opts.during_report = Some((
        tasks,
        Box::new(|number, t| {
            (number == 1).then(|| {
                t.add_we_signaled("go", vec![]);
                next_task(t)
            })
        }),
    ));
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        Some(Job::SignalWorkflow(_)) => vec![complete()],
        _ => vec![],
    };
    let jobs = run.drive(workflow).await;
    // The cached run takes task 2 once the report is done, with no eviction and no replay.
    assert_eq!(jobs.len(), 3, "{jobs:?}");
    assert!(
        matches!(jobs[1][..], [Job::ResolveStreamAppend(_)]),
        "{jobs:?}"
    );
    assert!(matches!(jobs[2][..], [Job::SignalWorkflow(_)]), "{jobs:?}");
    assert_eq!(run.sent().len(), 2);
    assert_eq!(store.requests().len(), 1);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[derive(Clone, Copy, Debug)]
enum UpdateTask {
    OfACachedRun,
    OfANewRun,
}

fn local_activity(seq: u32) -> Command {
    let local_activity = ScheduleLocalActivity {
        seq,
        activity_id: seq.to_string(),
        activity_type: "la".to_string(),
        start_to_close_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        ..Default::default()
    };
    local_activity.into()
}

fn timer(seq: u32) -> Command {
    let timer = StartTimer {
        seq,
        start_to_fire_timeout: Some(Duration::from_secs(1).try_into().unwrap()),
    };
    timer.into()
}

fn update_response(
    activation: &WorkflowActivation,
    response: update_response::Response,
) -> Command {
    let Some(Job::DoUpdate(update)) = &activation.jobs[0].variant else {
        panic!("{activation:?}");
    };
    UpdateResponse {
        protocol_instance_id: update.protocol_instance_id.clone(),
        response: Some(response),
    }
    .into()
}

#[rstest::rstest]
#[case(UpdateTask::OfACachedRun)]
#[case(UpdateTask::OfANewRun)]
#[tokio::test]
async fn a_call_of_an_update_handler_runs_after_the_update_is_accepted(#[case] case: UpdateTask) {
    let store = store(&["s"]).await;
    // Task 1 starts a timer, so that task 2 is a task of its own in a replay.
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    t.add_timer_started("1".to_string());
    let mut update = next_task(&mut t);
    update.add_update_request("u", 6);
    let run_id = t.get_orig_run_id().to_string();
    let mut opts = StartOpts::new(&store);
    opts.responses = match case {
        UpdateTask::OfACachedRun => vec![ResponseType::ToTaskNum(1), ResponseType::Raw(update)],
        UpdateTask::OfANewRun => vec![ResponseType::Raw(update)],
    };
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::DoUpdate(_)) => {
            let accepted = update_response::Response::Accepted(());
            vec![update_response(activation, accepted), append(1, "s")]
        }
        _ => vec![complete()],
    };
    let jobs = run.drive(workflow).await;
    assert!(
        matches!(jobs.last().unwrap()[..], [Job::ResolveStreamAppend(_)]),
        "{jobs:?}"
    );
    let id = producer_key(&producer(&run_id, Default::default())).unwrap();
    assert_eq!(writers(&store, "s"), [(id, 7, 1)]);
    // A completion at the heartbeat would force a new task.
    let sent = run.sent();
    assert_eq!(
        sent.len(),
        1 + usize::from(matches!(case, UpdateTask::OfACachedRun))
    );
    assert!(
        sent.iter()
            .all(|completion| !completion.force_create_new_workflow_task)
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// Rejects the Update `u`, whose request came in an earlier activation.
fn reject_update() -> Command {
    UpdateResponse {
        protocol_instance_id: "u".to_string(),
        response: Some(update_response::Response::Rejected(Failure::default())),
    }
    .into()
}

// The server drops the completion of a speculative task, which an Update makes, when it has no
// command and only rejects Updates. A ledger isn't a command, so a completion with one forces a
// new task, which makes the server keep it. Here task 2 has a Signal too, so that Workflow code
// can append in it, which the validator of an Update can't.
#[rstest::rstest]
#[case::with_an_append(true)]
#[case::with_no_stream_call(false)]
#[tokio::test]
async fn a_completion_with_a_ledger_that_only_rejects_updates_forces_a_task(#[case] appends: bool) {
    let store = store(&["s"]).await;
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    t.add_timer_started("1".to_string());
    t.add_we_signaled("go", vec![]);
    let mut update = next_task(&mut t);
    update.add_update_request("u", 7);
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::ToTaskNum(1), ResponseType::Raw(update)];
    opts.served = None;
    let run = start(t, opts);
    let signaled = |activation: &WorkflowActivation| {
        (activation.jobs.iter()).any(|job| matches!(job.variant, Some(Job::SignalWorkflow(_))))
    };
    let workflow = |activation: &WorkflowActivation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        _ if signaled(activation) && appends => vec![append(1, "s"), reject_update()],
        _ if signaled(activation) => vec![reject_update()],
        _ => vec![],
    };
    let respond = async {
        while run.sent().len() < 2 {
            let activation = run.worker.poll_workflow_activation().await.unwrap();
            let commands = workflow(&activation);
            let completion = WorkflowActivationCompletion::from_cmds(activation.run_id, commands);
            run.worker
                .complete_workflow_activation(completion)
                .await
                .unwrap();
        }
    };
    tokio::time::timeout(Duration::from_secs(10), respond)
        .await
        .expect("Task 2 never completed");
    let completion = &run.sent()[1];
    assert_eq!(command_types(completion), []);
    assert_eq!(completion.messages.len(), 1);
    let appended = ledger(completion).map(|ledger| ledger.append_results.len());
    assert_eq!(appended, appends.then_some(1));
    assert_eq!(completion.force_create_new_workflow_task, appends);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// Returns Workflow code that a generated test runs: the commands of each activation that has a
/// job other than a query or an Update, in order. The last of them ends the Workflow.
fn script(rng: &mut StdRng) -> Vec<Vec<Command>> {
    let (mut appends, mut closes, mut locals, mut timers) = (0, 0, 0, 0);
    let steps = rng.random_range(3..=10);
    (1..=steps)
        .map(|step| {
            let mut commands = vec![];
            for _ in 0..rng.random_range(0..=3) {
                let id = format!("s{}", rng.random_range(0..3));
                match rng.random_range(0..10) {
                    0..=4 => {
                        appends += 1;
                        commands.push(append(appends, &id));
                    }
                    5 => {
                        closes += 1;
                        commands.push(close(closes, &id));
                    }
                    6..=8 => {
                        locals += 1;
                        commands.push(local_activity(locals));
                    }
                    _ => {
                        timers += 1;
                        commands.push(timer(timers));
                    }
                }
            }
            if step == steps {
                // A Local Activity that starts with the last command can resolve before the
                // completion goes out, and its marker then follows the last command.
                commands.retain(|command| !matches!(command, Command::ScheduleLocalActivity(_)));
                commands.push(complete());
            }
            commands
        })
        .collect()
}

/// How far the Workflow code of a script got.
#[derive(Default)]
struct Progress {
    /// The next step of the script.
    step: usize,
    /// The Local Activities that have no result yet.
    running: usize,
    /// The step that each activation with an event took, empty if it took none.
    taken: Vec<Vec<Command>>,
}

/// Returns the commands of the Workflow code of `script` for `activation`. It answers each query
/// and rejects each Update, which leave no event, and takes the next step of `script` for an
/// activation with any other job.
///
/// It keeps the last step while a Local Activity runs. If the result comes while Workflow code
/// sends the last command, Core gives it in one more activation and records its marker after that
/// command. A replay then stops at the last command while it still replays, so the replay Worker
/// never evicts the run.
fn scripted(
    script: &[Vec<Command>],
    progress: &mut Progress,
    activation: &WorkflowActivation,
) -> Vec<Command> {
    let mut commands = vec![];
    let mut leaves_events = false;
    for job in &activation.jobs {
        match job.variant.as_ref() {
            Some(Job::QueryWorkflow(query)) => commands.push(
                QueryResult {
                    query_id: query.query_id.clone(),
                    variant: Some(query_result::Variant::Succeeded(QuerySuccess::default())),
                }
                .into(),
            ),
            Some(Job::DoUpdate(update)) => commands.push(
                UpdateResponse {
                    protocol_instance_id: update.protocol_instance_id.clone(),
                    response: Some(update_response::Response::Rejected(Failure::default())),
                }
                .into(),
            ),
            Some(Job::ResolveActivity(_)) => {
                progress.running -= 1;
                leaves_events = true;
            }
            _ => leaves_events = true,
        }
    }
    if !leaves_events {
        return commands;
    }
    let last = progress.step + 1 == script.len();
    let step = match script.get(progress.step) {
        Some(step) if !last || progress.running == 0 => {
            progress.step += 1;
            step.clone()
        }
        _ => vec![],
    };
    progress.running += (step.iter())
        .filter(|command| matches!(command, Command::ScheduleLocalActivity(_)))
        .count();
    commands.extend(step.iter().cloned());
    progress.taken.push(step);
    commands
}

/// Returns the number of activations of the first run (`first`), from `at` on, whose jobs are
/// the jobs of one activation of a replay (`jobs`), in any order. Returns 0 if there are none.
fn merged(first: &[Vec<String>], at: usize, jobs: &[String]) -> usize {
    let mut left: Vec<&String> = jobs.iter().collect();
    for (count, activation) in (1..).zip(&first[at.min(first.len())..]) {
        for job in activation {
            let Some(place) = left.iter().position(|other| *other == job) else {
                return 0;
            };
            left.remove(place);
        }
        if left.is_empty() {
            return count;
        }
    }
    0
}

/// Returns the commands of a replay for an activation with the jobs `jobs`. With chunking on, a
/// replay can give the jobs of several activations of the first run (`first`) in one, and Workflow
/// code then takes the step that the first run took in each of them (`taken`).
fn replayed(
    first: &[Vec<String>],
    taken: &[Vec<Command>],
    at: &mut usize,
    jobs: &[String],
) -> Vec<Command> {
    let count = merged(first, *at, jobs).max(1);
    let steps = taken.iter().skip(*at).take(count);
    *at += count;
    steps.flatten().cloned().collect()
}

/// Returns the results of one kind, `local` or Stream calls, in the order of their activations.
fn of_kind(jobs: &[Vec<String>], local: bool) -> Vec<&String> {
    let is_local = |job: &&String| job.starts_with("local");
    let is_stream = |job: &&String| job.starts_with("append") || job.starts_with("close");
    (jobs.iter().flatten())
        .filter(|job| if local { is_local(job) } else { is_stream(job) })
        .collect()
}

/// Returns the jobs of each activation with no Local Activity result, leaving out the activations
/// with no other job.
fn without_locals(jobs: &[Vec<String>]) -> Vec<Vec<String>> {
    let kept = jobs.iter().map(|jobs| {
        (jobs.iter())
            .filter(|job| !job.starts_with("local"))
            .cloned()
            .collect::<Vec<_>>()
    });
    kept.filter(|jobs| !jobs.is_empty()).collect()
}

/// Returns a job as `append 3`, `append 3 failed`, `close 1`, `local 2` or the name of its kind.
fn summary(job: &Job) -> String {
    match job {
        Job::ResolveStreamAppend(ResolveStreamAppend {
            seq,
            status: Some(resolve_stream_append::Status::Failed(_)),
        }) => format!("append {seq} failed"),
        Job::ResolveStreamAppend(append) => format!("append {}", append.seq),
        Job::ResolveStreamClose(close) if close.failed.is_some() => {
            format!("close {} failed", close.seq)
        }
        Job::ResolveStreamClose(close) => format!("close {}", close.seq),
        Job::ResolveActivity(local) => format!("local {}", local.seq),
        other => format!("{other:?}")
            .split(['(', ' '])
            .next()
            .unwrap()
            .to_string(),
    }
}

/// Returns the jobs of each activation that leaves an event in History, with no query and no
/// Update.
fn with_events(jobs: &[Vec<Job>]) -> Vec<Vec<String>> {
    let kept = jobs.iter().map(|jobs| {
        (jobs.iter())
            .filter(|job| !matches!(job, Job::QueryWorkflow(_) | Job::DoUpdate(_)))
            .map(summary)
            .collect::<Vec<_>>()
    });
    kept.filter(|jobs| !jobs.is_empty()).collect()
}

/// The cases that the generated runs reached, over all seeds.
#[derive(Debug, Default)]
struct Reached {
    /// The Stream results that came in the activation right after the one of their call.
    in_round: usize,
    /// The Stream results that came in a later task than their call.
    late: usize,
    /// The activations with the result of a Local Activity and a Stream result.
    mixed: usize,
    queries: usize,
    updates: usize,
    /// The completions that asked for a new task.
    forced: usize,
}

// The first run of generated Workflow code, with Local Activities, queries, rejected Updates and
// Store calls that wait, writes a History whose replay gives the same jobs in the same activations.
// Queries and rejected Updates leave no event, so the replay has none of their jobs. The same
// History with no flag in its first completion replays with chunking on, as for a Workflow that
// started on a Worker with no Stream runtime. One activation of that replay can have the jobs of
// several, and a Local Activity result can come earlier, but each kind of result keeps its order.
// With no Local Activity, the jobs keep the order of the first run.
#[tokio::test]
async fn a_replay_gives_the_activations_of_the_first_run() {
    let ids = ["s0", "s1", "s2"];
    let mut reached = Reached::default();
    for seed in 0..40 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let script = script(&mut rng);
        let store = store(&ids).await;
        let mut opts = StartOpts::new(&store);
        opts.configure =
            |wc| wc.stream_runtimes.settings.task_time = Some(Duration::from_millis(100));
        // The server gives each task with a Signal, and some of them with a query or an Update.
        // A forced task has no new event. Each completion lets the Store answer the calls that
        // wait, so a task that waits for one ends.
        let (server_rng, released) = (Mutex::new(StdRng::seed_from_u64(!seed)), store.clone());
        opts.eager = Some(Box::new(move |completion, t| {
            for id in ids {
                released.release_stream(id);
            }
            let types = command_types(completion);
            if types.contains(&CommandType::CompleteWorkflowExecution) {
                return None;
            }
            let mut rng = server_rng.lock();
            let forced = completion.force_create_new_workflow_task;
            if !forced {
                t.add_we_signaled("go", vec![]);
            }
            let mut task = next_task(t);
            if rng.random_bool(0.3) {
                let question = WorkflowQuery {
                    query_type: "q".to_string(),
                    ..Default::default()
                };
                task.queries
                    .insert(format!("q{}", t.current_event_id()), question);
            }
            if !forced && rng.random_bool(0.3) {
                let update = format!("u{}", t.current_event_id());
                task.add_update_request(update, t.current_event_id() - 1);
            }
            Some(task)
        }));
        let run = start(history(Duration::from_secs(1)), opts);
        let (mut progress, mut made, mut number) = (Progress::default(), HashMap::new(), 0);
        let workflow = |activation: &WorkflowActivation| {
            // The Store answers some of the calls that wait, and some calls of this activation
            // wait.
            for id in ids.iter().filter(|_| rng.random_bool(0.5)) {
                store.release_stream(id);
            }
            let commands = scripted(&script, &mut progress, activation);
            for command in &commands {
                let (call, stream) = match command {
                    Command::AppendToStream(append) => {
                        (format!("append {}", append.seq), &append.stream)
                    }
                    Command::CloseStream(close) => (format!("close {}", close.seq), &close.stream),
                    _ => continue,
                };
                made.insert(call, number);
                if rng.random_bool(0.3) {
                    store.hold_stream(&stream.as_ref().unwrap().id);
                }
            }
            number += 1;
            commands
        };
        let mut locals_rng = StdRng::seed_from_u64(seed + 1);
        let locals = async {
            while let Ok(task) = run.worker.poll_activity_task().await {
                let wait = Duration::from_millis(locals_rng.random_range(0..30));
                tokio::time::sleep(wait).await;
                let completion = ActivityTaskCompletion {
                    task_token: task.task_token,
                    result: Some(ActivityExecutionResult::ok(vec![1].into())),
                };
                run.worker.complete_activity_task(completion).await.unwrap();
            }
            std::future::pending().await
        };
        let live = tokio::select! {
            jobs = run.drive(workflow) => jobs,
            () = locals => unreachable!(),
        };
        run.worker.drain_pollers_and_shutdown_ref().await;
        let first = with_events(&live);
        let has_local = (script.iter().flatten())
            .any(|command| matches!(command, Command::ScheduleLocalActivity(_)));
        for chunking in [false, true] {
            let mut at = 0;
            let workflow = |activation: &WorkflowActivation| {
                let jobs: Vec<Job> = (activation.jobs.iter())
                    .filter_map(|job| job.variant.clone())
                    .collect();
                let jobs = with_events(&[jobs]).concat();
                replayed(&first, &progress.taken, &mut at, &jobs)
            };
            let replayed = with_events(&run.replay_with(chunking, workflow).await);
            if !chunking {
                assert_eq!(replayed, first, "seed {seed}");
                continue;
            }
            // Without the Local Activity results, each activation of the replay has the jobs of
            // one or more activations of the first run, in turn. A task that completes with no
            // command and no ledger joins the next one, and a Local Activity result of the next
            // one can then come earlier, also before a Stream result of an earlier activation.
            let (rest, first_rest) = (without_locals(&replayed), without_locals(&first));
            let mut at = 0;
            for jobs in &rest {
                let count = merged(&first_rest, at, jobs);
                assert!(count > 0, "seed {seed}: {replayed:?} {first:?}");
                at += count;
            }
            assert_eq!(at, first_rest.len(), "seed {seed}: {replayed:?} {first:?}");
            for local in [false, true] {
                assert_eq!(
                    of_kind(&replayed, local),
                    of_kind(&first, local),
                    "seed {seed}"
                );
            }
            if !has_local {
                assert_eq!(replayed.concat(), first.concat(), "seed {seed}");
            }
        }

        for (at, jobs) in live.iter().enumerate() {
            let summaries: Vec<String> = jobs.iter().map(summary).collect();
            for done in summaries
                .iter()
                .filter_map(|s| made.get(s.trim_end_matches(" failed")))
            {
                match at == done + 1 {
                    true => reached.in_round += 1,
                    false => reached.late += 1,
                }
            }
            let local = summaries.iter().any(|s| s.starts_with("local"));
            let stream =
                (summaries.iter()).any(|s| s.starts_with("append") || s.starts_with("close"));
            reached.mixed += usize::from(local && stream);
            for job in jobs {
                reached.queries += usize::from(matches!(job, Job::QueryWorkflow(_)));
                reached.updates += usize::from(matches!(job, Job::DoUpdate(_)));
            }
        }
        let sent = run.sent();
        reached.forced += sent
            .iter()
            .filter(|c| c.force_create_new_workflow_task)
            .count();
    }
    let Reached {
        in_round,
        late,
        mixed,
        queries,
        updates,
        forced,
    } = reached;
    let counts = [in_round, late, mixed, queries, updates, forced];
    assert!(counts.iter().all(|count| *count > 0), "{reached:?}");
}
