//! The Stream calls of one run. Nothing here does I/O. The machines of the run drive the calls,
//! and the Worker's [`StreamManager`](super::StreamManager) runs the requests that they start.

use super::{
    DEFAULT_PAGE_BYTES, DEFAULT_PAGE_RECORDS, MarkerReads, StreamRuntimes, TaskState,
    manager::{Call, Op, Outcome, ReadCall, Request, StreamResolution},
    marker::{MarkerBuilder, ReadDone, StreamMarker, Waiting, append_job, close_job, read_job},
    replay::{ReplayError, needs_records, offset_text},
};
use crate::worker::workflow::WFMachinesError;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt, mem,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::{
        external_data::{
            StreamAppendResult, StreamCloseIntent, StreamCloseResult, StreamMarkerData,
            StreamReadResult,
        },
        workflow_activation::workflow_activation_job,
        workflow_commands::{AppendToStream, CloseStream, ReadStream, RequestCancelStreamRead},
        workflow_completion,
    },
    temporal::api::{common::v1::Payload, failure::v1::failure::FailureInfo},
};
use temporalio_streams::{
    AnyError, AnyReason,
    attempt::attempt_key,
    ids::{check_offset, check_stream_id},
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, ReadRecordsRequest,
        StreamRecord, StreamRef, WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
    },
};

/// The maximum length of a Store name in a Workflow call, in bytes. A marker records the Store of
/// each call, so Core fails the Workflow Task of a call with a longer name.
const MAX_STORE_NAME_BYTES: usize = 4096;

/// The counter of the ids of running calls, for every run of the process. A late result of a call
/// of an evicted run then never matches a call of the run that replaces it.
static CALL_IDS: AtomicU64 = AtomicU64::new(0);

impl From<ReplayError> for WFMachinesError {
    fn from(error: ReplayError) -> Self {
        match error {
            ReplayError::Gone(message) => WFMachinesError::StreamReplay(message),
            ReplayError::Unavailable(message) => WFMachinesError::Stream(message),
        }
    }
}

/// The type of the failure of a [`WFMachinesError::StreamReplay`], which can fail the Workflow.
/// It's a fact of the format of that failure, which History keeps.
const STREAM_REPLAY_ERROR_TYPE: &str = "StreamReplayError";

/// Gives the failure of a [`WFMachinesError::StreamReplay`] its type.
pub(crate) fn set_replay_failure_type(failure: &mut workflow_completion::Failure) {
    if let Some(FailureInfo::ApplicationFailureInfo(info)) =
        (failure.failure.as_mut()).and_then(|failure| failure.failure_info.as_mut())
    {
        info.r#type = STREAM_REPLAY_ERROR_TYPE.to_string();
    }
}

/// Returns true for the failure of a [`WFMachinesError::StreamReplay`].
pub(crate) fn is_stream_replay_failure(failure: &workflow_completion::Failure) -> bool {
    let info = (failure.failure.as_ref()).and_then(|failure| failure.failure_info.as_ref());
    matches!(info, Some(FailureInfo::ApplicationFailureInfo(info))
        if info.r#type == STREAM_REPLAY_ERROR_TYPE)
}

/// How far a run is through its History.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The run replays History, so each call waits for its result in a marker.
    Replaying,
    /// The replay is over, so calls run.
    Live,
    /// The run is on a replay Worker, which makes no live calls.
    ReplayOnly,
    /// The run stopped its calls, at a failed task or at the command that ends the Workflow, so it
    /// records no new call.
    Stopped,
}

/// A call by its kind and seq. The results of one activation come in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CallKey {
    Append(u32),
    Close(u32),
    Read(u32),
}

impl fmt::Display for CallKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallKey::Append(seq) => write!(f, "append {seq}"),
            CallKey::Close(seq) => write!(f, "close {seq}"),
            CallKey::Read(seq) => write!(f, "read {seq}"),
        }
    }
}

struct Append {
    stream: StreamRef,
    state: AppendState,
}

enum AppendState {
    /// The append is in its queue. A replayed one gets its attempt at go-live.
    Waiting {
        bodies: Vec<Payload>,
        attempt: Option<WorkflowTaskAttempt>,
    },
    Running(u64),
    /// The result waits for an activation, in the form that the marker records it.
    Done(Waiting<StreamAppendResult>),
    /// History has the result, which waits for its activation.
    Replayed,
}

struct Close {
    stream: StreamRef,
    state: CloseState,
}

enum CloseState {
    /// No marker has the intent yet.
    Asked,
    /// The marker of the task has the intent. A replayed close gets its attempt at go-live.
    Sealed(Option<WorkflowTaskAttempt>),
    /// History has the intent, so the close is in its queue.
    Waiting(Option<WorkflowTaskAttempt>),
    Running(u64),
    /// The result waits for an activation, in the form that the marker records it.
    Done(Waiting<StreamCloseResult>),
    /// History has the result, which waits for its activation.
    Replayed,
}

struct Read {
    stream: StreamRef,
    from_offset: Vec<u8>,
    state: ReadState,
}

enum ReadState {
    /// The read hasn't started. A replayed one starts once the replay is over.
    Waiting(ReadCall),
    /// Workflow code cancelled the read in a replay, before History gave its result.
    Cancelled,
    Running(u64),
    /// The result waits for an activation, in the form that the marker records it.
    Done(ReadDone),
    /// History has the result, which waits for its activation.
    Replayed,
    /// A graceful shutdown stopped the read, so another Worker runs it again.
    Parked,
}

/// The entries of History's markers whose calls Workflow code hasn't issued yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, StreamAppendResult>,
    intents: HashMap<u32, StreamCloseIntent>,
    closes: HashMap<u32, StreamCloseResult>,
    reads: HashMap<u32, HeldRead>,
}

impl Held {
    /// Returns the activation and the call of the first result, in the order of the first run.
    fn first_result(&self) -> Option<(u64, CallKey)> {
        let appends =
            (self.appends.iter()).map(|(seq, a)| (a.activation_index, CallKey::Append(*seq)));
        let closes =
            (self.closes.iter()).map(|(seq, c)| (c.activation_index, CallKey::Close(*seq)));
        let reads = (self.reads.iter())
            .map(|(seq, read)| (read.entry.activation_index, CallKey::Read(*seq)));
        appends.chain(closes).chain(reads).min()
    }
}

/// A read result of History, with the Records that the replay has for its page.
struct HeldRead {
    entry: StreamReadResult,
    records: Result<Vec<StreamRecord>, ReplayError>,
}

/// The Stream calls of one run. A call leaves the run once its result is in a marker.
pub(crate) struct StreamCalls {
    /// The Producer of the run. A write adds the attempt of its Workflow Task.
    producer: WorkflowProducer,
    runtimes: StreamRuntimes,
    phase: Phase,
    /// The Workflow Task that the run has in hand.
    task: TaskState,
    appends: HashMap<u32, Append>,
    closes: HashMap<u32, Close>,
    reads: HashMap<u32, Read>,
    /// The appends and closes of each Stream, in the order that they joined. Only the head runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    last_close: u32,
    last_read: u32,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    held: Held,
    /// History's results whose calls Workflow code issued, by the activation that gets them.
    ready: BTreeMap<(u64, CallKey), workflow_activation_job::Variant>,
    /// The last result of `ready` that a release gave to the next activation.
    released: Option<(u64, CallKey)>,
    marker: MarkerBuilder,
    requests: Vec<Request>,
}

impl StreamCalls {
    /// Creates the calls of a new run, which writes as the Workflow Producer with these ids.
    pub(crate) fn new(
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
        runtimes: StreamRuntimes,
        phase: Phase,
    ) -> Self {
        debug_assert_ne!(phase, Phase::Stopped);
        let marker = MarkerBuilder::new(runtimes.settings);
        Self {
            producer: WorkflowProducer {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                attempt: None,
            },
            runtimes,
            phase,
            task: TaskState::NoTask,
            appends: HashMap::new(),
            closes: HashMap::new(),
            reads: HashMap::new(),
            queues: HashMap::new(),
            last_append: 0,
            last_close: 0,
            last_read: 0,
            done: BTreeSet::new(),
            held: Held::default(),
            ready: BTreeMap::new(),
            released: None,
            marker,
            requests: vec![],
        }
    }

    /// Sets the task that the run has in hand. When the task opens, the head of each queue starts.
    pub(crate) fn set_task(&mut self, state: TaskState) {
        self.task = state;
        self.start_heads();
    }

    /// Takes an append command.
    pub(crate) fn append(&mut self, command: AppendToStream) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        let AppendToStream {
            seq,
            stream,
            bodies,
        } = command;
        next_seq("append", seq, &mut self.last_append)?;
        let stream = valid_stream(CallKey::Append(seq), stream)?;
        self.check_runtime(&stream)?;
        let attempt = match self.phase {
            Phase::Live => self.task.attempt(),
            Phase::Replaying | Phase::ReplayOnly | Phase::Stopped => None,
        };
        let state = AppendState::Waiting { bodies, attempt };
        let queue = key(&stream);
        self.queues
            .entry(queue.clone())
            .or_default()
            .push_back(CallKey::Append(seq));
        self.appends.insert(seq, Append { stream, state });
        self.match_append(seq)?;
        self.start_head(&queue);
        Ok(())
    }

    /// Takes a close command. The marker of the task records its intent, and the close runs once
    /// History has that intent.
    pub(crate) fn close(&mut self, command: CloseStream) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        let CloseStream { seq, stream } = command;
        next_seq("close", seq, &mut self.last_close)?;
        let stream = valid_stream(CallKey::Close(seq), stream)?;
        self.check_runtime(&stream)?;
        let state = CloseState::Asked;
        self.closes.insert(seq, Close { stream, state });
        self.match_intent(seq)
    }

    /// Takes a read command, and starts the read if the run is live.
    pub(crate) fn read(&mut self, command: ReadStream) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        let ReadStream {
            seq,
            stream,
            from_offset,
            wait,
            records_in_history,
            max_records,
            max_bytes,
        } = command;
        next_seq("read", seq, &mut self.last_read)?;
        let call = CallKey::Read(seq);
        let stream = valid_stream(call, stream)?;
        let wait = check_offset(&from_offset, "The offset", AnyReason::InvalidArgument)
            .and_then(|()| check_page_size(max_records, max_bytes))
            .and_then(|()| wait.map_or(Ok(Duration::ZERO), wait_duration))
            .map_err(|error| {
                WFMachinesError::Fatal(format!("Stream {call} is invalid: {}", error.message))
            })?;
        self.check_runtime(&stream)?;
        // A page must fit an empty task, and an empty marker if the marker holds its Records.
        let settings = self.runtimes.settings;
        let mut room = settings.max_task_record_bytes();
        if records_in_history {
            room = room.min(settings.max_marker_record_bytes());
        }
        let max_bytes = match max_bytes {
            0 => DEFAULT_PAGE_BYTES,
            max => max,
        };
        let request = ReadRecordsRequest {
            stream_id: stream.id.clone(),
            from_offset: from_offset.clone(),
            max_records: match max_records {
                0 => DEFAULT_PAGE_RECORDS,
                max => max,
            },
            max_bytes: max_bytes.min(i64::try_from(room).unwrap_or(i64::MAX)),
            wait: None,
        };
        let state = ReadState::Waiting(ReadCall {
            request,
            wait,
            records_in_history,
        });
        let read = Read {
            stream,
            from_offset,
            state,
        };
        self.reads.insert(seq, read);
        self.match_read(seq)?;
        if self.phase == Phase::Live {
            self.run_read(seq);
        }
        Ok(())
    }

    /// Takes the cancel of a read. Core stops a read that runs and drops a result that waits, and
    /// the read gets a `cancelled` job. In a replay, the read gets the job that History has.
    pub(crate) fn cancel_read(
        &mut self,
        command: RequestCancelStreamRead,
    ) -> Result<(), WFMachinesError> {
        let seq = command.seq;
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        if seq > self.last_read {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Workflow code cancelled Stream read {seq}, which it never issued"
            )));
        }
        // The run no longer has a read that got its job, so the cancel changes nothing.
        let Some(read) = self.reads.get_mut(&seq) else {
            return Ok(());
        };
        match read.state {
            ReadState::Waiting(_) => read.state = ReadState::Cancelled,
            ReadState::Running(_) | ReadState::Done(_) => {
                if let ReadState::Running(call_id) = read.state {
                    stop_call(&mut self.requests, call_id);
                }
                let from_offset = read.from_offset.clone();
                let cancelled = ReadDone::cancelled(&read.stream, seq, from_offset);
                read.state = ReadState::Done(cancelled);
                self.done.insert(CallKey::Read(seq));
            }
            ReadState::Cancelled | ReadState::Replayed | ReadState::Parked => {}
        }
        Ok(())
    }

    /// Takes the result of a running call, and starts the next call of its Stream. The result of
    /// a call that was stopped finds no call.
    pub(crate) fn resolve(&mut self, resolution: StreamResolution) {
        let StreamResolution {
            call_id,
            seq,
            outcome,
        } = resolution;
        let (call, queue) = match outcome {
            // A read is in no queue.
            Outcome::Read(result) => {
                let Some(read) = self.reads.get_mut(&seq) else {
                    return;
                };
                if !matches!(read.state, ReadState::Running(id) if id == call_id) {
                    return;
                }
                read.state = ReadState::Done(result);
                self.done.insert(CallKey::Read(seq));
                return;
            }
            Outcome::Append(result) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                if !matches!(append.state, AppendState::Running(id) if id == call_id) {
                    return;
                }
                append.state = AppendState::Done(Waiting::new(StreamAppendResult {
                    seq,
                    stream_id: append.stream.id.clone(),
                    store: append.stream.store.clone(),
                    activation_index: 0,
                    result: Some(result),
                }));
                (CallKey::Append(seq), key(&append.stream))
            }
            Outcome::Close(failure) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                if !matches!(close.state, CloseState::Running(id) if id == call_id) {
                    return;
                }
                close.state = CloseState::Done(Waiting::new(StreamCloseResult {
                    seq,
                    activation_index: 0,
                    failure,
                }));
                (CallKey::Close(seq), key(&close.stream))
            }
        };
        self.done.insert(call);
        self.leave_queue(&queue, call);
    }

    /// Returns true if the activation at index `activation` of the task gets a result. A result
    /// from History waits for the Local Activity result of the next marker, at index `marker`, if
    /// the first run gave that one before it or with it.
    pub(crate) fn has_jobs(&self, activation: u64, marker: Option<u64>) -> bool {
        // A run that stopped its calls gives no result, even one that History has.
        self.phase != Phase::Stopped
            && ((self.ready.keys().next()).is_some_and(|key| self.is_due(key, activation, marker))
                || self.next_result_fits(activation) == Some(true))
    }

    /// Returns the results for the activation at index `activation` of the task, as
    /// [`Self::has_jobs`] decides them. The marker records each live result with that index.
    pub(crate) fn take_jobs(
        &mut self,
        activation: u64,
        marker: Option<u64>,
    ) -> Vec<workflow_activation_job::Variant> {
        let mut jobs = vec![];
        while let Some(key) = self.ready.keys().next().copied()
            && self.is_due(&key, activation, marker)
        {
            let job = self.ready.remove(&key).expect("The key was just read");
            self.leave(key.1);
            jobs.push(job);
        }
        self.released = None;
        if !self.task.is_open() {
            return jobs;
        }
        while let Some(&call) = self.done.first() {
            let Some(job) = self.record(call, activation) else {
                break;
            };
            self.done.remove(&call);
            self.leave(call);
            jobs.push(job);
        }
        jobs
    }

    /// Holds the entries of a marker from History, each for its call, with the Records that the
    /// replay read for its read entries.
    pub(crate) fn peeked(
        &mut self,
        marker: StreamMarker,
        mut replayed: MarkerReads,
    ) -> Result<(), WFMachinesError> {
        let StreamMarkerData {
            close_intents,
            appends,
            closes,
            reads,
        } = marker.data;
        for intent in close_intents {
            let seq = intent.seq;
            if self.held.intents.insert(seq, intent).is_some() {
                return Err(second("intent", CallKey::Close(seq)));
            }
            self.match_intent(seq)?;
        }
        for append in appends {
            let seq = append.seq;
            if self.held.appends.insert(seq, append).is_some() {
                return Err(second("result", CallKey::Append(seq)));
            }
            self.match_append(seq)?;
        }
        for close in closes {
            let seq = close.seq;
            if self.held.closes.insert(seq, close).is_some() {
                return Err(second("result", CallKey::Close(seq)));
            }
            self.match_result(seq)?;
        }
        let mut in_history = marker.records.into_iter();
        for (index, entry) in reads.into_iter().enumerate() {
            let seq = entry.seq;
            let held = in_history.next().unwrap_or_default();
            let records = if needs_records(&entry) {
                (replayed.get_mut(index).and_then(Option::take)).unwrap_or_else(|| {
                    Err(ReplayError::Unavailable(format!(
                        "Core read no Records of Stream read {seq} for its replay"
                    )))
                })
            } else {
                Ok(held)
            };
            let held = HeldRead { entry, records };
            if self.held.reads.insert(seq, held).is_some() {
                return Err(second("result", CallKey::Read(seq)));
            }
            self.match_read(seq)?;
        }
        Ok(())
    }

    /// Handles the event of a marker. Each entry of the marker must have reached its call by now,
    /// and the closes whose intents it has join their queues.
    pub(crate) fn marker_recorded(&mut self) -> Result<(), WFMachinesError> {
        let never = |what: &str, call: CallKey| {
            WFMachinesError::Nondeterminism(format!(
                "History has {what} of Stream {call} that Workflow code never issued"
            ))
        };
        if let Some(seq) = self.held.intents.keys().min() {
            return Err(never("an intent", CallKey::Close(*seq)));
        }
        if let Some(seq) = self.held.appends.keys().min() {
            return Err(never("a result", CallKey::Append(*seq)));
        }
        if let Some(seq) = self.held.closes.keys().min() {
            return Err(never("a result", CallKey::Close(*seq)));
        }
        if let Some(seq) = self.held.reads.keys().min() {
            return Err(never("a result", CallKey::Read(*seq)));
        }
        if let Some((_, call)) = self.ready.keys().next() {
            return Err(WFMachinesError::Nondeterminism(format!(
                "History has a result of Stream {call} that Workflow code never got"
            )));
        }
        let mut sealed: Vec<u32> = (self.closes.iter())
            .filter(|(_, close)| matches!(close.state, CloseState::Sealed(_)))
            .map(|(seq, _)| *seq)
            .collect();
        sealed.sort_unstable();
        for seq in sealed {
            let close = self
                .closes
                .get_mut(&seq)
                .expect("The close was just listed");
            if let CloseState::Sealed(attempt) = close.state {
                close.state = CloseState::Waiting(attempt);
            }
            let queue = key(&close.stream);
            self.queues
                .entry(queue.clone())
                .or_default()
                .push_back(CallKey::Close(seq));
            self.start_head(&queue);
        }
        Ok(())
    }

    /// Ends the replay. Each call that waits takes the attempt of the task in hand, and the head of
    /// each queue starts.
    pub(crate) fn go_live(&mut self) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Replaying {
            return Ok(());
        }
        let appends = (self.appends.iter()).map(|(seq, append)| (CallKey::Append(*seq), append));
        let unserved = (appends.map(|(call, append)| (call, &append.stream)))
            .chain((self.closes.iter()).map(|(seq, close)| (CallKey::Close(*seq), &close.stream)))
            .chain((self.reads.iter()).map(|(seq, read)| (CallKey::Read(*seq), &read.stream)))
            .filter(|(_, stream)| self.runtimes.get(&stream.store).is_none())
            .min_by_key(|(call, _)| *call);
        if let Some((_, stream)) = unserved {
            return Err(no_runtime(stream));
        }
        self.phase = Phase::Live;
        let attempt = self.task.attempt();
        for append in self.appends.values_mut() {
            if let AppendState::Waiting {
                attempt: waiting, ..
            } = &mut append.state
            {
                *waiting = attempt;
            }
        }
        for close in self.closes.values_mut() {
            if let CloseState::Sealed(waiting) | CloseState::Waiting(waiting) = &mut close.state {
                *waiting = attempt;
            }
        }
        let mut reads: Vec<u32> = self.reads.keys().copied().collect();
        reads.sort_unstable();
        for seq in reads {
            let read = self.reads.get_mut(&seq).expect("The read was just listed");
            if let ReadState::Cancelled = read.state {
                let from_offset = read.from_offset.clone();
                let cancelled = ReadDone::cancelled(&read.stream, seq, from_offset);
                read.state = ReadState::Done(cancelled);
                self.done.insert(CallKey::Read(seq));
            }
            self.run_read(seq);
        }
        self.start_heads();
        Ok(())
    }

    /// Drops each call at a command that ends the Workflow, as Core drops a Local Activity there.
    /// The marker keeps the results that Workflow code already got in the task.
    pub(crate) fn stop_for_terminal(&mut self) {
        let mut unsent = HashSet::new();
        self.requests.retain(|request| match request {
            Request::Run(call) => {
                unsent.insert(call.call_id);
                false
            }
            Request::Stop(_) => true,
        });
        let appends = (self.appends.values()).filter_map(|append| match append.state {
            AppendState::Running(call_id) => Some(call_id),
            _ => None,
        });
        let closes = (self.closes.values()).filter_map(|close| match close.state {
            CloseState::Running(call_id) => Some(call_id),
            _ => None,
        });
        let reads = (self.reads.values()).filter_map(|read| match read.state {
            ReadState::Running(call_id) => Some(call_id),
            _ => None,
        });
        let running = appends.chain(closes).chain(reads);
        let running = running.filter(|call_id| !unsent.contains(call_id));
        self.requests.extend(running.map(Request::Stop));
        self.phase = Phase::Stopped;
        self.appends.clear();
        self.closes.clear();
        self.reads.clear();
        self.queues.clear();
        self.done.clear();
    }

    /// Returns true while a call has no result in a marker, so the run needs another task.
    pub(crate) fn needs_next_task(&self) -> bool {
        !self.appends.is_empty() || !self.closes.is_empty() || !self.reads.is_empty()
    }

    /// Returns true if the marker of the task has an entry.
    pub(crate) fn has_marker(&self) -> bool {
        !self.marker.is_empty()
    }

    /// Returns true once the marker of the task, or the task, has no room for a result that
    /// waits. The task must then complete with a new one forced.
    pub(crate) fn must_complete(&self, activation: u64) -> bool {
        self.next_result_fits(activation) == Some(false)
    }

    /// Returns the number of live calls that hold the task open, as a Local Activity does.
    pub(crate) fn outstanding(&self) -> usize {
        if self.phase != Phase::Live {
            return 0;
        }
        self.outstanding_writes() + self.running_reads()
    }

    /// Returns true if only reads hold the task open. A command, a message or a close intent for
    /// the server must not wait for a read.
    pub(crate) fn holds_only_reads(&self) -> bool {
        self.phase == Phase::Live && self.outstanding_writes() == 0 && self.running_reads() > 0
    }

    /// Returns true while a close waits for a marker to take its intent.
    pub(crate) fn has_intents(&self) -> bool {
        (self.closes.values()).any(|close| matches!(close.state, CloseState::Asked))
    }

    /// Stops each read that runs, at a graceful shutdown of the Worker. The read gets no job
    /// here, and another Worker runs it again after its replay.
    pub(crate) fn park_reads(&mut self) {
        for read in self.reads.values_mut() {
            if let ReadState::Running(call_id) = read.state {
                stop_call(&mut self.requests, call_id);
                read.state = ReadState::Parked;
            }
        }
    }

    /// Returns true while a read that a shutdown stopped waits for another Worker.
    pub(crate) fn has_parked_reads(&self) -> bool {
        (self.reads.values()).any(|read| matches!(read.state, ReadState::Parked))
    }

    /// Returns the activation of the first result from History whose call Workflow code issued.
    pub(crate) fn next_ready_activation(&self) -> Option<u64> {
        self.ready.keys().next().map(|(at, _)| *at)
    }

    /// Returns the activation of the first result from History that Workflow code doesn't have.
    pub(crate) fn first_result_activation(&self) -> Option<u64> {
        let held = self.held.first_result().map(|(at, _)| at);
        self.next_ready_activation().into_iter().chain(held).min()
    }

    /// Releases the results from History for the next activation, whatever their index. It takes
    /// them in the order of the first run, up to the first one whose call Workflow code hasn't
    /// issued. A replay with chunking on that has no job to give does the same for the results of
    /// Local Activities.
    pub(crate) fn release_ready(&mut self) {
        let held = self.held.first_result();
        let comes = |key: &&(u64, CallKey)| held.is_none_or(|held| **key < held);
        self.released = self.ready.keys().take_while(comes).last().copied();
    }

    fn is_due(&self, key: &(u64, CallKey), activation: u64, marker: Option<u64>) -> bool {
        (key.0 <= activation || self.released.is_some_and(|released| *key <= released))
            && marker.is_none_or(|marker| key.0 < marker)
    }

    /// Returns the marker of the task as it completes, with the intent of each close that fits.
    pub(crate) fn take_marker(&mut self) -> Option<StreamMarker> {
        if matches!(self.phase, Phase::Replaying | Phase::ReplayOnly) {
            return None;
        }
        let attempt = self.task.attempt();
        let mut asked: Vec<u32> = (self.closes.iter())
            .filter(|(_, close)| matches!(close.state, CloseState::Asked))
            .map(|(seq, _)| *seq)
            .collect();
        asked.sort_unstable();
        for seq in asked {
            let close = self
                .closes
                .get_mut(&seq)
                .expect("The close was just listed");
            if !self.marker.close_intent(&close.stream, seq) {
                break;
            }
            close.state = CloseState::Sealed(attempt);
        }
        self.marker.take()
    }

    /// Returns the calls that the Worker must start or stop.
    pub(crate) fn take_requests(&mut self) -> Vec<Request> {
        std::mem::take(&mut self.requests)
    }

    /// Drops every call, the marker of the task and the entries of History, and leaves the run
    /// with no task, as a failed task does. The run records no call after this.
    pub(crate) fn stop(&mut self) {
        self.stop_for_terminal();
        self.task = TaskState::NoTask;
        self.held = Held::default();
        self.ready.clear();
        self.released = None;
        self.marker = MarkerBuilder::new(self.runtimes.settings);
        self.requests.clear();
    }

    fn outstanding_writes(&self) -> usize {
        let appends = (self.appends.values())
            .filter(|append| {
                matches!(
                    append.state,
                    AppendState::Waiting { .. } | AppendState::Running(_)
                )
            })
            .count();
        let closes = (self.closes.values())
            .filter(|close| matches!(close.state, CloseState::Waiting(_) | CloseState::Running(_)))
            .count();
        appends + closes
    }

    fn running_reads(&self) -> usize {
        (self.reads.values())
            .filter(|read| matches!(read.state, ReadState::Running(_)))
            .count()
    }

    fn check_runtime(&self, stream: &StreamRef) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Live && self.runtimes.get(&stream.store).is_none() {
            return Err(no_runtime(stream));
        }
        Ok(())
    }

    fn match_append(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(append) = self.appends.get_mut(&seq) else {
            return Ok(());
        };
        let Some(entry) = self.held.appends.remove(&seq) else {
            return Ok(());
        };
        let call = CallKey::Append(seq);
        check_stream(call, &append.stream, &entry.stream_id, &entry.store)?;
        if !matches!(append.state, AppendState::Waiting { .. }) {
            return Err(second("result", call));
        }
        append.state = AppendState::Replayed;
        let job = append_job(&entry).into();
        self.ready.insert((entry.activation_index, call), job);
        Ok(())
    }

    fn match_intent(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(close) = self.closes.get_mut(&seq) else {
            return Ok(());
        };
        let Some(intent) = self.held.intents.remove(&seq) else {
            return Ok(());
        };
        let call = CallKey::Close(seq);
        check_stream(call, &close.stream, &intent.stream_id, &intent.store)?;
        if !matches!(close.state, CloseState::Asked) {
            return Err(second("intent", call));
        }
        close.state = CloseState::Sealed(None);
        Ok(())
    }

    fn match_result(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(close) = self.closes.get_mut(&seq) else {
            return Ok(());
        };
        let Some(entry) = self.held.closes.remove(&seq) else {
            return Ok(());
        };
        if !matches!(close.state, CloseState::Waiting(_)) {
            return Err(WFMachinesError::Nondeterminism(format!(
                "History has a result of Stream close {seq} with no intent before it, or a \
                 second one"
            )));
        }
        close.state = CloseState::Replayed;
        let at = (entry.activation_index, CallKey::Close(seq));
        self.ready.insert(at, close_job(&entry).into());
        Ok(())
    }

    /// Gives a replayed read the result that History has for it. An entry of another read is a
    /// nondeterminism error, also if the replay has no Records for that entry.
    fn match_read(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(read) = self.reads.get_mut(&seq) else {
            return Ok(());
        };
        let Some(held) = self.held.reads.remove(&seq) else {
            return Ok(());
        };
        let (call, entry) = (CallKey::Read(seq), &held.entry);
        check_stream(call, &read.stream, &entry.stream_id, &entry.store)?;
        if entry.from_offset != read.from_offset {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Stream read {seq} gives the Records after {}, and History has them after {}",
                offset_text(&read.from_offset),
                offset_text(&entry.from_offset)
            )));
        }
        if !matches!(read.state, ReadState::Waiting(_) | ReadState::Cancelled) {
            return Err(second("result", call));
        }
        let records = held.records?;
        read.state = ReadState::Replayed;
        let job = read_job(entry, records).into();
        self.ready.insert((entry.activation_index, call), job);
        Ok(())
    }

    /// Starts read `seq` if it waits.
    fn run_read(&mut self, seq: u32) {
        let Some(read) = self.reads.get_mut(&seq) else {
            return;
        };
        if !matches!(read.state, ReadState::Waiting(_)) {
            return;
        }
        let call_id = CALL_IDS.fetch_add(1, Ordering::Relaxed);
        let ReadState::Waiting(call) = mem::replace(&mut read.state, ReadState::Running(call_id))
        else {
            return;
        };
        let call = Call {
            call_id,
            seq,
            store: read.stream.store.clone(),
            op: Op::Read(call),
        };
        self.requests.push(Request::Run(call));
    }

    /// Returns whether the marker and the task have room for the first result that waits, if the
    /// task is open.
    fn next_result_fits(&self, activation: u64) -> Option<bool> {
        let call = *self.done.first()?;
        if !self.task.is_open() {
            return None;
        }
        match call {
            CallKey::Append(seq) => {
                let AppendState::Done(result) = &self.appends[&seq].state else {
                    return None;
                };
                Some(self.marker.has_room_for_append(result, activation))
            }
            CallKey::Close(seq) => {
                let CloseState::Done(result) = &self.closes[&seq].state else {
                    return None;
                };
                Some(self.marker.has_room_for_close(result, activation))
            }
            CallKey::Read(seq) => {
                let ReadState::Done(result) = &self.reads[&seq].state else {
                    return None;
                };
                Some(self.marker.has_room_for_read(result, activation))
            }
        }
    }

    /// Records a live result in the marker, and returns its job. Returns `None` if the marker or
    /// the task has no room for it.
    fn record(
        &mut self,
        call: CallKey,
        activation: u64,
    ) -> Option<workflow_activation_job::Variant> {
        match call {
            CallKey::Append(seq) => {
                let AppendState::Done(result) = &self.appends[&seq].state else {
                    return None;
                };
                if !self.marker.append_result(result, activation) {
                    return None;
                }
                Some(append_job(&result.entry).into())
            }
            CallKey::Close(seq) => {
                let CloseState::Done(result) = &self.closes[&seq].state else {
                    return None;
                };
                if !self.marker.close_result(result, activation) {
                    return None;
                }
                Some(close_job(&result.entry).into())
            }
            CallKey::Read(seq) => {
                let ReadState::Done(result) = &self.reads[&seq].state else {
                    return None;
                };
                if !self.marker.has_room_for_read(result, activation) {
                    return None;
                }
                let ReadState::Done(result) = self.reads.remove(&seq)?.state else {
                    return None;
                };
                Some(self.marker.read_result(result, activation).into())
            }
        }
    }

    /// Starts the call at the head of each queue, once the run is live and its task is open. It
    /// runs only when the task opens and at go-live, since every other change starts its own head.
    fn start_heads(&mut self) {
        if self.phase != Phase::Live || !self.task.is_open() {
            return;
        }
        let heads: Vec<CallKey> = (self.queues.values())
            .filter_map(|queue| queue.front().copied())
            .collect();
        for call in heads {
            self.start(call);
        }
    }

    /// Starts the call at the head of the queue of one Stream, once the run is live and its task
    /// is open.
    fn start_head(&mut self, queue: &(String, String)) {
        if self.phase != Phase::Live || !self.task.is_open() {
            return;
        }
        if let Some(&call) = self.queues.get(queue).and_then(VecDeque::front) {
            self.start(call);
        }
    }

    /// Starts `call` if it waits with an attempt. A call that already runs, or that has no attempt
    /// yet, stays as it is.
    fn start(&mut self, call: CallKey) {
        let (seq, call_id, stream, op) = match call {
            CallKey::Append(seq) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                let AppendState::Waiting {
                    attempt: Some(attempt),
                    ..
                } = append.state
                else {
                    return;
                };
                let call_id = CALL_IDS.fetch_add(1, Ordering::Relaxed);
                let running = AppendState::Running(call_id);
                let AppendState::Waiting { bodies, .. } =
                    std::mem::replace(&mut append.state, running)
                else {
                    return;
                };
                let producer = producer(&self.producer, attempt);
                let request = AppendRecordsRequest {
                    stream_id: append.stream.id.clone(),
                    request_id: request_id(&self.producer.run_id, &producer, seq),
                    producer: Some(producer),
                    records: (bodies.into_iter())
                        .map(|body| AppendRecord { body: Some(body) })
                        .collect(),
                };
                (seq, call_id, &append.stream, Op::Append(request))
            }
            CallKey::Close(seq) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                let CloseState::Waiting(Some(attempt)) = close.state else {
                    return;
                };
                let call_id = CALL_IDS.fetch_add(1, Ordering::Relaxed);
                close.state = CloseState::Running(call_id);
                let request = CloseStreamRequest {
                    stream_id: close.stream.id.clone(),
                    producer: Some(producer(&self.producer, attempt)),
                };
                (seq, call_id, &close.stream, Op::Close(request))
            }
            // A read is in no queue.
            CallKey::Read(_) => return,
        };
        let call = Call {
            call_id,
            seq,
            store: stream.store.clone(),
            op,
        };
        self.requests.push(Request::Run(call));
    }

    /// Removes `call` from the run. A live result already left its queue when it came.
    fn leave(&mut self, call: CallKey) {
        let stream = match call {
            CallKey::Append(seq) => (self.appends.remove(&seq))
                .filter(|append| !matches!(append.state, AppendState::Done(_)))
                .map(|append| append.stream),
            CallKey::Close(seq) => (self.closes.remove(&seq))
                .filter(|close| !matches!(close.state, CloseState::Done(_)))
                .map(|close| close.stream),
            CallKey::Read(seq) => {
                self.reads.remove(&seq);
                None
            }
        };
        if let Some(stream) = stream {
            self.leave_queue(&key(&stream), call);
        }
    }

    /// Removes `call` from its queue. If it was the head, the next call of the queue starts.
    fn leave_queue(&mut self, queue: &(String, String), call: CallKey) {
        let Some(calls) = self.queues.get_mut(queue) else {
            return;
        };
        let was_head = calls.front() == Some(&call);
        if was_head {
            calls.pop_front();
        } else {
            calls.retain(|queued| *queued != call);
        }
        if calls.is_empty() {
            self.queues.remove(queue);
        } else if was_head {
            self.start_head(queue);
        }
    }
}

/// Checks that `seq` is above the last one of its kind. A seq that came twice would give two
/// appends one request id, and a Store would take the second for a resend of the first.
fn next_seq(kind: &str, seq: u32, last: &mut u32) -> Result<(), WFMachinesError> {
    if seq == 0 {
        return Err(WFMachinesError::Fatal(format!(
            "Stream {kind} has seq 0, and a seq starts at 1"
        )));
    }
    if seq <= *last {
        return Err(WFMachinesError::Fatal(format!(
            "Stream {kind} {seq} came after {kind} {last}"
        )));
    }
    *last = seq;
    Ok(())
}

fn check_store_name(store: &str) -> Result<(), AnyError> {
    if store.len() > MAX_STORE_NAME_BYTES {
        return Err(AnyError::invalid_argument(format!(
            "The Store name has {} bytes, more than {MAX_STORE_NAME_BYTES}",
            store.len()
        )));
    }
    Ok(())
}

/// Returns the Stream of the command of `call`, or an error if the command has none, or one with
/// an id or a Store name out of range. Such a command is not valid, so it fails its Workflow Task.
fn valid_stream(call: CallKey, stream: Option<StreamRef>) -> Result<StreamRef, WFMachinesError> {
    let stream =
        stream.ok_or_else(|| WFMachinesError::Fatal(format!("Stream {call} has no Stream")))?;
    check_stream_id(&stream.id)
        .and_then(|()| check_store_name(&stream.store))
        .map_err(|error| {
            WFMachinesError::Fatal(format!("Stream {call} is invalid: {}", error.message))
        })?;
    Ok(stream)
}

/// Checks that neither maximum of the page of a read is negative.
fn check_page_size(max_records: i32, max_bytes: i64) -> Result<(), AnyError> {
    let maximums = [
        ("max_records", i64::from(max_records)),
        ("max_bytes", max_bytes),
    ];
    match maximums.into_iter().find(|(_, max)| *max < 0) {
        Some((field, max)) => Err(AnyError::invalid_argument(format!(
            "The `{field}` of the read is {max}, which is negative"
        ))),
        None => Ok(()),
    }
}

/// Returns the wait of a read, or a failure for a duration that is negative or that has its
/// nanoseconds out of range.
fn wait_duration(wait: prost_types::Duration) -> Result<Duration, AnyError> {
    let prost_types::Duration { seconds, nanos } = wait;
    match (u64::try_from(seconds), u32::try_from(nanos)) {
        (Ok(whole), Ok(part)) if part < 1_000_000_000 => Ok(Duration::new(whole, part)),
        _ => Err(AnyError::invalid_argument(format!(
            "The wait of the read has {seconds} seconds and {nanos} nanoseconds, which is \
             negative or has its nanoseconds out of range"
        ))),
    }
}

fn no_runtime(stream: &StreamRef) -> WFMachinesError {
    WFMachinesError::Stream(format!(
        "This Worker has no Stream runtime for the Store `{}`",
        stream.store
    ))
}

/// Checks that a call is for the Stream that History has, in the Store that History has. A call
/// with no Store name matches any Store.
fn check_stream(
    call: CallKey,
    sent: &StreamRef,
    id: &str,
    store: &str,
) -> Result<(), WFMachinesError> {
    if sent.id == id && (sent.store.is_empty() || sent.store == store) {
        return Ok(());
    }
    Err(WFMachinesError::Nondeterminism(format!(
        "Stream {call} is for Stream `{}` in Store `{}`, and History has it for Stream `{id}` in \
         Store `{store}`",
        sent.id, sent.store
    )))
}

fn second(what: &str, call: CallKey) -> WFMachinesError {
    WFMachinesError::Nondeterminism(format!("History has a second {what} of Stream {call}"))
}

fn producer(run: &WorkflowProducer, attempt: WorkflowTaskAttempt) -> Producer {
    Producer {
        kind: Some(Kind::Workflow(WorkflowProducer {
            attempt: Some(attempt),
            ..run.clone()
        })),
    }
}

/// Returns the request id of append `seq` of the run `run_id` in the attempt of `producer`. It's
/// the same exactly when the run, the attempt and the seq are, and it joins these parts with `/`.
///
/// - the run id
/// - the [`attempt_key`] of the attempt, in hex (80 characters)
/// - the seq
fn request_id(run_id: &str, producer: &Producer, seq: u32) -> String {
    let key: String = (attempt_key(producer.kind.as_ref()).iter())
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{run_id}/{key}/{seq}")
}

fn key(stream: &StreamRef) -> (String, String) {
    (stream.store.clone(), stream.id.clone())
}

/// Stops the call `call_id`. A call that the Worker didn't start yet never starts.
fn stop_call(requests: &mut Vec<Request>, call_id: u64) {
    let queued = (requests.iter())
        .position(|request| matches!(request, Request::Run(call) if call.call_id == call_id));
    match queued {
        Some(at) => {
            requests.remove(at);
        }
        None => requests.push(Request::Stop(call_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        MAX_MARKER_ONE_RECORD_BYTES, StreamManager, StreamSettings, digest::digest,
        manager::recorded, marker::marker_details,
    };
    use super::*;
    use crate::{
        telemetry::metrics::MetricsContext,
        test_help::stream_store::{MemoryStore, Request as StoreRequest},
    };
    use prost::Message;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use temporalio_common::protos::coresdk::{
        external_data::{
            StreamAppendOffsets, StreamFailure, StreamReadPage, stream_append_result,
            stream_read_result,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, resolve_stream_append, resolve_stream_read,
        },
    };
    use temporalio_streams::{
        AnyError, StreamRuntime,
        proto::ReadRecordsResponse,
        record::{StoreLimits, field_len},
    };
    use tokio::runtime::Handle;
    use workflow_activation_job::Variant;

    /// The number of seeds that each generated test runs.
    const SEEDS: u64 = 400;

    fn runtimes(stores: &[&str]) -> StreamRuntimes {
        StreamRuntimes::new(stores.iter().map(|name| {
            let store = Arc::new(MemoryStore::default());
            let runtime = StreamRuntime::new(store, StoreLimits::default());
            (name.to_string(), Arc::new(runtime))
        }))
    }

    fn calls(phase: Phase, task: i64) -> StreamCalls {
        let mut calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), phase);
        calls.set_task(TaskState::Open(task_attempt(task)));
        calls
    }

    /// Completes the task in hand, and opens the next one.
    fn next_task(calls: &mut StreamCalls, task: i64) -> Option<StreamMarker> {
        let marker = calls.take_marker();
        calls.set_task(TaskState::NoTask);
        calls.set_task(TaskState::Open(task_attempt(task)));
        marker
    }

    fn task_attempt(task: i64) -> WorkflowTaskAttempt {
        WorkflowTaskAttempt {
            started_event_id: task,
            attempt: 1,
            started_time: None,
        }
    }

    fn stream(id: &str) -> StreamRef {
        StreamRef {
            id: id.to_string(),
            store: "mem".to_string(),
        }
    }

    fn append(seq: u32, id: &str) -> AppendToStream {
        AppendToStream {
            seq,
            stream: Some(stream(id)),
            bodies: vec![Payload::default()],
        }
    }

    fn close(seq: u32, id: &str) -> CloseStream {
        CloseStream {
            seq,
            stream: Some(stream(id)),
        }
    }

    fn appended(seq: u32) -> stream_append_result::Result {
        stream_append_result::Result::Appended(StreamAppendOffsets {
            first_offset: vec![seq as u8],
            last_offset: vec![seq as u8],
        })
    }

    /// Returns the result of append `seq` to the Stream `id`, as a run holds it before a marker
    /// records it.
    fn append_done(seq: u32, id: &str) -> Waiting<StreamAppendResult> {
        Waiting::new(StreamAppendResult {
            seq,
            stream_id: id.to_string(),
            store: "mem".to_string(),
            activation_index: 0,
            result: Some(appended(seq)),
        })
    }

    fn refused(message: impl Into<String>) -> StreamFailure {
        recorded(&AnyError::invalid_argument(message))
    }

    /// Returns a marker that `write` fills.
    fn history(write: impl FnOnce(&mut MarkerBuilder)) -> StreamMarker {
        let mut marker = MarkerBuilder::new(StreamSettings::default());
        write(&mut marker);
        marker.take().unwrap()
    }

    /// Returns the calls that started since the last look, as `append 1 of s at 3`, with the
    /// started event id of their attempt.
    fn started(calls: &mut StreamCalls) -> Vec<String> {
        let mut started: Vec<String> = (calls.take_requests().into_iter())
            .map(|request| {
                let (call, stream, producer) = match request {
                    Request::Run(Call { seq, op, .. }) => match op {
                        Op::Append(r) => (CallKey::Append(seq), r.stream_id, r.producer),
                        Op::Close(r) => (CallKey::Close(seq), r.stream_id, r.producer),
                        Op::Read(_) => return format!("read {seq}"),
                    },
                    Request::Stop(_) => return "stop".to_string(),
                };
                let Some(Kind::Workflow(workflow)) = producer.unwrap().kind else {
                    panic!("{call} has no Workflow Producer");
                };
                let at = workflow.attempt.unwrap().started_event_id;
                format!("{call} of {stream} at {at}")
            })
            .collect();
        started.sort();
        started
    }

    /// Gives a running call its result, a failure with `message` if there is one.
    fn finish(calls: &mut StreamCalls, call: CallKey, message: Option<&str>) {
        let failed = message.map(refused);
        let (call_id, seq, outcome) = match call {
            CallKey::Append(seq) => {
                let AppendState::Running(call_id) = calls.appends[&seq].state else {
                    panic!("{call} doesn't run");
                };
                let result = match failed {
                    Some(failure) => stream_append_result::Result::Failure(failure),
                    None => appended(seq),
                };
                (call_id, seq, Outcome::Append(result))
            }
            CallKey::Close(seq) => {
                let CloseState::Running(call_id) = calls.closes[&seq].state else {
                    panic!("{call} doesn't run");
                };
                (call_id, seq, Outcome::Close(failed))
            }
            CallKey::Read(_) => panic!("{call} isn't an append or a close"),
        };
        calls.resolve(StreamResolution {
            call_id,
            seq,
            outcome,
        });
    }

    /// Returns the results of the activation at `activation`, as `append 1` or `close 2: <text>`.
    fn jobs(calls: &mut StreamCalls, activation: u64) -> Vec<String> {
        (calls.take_jobs(activation, None).into_iter())
            .map(|job| match job {
                Variant::ResolveStreamAppend(ResolveStreamAppend {
                    seq,
                    status: Some(resolve_stream_append::Status::Failed(failure)),
                }) => format!("append {seq}: {}", failure.message),
                Variant::ResolveStreamAppend(append) => format!("append {}", append.seq),
                Variant::ResolveStreamClose(ResolveStreamClose {
                    seq,
                    failed: Some(failure),
                }) => format!("close {seq}: {}", failure.message),
                Variant::ResolveStreamClose(close) => format!("close {}", close.seq),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    /// Returns the key of a call that runs, and the id of its Stream.
    fn target(call: &Call) -> (CallKey, &str) {
        match &call.op {
            Op::Append(request) => (CallKey::Append(call.seq), &request.stream_id),
            Op::Close(request) => (CallKey::Close(call.seq), &request.stream_id),
            Op::Read(read) => (CallKey::Read(call.seq), &read.request.stream_id),
        }
    }

    /// Moves the calls that the run starts into `in_flight`, and returns the ones that it stops.
    fn dispatch(seed: u64, calls: &mut StreamCalls, in_flight: &mut Vec<Call>) -> Vec<Call> {
        let mut stopped = vec![];
        for request in calls.take_requests() {
            match request {
                Request::Run(call) => {
                    let request = match &call.op {
                        Op::Append(request) => request.encode_to_vec(),
                        Op::Close(request) => request.encode_to_vec(),
                        Op::Read(read) => read.request.encode_to_vec(),
                    };
                    // A Store gets the id of the call's ref, never its Store name.
                    let store = call.store.as_bytes();
                    let has_store = request.windows(store.len()).any(|part| part == store);
                    assert!(!has_store, "seed {seed}: {call:?}");
                    in_flight.push(call);
                }
                Request::Stop(call_id) => {
                    let at = in_flight.iter().position(|call| call.call_id == call_id);
                    stopped.extend(at.map(|at| in_flight.swap_remove(at)));
                }
            }
        }
        stopped
    }

    /// Returns the Store's answer to `call`, which is `failure` if there is one.
    fn answer(call: &Call, failure: Option<StreamFailure>) -> StreamResolution {
        let outcome = match (&call.op, failure) {
            (Op::Append(_), None) => Outcome::Append(appended(call.seq)),
            (Op::Append(_), Some(failure)) => {
                Outcome::Append(stream_append_result::Result::Failure(failure))
            }
            (Op::Close(_), failure) => Outcome::Close(failure),
            (Op::Read(_), _) => unreachable!("A read returned its page"),
        };
        StreamResolution {
            call_id: call.call_id,
            seq: call.seq,
            outcome,
        }
    }

    #[test]
    fn a_live_call_keeps_the_attempt_of_the_task_in_hand() {
        let mut calls = calls(Phase::Live, 3);
        calls.append(append(1, "s")).unwrap();
        calls.append(append(2, "s")).unwrap();
        calls.close(close(1, "s")).unwrap();
        assert_eq!(started(&mut calls), ["append 1 of s at 3"]);
        // Closes on Streams with long ids fill the marker, so the last intent waits for task 7.
        let long = |seq: u32| format!("{seq:02}{}", "s".repeat(4000));
        for seq in 2..=70 {
            calls.close(close(seq, &long(seq))).unwrap();
        }
        let first = next_task(&mut calls, 7).unwrap();
        let sealed = first.data.close_intents.len() as u32;
        assert!(sealed < 70, "{sealed}");
        calls.marker_recorded().unwrap();
        finish(&mut calls, CallKey::Append(1), None);
        let mut expected = vec!["append 2 of s at 3".to_string()];
        expected.extend((2..=sealed).map(|seq| format!("close {seq} of {} at 3", long(seq))));
        expected.sort();
        assert_eq!(started(&mut calls), expected);
        finish(&mut calls, CallKey::Append(2), None);
        assert_eq!(started(&mut calls), ["close 1 of s at 3"]);
        next_task(&mut calls, 11).unwrap();
        calls.marker_recorded().unwrap();
        let rest = started(&mut calls);
        assert_eq!(rest.len(), 70 - sealed as usize);
        assert!(rest.iter().all(|call| call.ends_with(" at 7")), "{rest:?}");
    }

    #[test]
    fn a_result_waits_with_its_call_until_an_activation_takes_it() {
        let mut live = calls(Phase::Live, 3);
        live.append(append(1, "s")).unwrap();
        live.append(append(2, "t")).unwrap();
        started(&mut live);
        finish(&mut live, CallKey::Append(2), None);
        finish(&mut live, CallKey::Append(1), None);
        live.set_task(TaskState::Completing(task_attempt(3)));
        assert!(!live.has_jobs(4, None));
        assert_eq!(jobs(&mut live, 4), Vec::<String>::new());
        let marker = next_task(&mut live, 7);
        assert_eq!(marker, None);
        assert!(live.has_jobs(0, None));
        assert_eq!(jobs(&mut live, 0), ["append 1", "append 2"]);
        live.append(append(3, "s")).unwrap();
        started(&mut live);
        finish(&mut live, CallKey::Append(3), None);
        assert_eq!(jobs(&mut live, 2), ["append 3"]);
        let marker = next_task(&mut live, 11).unwrap();
        let entries: Vec<_> = (marker.data.appends.iter())
            .map(|append| (append.seq, append.activation_index))
            .collect();
        assert_eq!(entries, [(1, 0), (2, 0), (3, 2)]);
    }

    #[test]
    fn a_release_ends_with_its_activation() {
        let marker = history(|marker| assert!(marker.append_result(&append_done(1, "1"), 2)));
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "1")).unwrap();
        replay.peeked(marker, vec![]).unwrap();
        replay.release_ready();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        // A result of the next task waits for its own activation.
        replay.append(append(2, "2")).unwrap();
        let next = history(|marker| assert!(marker.append_result(&append_done(2, "2"), 1)));
        replay.peeked(next, vec![]).unwrap();
        assert!(!replay.has_jobs(0, None));
    }

    #[test]
    fn a_result_waits_behind_a_read_that_workflow_code_has_not_sent() {
        // The first run gave append 1 in activation 1, read 1 in 2 and append 2 in 3, and
        // Workflow code sent the read at the result of append 1.
        let marker = history(|marker| {
            assert!(marker.append_result(&append_done(1, "1"), 1));
            marker.read_result(ReadDone::cancelled(&stream("r"), 1, vec![]), 2);
            assert!(marker.append_result(&append_done(2, "2"), 3));
        });
        let mut replay = calls(Phase::Replaying, 9);
        for seq in [1, 2] {
            replay.append(append(seq, &seq.to_string())).unwrap();
        }
        replay.peeked(marker, vec![]).unwrap();
        replay.release_ready();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
    }

    #[test]
    fn two_appends_have_one_request_id_exactly_when_their_run_attempt_and_seq_are_equal() {
        let runs = [
            "0193c2e4-7f3a-7c1d-9b2e-1a2b3c4d5e6f",
            "0193c2e4-7f3a-7c1d-9b2e-1a2b3c4d5e70",
        ];
        let mut equal = 0;
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            // Few values for each field, so that some appends are equal.
            let appends: Vec<(&str, WorkflowTaskAttempt, u32)> = (0..8)
                .map(|_| {
                    let started_time = rng.random_bool(0.8).then(|| prost_types::Timestamp {
                        seconds: 1_763_600_000 + rng.random_range(0..2),
                        nanos: [0, 999_999_999][rng.random_range(0..2)],
                    });
                    let attempt = WorkflowTaskAttempt {
                        started_event_id: [3, 31, 313][rng.random_range(0..3)],
                        attempt: [1, 13, 131][rng.random_range(0..3)],
                        started_time,
                    };
                    let seq = [1, 11, 111][rng.random_range(0..3)];
                    (runs[rng.random_range(0..2)], attempt, seq)
                })
                .collect();
            let id = |(run, attempt, seq): &(&str, WorkflowTaskAttempt, u32)| {
                let of_run = WorkflowProducer {
                    run_id: run.to_string(),
                    ..Default::default()
                };
                request_id(run, &producer(&of_run, *attempt), *seq)
            };
            for a in &appends {
                for b in &appends {
                    assert_eq!(id(a) == id(b), a == b, "seed {seed}: {a:?} and {b:?}");
                    equal += usize::from(a == b && !std::ptr::eq(a, b));
                }
            }
        }
        assert!(equal > 0, "No two appends were equal");
    }

    /// A command of Workflow code in a generated script.
    #[derive(Clone, Debug)]
    enum Command {
        Append(AppendToStream),
        Close(CloseStream),
        Read(ReadStream),
        CancelRead(RequestCancelStreamRead),
        /// A command that ends the Workflow.
        Terminal,
    }

    /// One activation, as the first run gave it.
    #[derive(Clone, Debug)]
    struct Activation {
        index: u64,
        jobs: Vec<Variant>,
        commands: Vec<Command>,
    }

    /// A Workflow Task that History has, with what the first run did in it.
    #[derive(Clone, Debug, Default)]
    struct Task {
        /// True if the events of the task give Workflow code a job, so its first activation comes
        /// even with no Stream result.
        has_events: bool,
        activations: Vec<Activation>,
        marker: Option<StreamMarker>,
        /// True if the last command of the Workflow went out with this task.
        ends_workflow: bool,
    }

    /// The jobs of each activation of one task, with the index of the activation.
    type TaskJobs = Vec<(u64, Vec<Variant>)>;

    impl Task {
        fn jobs(&self) -> TaskJobs {
            (self.activations.iter())
                .map(|activation| (activation.index, activation.jobs.clone()))
                .collect()
        }

        fn commands_at(&self, index: u64) -> Vec<Command> {
            (self.activations.iter())
                .find(|activation| activation.index == index)
                .map_or(vec![], |activation| activation.commands.clone())
        }
    }

    /// A run that a generated test drives as the machines of Core do.
    struct Run {
        calls: StreamCalls,
        /// The index of the next activation of the task in hand.
        activation: u64,
        /// True once the last command of the Workflow came.
        ended: bool,
    }

    impl Run {
        /// Gives Workflow code the jobs of the next activation.
        fn take_jobs(&mut self) -> Vec<Variant> {
            let jobs = self.calls.take_jobs(self.activation, None);
            self.activation += 1;
            jobs
        }

        /// Takes the commands of the activation that Workflow code has. With `heartbeat`, the
        /// heartbeat of that task came while Workflow code had the activation.
        fn send(
            &mut self,
            commands: &[Command],
            heartbeat: Option<WorkflowTaskAttempt>,
        ) -> Result<(), WFMachinesError> {
            if let Some(task) = heartbeat {
                self.calls.set_task(TaskState::Completing(task));
            }
            for command in commands {
                match command {
                    Command::Append(append) => self.calls.append(append.clone())?,
                    Command::Close(close) => self.calls.close(close.clone())?,
                    Command::Read(read) => self.calls.read(read.clone())?,
                    Command::CancelRead(cancel) => self.calls.cancel_read(*cancel)?,
                    Command::Terminal => {
                        self.calls.stop_for_terminal();
                        self.ended = true;
                    }
                }
            }
            Ok(())
        }

        /// Returns the calls that the run still tracks.
        fn tracked(&self) -> BTreeSet<CallKey> {
            (self.calls.appends.keys().map(|seq| CallKey::Append(*seq)))
                .chain(self.calls.closes.keys().map(|seq| CallKey::Close(*seq)))
                .chain(self.calls.reads.keys().map(|seq| CallKey::Read(*seq)))
                .collect()
        }
    }

    /// A Stream that the reads of a generated script read, as its Store has it.
    struct ModelStream {
        records: Vec<StreamRecord>,
        closed: bool,
    }

    type Streams = BTreeMap<String, ModelStream>;

    /// Returns the Records of `stream` after `offset`.
    fn after<'a>(stream: &'a ModelStream, offset: &[u8]) -> &'a [StreamRecord] {
        let from = (stream.records.iter()).position(|record| record.offset.as_slice() > offset);
        &stream.records[from.unwrap_or(stream.records.len())..]
    }

    /// The reads of `history` that keep their Records in History.
    fn in_history(history: &[Task]) -> BTreeSet<u32> {
        (history.iter())
            .flat_map(|task| &task.activations)
            .flat_map(|activation| &activation.commands)
            .filter_map(|command| match command {
                Command::Read(read) if read.records_in_history => Some(read.seq),
                _ => None,
            })
            .collect()
    }

    /// Returns the page of a read entry, if its result is one.
    fn page_of(entry: &StreamReadResult) -> Option<&StreamReadPage> {
        match &entry.result {
            Some(stream_read_result::Result::Page(page)) => Some(page),
            _ => None,
        }
    }

    /// Returns what the Store gives a replay for the read entries of the marker of task `task`.
    /// `gone` lists the entries whose Records the Store lost, with the message of each.
    fn store_reads(
        task: usize,
        marker: &StreamMarkerData,
        streams: &Streams,
        in_history: &BTreeSet<u32>,
        gone: &dyn Fn(usize, usize) -> Option<String>,
    ) -> MarkerReads {
        (marker.reads.iter().enumerate())
            .map(|(index, entry)| {
                let count = page_of(entry)?.record_count as usize;
                if in_history.contains(&entry.seq) || count == 0 {
                    return None;
                }
                if let Some(message) = gone(task, index) {
                    return Some(Err(ReplayError::Gone(message)));
                }
                let stream = &streams[&entry.stream_id];
                let records = after(stream, &entry.from_offset).iter().take(count);
                Some(Ok(records.cloned().collect()))
            })
            .collect()
    }

    /// What a replay gives.
    struct Replayed {
        jobs: Vec<TaskJobs>,
        /// The task in which the last command of the Workflow went out.
        sent_in: Option<usize>,
        run: Run,
    }

    /// Replays `history` with the commands of the first run of `seed`, and goes live in the task
    /// `live`, as a new run of Core does. The Store has the Records of `streams`, but for the read
    /// entries that `gone` lists.
    fn replay(
        seed: u64,
        history: &[Task],
        live: WorkflowTaskAttempt,
        (streams, runtimes): (&Streams, &StreamRuntimes),
        gone: &dyn Fn(usize, usize) -> Option<String>,
    ) -> Result<Replayed, WFMachinesError> {
        let in_history = in_history(history);
        let calls = StreamCalls::new("n", "w", "r", runtimes.clone(), Phase::Replaying);
        let mut run = Run {
            calls,
            activation: 0,
            ended: false,
        };
        run.calls.set_task(TaskState::Open(live));
        let mut replayed = vec![];
        let mut sent_in = None;
        for (index, task) in history.iter().enumerate() {
            if index > 0 && history[index - 1].marker.is_some() {
                run.calls.marker_recorded()?;
            }
            // Core peeks the marker that the task's completion recorded before it activates.
            if let Some(marker) = &task.marker {
                let reads = store_reads(index, &marker.data, streams, &in_history, gone);
                run.calls.peeked(marker.clone(), reads)?;
            }
            run.activation = 0;
            let mut activations = vec![];
            let mut first = task.has_events;
            while !run.ended {
                if !first && !run.calls.has_jobs(run.activation, None) {
                    // For a History with chunking off, Core sets its activation index to that of
                    // the next result from History when nothing else needs an activation.
                    match run.calls.next_ready_activation() {
                        Some(at) if at > run.activation => run.activation = at,
                        _ => break,
                    }
                    continue;
                }
                first = false;
                let at = run.activation;
                let jobs = run.take_jobs();
                run.send(&task.commands_at(at), None)?;
                activations.push((at, jobs));
            }
            if run.ended && sent_in.is_none() {
                sent_in = Some(index);
            }
            let requests = run.calls.take_requests();
            assert!(requests.is_empty(), "seed {seed}: {requests:?}");
            replayed.push(activations);
        }
        if history.last().is_some_and(|task| task.marker.is_some()) {
            run.calls.marker_recorded()?;
        }
        run.activation = 0;
        run.calls.go_live()?;
        Ok(Replayed {
            jobs: replayed,
            sent_in,
            run,
        })
    }

    /// The things that a generated script reached, over all seeds.
    #[derive(Default)]
    struct Seen {
        full_markers: usize,
        heartbeats: usize,
        failed_tasks: usize,
        /// The last commands that came while a call had no result in a marker.
        dropped: usize,
        /// The commands with a Stream id, a Store name, an offset, a wait or a page size out of
        /// range.
        invalid: usize,
        bad_seqs: usize,
        pages: usize,
        failed_reads: usize,
        replay_errors: usize,
        /// Tasks that completed with a page that didn't fit, which the next task gave.
        full_rooms: usize,
        /// The cancels of a read that ran, of one whose page or failure waited for an activation,
        /// and of one that had its job.
        cancels: [usize; 3],
        /// The reads that a replay cancelled with no result in History, which got their job once
        /// the replay was over.
        cancelled_at_go_live: usize,
        /// The reads with a cancel that a replay gave the result of History.
        cancelled_in_history: usize,
        /// The results of calls that Core had stopped.
        late_results: usize,
    }

    /// The state as of the last task in History, which a failed task goes back to.
    #[derive(Clone, Default)]
    struct Committed {
        seqs: (u32, u32, u32),
        unresolved: BTreeSet<CallKey>,
        cancelled: BTreeSet<u32>,
    }

    /// The first run of a generated script. It writes History, and checks a replay of it at each
    /// failed task and at the end.
    struct FirstRun<'a> {
        seed: u64,
        rng: StdRng,
        seen: &'a mut Seen,
        run: Run,
        history: Vec<Task>,
        task: Task,
        task_attempt: WorkflowTaskAttempt,
        /// The Store calls in flight, which the Store answers in any order.
        in_flight: Vec<Call>,
        streams: Streams,
        runtimes: StreamRuntimes,
        /// Results that came while no task was in hand. Core applies them once the next task's
        /// events are applied.
        deferred: Vec<StreamResolution>,
        last_append: u32,
        last_close: u32,
        last_read: u32,
        /// The calls whose result no marker has.
        unresolved: BTreeSet<CallKey>,
        /// The reads whose job Workflow code got in the task in hand.
        got: BTreeSet<u32>,
        /// The reads that Workflow code cancelled before it got their job. Each of them gets a job
        /// that says so, and no other read does.
        cancelled: BTreeSet<u32>,
        /// The state as of the last task in History, which a failed task goes back to.
        committed: Committed,
    }

    impl<'a> FirstRun<'a> {
        fn new(seed: u64, seen: &'a mut Seen) -> Self {
            let mut rng = StdRng::seed_from_u64(seed);
            // In some seeds, small bounds of Records make a page wait for the next task.
            let mut runtimes = runtimes(&["mem"]);
            if rng.random_bool(0.3) {
                runtimes.settings.bounds.max_task_record_bytes = rng.random_range(1..=400);
                runtimes.settings.bounds.max_marker_record_bytes = rng.random_range(1..=100);
            }
            let run = Run {
                calls: StreamCalls::new("n", "w", "r", runtimes.clone(), Phase::Live),
                activation: 0,
                ended: false,
            };
            let streams = (0..2)
                .map(|index| {
                    let records = (1..=rng.random_range(0u8..30))
                        .map(|at| StreamRecord {
                            offset: vec![at * 3],
                            body: Some(Payload {
                                data: vec![index, at],
                                ..Default::default()
                            }),
                            producer: None,
                        })
                        .collect();
                    let closed = rng.random_bool(0.5);
                    (format!("r{index}"), ModelStream { records, closed })
                })
                .collect();
            Self {
                seed,
                rng,
                seen,
                run,
                history: vec![],
                task: Task::default(),
                task_attempt: task_attempt(3),
                in_flight: vec![],
                streams,
                runtimes,
                deferred: vec![],
                last_append: 0,
                last_close: 0,
                last_read: 0,
                unresolved: BTreeSet::new(),
                got: BTreeSet::new(),
                cancelled: BTreeSet::new(),
                committed: Committed::default(),
            }
        }

        /// Returns the Streams of the Store, and the runtimes of the Worker.
        fn store(&self) -> (&Streams, &StreamRuntimes) {
            (&self.streams, &self.runtimes)
        }

        /// Runs the script for up to `tasks` tasks, or until the Workflow ends.
        fn play(&mut self, tasks: usize) {
            let mut started = false;
            for _ in 0..tasks {
                if !started {
                    self.start_task();
                }
                match self.run_task() {
                    Ok(()) => {
                        if self.history.last().unwrap().ends_workflow {
                            return;
                        }
                        self.between_tasks();
                        self.task_attempt = WorkflowTaskAttempt {
                            started_event_id: self.task_attempt.started_event_id + 4,
                            attempt: 1,
                            started_time: None,
                        };
                        started = false;
                    }
                    Err(()) => {
                        self.retry();
                        started = true;
                    }
                }
            }
        }

        fn start_task(&mut self) {
            self.run.calls.set_task(TaskState::Open(self.task_attempt));
            if self
                .history
                .last()
                .is_some_and(|task| task.marker.is_some())
            {
                self.run.calls.marker_recorded().unwrap();
            }
            self.run.activation = 0;
            for resolution in std::mem::take(&mut self.deferred) {
                self.run.calls.resolve(resolution);
            }
            self.dispatch();
        }

        /// Runs the task in hand until it completes, and returns an error if it fails.
        fn run_task(&mut self) -> Result<(), ()> {
            self.task.has_events = self.history.is_empty() || self.rng.random_bool(0.6);
            if self.task.has_events || self.run.calls.has_jobs(self.run.activation, None) {
                self.activate()?;
            }
            loop {
                if self.run.ended {
                    self.complete();
                    return Ok(());
                }
                if self.run.calls.has_jobs(self.run.activation, None) {
                    self.activate()?;
                    continue;
                }
                if self.run.calls.must_complete(self.run.activation) {
                    self.seen.full_markers += 1;
                    let waits = self.run.calls.done.first();
                    self.seen.full_rooms += usize::from(matches!(waits, Some(CallKey::Read(_))));
                    self.complete();
                    return Ok(());
                }
                // The heartbeat came while the last activation was out.
                if !self.run.calls.task.is_open() {
                    self.complete();
                    return Ok(());
                }
                if self.run.calls.outstanding() == 0 {
                    self.complete();
                    return Ok(());
                }
                let seed = self.seed;
                assert!(
                    !self.in_flight.is_empty(),
                    "seed {seed}: a call waits, and none runs"
                );
                match self.rng.random_range(0..40) {
                    // A few answers can come before Core gives the next activation.
                    0..=35 => {
                        for _ in 0..self.rng.random_range(1..=8) {
                            if let Some(resolution) = self.step() {
                                self.run.calls.resolve(resolution);
                                self.dispatch();
                            }
                        }
                    }
                    36..=38 => {
                        // The heartbeat completes the task. Results that come before the
                        // completion wait for the next task.
                        self.seen.heartbeats += 1;
                        self.run
                            .calls
                            .set_task(TaskState::Completing(self.task_attempt));
                        for _ in 0..self.rng.random_range(0..=2) {
                            if let Some(resolution) = self.step() {
                                self.run.calls.resolve(resolution);
                                self.dispatch();
                            }
                        }
                        self.complete();
                        return Ok(());
                    }
                    _ => return Err(()),
                }
            }
        }

        /// Lets the Store answer a random call in flight, and returns the result if one runs.
        fn step(&mut self) -> Option<StreamResolution> {
            let running = self.in_flight.len();
            let at = (running > 0).then(|| self.rng.random_range(0..running))?;
            let call = self.in_flight.swap_remove(at);
            Some(self.answer(call))
        }

        /// Gives Workflow code an activation, and takes the commands that the script makes up.
        fn activate(&mut self) -> Result<(), ()> {
            let index = self.run.activation;
            let jobs = self.run.take_jobs();
            for job in &jobs {
                if let Variant::ResolveStreamRead(read) = job {
                    let status = read.status.as_ref().unwrap();
                    let is_cancelled = matches!(status, resolve_stream_read::Status::Cancelled(()));
                    let expected = self.cancelled.contains(&read.seq);
                    assert_eq!(is_cancelled, expected, "seed {}: {read:?}", self.seed);
                    self.got.insert(read.seq);
                }
            }
            let (commands, fatal) = self.commands();
            // The heartbeat comes more often with a cancel, whose job then waits for the next task.
            let cancels = |command: &Command| matches!(command, Command::CancelRead(_));
            let often = if commands.iter().any(cancels) {
                0.4
            } else {
                0.05
            };
            let heartbeat = (self.rng.random_bool(often)).then_some(self.task_attempt);
            self.seen.heartbeats += usize::from(heartbeat.is_some());
            // A cancel ends each read whose job Workflow code doesn't have yet.
            let mut cancelled = self.cancelled.clone();
            let mut open: BTreeSet<CallKey> = self.unresolved.clone();
            for command in &commands {
                match command {
                    Command::Read(read) => {
                        open.insert(CallKey::Read(read.seq));
                    }
                    Command::CancelRead(cancel) => {
                        let seq = cancel.seq;
                        let had_job =
                            !open.contains(&CallKey::Read(seq)) || self.got.contains(&seq);
                        let read = self.run.calls.reads.get(&seq);
                        let state = match read.map(|read| &read.state) {
                            _ if had_job => 2,
                            Some(ReadState::Done(_)) if !cancelled.contains(&seq) => 1,
                            _ => 0,
                        };
                        self.seen.cancels[state] += 1;
                        if !had_job {
                            cancelled.insert(seq);
                        }
                    }
                    _ => {}
                }
            }
            let result = self.run.send(&commands, heartbeat);
            if let Some(expected) = fatal {
                assert_eq!(result, Err(expected), "seed {}", self.seed);
                return Err(());
            }
            self.cancelled = cancelled;
            result.unwrap_or_else(|error| panic!("seed {}: {error:?}", self.seed));
            for command in &commands {
                match command {
                    Command::Append(append) => {
                        self.unresolved.insert(CallKey::Append(append.seq));
                    }
                    Command::Close(close) => {
                        self.unresolved.insert(CallKey::Close(close.seq));
                    }
                    Command::Read(read) => {
                        self.unresolved.insert(CallKey::Read(read.seq));
                    }
                    Command::CancelRead(_) => {}
                    Command::Terminal => {
                        self.seen.dropped += usize::from(!self.unresolved.is_empty());
                        self.unresolved.clear();
                    }
                }
            }
            self.task.activations.push(Activation {
                index,
                jobs,
                commands,
            });
            self.dispatch();
            Ok(())
        }

        /// Returns the commands of an activation. If the last one isn't valid, it comes with the
        /// error that fails the Workflow Task.
        fn commands(&mut self) -> (Vec<Command>, Option<WFMachinesError>) {
            let mut commands = vec![];
            // Workflow code often cancels a read whose result waits for room in the task.
            let waits = (self.run.calls.done.iter()).find_map(|call| match call {
                CallKey::Read(seq) => Some(*seq),
                _ => None,
            });
            if let Some(seq) = waits.filter(|_| self.rng.random_bool(0.3)) {
                commands.push(Command::CancelRead(RequestCancelStreamRead { seq }));
            }
            for _ in 0..self.rng.random_range(0..=3) {
                let id = format!("s{}", self.rng.random_range(0..3));
                match self.rng.random_range(0..100) {
                    0..=37 => {
                        self.last_append += 1;
                        commands.push(Command::Append(append(self.last_append, &id)));
                    }
                    38..=51 => {
                        // A read from the start of its Stream, or from one of its Records.
                        self.last_read += 1;
                        let id = format!("r{}", self.rng.random_range(0..2));
                        let records = &self.streams[&id].records;
                        let from_offset = match self.rng.random_range(0..=records.len()) {
                            0 => vec![],
                            at => records[at - 1].offset.clone(),
                        };
                        let max_records = match self.rng.random_bool(0.5) {
                            true => 0,
                            false => self.rng.random_range(1..=8),
                        };
                        commands.push(Command::Read(ReadStream {
                            seq: self.last_read,
                            stream: Some(stream(&id)),
                            from_offset,
                            wait: None,
                            records_in_history: self.rng.random_bool(0.5),
                            max_records,
                            max_bytes: 0,
                        }));
                    }
                    52..=66 => {
                        self.last_close += 1;
                        commands.push(Command::Close(close(self.last_close, &id)));
                    }
                    // The cancel of a read, which most often has no job yet.
                    67..=71 => {
                        let open: Vec<u32> = (self.unresolved.iter())
                            .filter_map(|call| match call {
                                CallKey::Read(seq) => Some(*seq),
                                _ => None,
                            })
                            .collect();
                        let seq = match open.is_empty() || self.rng.random_bool(0.2) {
                            true if self.last_read == 0 => continue,
                            true => self.rng.random_range(1..=self.last_read),
                            false => open[self.rng.random_range(0..open.len())],
                        };
                        commands.push(Command::CancelRead(RequestCancelStreamRead { seq }));
                    }
                    72..=74 => {
                        self.seen.invalid += 1;
                        let long = "x".repeat(4097);
                        let (command, error) = match self.rng.random_range(0..6) {
                            5 => {
                                // A negative maximum of the page.
                                let below = self.rng.random_range(i32::MIN..0);
                                let (field, max_records, max_bytes) =
                                    match self.rng.random_bool(0.5) {
                                        true => ("max_records", below, 0),
                                        false => ("max_bytes", 0, i64::from(below)),
                                    };
                                self.last_read += 1;
                                let seq = self.last_read;
                                let read = ReadStream {
                                    seq,
                                    stream: Some(stream("r0")),
                                    max_records,
                                    max_bytes,
                                    ..Default::default()
                                };
                                let error = format!(
                                    "Stream read {seq} is invalid: The `{field}` of the read is \
                                     {below}, which is negative"
                                );
                                (Command::Read(read), error)
                            }
                            3 => {
                                self.last_read += 1;
                                let seq = self.last_read;
                                let read = ReadStream {
                                    seq,
                                    stream: Some(stream("r0")),
                                    from_offset: vec![0; 1025],
                                    ..Default::default()
                                };
                                let error = format!(
                                    "Stream read {seq} is invalid: The offset has 1025 bytes, \
                                     more than 1024"
                                );
                                (Command::Read(read), error)
                            }
                            4 => {
                                // A negative wait, or one with its nanoseconds out of range.
                                let (seconds, nanos) = match self.rng.random_bool(0.5) {
                                    true => (self.rng.random_range(i64::MIN..0), 0),
                                    false => match self.rng.random_bool(0.5) {
                                        true => {
                                            (0, self.rng.random_range(1_000_000_000..=i32::MAX))
                                        }
                                        false => (0, self.rng.random_range(i32::MIN..0)),
                                    },
                                };
                                self.last_read += 1;
                                let seq = self.last_read;
                                let read = ReadStream {
                                    seq,
                                    stream: Some(stream("r0")),
                                    wait: Some(prost_types::Duration { seconds, nanos }),
                                    ..Default::default()
                                };
                                let error = format!(
                                    "Stream read {seq} is invalid: The wait of the read has \
                                     {seconds} seconds and {nanos} nanoseconds, which is negative \
                                     or has its nanoseconds out of range"
                                );
                                (Command::Read(read), error)
                            }
                            0 => {
                                self.last_append += 1;
                                let seq = self.last_append;
                                let error = format!(
                                    "Stream append {seq} is invalid: The Stream id has 4097 \
                                     bytes, more than 4096"
                                );
                                (Command::Append(append(seq, &long)), error)
                            }
                            1 => {
                                self.last_append += 1;
                                let seq = self.last_append;
                                let error = format!(
                                    "Stream append {seq} is invalid: The Stream id is empty"
                                );
                                (Command::Append(append(seq, "")), error)
                            }
                            _ => {
                                self.last_close += 1;
                                let seq = self.last_close;
                                let mut long_store = close(seq, &id);
                                long_store.stream.as_mut().unwrap().store = long;
                                let error = format!(
                                    "Stream close {seq} is invalid: The Store name has 4097 \
                                     bytes, more than 4096"
                                );
                                (Command::Close(long_store), error)
                            }
                        };
                        commands.push(command);
                        return (commands, Some(WFMachinesError::Fatal(error)));
                    }
                    80..=82 => {
                        self.seen.bad_seqs += 1;
                        let error = |kind: &str, seq: u32| match seq {
                            0 => format!("Stream {kind} has seq 0, and a seq starts at 1"),
                            _ => format!("Stream {kind} {seq} came after {kind} {seq}"),
                        };
                        let (command, error) = match self.rng.random_range(0..4) {
                            0 => {
                                let seq = self.last_append;
                                (Command::Append(append(seq, &id)), error("append", seq))
                            }
                            1 => {
                                let seq = self.last_close;
                                (Command::Close(close(seq, &id)), error("close", seq))
                            }
                            2 => {
                                let seq = self.last_read;
                                let read = ReadStream {
                                    seq,
                                    stream: Some(stream("r0")),
                                    ..Default::default()
                                };
                                (Command::Read(read), error("read", seq))
                            }
                            // The cancel of a read that Workflow code never issued.
                            _ => {
                                let seq = self.last_read + 1;
                                let cancel = RequestCancelStreamRead { seq };
                                let error = WFMachinesError::Nondeterminism(format!(
                                    "Workflow code cancelled Stream read {seq}, which it never \
                                     issued"
                                ));
                                commands.push(Command::CancelRead(cancel));
                                return (commands, Some(error));
                            }
                        };
                        commands.push(command);
                        return (commands, Some(WFMachinesError::Fatal(error)));
                    }
                    83..=86 => {
                        commands.push(Command::Terminal);
                        return (commands, None);
                    }
                    87..=89 => {
                        // A burst of appends, each on a Stream of its own, can fill a marker.
                        for _ in 0..self.rng.random_range(40..=90) {
                            self.last_append += 1;
                            let long = "x".repeat(self.rng.random_range(0..3000));
                            let id = format!("b{}{long}", self.last_append);
                            commands.push(Command::Append(append(self.last_append, &id)));
                        }
                    }
                    _ => {}
                }
            }
            (commands, None)
        }

        /// Returns the Store's answer to `call`, which was in flight.
        fn answer(&mut self, call: Call) -> StreamResolution {
            let failure = match &call.op {
                Op::Append(request) => {
                    // A large failure fills the marker in a few dozen results.
                    let burst = request.stream_id.starts_with('b');
                    let large = if burst { 8 } else { 1 };
                    match self.rng.random_range(0..10) {
                        at if at < large => Some(refused("x".repeat(4000))),
                        at if at < large + 2 => Some(refused("refused")),
                        _ => None,
                    }
                }
                Op::Close(_) => self.rng.random_bool(0.3).then(|| refused("refused")),
                // A page of any number of the Records that the read asked for, or one larger
                // Record. A page with none says that no Record came within the wait of the read.
                Op::Read(read) => {
                    let request = &read.request;
                    let stream = &self.streams[&request.stream_id];
                    let left = after(stream, &request.from_offset);
                    let page = if self.rng.random_bool(0.03) {
                        self.seen.failed_reads += 1;
                        Err(refused(format!("Read {} failed", call.seq)))
                    } else {
                        let most = left.len().min(request.max_records as usize);
                        let mut bytes = 0;
                        let fitting = (left[..self.rng.random_range(0..=most)].iter())
                            .take_while(|record| {
                                bytes += field_len(record.encoded_len());
                                bytes <= request.max_bytes as usize
                            })
                            .count();
                        let count = fitting.max(most.min(1));
                        self.seen.pages += 1;
                        Ok(ReadRecordsResponse {
                            records: left[..count].to_vec(),
                            done: stream.closed && count == left.len(),
                            ..Default::default()
                        })
                    };
                    let of = self::stream(&request.stream_id);
                    let from_offset = request.from_offset.clone();
                    let in_history = read.records_in_history;
                    let done = ReadDone::new(&of, call.seq, from_offset, page, in_history);
                    return StreamResolution {
                        call_id: call.call_id,
                        seq: call.seq,
                        outcome: Outcome::Read(done),
                    };
                }
            };
            answer(&call, failure)
        }

        fn dispatch(&mut self) {
            let stopped = dispatch(self.seed, &mut self.run.calls, &mut self.in_flight);
            // The Store can answer a call while Core stops it, and Core still gets that result.
            for call in stopped {
                let late = self.answer(call);
                if self.rng.random_bool(0.5) {
                    self.seen.late_results += 1;
                    self.run.calls.resolve(late);
                }
            }
        }

        /// Completes the task in hand, and checks that the run tracks exactly the calls whose
        /// result no marker has.
        fn complete(&mut self) {
            let marker = self.run.calls.take_marker();
            self.run.calls.set_task(TaskState::NoTask);
            for entry in marker.iter().map(|marker| &marker.data) {
                for append in &entry.appends {
                    self.unresolved.remove(&CallKey::Append(append.seq));
                }
                for close in &entry.closes {
                    self.unresolved.remove(&CallKey::Close(close.seq));
                }
                for read in &entry.reads {
                    self.unresolved.remove(&CallKey::Read(read.seq));
                }
            }
            let mut task = std::mem::take(&mut self.task);
            task.marker = marker;
            task.ends_workflow = self.run.ended;
            self.history.push(task);
            self.committed = Committed {
                seqs: (self.last_append, self.last_close, self.last_read),
                unresolved: self.unresolved.clone(),
                cancelled: self.cancelled.clone(),
            };
            self.got.clear();
            let seed = self.seed;
            assert_eq!(self.run.tracked(), self.unresolved, "seed {seed}");
            // A cancelled read gets its job. Until a marker has it, the job waits with the read.
            let no_job = (self.cancelled.iter().map(|seq| CallKey::Read(*seq)))
                .find(|read| self.unresolved.contains(read) && !self.run.calls.done.contains(read));
            assert_eq!(no_job, None, "seed {seed}: a cancelled read gets no job");
            let running = self.in_flight.len();
            assert!(
                !self.run.ended || running == 0,
                "seed {seed}: {running} calls run"
            );
            assert!(self.run.calls.ready.is_empty(), "seed {seed}");
            let queued = self.run.calls.queues.values().flatten();
            let waiting = self.run.calls.done.iter().chain(queued);
            assert!(
                waiting
                    .into_iter()
                    .all(|call| self.unresolved.contains(call)),
                "seed {seed}"
            );
        }

        /// Lets the Store answer while no task is in hand. Core applies these results once the
        /// next task's events are applied.
        fn between_tasks(&mut self) {
            for _ in 0..self.rng.random_range(0..=2) {
                if let Some(resolution) = self.step() {
                    self.deferred.push(resolution);
                }
            }
        }

        /// Fails the task in hand. A new run replays History and goes live in the next attempt.
        fn retry(&mut self) {
            self.seen.failed_tasks += 1;
            self.run.calls.stop();
            self.in_flight.clear();
            self.deferred.clear();
            self.got.clear();
            let committed = self.committed.clone();
            (self.last_append, self.last_close, self.last_read) = committed.seqs;
            self.unresolved = committed.unresolved;
            self.cancelled = committed.cancelled;
            self.task = Task::default();
            self.task_attempt.attempt += 1;
            self.run = self.check_replay(self.task_attempt);
            self.dispatch();
        }

        /// Replays History, checks it against the first run, and returns the replayed run.
        fn check_replay(&mut self, live: WorkflowTaskAttempt) -> Run {
            let seed = self.seed;
            let Replayed {
                jobs: replayed,
                sent_in,
                mut run,
            } = replay(seed, &self.history, live, self.store(), &|_, _| None)
                .unwrap_or_else(|error| panic!("seed {seed}: {error:?}"));
            let first: Vec<_> = self.history.iter().map(Task::jobs).collect();
            assert_eq!(replayed, first, "seed {seed}");
            let ended = self.history.iter().position(|task| task.ends_workflow);
            assert_eq!(sent_in, ended, "seed {seed}");
            // The replay resumes each call whose result no marker has, and only those.
            assert_eq!(run.tracked(), self.unresolved, "seed {seed}");
            let recorded: BTreeSet<CallKey> = (self.history.iter())
                .filter_map(|task| Some(&task.marker.as_ref()?.data))
                .flat_map(|marker| {
                    (marker
                        .appends
                        .iter()
                        .map(|append| CallKey::Append(append.seq)))
                    .chain(marker.closes.iter().map(|close| CallKey::Close(close.seq)))
                })
                .collect();
            let reads = self.reads();
            // A read with a cancel and no result in History has the job of its cancel once the
            // replay is over, also if the Store gave an answer that no marker has.
            let cancels: BTreeSet<u32> = (self.history.iter())
                .flat_map(|task| &task.activations)
                .flat_map(|activation| &activation.commands)
                .filter_map(|command| match command {
                    Command::CancelRead(cancel) => Some(cancel.seq),
                    _ => None,
                })
                .collect();
            let open = run.calls.reads.iter();
            let with_job = open.filter(|(_, read)| matches!(read.state, ReadState::Done(_)));
            let with_job: BTreeSet<u32> = with_job.map(|(seq, _)| *seq).collect();
            let cancelled: BTreeSet<u32> = (run.calls.reads.keys().copied())
                .filter(|seq| cancels.contains(seq))
                .collect();
            assert_eq!(with_job, cancelled, "seed {seed}");
            self.seen.cancelled_at_go_live += with_job.len();
            self.cancelled.extend(with_job);
            let in_history = cancels.iter().filter(|seq| reads[seq].job.is_some());
            self.seen.cancelled_in_history += in_history.count();
            for request in run.calls.take_requests() {
                let Request::Run(Call { seq, op, .. }) = &request else {
                    panic!("seed {seed}: {request:?}");
                };
                let (key, sent) = match op {
                    Op::Append(append) => (CallKey::Append(*seq), &append.producer),
                    Op::Close(close) => (CallKey::Close(*seq), &close.producer),
                    Op::Read(read) => {
                        // A read with no result in History runs again as its command asked.
                        let got = &reads[seq];
                        assert!(got.job.is_none(), "seed {seed}: read {seq} ran again");
                        let from = &read.request.from_offset;
                        assert_eq!(*from, got.from_offset, "seed {seed}: read {seq}");
                        run.calls.requests.push(request);
                        continue;
                    }
                };
                assert!(!recorded.contains(&key), "seed {seed}: {key} ran again");
                let expected = producer(&run.calls.producer, live);
                assert_eq!(*sent, Some(expected), "seed {seed}");
                run.calls.requests.push(request);
            }
            run
        }

        /// Returns what Workflow code got of each read of History, and checks that each read got
        /// at most one job, whose page has the next Records of the read's Stream.
        fn reads(&self) -> BTreeMap<u32, ReadSeen> {
            let seed = self.seed;
            let mut reads = BTreeMap::new();
            let activations = (self.history.iter()).flat_map(|task| &task.activations);
            for activation in activations {
                for job in &activation.jobs {
                    let Variant::ResolveStreamRead(job) = job else {
                        continue;
                    };
                    let read: &mut ReadSeen = reads.entry(job.seq).or_default();
                    let second = read.job.replace(job.status.clone().unwrap());
                    assert!(
                        second.is_none(),
                        "seed {seed}: read {} got two jobs",
                        job.seq
                    );
                }
                for command in &activation.commands {
                    if let Command::Read(command) = command {
                        let read: &mut ReadSeen = reads.entry(command.seq).or_default();
                        read.stream = command.stream.as_ref().unwrap().id.clone();
                        read.from_offset.clone_from(&command.from_offset);
                    }
                }
            }
            for (seq, read) in &reads {
                let Some(resolve_stream_read::Status::Page(page)) = &read.job else {
                    continue;
                };
                let stream = &self.streams[&read.stream];
                let after = after(stream, &read.from_offset);
                assert!(after.starts_with(&page.records), "seed {seed}: read {seq}");
                let last = stream.closed && after.len() == page.records.len();
                assert_eq!(page.done, last, "seed {seed}: read {seq}");
            }
            reads
        }

        /// Replays History with a random set of read entries whose Records the Store lost, and
        /// checks that the replay fails with the error of the first that Workflow code reaches.
        /// It reaches an entry at the start of the entry's task if it started the read before,
        /// or else at the command that starts the read.
        fn check_lost_records(&mut self, live: WorkflowTaskAttempt) {
            let seed = self.seed;
            let mut started = BTreeMap::new();
            for (task, of) in self.history.iter().enumerate() {
                for (at, activation) in of.activations.iter().enumerate() {
                    for (position, command) in activation.commands.iter().enumerate() {
                        if let Command::Read(read) = command {
                            started.insert(read.seq, (task, at, position));
                        }
                    }
                }
            }
            let in_history = in_history(&self.history);
            let mut reached = vec![];
            for (task, of) in self.history.iter().enumerate() {
                let Some(marker) = &of.marker else {
                    continue;
                };
                for (index, entry) in marker.data.reads.iter().enumerate() {
                    let count = page_of(entry).map_or(0, |page| page.record_count);
                    if in_history.contains(&entry.seq) || count == 0 {
                        continue;
                    }
                    let reach = match started[&entry.seq] {
                        (at, ..) if at < task => (task, 0, index, 0),
                        (_, activation, position) => (task, 1, activation, position),
                    };
                    reached.push((reach, (task, index)));
                }
            }
            let lost: BTreeSet<(usize, usize)> = (reached.iter())
                .filter(|_| self.rng.random_bool(0.4))
                .map(|(_, entry)| *entry)
                .collect();
            let Some((_, (task, index))) = (reached.iter())
                .filter(|(_, entry)| lost.contains(entry))
                .min()
            else {
                return;
            };
            let message = |task: usize, index: usize| format!("Entry {index} of task {task}");
            let gone = |task, index| lost.contains(&(task, index)).then(|| message(task, index));
            let error = replay(seed, &self.history, live, self.store(), &gone).err();
            assert_eq!(
                error,
                Some(WFMachinesError::StreamReplay(message(*task, *index))),
                "seed {seed}"
            );
            self.seen.replay_errors += 1;
        }
    }

    /// What Workflow code got of one read in History.
    #[derive(Default)]
    struct ReadSeen {
        stream: String,
        from_offset: Vec<u8>,
        job: Option<resolve_stream_read::Status>,
    }

    // A replay reads the Records of a page from the Store, unless History has them. A read with a
    // cancel gets the job that History has, or the job of its cancel once the replay is over.
    #[test]
    fn a_replay_of_history_gives_workflow_code_the_same_jobs_in_the_same_activations() {
        let mut seen = Seen::default();
        for seed in 0..SEEDS {
            let mut first = FirstRun::new(seed, &mut seen);
            first.play(12);
            let live = WorkflowTaskAttempt {
                started_event_id: first.task_attempt.started_event_id + 4,
                ..first.task_attempt
            };
            first.check_replay(live);
            first.check_lost_records(live);
        }
        // The scripts reach each case that the test is for.
        let Seen {
            full_markers,
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
            bad_seqs,
            pages,
            failed_reads,
            replay_errors,
            full_rooms,
            cancels,
            cancelled_at_go_live,
            cancelled_in_history,
            late_results,
        } = seen;
        let counts = [
            full_markers,
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
            bad_seqs,
            pages,
            failed_reads,
            replay_errors,
            full_rooms,
            cancelled_at_go_live,
            cancelled_in_history,
            late_results,
        ];
        let counts = [&counts[..], &cancels[..]].concat();
        assert!(counts.iter().all(|count| *count > 0), "{counts:?}");
    }

    #[test]
    fn the_calls_of_one_stream_go_to_the_store_one_at_a_time_in_call_order() {
        let mut together = 0;
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut calls = calls(Phase::Live, 3);
            // The calls of each Stream that have no result yet, in the order that they joined its
            // queue. An append joins at its command, and a close once History has its intent.
            let mut queues: BTreeMap<String, VecDeque<CallKey>> = BTreeMap::new();
            let mut in_flight: Vec<Call> = vec![];
            let (mut appends, mut closes, mut task, mut ended) = (0, 0, 3, false);
            for _ in 0..60 {
                let id = format!("s{}", rng.random_range(0..4));
                match rng.random_range(0..20) {
                    _ if ended => {}
                    0..=11 => {
                        appends += 1;
                        calls.append(append(appends, &id)).unwrap();
                        queues
                            .entry(id)
                            .or_default()
                            .push_back(CallKey::Append(appends));
                    }
                    12..=15 => {
                        closes += 1;
                        calls.close(close(closes, &id)).unwrap();
                    }
                    16 => {
                        // The last command of the Workflow stops each call.
                        calls.stop_for_terminal();
                        queues.clear();
                        ended = true;
                    }
                    _ => {}
                }
                if rng.random_bool(0.15) {
                    task += 4;
                    if let Some(marker) = next_task(&mut calls, task) {
                        calls.marker_recorded().unwrap();
                        for intent in &marker.data.close_intents {
                            queues
                                .entry(intent.stream_id.clone())
                                .or_default()
                                .push_back(CallKey::Close(intent.seq));
                        }
                    }
                }
                dispatch(seed, &mut calls, &mut in_flight);
                // The Store answers some of the calls in flight, in any order.
                for _ in 0..rng.random_range(0..=2) {
                    if in_flight.is_empty() {
                        break;
                    }
                    let call = in_flight.swap_remove(rng.random_range(0..in_flight.len()));
                    let (key, id) = target(&call);
                    let oldest = queues.get_mut(id).and_then(VecDeque::pop_front);
                    assert_eq!(oldest, Some(key), "seed {seed}: Stream {id}");
                    let failure = rng.random_bool(0.3).then(|| refused("no"));
                    calls.resolve(answer(&call, failure));
                    dispatch(seed, &mut calls, &mut in_flight);
                }
                // Each Stream with calls that wait has its oldest one in flight, and only that
                // one, whatever the other Streams do.
                for id in (0..4).map(|index| format!("s{index}")) {
                    let of_stream = in_flight.iter().map(target).filter(|(_, of)| *of == id);
                    let running: Vec<CallKey> = of_stream.map(|(key, _)| key).collect();
                    let oldest = queues.get(&id).and_then(|queue| queue.front().copied());
                    assert_eq!(running, Vec::from_iter(oldest), "seed {seed}: Stream {id}");
                }
                let streams: HashSet<&str> = in_flight.iter().map(|call| target(call).1).collect();
                together += usize::from(streams.len() > 1);
            }
        }
        assert!(together > 0);
    }

    /// Returns a page for the read `call`, of up to what it asked for, or of one larger Record,
    /// which has up to `max_record_bytes` in its stored form. A read whose Record no marker can
    /// hold goes into `too_large`.
    fn page_of_any_size(
        rng: &mut StdRng,
        (call, read): (&Call, &ReadCall),
        too_large: &mut HashSet<u32>,
        max_record_bytes: usize,
    ) -> StreamResolution {
        let in_history = read.records_in_history;
        let mut records = make_page(rng, read.request.max_bytes as usize);
        if in_history && rng.random_bool(0.1) {
            // The stored form of a Record of this size is 8 bytes longer than its data.
            let data = vec![0; max_record_bytes - rng.random_range(8..64)];
            let body = Payload {
                data,
                ..Default::default()
            };
            if field_len(body.encoded_len()) > MAX_MARKER_ONE_RECORD_BYTES {
                too_large.insert(call.seq);
            }
            records = vec![StreamRecord {
                offset: vec![1],
                body: Some(body),
                producer: None,
            }];
        }
        let page = Ok(ReadRecordsResponse {
            records,
            ..Default::default()
        });
        let stream = stream(&read.request.stream_id);
        let done = ReadDone::new(&stream, call.seq, vec![], page, in_history);
        StreamResolution {
            call_id: call.call_id,
            seq: call.seq,
            outcome: Outcome::Read(done),
        }
    }

    fn key_of(job: &Variant) -> CallKey {
        match job {
            Variant::ResolveStreamAppend(append) => CallKey::Append(append.seq),
            Variant::ResolveStreamClose(close) => CallKey::Close(close.seq),
            Variant::ResolveStreamRead(read) => CallKey::Read(read.seq),
            other => panic!("{other:?}"),
        }
    }

    /// Returns a page of up to `max_bytes` of Records, or one larger Record, as a Store gives it.
    fn make_page(rng: &mut StdRng, max_bytes: usize) -> Vec<StreamRecord> {
        let mut records = vec![];
        let mut bytes = 0;
        for offset in 1..=rng.random_range(1..=8u8) {
            let record = StreamRecord {
                offset: vec![offset],
                body: Some(Payload {
                    data: vec![0; rng.random_range(0..300)],
                    ..Default::default()
                }),
                producer: None,
            };
            let len = field_len(record.encoded_len());
            if !records.is_empty() && bytes + len > max_bytes {
                break;
            }
            bytes += len;
            records.push(record);
        }
        records
    }

    /// Checks the read entries of the marker of a task against the pages that Core took in the
    /// task, by activation and seq, and checks the two bounds of the Records of a task.
    fn check_pages(
        seed: u64,
        settings: &StreamSettings,
        marker: &StreamMarker,
        pages: &[(u64, u32, Vec<StreamRecord>)],
    ) {
        // The details of a marker stay under the blob size limit of the server, 2 MiB by default.
        let details = marker_details(marker.clone());
        let payloads = details.values().flat_map(|payloads| &payloads.payloads);
        let size: usize = payloads.map(Message::encoded_len).sum();
        assert!(size < 2 * 1024 * 1024, "seed {seed}: {size}");
        let held = marker.data.reads.iter().zip(&marker.records);
        let held: Vec<_> = held.filter(|(entry, _)| page_of(entry).is_some()).collect();
        let entries: Vec<(u64, u32)> = (held.iter())
            .map(|(read, _)| (read.activation_index, read.seq))
            .collect();
        let taken: Vec<(u64, u32)> = pages.iter().map(|(at, seq, _)| (*at, *seq)).collect();
        assert_eq!(entries, taken, "seed {seed}");
        for ((entry, held), (_, _, records)) in held.into_iter().zip(pages) {
            let page = page_of(entry).unwrap();
            assert_eq!(page.record_count as usize, records.len(), "seed {seed}");
            if page.digest.is_empty() {
                assert_eq!((held, page.record_bytes), (records, 0), "seed {seed}");
            } else {
                assert!(held.is_empty(), "seed {seed}");
                let bytes = (records.iter())
                    .map(|record| field_len(record.encoded_len()))
                    .sum::<usize>() as u64;
                let recorded = (page.digest.clone(), page.record_bytes);
                assert_eq!(recorded, (digest(records), bytes), "seed {seed}");
            }
        }
        let bytes = |records: &mut dyn Iterator<Item = &StreamRecord>| {
            records
                .map(|record| field_len(record.encoded_len()))
                .sum::<usize>()
        };
        // An empty task or marker takes any page, and a page larger than the room is one Record.
        let given = bytes(&mut pages.iter().flat_map(|(_, _, records)| records));
        let one_record = matches!(pages, [(_, _, records)] if records.len() == 1);
        assert!(
            given <= settings.bounds.max_task_record_bytes || one_record,
            "seed {seed}: {given}"
        );
        let held = bytes(&mut marker.records.iter().flatten());
        let first_alone = marker.records.iter().flatten().count() == 1;
        assert!(
            held <= settings.bounds.max_marker_record_bytes || first_alone,
            "seed {seed}: {held}"
        );
    }

    // A page can wait for the next task, since a read gets a page of any size up to what it asked
    // for. Each result goes whole into one marker, and no task gives more Records than its bounds.
    #[test]
    fn each_result_and_intent_is_in_exactly_one_marker_in_order() {
        let (mut full, mut oversized, mut out_of_order) = (0, 0, 0);
        let (mut pages_wait, mut larger_first, mut failed_reads) = (0, 0, 0);
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            // A small bound fills a marker after a few entries, and the default after many.
            let mut runtimes = runtimes(&["mem"]);
            if rng.random_bool(0.5) {
                runtimes.settings.bounds.max_marker_entry_bytes = rng.random_range(1..=16 * 1024);
            }
            let max_entry = runtimes.settings.bounds.max_marker_entry_bytes;
            // Small bounds of Records fill a task or a marker with a few pages, and a Record can
            // be larger than a marker holds.
            if rng.random_bool(0.5) {
                runtimes.settings.bounds.max_task_record_bytes = rng.random_range(1..=4000);
                runtimes.settings.bounds.max_marker_record_bytes = rng.random_range(1..=1000);
            }
            let settings = runtimes.settings;
            // The limits of a Store's runtime can have a larger Record size than the default one,
            // which is the size that a marker holds.
            let max_record_bytes = MAX_MARKER_ONE_RECORD_BYTES * rng.random_range(1..=2);
            let mut calls = StreamCalls::new("n", "w", "r", runtimes, Phase::Live);
            calls.set_task(TaskState::Open(task_attempt(3)));
            let budget = rng.random_range(1..=150);
            let (mut appends, mut closes, mut reads) = (0, 0, 0);
            let mut in_flight: Vec<Call> = vec![];
            let mut too_large = HashSet::new();
            // The results that Core took in each task, in order, with their activation.
            let mut taken: Vec<Vec<(u64, CallKey)>> = vec![vec![]];
            let mut markers = vec![];
            let (mut activation, mut task) = (0, 3);
            // The highest seq of each kind that Core took so far.
            let mut highest = (0, 0);
            // The pages that Core took in the task in hand.
            let mut pages = vec![];
            for round in 0.. {
                assert!(round < 100_000, "seed {seed}: the run never ends");
                // Most calls share a few Streams, so they wait in a queue. The others have a
                // Stream of their own, whose long id fills a marker sooner.
                for _ in 0..rng.random_range(0..=4) {
                    if appends + closes + reads == budget {
                        break;
                    }
                    let id = match rng.random_bool(0.6) {
                        true => format!("s{}", rng.random_range(0..4)),
                        false => {
                            let long = "a".repeat(rng.random_range(0..3000));
                            format!("own{}{long}", appends + closes + reads)
                        }
                    };
                    match rng.random_range(0..10) {
                        0..=5 => {
                            appends += 1;
                            calls.append(append(appends, &id)).unwrap();
                        }
                        6..=7 => {
                            closes += 1;
                            calls.close(close(closes, &id)).unwrap();
                        }
                        _ => {
                            reads += 1;
                            let read = ReadStream {
                                seq: reads,
                                stream: Some(stream(&id)),
                                records_in_history: rng.random_bool(0.5),
                                ..Default::default()
                            };
                            calls.read(read).unwrap();
                        }
                    }
                }
                dispatch(seed, &mut calls, &mut in_flight);
                // The Store answers some of the calls in flight, in any order. Most appends and
                // closes fail with a long message, which fills a marker sooner.
                for _ in 0..rng.random_range(0..=in_flight.len().min(6)) {
                    let call = in_flight.swap_remove(rng.random_range(0..in_flight.len()));
                    let resolution = if let Op::Read(read) = &call.op {
                        let read = (&call, read);
                        page_of_any_size(&mut rng, read, &mut too_large, max_record_bytes)
                    } else {
                        let bytes = rng.random_range(0..4200);
                        let failure = rng.random_bool(0.7).then(|| refused("x".repeat(bytes)));
                        answer(&call, failure)
                    };
                    calls.resolve(resolution);
                    dispatch(seed, &mut calls, &mut in_flight);
                }
                // Core takes the results that wait in their order, up to the first that doesn't
                // fit, and the marker records them with the activation.
                let waiting: Vec<CallKey> = calls.done.iter().copied().collect();
                let took = if calls.has_jobs(activation, None) && rng.random_bool(0.8) {
                    let jobs = calls.take_jobs(activation, None);
                    let results: Vec<CallKey> = jobs.iter().map(key_of).collect();
                    assert_eq!(results, waiting[..results.len()], "seed {seed}");
                    for job in jobs {
                        let Variant::ResolveStreamRead(read) = job else {
                            continue;
                        };
                        // A read fails if no marker can hold a Record of its page.
                        let too_large = too_large.contains(&read.seq);
                        match read.status {
                            Some(resolve_stream_read::Status::Page(page)) if !too_large => {
                                pages.push((activation, read.seq, page.records));
                            }
                            Some(resolve_stream_read::Status::Failed(_)) if too_large => {
                                failed_reads += 1;
                            }
                            _ => panic!("seed {seed}: {read:?}"),
                        }
                    }
                    results.len()
                } else {
                    0
                };
                assert!(
                    waiting[took..].iter().all(|call| calls.done.contains(call)),
                    "seed {seed}"
                );
                // A take stops only at a result that doesn't fit, so one that still waits means
                // that the marker or the task is full and the task must complete.
                if took > 0 {
                    let waits = calls.done.first().copied();
                    assert_eq!(
                        calls.must_complete(activation),
                        waits.is_some(),
                        "seed {seed}"
                    );
                    full += usize::from(waits.is_some());
                    pages_wait += usize::from(matches!(waits, Some(CallKey::Read(_))));
                }
                for call in &waiting[..took] {
                    let (seq, high) = match call {
                        CallKey::Append(seq) => (*seq, &mut highest.0),
                        CallKey::Close(seq) => (*seq, &mut highest.1),
                        CallKey::Read(_) => continue,
                    };
                    out_of_order += usize::from(seq < *high);
                    *high = seq.max(*high);
                }
                let entries = taken.last_mut().unwrap();
                entries.extend(waiting[..took].iter().map(|call| (activation, *call)));
                if took > 0 {
                    activation += 1;
                    continue;
                }
                // A full marker completes the task, and so does a heartbeat at any time.
                let finished = appends + closes + reads == budget && !calls.needs_next_task();
                if finished || calls.must_complete(activation) || rng.random_bool(0.03) {
                    task += 4;
                    let marker = next_task(&mut calls, task);
                    let taken_marker = marker.clone().unwrap_or_default();
                    check_pages(seed, &settings, &taken_marker, &pages);
                    let held: Vec<&StreamRecord> = taken_marker.records.iter().flatten().collect();
                    larger_first += usize::from(matches!(held[..], [record]
                        if field_len(record.encoded_len())
                            > settings.bounds.max_marker_record_bytes));
                    pages.clear();
                    if marker.is_some() {
                        calls.marker_recorded().unwrap();
                    }
                    markers.push(marker.unwrap_or_default());
                    taken.push(vec![]);
                    activation = 0;
                    if finished {
                        break;
                    }
                }
            }
            // Each marker records the results of its task in the order that Core took them, with
            // their activations. The appends come first, then the closes, and then the reads.
            for (index, marker) in markers.iter().map(|marker| &marker.data).enumerate() {
                let appended = marker.appends.iter();
                let recorded: Vec<(u64, CallKey)> = (appended)
                    .map(|append| (append.activation_index, CallKey::Append(append.seq)))
                    .chain(
                        marker
                            .closes
                            .iter()
                            .map(|c| (c.activation_index, CallKey::Close(c.seq))),
                    )
                    .chain(
                        marker
                            .reads
                            .iter()
                            .map(|r| (r.activation_index, CallKey::Read(r.seq))),
                    )
                    .collect();
                let mut expected = taken[index].clone();
                expected.sort_by_key(|(_, call)| match call {
                    CallKey::Append(_) => 0,
                    CallKey::Close(_) => 1,
                    CallKey::Read(_) => 2,
                });
                assert_eq!(recorded, expected, "seed {seed}: task {index}");
            }
            let once = |seqs: Vec<u32>, count: u32| {
                let mut sorted = seqs.clone();
                sorted.sort_unstable();
                assert_eq!(
                    sorted,
                    (1..=count).collect::<Vec<_>>(),
                    "seed {seed}: {seqs:?}"
                );
            };
            let all = |seqs: &dyn Fn(&StreamMarkerData) -> Vec<u32>| -> Vec<u32> {
                markers
                    .iter()
                    .flat_map(|marker| seqs(&marker.data))
                    .collect()
            };
            once(all(&|m| m.appends.iter().map(|a| a.seq).collect()), appends);
            once(all(&|m| m.closes.iter().map(|c| c.seq).collect()), closes);
            once(all(&|m| m.reads.iter().map(|r| r.seq).collect()), reads);
            let intents = all(&|m| m.close_intents.iter().map(|i| i.seq).collect());
            assert_eq!(intents, (1..=closes).collect::<Vec<_>>(), "seed {seed}");
            for marker in markers.iter().map(|marker| &marker.data) {
                let entries = marker.appends.len()
                    + marker.close_intents.len()
                    + marker.closes.len()
                    + marker.reads.len();
                // The bound of the entries is on the JSON of the marker's data.
                let size = serde_json::to_string(marker).unwrap().len();
                assert!(size <= max_entry || entries == 1, "seed {seed}: {size}");
                oversized += usize::from(size > max_entry);
            }
        }
        // The scripts reach each case that the test is for.
        let counts = [
            full,
            oversized,
            out_of_order,
            pages_wait,
            larger_first,
            failed_reads,
        ];
        assert!(counts.iter().all(|count| *count > 0), "{counts:?}");
    }

    /// A change of History or of Workflow code that a replay must not take for the first run.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Change {
        /// The command of a call is on another Stream.
        Stream,
        /// Workflow code never issues a call that History has an entry of.
        Missing,
        /// History has an entry twice.
        Twice,
        /// History has an entry for a seq that no command has.
        Seq,
        /// History has an entry as the other kind of call.
        Kind,
        /// History has an entry at another activation.
        Activation,
        /// The command of a read starts at another offset.
        Offset,
    }

    /// Makes a random change to `history`, if it has an entry to change. Returns it with the task
    /// of the changed entry.
    fn mutate(rng: &mut StdRng, history: &mut [Task]) -> Option<(Change, usize)> {
        let entries: Vec<(usize, CallKey)> = (history.iter().enumerate())
            .filter_map(|(index, task)| Some((index, &task.marker.as_ref()?.data)))
            .flat_map(|(index, marker)| {
                (marker
                    .appends
                    .iter()
                    .map(move |append| (index, CallKey::Append(append.seq))))
                .chain(
                    marker
                        .closes
                        .iter()
                        .map(move |close| (index, CallKey::Close(close.seq))),
                )
                .chain((marker.reads.iter()).map(move |read| (index, CallKey::Read(read.seq))))
            })
            .collect();
        if entries.is_empty() {
            return None;
        }
        let (task, call) = entries[rng.random_range(0..entries.len())];
        let change = [
            Change::Stream,
            Change::Missing,
            Change::Twice,
            Change::Seq,
            Change::Kind,
            Change::Activation,
            Change::Offset,
        ][rng.random_range(0..7)];
        if let Change::Stream | Change::Missing | Change::Offset = change {
            let commands = (history.iter_mut())
                .flat_map(|task| task.activations.iter_mut())
                .map(|activation| &mut activation.commands)
                .find(|commands| commands.iter().any(|command| is_command_of(command, call)))?;
            let at = commands
                .iter()
                .position(|command| is_command_of(command, call))?;
            match (change, &mut commands[at]) {
                (Change::Missing, _) => {
                    commands.remove(at);
                }
                (Change::Offset, Command::Read(read)) => read.from_offset.push(0),
                (Change::Offset, _) => return None,
                (_, Command::Append(append)) => append.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Close(close)) => close.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Read(read)) => read.stream.as_mut().unwrap().id.push('!'),
                (_, Command::CancelRead(_) | Command::Terminal) => return None,
            }
            return Some((change, task));
        }
        let StreamMarker {
            data: marker,
            records,
        } = history[task].marker.as_mut().unwrap();
        match call {
            CallKey::Append(seq) => {
                let at = marker.appends.iter().position(|append| append.seq == seq)?;
                match change {
                    Change::Twice => marker.appends.push(marker.appends[at].clone()),
                    Change::Seq => marker.appends[at].seq = 999,
                    Change::Activation => marker.appends[at].activation_index += 1,
                    _ => {
                        let entry = marker.appends.remove(at);
                        marker.closes.push(StreamCloseResult {
                            seq,
                            activation_index: entry.activation_index,
                            failure: None,
                        });
                    }
                }
            }
            CallKey::Close(seq) => {
                let at = marker.closes.iter().position(|close| close.seq == seq)?;
                match change {
                    Change::Twice => marker.closes.push(marker.closes[at].clone()),
                    Change::Seq => marker.closes[at].seq = 999,
                    Change::Activation => marker.closes[at].activation_index += 1,
                    _ => {
                        let entry = marker.closes.remove(at);
                        marker.appends.push(StreamAppendResult {
                            seq,
                            stream_id: "s0".to_string(),
                            store: "mem".to_string(),
                            activation_index: entry.activation_index,
                            result: Some(
                                stream_append_result::Result::Appended(Default::default()),
                            ),
                        });
                    }
                }
            }
            CallKey::Read(seq) => {
                let at = marker.reads.iter().position(|read| read.seq == seq)?;
                match change {
                    Change::Twice => {
                        marker.reads.push(marker.reads[at].clone());
                        records.push(records[at].clone());
                    }
                    Change::Seq => marker.reads[at].seq = 999,
                    Change::Activation => marker.reads[at].activation_index += 1,
                    _ => {
                        let entry = marker.reads.remove(at);
                        records.remove(at);
                        marker.appends.push(StreamAppendResult {
                            seq,
                            stream_id: entry.stream_id,
                            store: entry.store,
                            activation_index: entry.activation_index,
                            result: Some(
                                stream_append_result::Result::Appended(Default::default()),
                            ),
                        });
                    }
                }
            }
        }
        Some((change, task))
    }

    fn is_command_of(command: &Command, call: CallKey) -> bool {
        match (command, call) {
            (Command::Append(append), CallKey::Append(seq)) => append.seq == seq,
            (Command::Close(close), CallKey::Close(seq)) => close.seq == seq,
            (Command::Read(read), CallKey::Read(seq)) => read.seq == seq,
            _ => false,
        }
    }

    #[test]
    fn changed_workflow_code_is_a_nondeterminism_error() {
        let mut seen = Seen::default();
        let mut mutated = 0;
        for seed in 0..SEEDS {
            let mut first = FirstRun::new(seed, &mut seen);
            first.play(6);
            let mut rng = StdRng::seed_from_u64(seed);
            let mut history = first.history.clone();
            let Some((change, task)) = mutate(&mut rng, &mut history) else {
                continue;
            };
            mutated += 1;
            let first_jobs: Vec<_> = first.history.iter().map(Task::jobs).collect();
            // Workflow code reaches the change before the read entries of later tasks, whose
            // Records the Store lost, so the change decides the error.
            let lost = !matches!(change, Change::Activation | Change::Kind);
            let gone = |at, _| (lost && at > task).then(|| "lost".to_string());
            match replay(seed, &history, task_attempt(1000), first.store(), &gone) {
                Err(WFMachinesError::Nondeterminism(_)) => {}
                // An entry at another activation, or of the other kind, can match a call. Then
                // Workflow code gets other jobs, which is as wrong as an error.
                Ok(replayed) if matches!(change, Change::Activation | Change::Kind) => {
                    assert_ne!(replayed.jobs, first_jobs, "seed {seed}: {change:?}");
                }
                other => panic!("seed {seed}: {change:?}: {:?}", other.map(|r| r.jobs)),
            }
        }
        assert!(mutated > SEEDS / 2, "{mutated}");

        // A call with no Store name matches any Store of History.
        let mut replay = calls(Phase::ReplayOnly, 9);
        let mut nameless = append(1, "s");
        nameless.stream.as_mut().unwrap().store.clear();
        replay.append(nameless).unwrap();
        let marker = history(|m| assert!(m.append_result(&append_done(1, "s"), 0)));
        replay.peeked(marker, vec![]).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_on_a_store_with_no_runtime_fails_the_task() {
        let stores = ["a", "b", "c"];
        let mut with_no_runtime = 0;
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let served: Vec<(&str, Arc<MemoryStore>)> = (stores.into_iter())
                .filter(|_| rng.random_bool(0.5))
                .map(|store| (store, Arc::new(MemoryStore::default())))
                .collect();
            let phase = [Phase::Live, Phase::Replaying, Phase::ReplayOnly][rng.random_range(0..3)];
            if served.is_empty() && phase != Phase::ReplayOnly {
                with_no_runtime += 1;
            }
            let runtimes = StreamRuntimes::new(served.iter().map(|(name, store)| {
                let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
                (name.to_string(), Arc::new(runtime))
            }));
            let mut calls = StreamCalls::new("n", "w", "r", runtimes.clone(), phase);
            calls.set_task(TaskState::Open(task_attempt(3)));
            let (mut appends, mut closes, mut reads) = (0, 0, 0);
            let (mut unserved, mut store_of) = (BTreeMap::new(), HashMap::new());
            for index in 0..rng.random_range(1..=6) {
                let store = stores[rng.random_range(0..3)];
                let stream = StreamRef {
                    id: format!("s{index}"),
                    store: store.to_string(),
                };
                store_of.insert(stream.id.clone(), store);
                let (call, result) = match rng.random_range(0..3) {
                    0 => {
                        appends += 1;
                        let command = AppendToStream {
                            seq: appends,
                            stream: Some(stream),
                            bodies: vec![Payload::default()],
                        };
                        (CallKey::Append(appends), calls.append(command))
                    }
                    1 => {
                        closes += 1;
                        let command = CloseStream {
                            seq: closes,
                            stream: Some(stream),
                        };
                        (CallKey::Close(closes), calls.close(command))
                    }
                    _ => {
                        reads += 1;
                        let command = ReadStream {
                            seq: reads,
                            stream: Some(stream),
                            ..Default::default()
                        };
                        (CallKey::Read(reads), calls.read(command))
                    }
                };
                let error = WFMachinesError::Stream(format!(
                    "This Worker has no Stream runtime for the Store `{store}`"
                ));
                let is_served = served.iter().any(|(name, _)| *name == store);
                if phase == Phase::Live && !is_served {
                    // The command fails the task, so the run records nothing of the call.
                    assert_eq!(result, Err(error), "seed {seed}");
                    let tracked = match call {
                        CallKey::Append(seq) => calls.appends.contains_key(&seq),
                        CallKey::Close(seq) => calls.closes.contains_key(&seq),
                        CallKey::Read(seq) => calls.reads.contains_key(&seq),
                    };
                    assert!(!tracked, "seed {seed}");
                    break;
                }
                assert_eq!(result, Ok(()), "seed {seed}");
                if !is_served {
                    unserved.insert(call, error);
                }
            }
            let marker = calls.take_marker().unwrap_or_default().data;
            calls.marker_recorded().unwrap();
            let mut sent = calls.take_requests();
            let at_go_live = calls.go_live();
            sent.extend(calls.take_requests());
            // No call on a Store with no runtime records an intent or reaches a Store.
            let streams: Vec<&str> = (marker.close_intents.iter())
                .map(|intent| intent.store.as_str())
                .chain(sent.iter().map(|request| match request {
                    Request::Run(call) => call.store.as_str(),
                    Request::Stop(_) => panic!("seed {seed}"),
                }))
                .collect();
            assert!(
                (streams.iter()).all(|store| served.iter().any(|(name, _)| name == store)),
                "seed {seed}"
            );
            // A replay fails the task when it goes live with an unserved call, which a replay
            // Worker never does.
            let expected = match (phase, unserved.into_values().next()) {
                (Phase::Replaying, Some(error)) => Err(error),
                _ => Ok(()),
            };
            assert_eq!(at_go_live, expected, "seed {seed}");
            // Each Store gets only the calls whose ref has its name.
            let manager = StreamManager::new(runtimes, Handle::current(), |_, _| {});
            manager.dispatch("r", sent, |_| MetricsContext::no_op());
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            for (name, store) in &served {
                for request in store.requests() {
                    let id = match request {
                        StoreRequest::Append(append) => append.stream_id,
                        StoreRequest::Close(close) => close.stream_id,
                        StoreRequest::Read(read) => read.stream_id,
                    };
                    assert_eq!(store_of[&id], *name, "seed {seed}: Stream {id}");
                }
            }
        }
        assert!(with_no_runtime > 0, "No seed had a Worker with no runtime");
    }
}
