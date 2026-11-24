//! The Stream calls of one run. Nothing here does I/O. The machines of the run drive the calls,
//! and the Worker's [`StreamManager`](super::StreamManager) runs the requests that they start.

use super::{
    StreamRuntimes, StreamSettings, TaskState,
    ledger::{LedgerBuilder, Waiting, append_job, close_job, seq_of},
    manager::{Call, Op, Outcome, Request, StreamResolution},
};
use crate::worker::workflow::WFMachinesError;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::workflow_activation_job,
        workflow_commands::{AppendToStream, CloseStream},
    },
    temporal::api::{
        common::v1::Payload,
        sdk::v1::{
            WorkflowTaskStreamCalls,
            workflow_task_stream_calls::{AppendResult, CloseIntent, CloseResult},
        },
    },
};
use temporalio_streams::{
    AnyError,
    attempt::attempt_key,
    ids::validate_stream_id,
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, StreamRef,
        WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
    },
};

/// The maximum length of a Store name in a Workflow call, in bytes. A ledger records the Store of
/// each call, so Core fails the Workflow Task of a call with a longer name.
const MAX_STORE_NAME_BYTES: usize = 4096;

/// The counter of the ids of running calls, for every run of the process. A late result of a call
/// of an evicted run then never matches a call of the run that replaces it.
static CALL_IDS: AtomicU64 = AtomicU64::new(0);

/// How far a run is through its History.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The run replays History, so each call waits for its result in a ledger.
    Replaying,
    /// The replay is over, so calls run.
    Live,
    /// The run is on a replay Worker, which makes no live calls.
    ReplayOnly,
    /// The run stopped its calls, at a failed task or at the command that ends the Workflow, so it
    /// records no new call.
    Stopped,
}

/// A call by its kind and seq. Core gives the results of one activation in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CallKey {
    Append(u32),
    Close(u32),
}

impl fmt::Display for CallKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallKey::Append(seq) => write!(f, "append {seq}"),
            CallKey::Close(seq) => write!(f, "close {seq}"),
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
    /// The result waits for an activation, in the form that the ledger records it.
    Done(Waiting<AppendResult>),
    /// History has the result, which waits for its activation.
    Replayed,
}

struct Close {
    stream: StreamRef,
    state: CloseState,
}

enum CloseState {
    /// No ledger has the intent yet.
    Asked,
    /// The ledger of the task has the intent. A replayed close gets its attempt at go-live.
    Sealed(Option<WorkflowTaskAttempt>),
    /// History has the intent, so the close is in its queue.
    Waiting(Option<WorkflowTaskAttempt>),
    Running(u64),
    /// The result waits for an activation, in the form that the ledger records it.
    Done(Waiting<CloseResult>),
    /// History has the result, which waits for its activation.
    Replayed,
}

/// The entries of History's ledgers whose calls Workflow code hasn't issued yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, AppendResult>,
    intents: HashMap<u32, CloseIntent>,
    closes: HashMap<u32, CloseResult>,
}

/// The Stream calls of one run. A call is removed from the run once its result is in a ledger.
pub(crate) struct StreamCalls {
    /// The Producer of the run. A write adds the attempt of its Workflow Task.
    producer: WorkflowProducer,
    runtimes: StreamRuntimes,
    phase: Phase,
    /// True if the server keeps the `sdk_metadata` of a completion, which has the ledger of the
    /// task.
    keeps_ledgers: bool,
    /// The state of the run's current Workflow Task.
    task: TaskState,
    appends: HashMap<u32, Append>,
    closes: HashMap<u32, Close>,
    /// The appends and closes of each Stream, in the order that they were queued. Only the head
    /// runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    last_close: u32,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    /// The appends of the last activation, until the activation of their round goes out.
    round: BTreeSet<CallKey>,
    /// The live calls whose result can't come in their round. Each one comes in the first
    /// activation of a later task.
    late: BTreeSet<CallKey>,
    /// True from the start of the task until its first activation goes out, or until its Stream
    /// time is up with no late result to give.
    first: bool,
    /// True once the task has no Stream time left.
    time_up: bool,
    /// True if a call started when the task had no Stream time left.
    started_late: bool,
    /// True if the task in hand came in one sequence with a task before it. Its Local Activity
    /// results then count activations from the start of that sequence, while a replay of the
    /// whole History starts a sequence at a task with Stream calls. So no append of the task has a
    /// round, or a replay could move such a result past a result of the round.
    joined: bool,
    held: Held,
    /// History's results whose calls Workflow code issued, for the next activation.
    ready: BTreeMap<CallKey, workflow_activation_job::Variant>,
    ledger: LedgerBuilder,
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
        keeps_ledgers: bool,
    ) -> Self {
        debug_assert_ne!(phase, Phase::Stopped);
        let ledger = LedgerBuilder::new(runtimes.settings);
        Self {
            producer: WorkflowProducer {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                attempt: None,
            },
            runtimes,
            phase,
            keeps_ledgers,
            task: TaskState::NoTask,
            appends: HashMap::new(),
            closes: HashMap::new(),
            queues: HashMap::new(),
            last_append: 0,
            last_close: 0,
            done: BTreeSet::new(),
            round: BTreeSet::new(),
            late: BTreeSet::new(),
            first: false,
            time_up: false,
            started_late: false,
            joined: false,
            held: Held::default(),
            ready: BTreeMap::new(),
            ledger,
            requests: vec![],
        }
    }

    /// Sets the state of the run's current task. When a task opens, it has its Stream time and its
    /// first activation to come, and the head of each queue starts. A round ends with its task.
    pub(crate) fn set_task(&mut self, state: TaskState) {
        if state.is_open() && !self.task.is_open() {
            self.first = true;
            self.time_up = false;
            self.started_late = false;
        }
        if !state.is_open() {
            let round = std::mem::take(&mut self.round);
            self.late.extend(round);
        }
        self.task = state;
        self.start_heads();
    }

    /// Notes whether the sequence of History that Core applied last has a task before the task in
    /// hand.
    pub(crate) fn set_joined(&mut self, joined: bool) {
        self.joined = joined;
    }

    /// Handles an append command.
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
        if self.phase == Phase::Live {
            let call = CallKey::Append(seq);
            if self.task.is_open() && !self.time_up && !self.joined {
                self.round.insert(call);
            } else {
                // A call with no Stream time left, or of a joined task, can't have its round, so
                // its result is late.
                self.late.insert(call);
                self.started_late |= self.task.is_open();
            }
        }
        self.start_head(&queue);
        Ok(())
    }

    /// Handles a close command. The ledger of the task records its intent, and the close runs once
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
        // A close runs after the completion of its task, so its result is always late.
        if self.phase == Phase::Live {
            self.late.insert(CallKey::Close(seq));
        }
        self.match_intent(seq)
    }

    /// Handles the result of a running call, and starts the next call of its Stream. It ignores the
    /// result of a call that was stopped.
    pub(crate) fn resolve(&mut self, resolution: StreamResolution) {
        let StreamResolution {
            call_id,
            seq,
            outcome,
        } = resolution;
        let (call, queue) = match outcome {
            Outcome::Append(result) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                if !matches!(append.state, AppendState::Running(id) if id == call_id) {
                    return;
                }
                append.state = AppendState::Done(Waiting::new(AppendResult {
                    seq: seq as i32,
                    r#ref: Some(append.stream.clone()),
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
                close.state = CloseState::Done(Waiting::new(CloseResult {
                    seq: seq as i32,
                    failure,
                }));
                (CallKey::Close(seq), key(&close.stream))
            }
        };
        self.done.insert(call);
        self.leave_queue(&queue, call);
    }

    /// Returns true if the next activation gets a result. While a hold lasts, no live result comes.
    pub(crate) fn has_jobs(&self) -> bool {
        // A run that stopped its calls gives no result, even one that History has.
        if self.phase == Phase::Stopped {
            return false;
        }
        // A live result goes into the ledger of an open task only.
        !self.ready.is_empty()
            || (self.task.is_open()
                && !self.holds_round()
                && !self.holds_first()
                && self.next_fits() == Some(true))
    }

    /// Returns whether the ledger has room for the first live result that the next activation can
    /// take, or `None` if there is no such result.
    fn next_fits(&self) -> Option<bool> {
        let call = self.deliverable().next()?;
        Some(match call {
            CallKey::Append(seq) => match &self.appends[&seq].state {
                AppendState::Done(result) => self.ledger.has_room_for(result),
                _ => false,
            },
            CallKey::Close(seq) => match &self.closes[&seq].state {
                CloseState::Done(result) => self.ledger.has_room_for(result),
                _ => false,
            },
        })
    }

    /// Returns the results of the next activation, in the order of [`CallKey`], and records each
    /// live one in the ledger. A replay gives the results of History that are due. A live run gives
    /// the results of the last round, and in the first activation of a task also the late results.
    /// A result with no room in the ledger is late, and so is each call of the round with no
    /// answer.
    pub(crate) fn take_jobs(&mut self) -> Vec<workflow_activation_job::Variant> {
        let ready = std::mem::take(&mut self.ready);
        let mut jobs = vec![];
        for (call, job) in ready {
            self.leave(call);
            jobs.push(job);
        }
        if !self.task.is_open() {
            return jobs;
        }
        let deliverable: Vec<CallKey> = self.deliverable().collect();
        let mut full = false;
        for call in deliverable {
            if !full && let Some(job) = self.record(call) {
                self.done.remove(&call);
                self.round.remove(&call);
                self.late.remove(&call);
                self.leave(call);
                jobs.push(job);
                continue;
            }
            // The ledger keeps the order of the results, so each later result waits too.
            full = true;
            self.round.remove(&call);
            self.late.insert(call);
        }
        let unanswered = std::mem::take(&mut self.round);
        self.late.extend(unanswered);
        self.first = false;
        jobs
    }

    /// Returns true while an append of the last activation has no answer and the task has Stream
    /// time left. Core then gives lang no activation and doesn't complete the task.
    pub(crate) fn holds_round(&self) -> bool {
        self.phase == Phase::Live
            && self.task.is_open()
            && !self.time_up
            && self.round.iter().any(|call| !self.done.contains(call))
    }

    /// Returns true while the first activation of the task waits for a late call, with Stream time
    /// left. A job from the task's events or a Local Activity result ends this hold, because the
    /// activation then goes out with the late results that are ready.
    pub(crate) fn holds_first(&self) -> bool {
        self.phase == Phase::Live
            && self.task.is_open()
            && self.first
            && !self.time_up
            && self.late_runs()
    }

    /// Ends the Stream time of the task. Each append of the round with no answer is late. With no
    /// late result to give, the first activation of the task doesn't go out for the late calls, so
    /// a result that comes after this is late too.
    pub(crate) fn time_up(&mut self) {
        self.time_up = true;
        let unanswered: Vec<CallKey> = (self.round.iter())
            .filter(|call| !self.done.contains(call))
            .copied()
            .collect();
        for call in unanswered {
            self.round.remove(&call);
            self.late.insert(call);
        }
        if self.first && !self.late.iter().any(|call| self.done.contains(call)) {
            self.first = false;
        }
    }

    /// Returns true if the task must complete for its Stream calls, as at a heartbeat. That's the
    /// case once no round holds the task and one of these holds.
    ///
    /// - A late result is ready after the first activation of the task, or a ready result has no
    ///   room in the ledger.
    /// - A call started with no Stream time left.
    /// - The task has work for the server (`has_work_for_server`), its Stream time is up, and a
    ///   late call runs.
    pub(crate) fn must_complete(&self, has_work_for_server: bool) -> bool {
        if self.phase != Phase::Live || !self.task.is_open() || self.holds_round() {
            return false;
        }
        // A ready result with no room in the ledger is late too.
        let late_ready = (!self.first && self.late.iter().any(|call| self.done.contains(call)))
            || self.next_fits() == Some(false);
        late_ready || self.started_late || (self.time_up && has_work_for_server && self.late_runs())
    }

    /// Returns true while a late call has no result.
    pub(crate) fn late_runs(&self) -> bool {
        self.late.iter().any(|call| !self.done.contains(call))
    }

    pub(crate) fn settings(&self) -> StreamSettings {
        self.runtimes.settings
    }

    /// Returns the live results that the next activation can take. Those are the results of the
    /// round, and in the first activation of a task also the late ones.
    fn deliverable(&self) -> impl Iterator<Item = CallKey> + '_ {
        (self.done.iter().copied())
            .filter(|call| self.round.contains(call) || (self.first && self.late.contains(call)))
    }

    /// Holds the entries of a ledger from History, each for its call.
    pub(crate) fn peeked(&mut self, data: WorkflowTaskStreamCalls) -> Result<(), WFMachinesError> {
        let WorkflowTaskStreamCalls {
            close_intents,
            append_results,
            close_results,
            ..
        } = data;
        for intent in close_intents {
            let seq = seq_of(intent.seq)?;
            if self.held.intents.insert(seq, intent).is_some() {
                return Err(second("intent", CallKey::Close(seq)));
            }
            self.match_intent(seq)?;
        }
        for append in append_results {
            let seq = seq_of(append.seq)?;
            if self.held.appends.insert(seq, append).is_some() {
                return Err(second("result", CallKey::Append(seq)));
            }
            self.match_append(seq)?;
        }
        for close in close_results {
            let seq = seq_of(close.seq)?;
            if self.held.closes.insert(seq, close).is_some() {
                return Err(second("result", CallKey::Close(seq)));
            }
            self.match_result(seq)?;
        }
        Ok(())
    }

    /// Handles the completion in History that has a ledger. Each entry of the ledger must already
    /// have its call, and each close whose intent the ledger has joins its queue.
    pub(crate) fn ledger_recorded(&mut self) -> Result<(), WFMachinesError> {
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
        if let Some(call) = self.ready.keys().next() {
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

    /// Ends the replay. Each waiting call gets the attempt of the current task, and the head of
    /// each queue starts. With `before_first_activation`, the task gave no live activation yet, so
    /// its next one is its first, which takes the late results.
    pub(crate) fn go_live(&mut self, before_first_activation: bool) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Replaying {
            return Ok(());
        }
        let appends = (self.appends.iter()).map(|(seq, append)| (CallKey::Append(*seq), append));
        let unserved = (appends.map(|(call, append)| (call, &append.stream)))
            .chain((self.closes.iter()).map(|(seq, close)| (CallKey::Close(*seq), &close.stream)))
            .filter(|(_, stream)| self.runtimes.get(&stream.store).is_none())
            .min_by_key(|(call, _)| *call);
        if let Some((_, stream)) = unserved {
            return Err(no_runtime(stream));
        }
        if !self.keeps_ledgers && self.needs_next_task() {
            return Err(no_ledger());
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
        // The task of each call with no result in History is over, so its result is late.
        let appends = self.appends.keys().map(|seq| CallKey::Append(*seq));
        let closes = self.closes.keys().map(|seq| CallKey::Close(*seq));
        self.late.extend(appends.chain(closes));
        self.first = before_first_activation && self.task.is_open();
        self.start_heads();
        Ok(())
    }

    /// Drops each call at a command that ends the Workflow, as Core drops a Local Activity there.
    /// The ledger keeps the results that Workflow code already got in the task.
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
        let running = appends
            .chain(closes)
            .filter(|call_id| !unsent.contains(call_id));
        self.requests.extend(running.map(Request::Stop));
        self.phase = Phase::Stopped;
        self.appends.clear();
        self.closes.clear();
        self.queues.clear();
        self.done.clear();
        self.round.clear();
        self.late.clear();
        self.started_late = false;
    }

    /// Returns true while a call has no result in a ledger, so the run needs another task.
    pub(crate) fn needs_next_task(&self) -> bool {
        !self.appends.is_empty() || !self.closes.is_empty()
    }

    /// Returns true if the ledger of the task has an entry.
    pub(crate) fn has_ledger(&self) -> bool {
        !self.ledger.is_empty()
    }

    /// Returns the number of live calls that hold the task open, as a Local Activity does.
    pub(crate) fn outstanding(&self) -> usize {
        if self.phase != Phase::Live {
            return 0;
        }
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

    /// Returns the ledger of the task as it completes, with the intent of each close that fits.
    ///
    /// A completion that leaves a call with no result has a ledger, with no entry if the task has
    /// none. A replay joins a task whose completion has no command and no ledger with the next
    /// task, and the late result of such a call would then come one activation later than in the
    /// first run.
    pub(crate) fn take_ledger(&mut self) -> Option<WorkflowTaskStreamCalls> {
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
            if !self.ledger.close_intent(&close.stream, seq) {
                break;
            }
            close.state = CloseState::Sealed(attempt);
        }
        let ledger = self.ledger.take();
        if ledger.is_none() && self.needs_next_task() {
            return Some(WorkflowTaskStreamCalls::default());
        }
        ledger
    }

    /// Returns the calls that the Worker must start or stop.
    pub(crate) fn take_requests(&mut self) -> Vec<Request> {
        std::mem::take(&mut self.requests)
    }

    /// Drops every call, the ledger of the task and the entries of History, and leaves the run
    /// with no task, as a failed task does. The run records no call after this.
    pub(crate) fn stop(&mut self) {
        self.stop_for_terminal();
        self.task = TaskState::NoTask;
        self.held = Held::default();
        self.ready.clear();
        self.ledger = LedgerBuilder::new(self.runtimes.settings);
        self.requests.clear();
    }

    /// Checks that a live call can run on the Store of `stream`, and that History can keep its
    /// result. A call whose result History can't keep must never run.
    fn check_runtime(&self, stream: &StreamRef) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Live {
            return Ok(());
        }
        if !self.keeps_ledgers {
            return Err(no_ledger());
        }
        if self.runtimes.get(&stream.store).is_none() {
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
        check_stream(call, &append.stream, entry.r#ref.as_ref())?;
        if !matches!(append.state, AppendState::Waiting { .. }) {
            return Err(second("result", call));
        }
        append.state = AppendState::Replayed;
        self.ready.insert(call, append_job(seq, &entry).into());
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
        check_stream(call, &close.stream, intent.r#ref.as_ref())?;
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
        self.ready
            .insert(CallKey::Close(seq), close_job(seq, &entry).into());
        Ok(())
    }

    /// Records a live result in the ledger, and returns its job. Returns `None` if the ledger has
    /// no room for it.
    fn record(&mut self, call: CallKey) -> Option<workflow_activation_job::Variant> {
        match call {
            CallKey::Append(seq) => {
                let AppendState::Done(result) = &self.appends[&seq].state else {
                    return None;
                };
                if !self.ledger.append_result(result) {
                    return None;
                }
                Some(append_job(seq, &result.entry).into())
            }
            CallKey::Close(seq) => {
                let CloseState::Done(result) = &self.closes[&seq].state else {
                    return None;
                };
                if !self.ledger.close_result(result) {
                    return None;
                }
                Some(close_job(seq, &result.entry).into())
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
                let request_id = request_id(&self.producer.run_id, &producer, seq);
                let request = AppendRecordsRequest {
                    r#ref: Some(append.stream.clone()),
                    producer: Some(producer),
                    records: (bodies.into_iter())
                        .map(|body| AppendRecord { body: Some(body) })
                        .collect(),
                };
                (
                    seq,
                    call_id,
                    &append.stream,
                    Op::Append(request, request_id),
                )
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
                    r#ref: Some(close.stream.clone()),
                    producer: Some(producer(&self.producer, attempt)),
                };
                (seq, call_id, &close.stream, Op::Close(request))
            }
        };
        let call = Call {
            call_id,
            seq,
            store: stream.store.clone(),
            op,
        };
        self.requests.push(Request::Run(call));
    }

    /// Removes `call` from the run. A live call left its queue when its result arrived.
    fn leave(&mut self, call: CallKey) {
        let stream = match call {
            CallKey::Append(seq) => (self.appends.remove(&seq))
                .filter(|append| !matches!(append.state, AppendState::Done(_)))
                .map(|append| append.stream),
            CallKey::Close(seq) => (self.closes.remove(&seq))
                .filter(|close| !matches!(close.state, CloseState::Done(_)))
                .map(|close| close.stream),
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

/// Checks that `seq` is above the last one of its kind and fits the `int32` of a ledger. A repeated
/// seq would give two appends one request id, and a Store would treat the second as a resend of the
/// first.
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
    if seq > i32::MAX as u32 {
        return Err(WFMachinesError::Fatal(format!(
            "Stream {kind} has seq {seq}, and History keeps a seq as an `int32`"
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
    validate_stream_id(&stream.id)
        .and_then(|()| check_store_name(&stream.store))
        .map_err(|error| {
            WFMachinesError::Fatal(format!("Stream {call} is invalid: {}", error.message))
        })?;
    Ok(stream)
}

fn no_ledger() -> WFMachinesError {
    WFMachinesError::Stream(
        "The server doesn't take the `sdk_metadata` of a Workflow Task completion, so History \
         can't keep the results of Stream calls"
            .to_string(),
    )
}

fn no_runtime(stream: &StreamRef) -> WFMachinesError {
    WFMachinesError::Stream(format!(
        "This Worker has no Stream runtime for the Store `{}`",
        stream.store
    ))
}

/// Checks that a call is for the Stream that History has, in the Store that History has. A call
/// with no Store name matches any Store. An entry with no ref matches no call, since each call has
/// a Stream id.
fn check_stream(
    call: CallKey,
    sent: &StreamRef,
    recorded: Option<&StreamRef>,
) -> Result<(), WFMachinesError> {
    let (id, store) = recorded.map_or(("", ""), |stream| (&stream.id[..], &stream.store[..]));
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
/// - the [`attempt_key`] of the attempt, in hex (56 characters)
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

#[cfg(test)]
mod tests {
    use super::super::{StreamManager, StreamSettings, manager::recorded};
    use super::*;
    use crate::test_help::stream_store::{MemoryStore, Request as StoreRequest};
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use temporalio_common::protos::{
        coresdk::workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, resolve_stream_append,
        },
        temporal::api::sdk::v1::workflow_task_stream_calls::{
            AppendOffsets, CallFailure, append_result,
        },
    };
    use temporalio_streams::{AnyError, StreamRuntime, record::StoreLimits};
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
        let mut calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), phase, true);
        calls.set_task(TaskState::Open(task_attempt(task)));
        calls
    }

    /// Completes the current task, and opens the next one.
    fn next_task(calls: &mut StreamCalls, task: i64) -> Option<WorkflowTaskStreamCalls> {
        let ledger = calls.take_ledger();
        calls.set_task(TaskState::NoTask);
        calls.set_task(TaskState::Open(task_attempt(task)));
        ledger
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

    fn appended(seq: u32) -> append_result::Result {
        append_result::Result::Appended(AppendOffsets {
            first_offset: vec![seq as u8],
            last_offset: vec![seq as u8],
        })
    }

    /// Returns the result of append `seq` to the Stream `id`, as a run holds it before a ledger
    /// records it.
    fn append_done(seq: u32, id: &str) -> Waiting<AppendResult> {
        Waiting::new(AppendResult {
            seq: seq as i32,
            r#ref: Some(stream(id)),
            result: Some(appended(seq)),
        })
    }

    fn refused(message: impl Into<String>) -> CallFailure {
        recorded(&AnyError::invalid_argument(message))
    }

    /// Returns a ledger that `write` fills.
    fn history(write: impl FnOnce(&mut LedgerBuilder)) -> WorkflowTaskStreamCalls {
        let mut ledger = LedgerBuilder::new(StreamSettings::default());
        write(&mut ledger);
        ledger.take().unwrap()
    }

    /// Returns the calls that started since the last look, as `append 1 of s at 3`, with the
    /// started event id of their attempt.
    fn started(calls: &mut StreamCalls) -> Vec<String> {
        let mut started: Vec<String> = (calls.take_requests().into_iter())
            .map(|request| {
                let (call, stream, producer) = match request {
                    Request::Run(Call { seq, op, .. }) => match op {
                        Op::Append(r, _) => (CallKey::Append(seq), r.r#ref, r.producer),
                        Op::Close(r) => (CallKey::Close(seq), r.r#ref, r.producer),
                    },
                    Request::Stop(_) => return "stop".to_string(),
                };
                let Some(Kind::Workflow(workflow)) = producer.unwrap().kind else {
                    panic!("{call} has no Workflow Producer");
                };
                let at = workflow.attempt.unwrap().started_event_id;
                format!("{call} of {} at {at}", stream.unwrap().id)
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
                    Some(failure) => append_result::Result::Failure(failure),
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
        };
        calls.resolve(StreamResolution {
            call_id,
            seq,
            outcome,
        });
    }

    /// Returns the results of the next activation, as `append 1` or `close 2: <text>`.
    fn jobs(calls: &mut StreamCalls) -> Vec<String> {
        (calls.take_jobs().into_iter())
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
            Op::Append(request, _) => (
                CallKey::Append(call.seq),
                &request.r#ref.as_ref().unwrap().id,
            ),
            Op::Close(request) => (
                CallKey::Close(call.seq),
                &request.r#ref.as_ref().unwrap().id,
            ),
        }
    }

    /// Moves the calls that the run starts into `in_flight`, and drops the ones that it stops.
    fn dispatch(calls: &mut StreamCalls, in_flight: &mut Vec<Call>) {
        for request in calls.take_requests() {
            match request {
                Request::Run(call) => in_flight.push(call),
                Request::Stop(call_id) => in_flight.retain(|call| call.call_id != call_id),
            }
        }
    }

    /// Returns the Store's answer to `call`, which is `failure` if there is one.
    fn answer(call: &Call, failure: Option<CallFailure>) -> StreamResolution {
        let outcome = match (&call.op, failure) {
            (Op::Append(..), None) => Outcome::Append(appended(call.seq)),
            (Op::Append(..), Some(failure)) => {
                Outcome::Append(append_result::Result::Failure(failure))
            }
            (Op::Close(_), failure) => Outcome::Close(failure),
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
        // Closes on Streams with long ids fill the ledger, so the last intent waits for task 7.
        let long = |seq: u32| format!("{seq:02}{}", "s".repeat(4000));
        for seq in 2..=70 {
            calls.close(close(seq, &long(seq))).unwrap();
        }
        let first = next_task(&mut calls, 7).unwrap();
        let sealed = first.close_intents.len() as u32;
        assert!(sealed < 70, "{sealed}");
        calls.ledger_recorded().unwrap();
        finish(&mut calls, CallKey::Append(1), None);
        let mut expected = vec!["append 2 of s at 3".to_string()];
        expected.extend((2..=sealed).map(|seq| format!("close {seq} of {} at 3", long(seq))));
        expected.sort();
        assert_eq!(started(&mut calls), expected);
        finish(&mut calls, CallKey::Append(2), None);
        assert_eq!(started(&mut calls), ["close 1 of s at 3"]);
        next_task(&mut calls, 11).unwrap();
        calls.ledger_recorded().unwrap();
        let rest = started(&mut calls);
        assert_eq!(rest.len(), 70 - sealed as usize);
        assert!(rest.iter().all(|call| call.ends_with(" at 7")), "{rest:?}");
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
        /// A command for the server, such as a timer.
        Server,
        /// A command that ends the Workflow.
        Terminal,
    }

    /// One activation, as the first run gave it, by its place in its task.
    #[derive(Clone, Debug)]
    struct Activation {
        index: u64,
        jobs: Vec<Variant>,
        commands: Vec<Command>,
    }

    /// A Workflow Task that History has, with what the first run did in it.
    #[derive(Clone, Debug, Default)]
    struct Task {
        /// True if the events of the task give Workflow code a job, so the task has a first
        /// activation even with no Stream result.
        has_events: bool,
        activations: Vec<Activation>,
        ledger: Option<WorkflowTaskStreamCalls>,
        /// True if this task sent the last command of the Workflow.
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
        /// The place of the next activation in the current task.
        activation: u64,
        /// True once Workflow code sent its last command.
        ended: bool,
        /// True once the task has a command for the server.
        has_work_for_server: bool,
    }

    impl Run {
        /// Gives Workflow code an activation, and takes its commands.
        fn activate(&mut self, commands: &[Command]) -> Result<Vec<Variant>, WFMachinesError> {
            let jobs = self.calls.take_jobs();
            self.activation += 1;
            for command in commands {
                match command {
                    Command::Append(append) => self.calls.append(append.clone())?,
                    Command::Close(close) => self.calls.close(close.clone())?,
                    Command::Server => self.has_work_for_server = true,
                    Command::Terminal => {
                        self.calls.stop_for_terminal();
                        self.ended = true;
                    }
                }
            }
            Ok(jobs)
        }

        /// Returns the appends and closes that the run still tracks.
        fn tracked(&self) -> BTreeSet<CallKey> {
            (self.calls.appends.keys().map(|seq| CallKey::Append(*seq)))
                .chain(self.calls.closes.keys().map(|seq| CallKey::Close(*seq)))
                .collect()
        }
    }

    /// What a replay gives.
    struct Replayed {
        jobs: Vec<TaskJobs>,
        /// The task that sent the last command of the Workflow.
        sent_in: Option<usize>,
        run: Run,
    }

    /// Replays `history` with the commands of the first run of `seed`, and goes live in the task
    /// `live`, as a new run of Core does.
    fn replay(
        seed: u64,
        history: &[Task],
        live: WorkflowTaskAttempt,
    ) -> Result<Replayed, WFMachinesError> {
        let calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), Phase::Replaying, true);
        let mut run = Run {
            calls,
            activation: 0,
            ended: false,
            has_work_for_server: false,
        };
        run.calls.set_task(TaskState::Open(live));
        let mut replayed = vec![];
        let mut sent_in = None;
        for (index, task) in history.iter().enumerate() {
            if index > 0 && history[index - 1].ledger.is_some() {
                run.calls.ledger_recorded()?;
            }
            // Core peeks the ledger that the task's completion recorded before it activates.
            if let Some(ledger) = &task.ledger {
                run.calls.peeked(ledger.clone())?;
            }
            run.activation = 0;
            let mut activations = vec![];
            let mut first = task.has_events;
            while !run.ended && (first || run.calls.has_jobs()) {
                first = false;
                let at = run.activation;
                let jobs = run.activate(&task.commands_at(at))?;
                activations.push((at, jobs));
            }
            if run.ended && sent_in.is_none() {
                sent_in = Some(index);
            }
            let requests = run.calls.take_requests();
            assert!(requests.is_empty(), "seed {seed}: {requests:?}");
            replayed.push(activations);
        }
        if history.last().is_some_and(|task| task.ledger.is_some()) {
            run.calls.ledger_recorded()?;
        }
        run.activation = 0;
        run.calls.go_live(true)?;
        Ok(Replayed {
            jobs: replayed,
            sent_in,
            run,
        })
    }

    /// The cases that the generated scripts reached, over all seeds.
    #[derive(Default)]
    struct Seen {
        /// The results that had no room in the ledger of their task.
        full_ledgers: usize,
        /// The rounds that ended by time with an append that had no answer.
        time_ups: usize,
        /// The late results that came in the first activation of a task.
        late_results: usize,
        /// The tasks that completed for a late result, as at a heartbeat.
        late_completions: usize,
        heartbeats: usize,
        failed_tasks: usize,
        /// The last commands that Workflow code sent while a call had no result in a ledger.
        dropped: usize,
        /// The commands with a Stream id or a Store name out of range.
        invalid: usize,
        bad_seqs: usize,
        /// The appends of a task that came in one sequence with the task before it.
        joined_appends: usize,
    }

    /// What the rules of rounds say about the task in hand. The first run keeps it apart from the
    /// state of the calls, and checks each decision of the calls against it.
    #[derive(Default)]
    struct Rules {
        /// The appends of the last activation, while their round lasts.
        round: BTreeSet<CallKey>,
        /// The calls whose result can only come in the first activation of a later task.
        late: BTreeSet<CallKey>,
        /// The calls whose Store answer arrived and that no activation took yet.
        answered: BTreeSet<CallKey>,
        time_up: bool,
        /// True once the task gave its first activation, or once its Stream time ended with no
        /// late result to give.
        first_sent: bool,
        /// True if a call started when the task had no Stream time left.
        started_late: bool,
        /// True if the task came in one sequence with the task before it, so no call has a round.
        joined: bool,
    }

    impl Rules {
        /// Returns true while Core may give no activation and may not complete the task.
        fn holds(&self) -> bool {
            let running =
                |calls: &BTreeSet<CallKey>| calls.iter().any(|c| !self.answered.contains(c));
            !self.time_up && (running(&self.round) || (!self.first_sent && running(&self.late)))
        }

        fn ready(&self, calls: &BTreeSet<CallKey>) -> bool {
            calls.iter().any(|call| self.answered.contains(call))
        }

        /// Checks the jobs of an activation, and moves on to its round.
        fn activation(&mut self, seed: u64, jobs: &[Variant], commands: &[Command]) {
            let taken: Vec<CallKey> = (jobs.iter())
                .map(|job| match job {
                    Variant::ResolveStreamAppend(append) => CallKey::Append(append.seq),
                    Variant::ResolveStreamClose(close) => CallKey::Close(close.seq),
                    other => panic!("{other:?}"),
                })
                .collect();
            // The first activation takes late results, and any other one the results of its round.
            let from = match self.first_sent {
                false => &self.late,
                true => &self.round,
            };
            assert!(
                taken
                    .iter()
                    .all(|c| from.contains(c) && self.answered.contains(c)),
                "seed {seed}: {taken:?}"
            );
            assert!(taken.is_sorted(), "seed {seed}: {taken:?}");
            for call in &taken {
                self.answered.remove(call);
                self.late.remove(call);
            }
            // A result of the round that didn't fit in the ledger is late.
            let round = std::mem::take(&mut self.round);
            self.late
                .extend(round.into_iter().filter(|call| !taken.contains(call)));
            self.first_sent = true;
            for command in commands {
                match command {
                    Command::Append(append) if !self.time_up && !self.joined => {
                        self.round.insert(CallKey::Append(append.seq));
                    }
                    Command::Append(append) => {
                        self.late.insert(CallKey::Append(append.seq));
                        self.started_late = true;
                    }
                    // The result of a close is always late.
                    Command::Close(close) => {
                        self.late.insert(CallKey::Close(close.seq));
                    }
                    Command::Server => {}
                    Command::Terminal => *self = Rules::default(),
                }
            }
        }

        fn time_up(&mut self) {
            self.time_up = true;
            let unanswered: Vec<CallKey> = (self.round.iter())
                .filter(|call| !self.answered.contains(call))
                .copied()
                .collect();
            for call in unanswered {
                self.round.remove(&call);
                self.late.insert(call);
            }
            if !self.ready(&self.late) {
                self.first_sent = true;
            }
        }

        /// Checks what the calls decide while no activation is outstanding.
        fn check(&self, seed: u64, calls: &StreamCalls, has_work_for_server: bool) {
            let (has_jobs, must_complete) =
                (calls.has_jobs(), calls.must_complete(has_work_for_server));
            if self.holds() {
                assert!(
                    !has_jobs && !must_complete,
                    "seed {seed}: a hold ended early"
                );
                return;
            }
            if has_jobs {
                return;
            }
            // An empty ledger takes any result, so a ready late result always fits in the first
            // activation. A ready result of the round waits only for room in the ledger.
            assert!(self.first_sent || !self.ready(&self.late), "seed {seed}");
            if self.ready(&self.round) {
                assert_eq!(
                    calls.next_fits(),
                    Some(false),
                    "seed {seed}: a ready result waits"
                );
            }
            let late_runs = self.late.iter().any(|call| !self.answered.contains(call));
            let expected = (self.first_sent && (self.ready(&self.late) || self.ready(&self.round)))
                || self.started_late
                || (self.time_up && has_work_for_server && late_runs);
            assert_eq!(must_complete, expected, "seed {seed}");
        }
    }

    /// Returns the call of a Store answer.
    fn call_of(resolution: &StreamResolution) -> CallKey {
        match resolution.outcome {
            Outcome::Append(_) => CallKey::Append(resolution.seq),
            Outcome::Close(_) => CallKey::Close(resolution.seq),
        }
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
        /// Results that arrived while the run had no task. Core applies them after the events of
        /// the next task.
        deferred: Vec<StreamResolution>,
        last_append: u32,
        last_close: u32,
        /// The calls whose result no ledger has.
        unresolved: BTreeSet<CallKey>,
        /// The state as of the last task in History, which a failed task goes back to.
        committed: (u32, u32, BTreeSet<CallKey>),
        rules: Rules,
    }

    impl<'a> FirstRun<'a> {
        fn new(seed: u64, seen: &'a mut Seen) -> Self {
            let mut runtimes = runtimes(&["mem"]);
            // A small ledger fills in a few results.
            runtimes.settings.bounds.max_ledger_bytes = 12_000;
            let calls = StreamCalls::new("n", "w", "r", runtimes, Phase::Live, true);
            let run = Run {
                calls,
                activation: 0,
                ended: false,
                has_work_for_server: false,
            };
            Self {
                seed,
                rng: StdRng::seed_from_u64(seed),
                seen,
                run,
                history: vec![],
                task: Task::default(),
                task_attempt: task_attempt(3),
                in_flight: vec![],
                deferred: vec![],
                last_append: 0,
                last_close: 0,
                unresolved: BTreeSet::new(),
                committed: Default::default(),
                rules: Rules::default(),
            }
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
                .is_some_and(|task| task.ledger.is_some())
            {
                self.run.calls.ledger_recorded().unwrap();
            }
            self.run.activation = 0;
            self.run.has_work_for_server = false;
            // A Worker that takes the run over can apply a task in one sequence with the task
            // before it.
            let joined = self.rng.random_bool(0.1);
            self.run.calls.set_joined(joined);
            let answered = std::mem::take(&mut self.rules.answered);
            self.rules = Rules {
                late: self.unresolved.clone(),
                answered: answered.intersection(&self.unresolved).copied().collect(),
                joined,
                ..Default::default()
            };
            for resolution in std::mem::take(&mut self.deferred) {
                self.resolve(resolution);
            }
            self.dispatch();
        }

        /// Gives the run the Store's answer to a call.
        fn resolve(&mut self, resolution: StreamResolution) {
            self.rules.answered.insert(call_of(&resolution));
            self.run.calls.resolve(resolution);
        }

        /// Runs the current task until it completes, and returns an error if it fails. The events
        /// of some tasks give a job, so their first activation goes out at once.
        fn run_task(&mut self) -> Result<(), ()> {
            self.task.has_events = self.history.is_empty() || self.rng.random_bool(0.6);
            if self.task.has_events {
                self.activate()?;
            }
            loop {
                if self.run.ended {
                    self.complete();
                    return Ok(());
                }
                let has_work_for_server = self.run.has_work_for_server;
                self.rules
                    .check(self.seed, &self.run.calls, has_work_for_server);
                if self.run.calls.has_jobs() {
                    self.activate()?;
                    continue;
                }
                if self.run.calls.must_complete(self.run.has_work_for_server) {
                    self.seen.late_completions += 1;
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
                    0..=31 => {
                        let resolution = self.answer();
                        self.resolve(resolution);
                        self.dispatch();
                    }
                    // The Stream time of the task is up.
                    32..=35 => {
                        self.seen.time_ups += usize::from(self.run.calls.holds_round());
                        self.run.calls.time_up();
                        self.rules.time_up();
                    }
                    36..=38 => {
                        // The heartbeat completes the task. Results that arrive before the
                        // completion wait for the next task.
                        self.seen.heartbeats += 1;
                        self.run
                            .calls
                            .set_task(TaskState::Completing(self.task_attempt));
                        for _ in 0..self.rng.random_range(0..=2) {
                            if !self.in_flight.is_empty() {
                                let resolution = self.answer();
                                self.resolve(resolution);
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

        /// Gives Workflow code an activation with commands that the script makes up.
        fn activate(&mut self) -> Result<(), ()> {
            let index = self.run.activation;
            if self.run.calls.first {
                let late = self.run.calls.late.iter();
                self.seen.late_results += late.filter(|c| self.run.calls.done.contains(c)).count();
            }
            let (commands, fatal) = self.commands();
            // The answers that arrived while Workflow code had the activation. Core handles them
            // after the commands of the completion.
            let came = [0, 0, 1, 2, 3, 8][self.rng.random_range(0..6)].min(self.in_flight.len());
            let late: Vec<StreamResolution> = (0..came).map(|_| self.answer()).collect();
            let result = self.run.activate(&commands);
            if let Some(expected) = fatal {
                assert_eq!(result, Err(expected), "seed {}", self.seed);
                return Err(());
            }
            let jobs = result.unwrap_or_else(|error| panic!("seed {}: {error:?}", self.seed));
            self.rules.activation(self.seed, &jobs, &commands);
            // A late result that waits after a take had no room in the ledger.
            let waits = self.run.calls.done.iter();
            self.seen.full_ledgers += waits.filter(|c| self.run.calls.late.contains(c)).count();
            for resolution in late {
                self.resolve(resolution);
            }
            for command in &commands {
                match command {
                    Command::Append(append) => {
                        self.seen.joined_appends += usize::from(self.rules.joined);
                        self.unresolved.insert(CallKey::Append(append.seq));
                    }
                    Command::Close(close) => {
                        self.unresolved.insert(CallKey::Close(close.seq));
                    }
                    Command::Server => {}
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

        /// Returns the commands of an activation. If the last one isn't valid, it also returns the
        /// error that fails the Workflow Task.
        fn commands(&mut self) -> (Vec<Command>, Option<WFMachinesError>) {
            let mut commands = vec![];
            for _ in 0..self.rng.random_range(0..=3) {
                let id = format!("s{}", self.rng.random_range(0..3));
                match self.rng.random_range(0..100) {
                    0..=44 => {
                        self.last_append += 1;
                        commands.push(Command::Append(append(self.last_append, &id)));
                    }
                    45..=69 => {
                        self.last_close += 1;
                        commands.push(Command::Close(close(self.last_close, &id)));
                    }
                    70..=72 => {
                        self.seen.invalid += 1;
                        let long = "x".repeat(4097);
                        let (command, error) = match self.rng.random_range(0..3) {
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
                        let (command, error) = if self.rng.random_bool(0.5) {
                            let seq = self.last_append;
                            let error = match seq {
                                0 => "Stream append has seq 0, and a seq starts at 1".to_string(),
                                _ => format!("Stream append {seq} came after append {seq}"),
                            };
                            (Command::Append(append(seq, &id)), error)
                        } else {
                            let seq = self.last_close;
                            let error = match seq {
                                0 => "Stream close has seq 0, and a seq starts at 1".to_string(),
                                _ => format!("Stream close {seq} came after close {seq}"),
                            };
                            (Command::Close(close(seq, &id)), error)
                        };
                        commands.push(command);
                        return (commands, Some(WFMachinesError::Fatal(error)));
                    }
                    83..=86 => {
                        commands.push(Command::Terminal);
                        return (commands, None);
                    }
                    90..=94 => commands.push(Command::Server),
                    87..=89 => {
                        // A burst of appends, each on a Stream of its own, can fill a ledger.
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

        /// Takes a random call in flight, and returns the Store's answer to it.
        fn answer(&mut self) -> StreamResolution {
            let call = (self.in_flight).swap_remove(self.rng.random_range(0..self.in_flight.len()));
            let failure = match &call.op {
                Op::Append(request, _) => {
                    // A large failure fills the ledger in a few dozen results.
                    let burst = request.r#ref.as_ref().unwrap().id.starts_with('b');
                    let large = if burst { 8 } else { 1 };
                    match self.rng.random_range(0..10) {
                        at if at < large => Some(refused("x".repeat(4000))),
                        at if at < large + 2 => Some(refused("refused")),
                        _ => None,
                    }
                }
                Op::Close(_) => self.rng.random_bool(0.3).then(|| refused("refused")),
            };
            answer(&call, failure)
        }

        fn dispatch(&mut self) {
            dispatch(&mut self.run.calls, &mut self.in_flight);
        }

        /// Completes the task in hand, and checks that the run tracks exactly the calls whose
        /// result no ledger has.
        fn complete(&mut self) {
            let ledger = self.run.calls.take_ledger();
            self.run.calls.set_task(TaskState::NoTask);
            for entry in ledger.iter() {
                for append in &entry.append_results {
                    self.unresolved.remove(&CallKey::Append(append.seq as u32));
                }
                for close in &entry.close_results {
                    self.unresolved.remove(&CallKey::Close(close.seq as u32));
                }
            }
            let mut task = std::mem::take(&mut self.task);
            task.ledger = ledger;
            task.ends_workflow = self.run.ended;
            self.history.push(task);
            self.committed = (self.last_append, self.last_close, self.unresolved.clone());
            let seed = self.seed;
            assert_eq!(self.run.tracked(), self.unresolved, "seed {seed}");
            let in_flight = &self.in_flight;
            assert!(
                !self.run.ended || in_flight.is_empty(),
                "seed {seed}: {in_flight:?}"
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

        /// Lets the Store answer while the run has no task. Core applies these results after the
        /// events of the next task.
        fn between_tasks(&mut self) {
            for _ in 0..self.rng.random_range(0..=2) {
                if !self.in_flight.is_empty() {
                    let resolution = self.answer();
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
            (self.last_append, self.last_close, self.unresolved) = self.committed.clone();
            self.task = Task::default();
            self.task_attempt.attempt += 1;
            self.run = self.check_replay(self.task_attempt);
            self.rules = Rules {
                late: self.unresolved.clone(),
                ..Default::default()
            };
            self.dispatch();
        }

        /// Replays History, checks it against the first run, and returns the replayed run.
        fn check_replay(&self, live: WorkflowTaskAttempt) -> Run {
            let seed = self.seed;
            let Replayed {
                jobs: replayed,
                sent_in,
                mut run,
            } = replay(seed, &self.history, live)
                .unwrap_or_else(|error| panic!("seed {seed}: {error:?}"));
            let first: Vec<_> = self.history.iter().map(Task::jobs).collect();
            assert_eq!(replayed, first, "seed {seed}");
            let ended = self.history.iter().position(|task| task.ends_workflow);
            assert_eq!(sent_in, ended, "seed {seed}");
            // The replay resumes each call whose result no ledger has, and only those.
            assert_eq!(run.tracked(), self.unresolved, "seed {seed}");
            let recorded: BTreeSet<CallKey> = (self.history.iter())
                .filter_map(|task| task.ledger.as_ref())
                .flat_map(|ledger| {
                    let appends = ledger.append_results.iter();
                    let closes = ledger.close_results.iter();
                    (appends.map(|append| CallKey::Append(append.seq as u32)))
                        .chain(closes.map(|close| CallKey::Close(close.seq as u32)))
                })
                .collect();
            for request in run.calls.take_requests() {
                let Request::Run(Call { seq, op, .. }) = &request else {
                    panic!("seed {seed}: {request:?}");
                };
                let (key, sent) = match op {
                    Op::Append(append, _) => (CallKey::Append(*seq), &append.producer),
                    Op::Close(close) => (CallKey::Close(*seq), &close.producer),
                };
                assert!(!recorded.contains(&key), "seed {seed}: {key} ran again");
                let expected = producer(&run.calls.producer, live);
                assert_eq!(*sent, Some(expected), "seed {seed}");
                run.calls.requests.push(request);
            }
            run
        }
    }

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
        }
        // The scripts reach each case that the test is for.
        let Seen {
            full_ledgers,
            time_ups,
            late_results,
            late_completions,
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
            bad_seqs,
            joined_appends,
        } = seen;
        let counts = [
            full_ledgers,
            time_ups,
            late_results,
            late_completions,
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
            bad_seqs,
            joined_appends,
        ];
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
                    if let Some(ledger) = next_task(&mut calls, task) {
                        calls.ledger_recorded().unwrap();
                        for intent in &ledger.close_intents {
                            queues
                                .entry(intent.r#ref.as_ref().unwrap().id.clone())
                                .or_default()
                                .push_back(CallKey::Close(intent.seq as u32));
                        }
                    }
                }
                dispatch(&mut calls, &mut in_flight);
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
                    dispatch(&mut calls, &mut in_flight);
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
        /// History has an entry in the ledger of the task before.
        Task,
    }

    /// Makes a random change to `history`, if it has an entry to change.
    fn mutate(rng: &mut StdRng, history: &mut [Task]) -> Option<Change> {
        let entries: Vec<(usize, CallKey)> = (history.iter().enumerate())
            .filter_map(|(index, task)| Some((index, task.ledger.as_ref()?)))
            .flat_map(|(index, ledger)| {
                let appends = ledger.append_results.iter();
                let closes = ledger.close_results.iter();
                (appends.map(move |append| (index, CallKey::Append(append.seq as u32))))
                    .chain(closes.map(move |close| (index, CallKey::Close(close.seq as u32))))
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
            Change::Task,
        ][rng.random_range(0..6)];
        if let Change::Stream | Change::Missing = change {
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
                (_, Command::Append(append)) => append.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Close(close)) => close.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Server | Command::Terminal) => return None,
            }
            return Some(change);
        }
        if change == Change::Task {
            let before = task.checked_sub(1)?;
            let ledger = history[task].ledger.as_mut().unwrap();
            let mut moved = WorkflowTaskStreamCalls::default();
            match call {
                CallKey::Append(seq) => {
                    let appends = &mut ledger.append_results;
                    let at = appends.iter().position(|append| append.seq == seq as i32)?;
                    moved.append_results.push(appends.remove(at));
                }
                CallKey::Close(seq) => {
                    let closes = &mut ledger.close_results;
                    let at = closes.iter().position(|close| close.seq == seq as i32)?;
                    moved.close_results.push(closes.remove(at));
                }
            }
            let target = history[before].ledger.get_or_insert_with(Default::default);
            target.append_results.extend(moved.append_results);
            target.close_results.extend(moved.close_results);
            return Some(change);
        }
        let ledger = history[task].ledger.as_mut().unwrap();
        let (appends, closes) = (&mut ledger.append_results, &mut ledger.close_results);
        match call {
            CallKey::Append(seq) => {
                let seq = seq as i32;
                let at = appends.iter().position(|append| append.seq == seq)?;
                match change {
                    Change::Twice => appends.push(appends[at].clone()),
                    Change::Seq => appends[at].seq = 999,
                    _ => {
                        appends.remove(at);
                        closes.push(CloseResult { seq, failure: None });
                    }
                }
            }
            CallKey::Close(seq) => {
                let seq = seq as i32;
                let at = closes.iter().position(|close| close.seq == seq)?;
                match change {
                    Change::Twice => closes.push(closes[at].clone()),
                    Change::Seq => closes[at].seq = 999,
                    _ => {
                        closes.remove(at);
                        appends.push(AppendResult {
                            seq,
                            r#ref: Some(stream("s0")),
                            result: Some(append_result::Result::Appended(Default::default())),
                        });
                    }
                }
            }
        }
        Some(change)
    }

    fn is_command_of(command: &Command, call: CallKey) -> bool {
        match (command, call) {
            (Command::Append(append), CallKey::Append(seq)) => append.seq == seq,
            (Command::Close(close), CallKey::Close(seq)) => close.seq == seq,
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
            let Some(change) = mutate(&mut rng, &mut history) else {
                continue;
            };
            mutated += 1;
            let first_jobs: Vec<_> = first.history.iter().map(Task::jobs).collect();
            match replay(seed, &history, task_attempt(1000)) {
                Err(WFMachinesError::Nondeterminism(_)) => {}
                // An entry in another ledger, or of the other kind, can match a call. Then Workflow
                // code gets other jobs, which is as wrong as an error.
                Ok(replayed) if matches!(change, Change::Task | Change::Kind) => {
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
        let ledger = history(|m| assert!(m.append_result(&append_done(1, "s"))));
        replay.peeked(ledger).unwrap();
        assert_eq!(jobs(&mut replay), ["append 1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_with_no_runtime_or_no_ledger_fails_the_task() {
        let stores = ["a", "b", "c"];
        let (mut with_no_runtime, mut with_no_ledger) = (0, 0);
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
            // A server that doesn't take the `sdk_metadata` of a completion keeps no ledger.
            let keeps_ledgers = rng.random_bool(0.7);
            with_no_ledger += usize::from(!keeps_ledgers && phase != Phase::ReplayOnly);
            let mut calls = StreamCalls::new("n", "w", "r", runtimes.clone(), phase, keeps_ledgers);
            calls.set_task(TaskState::Open(task_attempt(3)));
            let (mut appends, mut closes) = (0, 0);
            let (mut unserved, mut store_of) = (BTreeMap::new(), HashMap::new());
            for index in 0..rng.random_range(1..=6) {
                let store = stores[rng.random_range(0..3)];
                let stream = StreamRef {
                    id: format!("s{index}"),
                    store: store.to_string(),
                };
                store_of.insert(stream.id.clone(), store);
                let (call, result) = if rng.random_bool(0.5) {
                    appends += 1;
                    let command = AppendToStream {
                        seq: appends,
                        stream: Some(stream),
                        bodies: vec![Payload::default()],
                    };
                    (CallKey::Append(appends), calls.append(command))
                } else {
                    closes += 1;
                    let command = CloseStream {
                        seq: closes,
                        stream: Some(stream),
                    };
                    (CallKey::Close(closes), calls.close(command))
                };
                let error = WFMachinesError::Stream(format!(
                    "This Worker has no Stream runtime for the Store `{store}`"
                ));
                let is_served = served.iter().any(|(name, _)| *name == store);
                if phase == Phase::Live && (!is_served || !keeps_ledgers) {
                    // The command fails the task, so the run records nothing of the call.
                    let error = if keeps_ledgers { error } else { no_ledger() };
                    assert_eq!(result, Err(error), "seed {seed}");
                    let tracked = match call {
                        CallKey::Append(seq) => calls.appends.contains_key(&seq),
                        CallKey::Close(seq) => calls.closes.contains_key(&seq),
                    };
                    assert!(!tracked, "seed {seed}");
                    break;
                }
                assert_eq!(result, Ok(()), "seed {seed}");
                if !is_served {
                    unserved.insert(call, error);
                }
            }
            let ledger = calls.take_ledger().unwrap_or_default();
            calls.ledger_recorded().unwrap();
            let mut sent = calls.take_requests();
            let at_go_live = calls.go_live(true);
            sent.extend(calls.take_requests());
            // No call on a Store with no runtime records an intent or reaches a Store.
            let streams: Vec<&str> = (ledger.close_intents.iter())
                .map(|intent| intent.r#ref.as_ref().unwrap().store.as_str())
                .chain(sent.iter().map(|request| match request {
                    Request::Run(call) => call.store.as_str(),
                    Request::Stop(_) => panic!("seed {seed}"),
                }))
                .collect();
            assert!(
                (streams.iter()).all(|store| served.iter().any(|(name, _)| name == store)),
                "seed {seed}"
            );
            // A replay fails the task when it goes live with an unserved call, or with any call
            // whose result History can't keep, which a replay Worker never does.
            let expected = match (phase, unserved.into_values().next()) {
                (Phase::Replaying, Some(error)) => Err(error),
                (Phase::Replaying, None) if !keeps_ledgers && appends + closes > 0 => {
                    Err(no_ledger())
                }
                _ => Ok(()),
            };
            assert_eq!(at_go_live, expected, "seed {seed}");
            // No call runs if History can't keep its result.
            assert!(keeps_ledgers || sent.is_empty(), "seed {seed}");
            // Each Store gets only the calls whose ref has its name.
            let manager = StreamManager::new(runtimes, Handle::current(), |_, _| {});
            manager.dispatch("r", sent);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            for (name, store) in &served {
                for request in store.requests() {
                    let id = match request {
                        StoreRequest::Append(append, _) => append.r#ref.unwrap().id,
                        StoreRequest::Close(close) => close.r#ref.unwrap().id,
                    };
                    assert_eq!(store_of[&id], *name, "seed {seed}: Stream {id}");
                }
            }
        }
        assert!(with_no_runtime > 0, "No seed had a Worker with no runtime");
        assert!(
            with_no_ledger > 0,
            "No seed had a server that keeps no ledger"
        );
    }
}
