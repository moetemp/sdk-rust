//! Runs the Stream calls of Workflow code. Their results arrive as local inputs, and each call
//! writes with the attempt of a Workflow Task.

mod calls;
mod digest;
mod manager;
mod marker;
mod replay;
#[cfg(test)]
mod test_store;

pub(crate) use calls::{Phase, StreamCalls, is_stream_replay_failure, set_replay_failure_type};
pub(crate) use manager::{StreamManager, StreamResolution};
pub(crate) use marker::{StreamMarker, is_stream_marker, marker_details, peeked_marker};
pub(super) use replay::StreamRecordsReq;
pub(crate) use replay::{MarkerReads, ReplayReader, ReplayReads};

use crate::{
    WorkerConfig,
    worker::workflow::{
        LocalResolution, LocalResolutionMsg, RunBasics, WorkflowTaskInfo,
        workflow_stream::{LocalInput, LocalInputs},
    },
};
use futures_util::future::{AbortRegistration, Abortable};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::temporal::api::stream::v1::WorkflowTaskAttempt;
use temporalio_streams::StreamRuntime;
use tokio::{runtime::Handle, sync::mpsc::UnboundedSender};
use tracing::Span;

/// The maximum size of the entries of one marker, which is the length of the JSON in its `data`
/// detail. The server terminates a Workflow that records a marker over its blob size limit, which
/// is 2 MiB by default.
const MAX_MARKER_ENTRY_BYTES: usize = 256 * 1024;

/// The maximum size of the Records that one Workflow Task gives Workflow code from all of its
/// reads. A replay reads digested Records again in parts of this size by the bytes that History
/// recorded. One marker over it still makes a separate part.
const MAX_TASK_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// The maximum size of the Records that one marker holds for reads with `records_in_history`, as
/// the pages of their reads encode them. A marker takes no more bytes for a Record than its page
/// does. One larger Record still fits as the marker's first.
const MAX_MARKER_RECORD_BYTES: usize = 1024 * 1024;

/// The maximum size of one Record that a marker holds, in its stored form. A read with
/// `records_in_history` fails for a larger Record, whatever the limits of the Store's runtime.
/// With [`MAX_MARKER_ENTRY_BYTES`], this keeps a marker under the blob size limit of the server.
const MAX_MARKER_ONE_RECORD_BYTES: usize = 1536 * 1024;

/// The maximum number of Records in the page of a read whose command has no `max_records`.
const DEFAULT_PAGE_RECORDS: i32 = 256;

/// The maximum size of the Records in the page of a read whose command has no `max_bytes`.
const DEFAULT_PAGE_BYTES: i64 = 4 * 1024 * 1024;

/// The Stream runtimes of a Worker, identified by the name of the Store that each one serves,
/// with the settings of its Stream calls.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes {
    runtimes: Arc<HashMap<String, Arc<StreamRuntime>>>,
    /// The settings of the Stream calls.
    pub settings: StreamSettings,
}

impl StreamRuntimes {
    /// Creates the runtimes, identified by Store name, with the default settings.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: Arc::new(runtimes.into_iter().collect()),
            settings: StreamSettings::default(),
        }
    }

    pub(crate) fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }

    /// Returns true if there's no runtime, so Workflow code on this Worker can't call a Store.
    pub(crate) fn is_empty(&self) -> bool {
        self.runtimes.is_empty()
    }
}

/// The settings of a Worker's Stream calls. A try that fails with
/// [`AnyReason::StoreUnavailable`](temporalio_streams::AnyReason::StoreUnavailable), or gets no
/// answer within its deadline, counts as an outage. A live call then tries again with no time
/// limit, and a replay read stops at `replay_retry_bound`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamSettings {
    /// The wait after the first failed try, 100 ms by default. Each later wait doubles, and the
    /// actual wait is a random value within 20 % of it. It must be more than 0 and at most
    /// `retry_max_interval`.
    pub retry_initial_interval: Duration,
    /// The maximum wait between two tries before its random part, 2 s by default.
    pub retry_max_interval: Duration,
    /// How long one try waits for the Store, 5 s by default. A read's try also waits as long as the
    /// read asks the Store to wait. It must be more than 0.
    pub try_deadline: Duration,
    /// The maximum time that one try of a read asks the Store to wait for a Record, 60 s by
    /// default. It must be more than 0. A read with a longer `wait` asks again after that, so a
    /// Store that stops answering counts as an outage within this wait and `try_deadline`.
    pub read_max_wait: Duration,
    /// How long a replay goes on trying a Store after its first try of that Store fails, each time
    /// it reads a part of the Records of History, 3 s by default. No try or wait goes past that
    /// time, and zero is one try. The first try can take `try_deadline` to fail, so the two
    /// together must stay under the Workflow Task timeout.
    pub replay_retry_bound: Duration,
    /// The maximum number of read entries whose Records a replay reads at the same time, 8 by
    /// default. It must be 1 or more.
    pub replay_concurrency: usize,
    /// The bounds that a test sets in place of the constants.
    #[cfg(test)]
    pub(crate) bounds: TestBounds,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            retry_initial_interval: Duration::from_millis(100),
            retry_max_interval: Duration::from_secs(2),
            try_deadline: Duration::from_secs(5),
            read_max_wait: Duration::from_secs(60),
            replay_retry_bound: Duration::from_secs(3),
            replay_concurrency: 8,
            #[cfg(test)]
            bounds: TestBounds::default(),
        }
    }
}

impl StreamSettings {
    /// Checks the lower bounds of the settings. Nothing bounds a setting from above.
    pub(crate) fn check(&self) -> Result<(), String> {
        let first = self.retry_initial_interval;
        if first.is_zero() || first > self.retry_max_interval {
            return Err(
                "The Stream setting `retry_initial_interval` must be more than 0 and at most \
                 `retry_max_interval`"
                    .to_string(),
            );
        }
        let zeros = [
            ("try_deadline", self.try_deadline.is_zero()),
            ("read_max_wait", self.read_max_wait.is_zero()),
            ("replay_concurrency", self.replay_concurrency == 0),
        ];
        if let Some((setting, _)) = zeros.iter().find(|(_, zero)| *zero) {
            return Err(format!(
                "The Stream setting `{setting}` must be more than 0"
            ));
        }
        Ok(())
    }

    /// Returns [`MAX_MARKER_ENTRY_BYTES`], or the bound that a test set.
    pub(crate) fn max_marker_entry_bytes(&self) -> usize {
        #[cfg(test)]
        let max = self.bounds.max_marker_entry_bytes;
        #[cfg(not(test))]
        let max = MAX_MARKER_ENTRY_BYTES;
        max
    }

    /// Returns [`MAX_TASK_RECORD_BYTES`], or the bound that a test set.
    pub(crate) fn max_task_record_bytes(&self) -> usize {
        #[cfg(test)]
        let max = self.bounds.max_task_record_bytes;
        #[cfg(not(test))]
        let max = MAX_TASK_RECORD_BYTES;
        max
    }

    /// Returns [`MAX_MARKER_RECORD_BYTES`], or the bound that a test set.
    pub(crate) fn max_marker_record_bytes(&self) -> usize {
        #[cfg(test)]
        let max = self.bounds.max_marker_record_bytes;
        #[cfg(not(test))]
        let max = MAX_MARKER_RECORD_BYTES;
        max
    }
}

/// The bounds of the Stream calls that a test sets in place of the constants.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TestBounds {
    pub(crate) max_marker_entry_bytes: usize,
    pub(crate) max_task_record_bytes: usize,
    pub(crate) max_marker_record_bytes: usize,
}

#[cfg(test)]
impl Default for TestBounds {
    fn default() -> Self {
        Self {
            max_marker_entry_bytes: MAX_MARKER_ENTRY_BYTES,
            max_task_record_bytes: MAX_TASK_RECORD_BYTES,
            max_marker_record_bytes: MAX_MARKER_RECORD_BYTES,
        }
    }
}

/// The state of the run's current Workflow Task, as its Stream calls see it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum TaskState {
    /// The run has no task, so no call starts and Workflow code gets no result.
    #[default]
    NoTask,
    /// Calls start, and each call that Workflow code makes writes with the attempt of the task.
    Open(WorkflowTaskAttempt),
    /// The run completes the task as soon as it can. Until the next task, no call starts and
    /// Workflow code gets no live result.
    Completing(WorkflowTaskAttempt),
}

impl TaskState {
    fn is_open(&self) -> bool {
        matches!(self, TaskState::Open(_))
    }

    fn attempt(&self) -> Option<WorkflowTaskAttempt> {
        match self {
            TaskState::Open(attempt) | TaskState::Completing(attempt) => Some(*attempt),
            TaskState::NoTask => None,
        }
    }
}

/// The Stream manager of a Worker, with the channel of local inputs that it and a heartbeat timer
/// send to.
pub(super) struct Streams {
    manager: StreamManager,
    local_tx: UnboundedSender<LocalInput>,
}

impl Streams {
    /// Creates the manager. It runs the calls on the caller's runtime, which is the Worker's.
    pub(super) fn new(config: &WorkerConfig, local_tx: UnboundedSender<LocalInput>) -> Self {
        let results = local_tx.clone();
        let sink = move |run_id, resolution| {
            let input = LocalInputs::LocalResolution(LocalResolutionMsg {
                run_id,
                res: LocalResolution::Stream(resolution),
            });
            let span = Span::current();
            let _ = results.send(LocalInput { input, span });
        };
        Self {
            manager: StreamManager::new(config.stream_runtimes.clone(), Handle::current(), sink),
            local_tx,
        }
    }
}

/// The Stream manager of a Worker for one run, with the id of that run.
pub(super) struct RunStreams {
    streams: Arc<Streams>,
    run_id: String,
}

impl RunStreams {
    pub(super) fn new(streams: Arc<Streams>, basics: &RunBasics) -> Self {
        Self {
            streams,
            run_id: basics.run_id.clone(),
        }
    }

    /// Starts and stops the Stream calls that the machines decided on.
    pub(super) fn dispatch(&self, calls: &mut StreamCalls) {
        let requests = calls.take_requests();
        if !requests.is_empty() {
            self.streams.manager.dispatch(&self.run_id, requests);
        }
    }

    pub(super) fn stop(&self, calls: &mut StreamCalls) {
        calls.stop();
        self.cancel();
    }

    /// Stops every call of the run in the manager. The calls of the machines stay as they are.
    pub(super) fn cancel(&self) {
        self.streams.manager.cancel_run(&self.run_id);
    }

    /// Returns the failure of the run's call that waits to try again after an outage.
    pub(super) fn outage(&self) -> Option<String> {
        self.streams.manager.outage(&self.run_id)
    }

    /// Sends a heartbeat timeout for the run at `deadline`. A Worker uses this when it has no Local
    /// Activity manager to run the timer. It must be called on the thread of the runs.
    pub(super) fn heartbeat_at(&self, deadline: Instant, abort: AbortRegistration) {
        let run_id = self.run_id.clone();
        let local_tx = self.streams.local_tx.clone();
        let span = Span::current();
        let timer = async move {
            tokio::time::sleep_until(deadline.into()).await;
            let input = LocalInputs::HeartbeatTimeout(run_id);
            let _ = local_tx.send(LocalInput { input, span });
        };
        // The timer runs on the thread of the runs, so a timer that the run aborted sends nothing.
        // A timeout that it sent before the abort arrives before the run's next task.
        tokio::spawn(Abortable::new(timer, abort));
    }
}

/// Returns the attempt of a Workflow Task, from its poll response.
pub(super) fn task_attempt(info: &WorkflowTaskInfo) -> WorkflowTaskAttempt {
    WorkflowTaskAttempt {
        started_event_id: info.started_event_id,
        attempt: i32::try_from(info.attempt).unwrap_or(i32::MAX),
        started_time: info.started_time.map(Into::into),
    }
}
