//! Runs the Stream calls of Workflow code for Core. This API can change in any release, and a Store
//! doesn't need any of it.

mod calls;
mod digest;
mod manager;
mod marker;
mod reader;
mod replay;

pub use calls::{CallError, Phase, StreamCalls};
pub use manager::{Request, StreamManager, StreamResolution};
pub use marker::{is_stream_marker, marker_details, peeked_marker};
pub use replay::{MarkerReads, ReplayError, ReplayReader, ReplayReads};

use crate::StreamRuntime;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::stream::v1::WorkflowTaskAttempt;
use tokio::sync::watch;

/// The Stream runtimes of a Worker, keyed by the name of the Store that each one serves, with the
/// settings of its Stream calls.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes {
    runtimes: Arc<HashMap<String, Arc<StreamRuntime>>>,
    /// The settings of the Stream calls.
    pub settings: StreamSettings,
}

impl StreamRuntimes {
    /// Creates the runtimes, keyed by Store name, with the default settings.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: Arc::new(runtimes.into_iter().collect()),
            settings: StreamSettings::default(),
        }
    }

    /// Returns the runtime of the Store `store`.
    pub fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }

    /// Returns true if there's no runtime, so Workflow code on this Worker can't call a Store.
    pub fn is_empty(&self) -> bool {
        self.runtimes.is_empty()
    }
}

/// The settings of a Worker's Stream calls. A try that fails with
/// [`AnyReason::StoreUnavailable`](crate::AnyReason::StoreUnavailable), or gets no answer
/// within its deadline, counts as an outage. A live call then tries again with no limit in time,
/// and a replay read stops at `replay_retry_bound`.
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
    /// The maximum size of the entries of one marker, 256 KiB by default. It must be more than 0.
    /// The server rejects a marker over its blob size limit, which is 2 MiB by default.
    pub max_marker_entry_bytes: usize,
    /// The maximum size of the Records in one page of a read, which is all that waits in Core for
    /// the next activation, 4 MiB by default. It must be more than 0.
    pub read_ahead_bytes: usize,
    /// The maximum number of Records in one page of a read, 256 by default. It must be 1 or more.
    pub read_page_records: usize,
    /// The maximum time that one read asks the Store to wait for a Record, 60 s by default. It must
    /// be more than 0. The read asks again after that, so a Store that stops answering counts as an
    /// outage within this wait and `try_deadline`.
    pub read_max_wait: Duration,
    /// The minimum time between two pages of a read, so that Records which come one at a time reach
    /// Workflow code in fewer activations. It's zero by default, which is no gap.
    pub read_min_gap: Duration,
    /// The maximum size of the Records that one Workflow Task gives Workflow code from all of its
    /// reads, 16 MiB by default. It must be more than 0. A replay reads digested Records again in
    /// parts of this size by the bytes that History recorded. One marker over it still makes a
    /// part of its own.
    pub max_task_record_bytes: usize,
    /// The maximum size of the Records that one marker holds for reads with `records_in_history`, 1
    /// MiB by default. It must be more than 0. One larger Record still fits as the marker's first.
    /// The server rejects a marker over its blob size limit, which is 2 MiB by default.
    pub max_marker_record_bytes: usize,
    /// How long a replay goes on trying a Store after its first try of that Store fails, each time
    /// it reads a part of the Records of History, 3 s by default. No try or wait goes past that
    /// time, and zero is one try. The first try can take `try_deadline` to fail, so the two
    /// together must stay under the Workflow Task timeout.
    pub replay_retry_bound: Duration,
    /// The maximum number of read entries whose Records a replay reads at the same time, 8 by
    /// default. It must be 1 or more.
    pub replay_concurrency: usize,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            retry_initial_interval: Duration::from_millis(100),
            retry_max_interval: Duration::from_secs(2),
            try_deadline: Duration::from_secs(5),
            max_marker_entry_bytes: 256 * 1024,
            read_ahead_bytes: 4 * 1024 * 1024,
            read_page_records: 256,
            read_max_wait: Duration::from_secs(60),
            read_min_gap: Duration::ZERO,
            max_task_record_bytes: 16 * 1024 * 1024,
            max_marker_record_bytes: 1024 * 1024,
            replay_retry_bound: Duration::from_secs(3),
            replay_concurrency: 8,
        }
    }
}

impl StreamSettings {
    /// Checks the lower bounds of the settings. Nothing bounds a setting from above.
    pub fn check(&self) -> Result<(), String> {
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
            ("max_marker_entry_bytes", self.max_marker_entry_bytes == 0),
            ("read_ahead_bytes", self.read_ahead_bytes == 0),
            ("read_page_records", self.read_page_records == 0),
            ("read_max_wait", self.read_max_wait.is_zero()),
            ("max_task_record_bytes", self.max_task_record_bytes == 0),
            ("max_marker_record_bytes", self.max_marker_record_bytes == 0),
            ("replay_concurrency", self.replay_concurrency == 0),
        ];
        if let Some((setting, _)) = zeros.iter().find(|(_, zero)| *zero) {
            return Err(format!(
                "The Stream setting `{setting}` must be more than 0"
            ));
        }
        Ok(())
    }
}

/// The Workflow Task that a run has in hand, as its Stream calls see it.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum TaskState {
    /// No task is in hand, so no call starts and no result reaches Workflow code.
    #[default]
    NoTask,
    /// Calls start, and a call that Workflow code makes takes the attempt of the task.
    Open(WorkflowTaskAttempt),
    /// The run completes the task at its next chance. No call starts, and no live result reaches
    /// Workflow code, before the next task.
    Completing(WorkflowTaskAttempt),
}

/// The one place that holds a run's [`TaskState`].
#[derive(Debug)]
struct RunGate(watch::Sender<TaskState>);

impl RunGate {
    fn new() -> Self {
        Self(watch::Sender::new(TaskState::NoTask))
    }

    fn set(&self, state: TaskState) {
        self.0.send_replace(state);
    }

    fn is_open(&self) -> bool {
        matches!(*self.0.borrow(), TaskState::Open(_))
    }

    fn attempt(&self) -> Option<WorkflowTaskAttempt> {
        match *self.0.borrow() {
            TaskState::Open(attempt) | TaskState::Completing(attempt) => Some(attempt),
            TaskState::NoTask => None,
        }
    }

    /// Returns a receiver for a reader, which wakes it each time the task state changes.
    fn subscribe(&self) -> watch::Receiver<TaskState> {
        self.0.subscribe()
    }
}

/// The bytes of Records that the task in hand can still give Workflow code, and that its marker
/// can still hold for reads with `records_in_history`. The run is its one writer, and its readers
/// only read it.
#[derive(Debug, Default)]
pub(crate) struct TaskRoom {
    task: AtomicUsize,
    marker: AtomicUsize,
}

impl TaskRoom {
    fn set(&self, (task, marker): (usize, usize)) {
        self.task.store(task, Ordering::Release);
        self.marker.store(marker, Ordering::Release);
    }

    /// Returns the bytes of Records that the task, and then its marker, can still take.
    fn get(&self) -> (usize, usize) {
        let task = self.task.load(Ordering::Acquire);
        (task, self.marker.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_try_deadline_is_refused() {
        let settings = StreamSettings {
            try_deadline: Duration::ZERO,
            ..Default::default()
        };
        let error = "The Stream setting `try_deadline` must be more than 0";
        assert_eq!(settings.check(), Err(error.to_string()));
        assert_eq!(StreamSettings::default().check(), Ok(()));
    }
}
