//! Runs the Stream calls of Workflow code for Core. This API can change in any release, and a Store
//! doesn't need any of it.

mod calls;
mod manager;
mod marker;

pub use calls::{CallError, Phase, StreamCalls};
pub use manager::{Request, StreamManager, StreamResolution};
pub use marker::{is_stream_marker, marker_details, peeked_marker};

use crate::StreamRuntime;
use std::{collections::HashMap, sync::Arc, time::Duration};
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
/// within its deadline, counts as an outage, and the call then tries again with no limit in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamSettings {
    /// The wait after the first failed try, 100 ms by default. Each later wait doubles, and the
    /// actual wait is a random value within 20 % of it. It must be more than 0 and at most
    /// `retry_max_interval`.
    pub retry_initial_interval: Duration,
    /// The maximum wait between two tries before its random part, 2 s by default.
    pub retry_max_interval: Duration,
    /// How long one try waits for the Store, 5 s by default. It must be more than 0.
    pub try_deadline: Duration,
    /// The maximum size of the entries of one marker, 256 KiB by default. It must be more than 0.
    /// The server rejects a marker over its blob size limit, which is 2 MiB by default.
    pub max_marker_entry_bytes: usize,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            retry_initial_interval: Duration::from_millis(100),
            retry_max_interval: Duration::from_secs(2),
            try_deadline: Duration::from_secs(5),
            max_marker_entry_bytes: 256 * 1024,
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
