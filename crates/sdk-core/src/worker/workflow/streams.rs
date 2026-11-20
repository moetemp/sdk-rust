//! Connects the Stream calls of Workflow code to Core. Their results come in as local inputs, their
//! errors become machine errors, and each call takes the attempt of a Workflow Task.

use crate::{
    WorkerConfig,
    worker::workflow::{
        LocalResolution, LocalResolutionMsg, WFMachinesError, WorkflowTaskInfo,
        workflow_stream::{LocalInput, LocalInputs},
    },
};
use futures_util::future::{AbortRegistration, Abortable};
use std::time::Instant;
use temporalio_common::protos::temporal::api::stream::v1::WorkflowTaskAttempt;
use temporalio_streams::workflow::{CallError, StreamManager};
use tokio::{runtime::Handle, sync::mpsc::UnboundedSender};
use tracing::Span;

/// The Stream manager of a Worker, with the channel of local inputs that it and a heartbeat timer
/// send to.
pub(super) struct Streams {
    pub(super) manager: StreamManager,
    local_tx: UnboundedSender<LocalInput>,
}

impl Streams {
    /// Creates the manager. It runs the calls on the runtime of the caller, which is the Worker's.
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

    /// Sends a heartbeat timeout for the run at `deadline`. A Worker uses this when it has no Local
    /// Activity manager to run the timer. It must be called on the thread of the runs.
    pub(super) fn heartbeat_at(&self, run_id: String, deadline: Instant, abort: AbortRegistration) {
        let local_tx = self.local_tx.clone();
        let span = Span::current();
        let timer = async move {
            tokio::time::sleep_until(deadline.into()).await;
            let input = LocalInputs::HeartbeatTimeout(run_id);
            let _ = local_tx.send(LocalInput { input, span });
        };
        // The timer runs on the thread of the runs, so a timer that the run aborted sends nothing.
        // A timeout that it sent before the abort reaches the run ahead of the run's next task.
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

impl From<CallError> for WFMachinesError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Nondeterminism(message) => WFMachinesError::Nondeterminism(message),
            CallError::Fatal(message) => WFMachinesError::Fatal(message),
            CallError::Unserved(message) => WFMachinesError::Stream(message),
            CallError::Gone(message) => WFMachinesError::StreamReplay(message),
        }
    }
}
