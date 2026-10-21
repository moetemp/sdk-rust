//! Hooks that let a plugin run code at fixed points of the worker's workflow task lifecycle.

use std::{
    fmt::Debug,
    sync::Arc,
    time::{Instant, SystemTime},
};

/// A component that lang passes to the worker through [crate::WorkerConfig::extension].
///
/// Core never inspects the extension beyond the hooks it asks for here.
pub trait WorkerExtension: Send + Sync + 'static {
    /// A name that identifies the extension in logs.
    fn name(&self) -> &str;

    /// The interceptor Core calls around each workflow task completion, if any. Core may ask
    /// more than once per worker, so return the same interceptor each time.
    fn workflow_task_interceptor(&self) -> Option<Arc<dyn WorkflowTaskInterceptor>> {
        None
    }
}

/// Hooks around each `RespondWorkflowTaskCompleted` call that Core makes for a run.
///
/// Core calls the hooks of one run in order, and the next workflow task of that run waits until
/// they return. Core waits at most five seconds on [WorkflowTaskInterceptor::after_complete], so
/// long work such as retries must run in the background. The replay worker never calls the hooks.
///
/// A completion that lang sends waits only on its own run. Core sends the local activity heartbeat
/// completion from within the poll for the next activation, so while its hooks run, that poll
/// gives no activation to other runs.
#[async_trait::async_trait]
pub trait WorkflowTaskInterceptor: Send + Sync + 'static {
    /// Called before every `RespondWorkflowTaskCompleted` for the run. This includes the
    /// completions that have no lang activation, which are the local activity heartbeat
    /// completion and the completion of output buffered before an eviction.
    ///
    /// An error, or a hook that has not returned by `task.deadline`, fails the workflow task
    /// instead of completing it, and the run is evicted.
    async fn before_complete(
        &self,
        task: &WorkflowTaskCompletionInfo,
    ) -> Result<(), WorkflowTaskInterceptError>;

    /// Called after the server answered the completion, or after the call failed.
    ///
    /// `accepted` is false when the call failed, and also when the answer has a
    /// `reset_history_event_id`, which means the server dropped a speculative task.
    async fn after_complete(&self, task: &WorkflowTaskCompletionInfo, accepted: bool);

    /// Called when Core fails a workflow task of the run, whether or not it reports the failure.
    fn on_task_failed(&self, _task: &WorkflowTaskFailedInfo) {}

    /// Called when Core removes the run from its cache. When an eviction sends buffered output,
    /// this comes after [WorkflowTaskInterceptor::after_complete] for that completion.
    fn on_evicted(&self, _run_id: &str) {}
}

/// What Core knows about a workflow task when it is about to complete it.
#[derive(Clone, Debug, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct WorkflowTaskCompletionInfo {
    /// The namespace of the run.
    pub namespace: String,
    /// The workflow id of the run.
    pub workflow_id: String,
    /// The run id.
    pub run_id: String,
    /// The first run id of the run chain.
    pub first_execution_run_id: String,
    /// The workflow type.
    pub workflow_type: String,
    /// The id of the task's `WorkflowTaskStarted` event, from the poll response. The server writes
    /// no events for transient attempts, so attempts after the second of one task share an id.
    pub started_event_id: i64,
    /// The task's attempt, from the poll response.
    pub attempt: u32,
    /// The time the server started the task, from the poll response. Unset if the server did not
    /// send it.
    pub started_time: Option<SystemTime>,
    /// Set when the completion has a command that closes the run. Continue-as-new does not
    /// count.
    pub closes_run: Option<RunCloseKind>,
    /// When the hook must have returned, so that the task does not time out on the server. Core
    /// measures it from the local receipt of the task, with a margin.
    pub deadline: Instant,
}

/// The command with which a completion closes its run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunCloseKind {
    /// `CompleteWorkflowExecution`
    Completed,
    /// `FailWorkflowExecution`. A retry policy can still start a new run in the chain.
    Failed,
    /// `CancelWorkflowExecution`
    Cancelled,
}

/// What Core knows about a workflow task that it fails.
#[derive(Clone, Debug, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct WorkflowTaskFailedInfo {
    /// The run id.
    pub run_id: String,
    /// The attempt of the failed task.
    pub attempt: u32,
    /// True when Core sends `RespondWorkflowTaskFailed`. Core reports only first attempts and
    /// lets later ones time out.
    pub reported: bool,
}

/// The error a [WorkflowTaskInterceptor] returns to fail a workflow task.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct WorkflowTaskInterceptError {
    /// The message that goes into the task failure.
    pub message: String,
}

impl WorkflowTaskInterceptError {
    /// Creates an error with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
