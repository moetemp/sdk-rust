//! Hooks that let a plugin run code at fixed points of the worker's workflow task lifecycle.

use std::{
    fmt::Debug,
    sync::Arc,
    time::{Instant, SystemTime},
};

/// A component that lang passes to the worker through [crate::WorkerConfig::extension].
///
/// Core never inspects the extension beyond the hooks it asks for here. Core never calls
/// [WorkerExtension::call] or [WorkerExtension::call_async]. A bridge exposes them to lang, so lang
/// code reaches the extension in the same native library without a bridge function of its own.
#[async_trait::async_trait]
pub trait WorkerExtension: Send + Sync + 'static {
    /// A name that identifies the extension in logs.
    fn name(&self) -> &str;

    /// The interceptor Core calls around each workflow task completion, if any. Core may ask
    /// more than once per worker, so return the same interceptor each time.
    fn workflow_task_interceptor(&self) -> Option<Arc<dyn WorkflowTaskInterceptor>> {
        None
    }

    /// Runs `method` with encoded input and returns the encoded output. It must not block, since
    /// lang may call it from workflow code.
    fn call(&self, method: &str, _input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
        Err(ExtensionError::unknown_method(method))
    }

    /// Like [WorkerExtension::call], for work that waits on I/O.
    async fn call_async(&self, method: String, _input: Vec<u8>) -> Result<Vec<u8>, ExtensionError> {
        Err(ExtensionError::unknown_method(&method))
    }
}

/// The error of a [WorkerExtension] call. It has the fields of a gRPC status, so a bridge can turn
/// it into the same lang error as a failed service call.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{code:?}: {message}")]
#[non_exhaustive]
pub struct ExtensionError {
    /// The status code.
    pub code: tonic::Code,
    /// The message for the caller.
    pub message: String,
    /// Encoded error details, empty if there are none.
    pub details: Vec<u8>,
}

impl ExtensionError {
    /// Creates an error with no details.
    pub fn new(code: tonic::Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: vec![],
        }
    }

    /// Adds encoded error details.
    pub fn with_details(mut self, details: Vec<u8>) -> Self {
        self.details = details;
        self
    }

    /// The error for a method the extension does not have.
    pub fn unknown_method(method: &str) -> Self {
        Self::new(
            tonic::Code::Unimplemented,
            format!("Unknown extension method {method}"),
        )
    }
}

/// Hooks around each `RespondWorkflowTaskCompleted` call that Core makes for a run.
///
/// Core calls the hooks of one run in order, and the next workflow task of that run waits until
/// they return. Other runs are not blocked. The replay worker never calls them.
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
    /// The id of the task's `WorkflowTaskStarted` event, from the poll response. A transient
    /// attempt has no such event yet, so it shares the id with the earlier attempts.
    pub started_event_id: i64,
    /// The task's attempt, from the poll response.
    pub attempt: u32,
    /// The time the server started the task, from the poll response. Unset if the server did not
    /// send it.
    pub started_time: Option<SystemTime>,
    /// The last task of the run that completed, from the poll response's
    /// `previous_started_event_id` and that task's events. Unset before the first completion.
    pub previous_completed: Option<WorkflowTaskEpoch>,
    /// The started event ids of the tasks that History shows as failed or timed out since
    /// `previous_completed`, oldest first. Attempts after the first that fail leave no event, so
    /// they are not here.
    #[builder(default)]
    pub failed_tasks: Vec<i64>,
    /// Set when the completion has a command that closes the run. Continue-as-new does not
    /// count.
    pub closes_run: Option<RunCloseKind>,
    /// When the hook must have returned, so that the task does not time out on the server. Core
    /// measures it from the local receipt of the task, with a margin.
    pub deadline: Instant,
}

/// One attempt of a workflow task, as History records it.
#[derive(Clone, Debug, PartialEq, Eq, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowTaskEpoch {
    /// The id of the task's `WorkflowTaskStarted` event.
    pub started_event_id: i64,
    /// The attempt of the task's `WorkflowTaskScheduled` event. Zero if Core no longer has it.
    pub attempt: u32,
    /// The time of the task's `WorkflowTaskStarted` event.
    pub started_time: Option<SystemTime>,
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

#[cfg(test)]
mod tests {
    use super::*;

    struct Named;

    impl WorkerExtension for Named {
        fn name(&self) -> &str {
            "named"
        }
    }

    #[tokio::test]
    async fn calls_to_an_extension_without_methods_are_unimplemented() {
        let extension: Arc<dyn WorkerExtension> = Arc::new(Named);
        let err = extension.call("Append", b"x").unwrap_err();
        assert_eq!(err.code, tonic::Code::Unimplemented);
        assert!(err.message.contains("Append"));
        let err = extension
            .call_async("Append".to_string(), vec![])
            .await
            .unwrap_err();
        assert_eq!(err.code, tonic::Code::Unimplemented);
    }

    struct Echo;

    #[async_trait::async_trait]
    impl WorkerExtension for Echo {
        fn name(&self) -> &str {
            "echo"
        }

        fn call(&self, method: &str, input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
            match method {
                "Echo" => Ok(input.to_vec()),
                _ => {
                    Err(ExtensionError::new(tonic::Code::NotFound, "no stream")
                        .with_details(vec![7]))
                }
            }
        }

        async fn call_async(
            &self,
            method: String,
            input: Vec<u8>,
        ) -> Result<Vec<u8>, ExtensionError> {
            self.call(&method, &input)
        }
    }

    #[tokio::test]
    async fn calls_reach_the_extension_methods() {
        let extension: Arc<dyn WorkerExtension> = Arc::new(Echo);
        assert_eq!(extension.call("Echo", b"abc").unwrap(), b"abc");
        assert_eq!(
            extension
                .call_async("Echo".to_string(), b"abc".to_vec())
                .await
                .unwrap(),
            b"abc"
        );
        let err = extension.call("Read", b"").unwrap_err();
        assert_eq!(err.code, tonic::Code::NotFound);
        assert_eq!(err.details, [7]);
    }
}
