//! What the stream layer asks Temporal about a stream's owner.
//!
//! A store can't see an owner close, so the layer above it asks: to find the chain a stream is
//! keyed by, to refuse a producer once the chain ended, and to end a read. Core implements this
//! over its client, and tests use a fake.

use temporalio_common::protos::temporal::api::enums::v1::{
    ActivityExecutionStatus, WorkflowExecutionStatus,
};

/// The owner of a stream, by kind and id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerId<'a> {
    /// A Workflow id.
    Workflow(&'a str),
    /// A standalone Activity id.
    Activity(&'a str),
}

/// How an owner's run stands, as the stream layer needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerStatus {
    /// The run has not reached a terminal status.
    Running,
    /// The run closed, and its successor in the chain writes on.
    ContinuedAsNew,
    /// The run closed and ended its chain.
    Ended,
}

impl OwnerStatus {
    /// The status of a Workflow run. A paused run is running, since it can resume and write.
    pub fn of_workflow(status: WorkflowExecutionStatus) -> Self {
        use WorkflowExecutionStatus::*;
        match status {
            Completed | Failed | Canceled | Terminated | TimedOut => Self::Ended,
            ContinuedAsNew => Self::ContinuedAsNew,
            Unspecified | Running | Paused => Self::Running,
        }
    }

    /// The status of a standalone Activity run. Retries, backoff, pause and reset keep the run, so
    /// only a terminal status ends it.
    pub fn of_activity(status: ActivityExecutionStatus) -> Self {
        use ActivityExecutionStatus::*;
        match status {
            Completed | Failed | Canceled | Terminated | TimedOut => Self::Ended,
            Unspecified | Running | Paused => Self::Running,
        }
    }
}

/// One run of an owner, as Temporal describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerDescription {
    /// The run described.
    pub run_id: String,
    /// The first run of the chain the run belongs to. Streams are keyed by it. An Activity run is
    /// its own chain.
    pub first_run_id: String,
    /// How the run stands.
    pub status: OwnerStatus,
}

impl OwnerDescription {
    /// Whether the run ended its chain. A run that continued as new did not, since its successor
    /// writes on.
    pub fn chain_ended(&self) -> bool {
        self.status == OwnerStatus::Ended
    }
}

/// Why a describe failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnerError {
    /// Temporal holds no such owner or run, or no longer does.
    #[error("not found: {0}")]
    NotFound(String),
    /// Temporal could not answer.
    #[error("{0}")]
    Failed(String),
}

/// Temporal, as the stream layer asks it about owners.
#[async_trait::async_trait]
pub trait OwnerClient: Send + Sync {
    /// Describes run `run_id` of `owner`, or the owner's latest run when `run_id` is empty.
    async fn describe(
        &self,
        namespace: &str,
        owner: OwnerId<'_>,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError>;
}
