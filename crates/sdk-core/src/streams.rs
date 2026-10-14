//! Streams for lang: one process-wide stream service over the configured store, asking the
//! client's server about streams' owners.

use std::sync::Arc;
use temporalio_client::Connection;
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution,
    enums::v1::StreamOwnerKind,
    stream::v1::StreamReference,
    workflowservice::v1::{
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse, NotifyStreamRequest,
    },
};
use temporalio_streams::{
    NotifierClient, NotifyError, OwnerClient, OwnerDescription, OwnerError, StreamNotification,
};
pub use temporalio_streams::{StreamError, StreamResult, StreamService, StreamStore, proto};
use tonic::IntoRequest;

/// Connects to the store `config` names. The service asks `connection`'s server about streams'
/// owners and sends the stream notifications the config turns on to it.
pub async fn connect_stream_service(
    config: proto::StreamStoreConfig,
    connection: Connection,
) -> StreamResult<Arc<StreamService>> {
    let owner = Arc::new(ConnectionOwnerClient {
        connection: connection.clone(),
    });
    let notifier = Arc::new(ConnectionNotifierClient { connection });
    Ok(Arc::new(
        StreamService::connect_with_notifier(config, owner, Some(notifier)).await?,
    ))
}

/// Sends stream notifications to a Temporal server's stream notifier through a client
/// connection.
struct ConnectionNotifierClient {
    connection: Connection,
}

#[async_trait::async_trait]
impl NotifierClient for ConnectionNotifierClient {
    async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError> {
        self.connection
            .workflow_service()
            .notify_stream(notify_request(notification).into_request())
            .await
            .map(|_| ())
            .map_err(notify_error)
    }
}

fn notify_request(notification: StreamNotification) -> NotifyStreamRequest {
    NotifyStreamRequest {
        namespace: notification.chain.namespace,
        // The run chain's first run keys the notifier, so one notifier serves the stream across
        // Continue-as-New and a new chain on the same Workflow id gets its own.
        stream_ref: Some(StreamReference {
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: notification.chain.workflow_id,
            run_id: notification.chain.first_run_id,
            topic: notification.topic,
        }),
        position: notification.position,
        counter: notification.counter,
        close: notification.close_result.is_some(),
        close_result: notification.close_result,
        ..Default::default()
    }
}

/// Answers that mean the server will never take the notification, so a retry can't help.
fn notify_error(status: tonic::Status) -> NotifyError {
    use tonic::Code;
    match status.code() {
        Code::Unimplemented => NotifyError::Unsupported(status.to_string()),
        Code::FailedPrecondition if namespace_in_transition(&status) => {
            NotifyError::Failed(status.to_string())
        }
        Code::InvalidArgument
        | Code::NotFound
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::FailedPrecondition => NotifyError::Refused(status.to_string()),
        _ => NotifyError::Failed(status.to_string()),
    }
}

/// A namespace that's failing over answers `FailedPrecondition` too, but only until the
/// redirection sees the active cluster, so a close mustn't stop on it.
fn namespace_in_transition(status: &tonic::Status) -> bool {
    use temporalio_common::protos::{
        google::rpc::Status as RpcStatus,
        temporal::api::errordetails::v1::{
            NamespaceInvalidStateFailure, NamespaceNotActiveFailure,
        },
    };
    let Ok(details) = <RpcStatus as prost::Message>::decode(status.details()) else {
        return false;
    };
    details.details.iter().any(|detail| {
        detail.to_msg::<NamespaceNotActiveFailure>().is_ok()
            || detail.to_msg::<NamespaceInvalidStateFailure>().is_ok()
    })
}

/// Asks a Temporal server about owners through a client connection.
struct ConnectionOwnerClient {
    connection: Connection,
}

#[async_trait::async_trait]
impl OwnerClient for ConnectionOwnerClient {
    async fn describe(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        let response = self
            .connection
            .workflow_service()
            .describe_workflow_execution(
                DescribeWorkflowExecutionRequest {
                    namespace: namespace.to_string(),
                    execution: Some(WorkflowExecution {
                        workflow_id: workflow_id.to_string(),
                        run_id: run_id.to_string(),
                    }),
                }
                .into_request(),
            )
            .await
            .map_err(owner_error)?;
        owner_description(response.into_inner())
    }
}

fn owner_error(status: tonic::Status) -> OwnerError {
    if status.code() == tonic::Code::NotFound {
        OwnerError::NotFound(status.message().to_string())
    } else {
        OwnerError::Failed(status.to_string())
    }
}

fn owner_description(
    response: DescribeWorkflowExecutionResponse,
) -> Result<OwnerDescription, OwnerError> {
    let info = response
        .workflow_execution_info
        .ok_or_else(|| OwnerError::Failed("the server described no execution".to_string()))?;
    let run_id = info
        .execution
        .as_ref()
        .map(|execution| execution.run_id.clone())
        .unwrap_or_default();
    // A run that started its chain may leave the first run id unset.
    let first_run_id = if info.first_run_id.is_empty() {
        run_id.clone()
    } else {
        info.first_run_id.clone()
    };
    Ok(OwnerDescription {
        status: info.status(),
        run_id,
        first_run_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::{
        enums::v1::WorkflowExecutionStatus, workflow::v1::WorkflowExecutionInfo,
    };

    fn described(run_id: &str, first_run_id: &str) -> DescribeWorkflowExecutionResponse {
        DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(WorkflowExecutionInfo {
                execution: Some(WorkflowExecution {
                    workflow_id: "wf".to_string(),
                    run_id: run_id.to_string(),
                }),
                first_run_id: first_run_id.to_string(),
                status: WorkflowExecutionStatus::ContinuedAsNew as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_description_names_the_run_its_chain_and_its_status() {
        assert_eq!(
            owner_description(described("run-2", "run-1")).unwrap(),
            OwnerDescription {
                run_id: "run-2".to_string(),
                first_run_id: "run-1".to_string(),
                status: WorkflowExecutionStatus::ContinuedAsNew,
            }
        );
    }

    #[test]
    fn a_run_without_a_first_run_id_starts_its_chain() {
        assert_eq!(
            owner_description(described("run-1", ""))
                .unwrap()
                .first_run_id,
            "run-1"
        );
    }

    #[test]
    fn not_found_says_the_owner_is_gone_and_anything_else_that_the_server_failed() {
        assert!(matches!(
            owner_error(tonic::Status::not_found("no such workflow")),
            OwnerError::NotFound(message) if message == "no such workflow"
        ));
        assert!(matches!(
            owner_error(tonic::Status::unavailable("down")),
            OwnerError::Failed(_)
        ));
        assert!(matches!(
            owner_description(DescribeWorkflowExecutionResponse::default()),
            Err(OwnerError::Failed(_))
        ));
    }

    #[test]
    fn a_notification_names_the_stream_by_its_chains_first_run() {
        let request = notify_request(StreamNotification {
            chain: proto::ChainId {
                namespace: "ns".to_string(),
                workflow_id: "wf".to_string(),
                first_run_id: "run-1".to_string(),
            },
            topic: "tokens".to_string(),
            position: "cursor".to_string(),
            counter: 7,
            close_result: None,
        });
        assert_eq!(request.namespace, "ns");
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: "wf".to_string(),
                run_id: "run-1".to_string(),
                topic: "tokens".to_string(),
            })
        );
        assert_eq!((request.position.as_str(), request.counter), ("cursor", 7));
        assert!(!request.close);
        assert!(request.close_result.is_none());
    }

    #[test]
    fn only_answers_a_retry_cant_change_refuse_a_notification() {
        assert!(matches!(
            notify_error(tonic::Status::new(tonic::Code::Unimplemented, "")),
            NotifyError::Unsupported(_)
        ));
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::FailedPrecondition,
        ] {
            assert!(
                matches!(
                    notify_error(tonic::Status::new(code, "")),
                    NotifyError::Refused(_)
                ),
                "{code:?}"
            );
        }
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::ResourceExhausted,
            tonic::Code::Internal,
        ] {
            assert!(
                matches!(
                    notify_error(tonic::Status::new(code, "")),
                    NotifyError::Failed(_)
                ),
                "{code:?}"
            );
        }
    }

    fn failed_precondition_with<T: prost::Message + prost::Name>(detail: &T) -> tonic::Status {
        let status = temporalio_common::protos::google::rpc::Status {
            code: tonic::Code::FailedPrecondition as i32,
            message: "namespace".to_string(),
            details: vec![prost_types::Any::from_msg(detail).unwrap()],
        };
        tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "namespace",
            prost::Message::encode_to_vec(&status).into(),
        )
    }

    #[test]
    fn a_namespace_in_failover_fails_a_notification_for_now() {
        use temporalio_common::protos::temporal::api::errordetails::v1::{
            NamespaceInvalidStateFailure, NamespaceNotActiveFailure,
        };
        for status in [
            failed_precondition_with(&NamespaceNotActiveFailure::default()),
            failed_precondition_with(&NamespaceInvalidStateFailure::default()),
        ] {
            assert!(matches!(notify_error(status), NotifyError::Failed(_)));
        }
    }
}
