//! Streams for lang: one process-wide stream service over the configured store, asking the
//! client's server about streams' owners.

use std::sync::Arc;
use temporalio_client::Connection;
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution,
    history::v1::HistoryEvent,
    workflowservice::v1::{
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
        GetWorkflowExecutionHistoryReverseRequest,
    },
};
use temporalio_streams::{OwnerClient, OwnerDescription, OwnerError};
pub use temporalio_streams::{StreamError, StreamResult, StreamService, StreamStore, proto};
use tonic::IntoRequest;

/// Connects to the store `config` names. The service asks `connection`'s server about streams'
/// owners, and its [StreamService::store] goes to each Worker's `stream_store`.
pub async fn connect_stream_service(
    config: proto::StreamStoreConfig,
    connection: Connection,
) -> StreamResult<Arc<StreamService>> {
    let owner = Arc::new(ConnectionOwnerClient { connection });
    Ok(Arc::new(StreamService::connect(config, owner).await?))
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

    async fn history_after(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
        floor: i64,
    ) -> Result<Vec<HistoryEvent>, OwnerError> {
        let mut newest_first = vec![];
        let mut page_token = vec![];
        loop {
            let page = self
                .connection
                .workflow_service()
                .get_workflow_execution_history_reverse(
                    GetWorkflowExecutionHistoryReverseRequest {
                        namespace: namespace.to_string(),
                        execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                        next_page_token: page_token,
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map_err(owner_error)?
                .into_inner();
            let reached_floor = take_events_after(
                page.history.map(|h| h.events).unwrap_or_default(),
                floor,
                &mut newest_first,
            );
            page_token = page.next_page_token;
            if reached_floor || page_token.is_empty() {
                break;
            }
        }
        newest_first.reverse();
        Ok(newest_first)
    }
}

/// Keeps the events of a newest-first page that come after `floor`, and says whether the page
/// reached it, so no older page is needed.
fn take_events_after(page: Vec<HistoryEvent>, floor: i64, kept: &mut Vec<HistoryEvent>) -> bool {
    for event in page {
        if event.event_id <= floor {
            return true;
        }
        kept.push(event);
    }
    false
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
        start_time: info.start_time.and_then(|time| time.try_into().ok()),
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
                start_time: None,
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

    fn event(event_id: i64) -> HistoryEvent {
        HistoryEvent {
            event_id,
            ..Default::default()
        }
    }

    #[test]
    fn a_newest_first_page_keeps_the_events_after_the_floor() {
        let mut kept = vec![];
        assert!(!take_events_after(vec![event(9), event(8)], 5, &mut kept));
        assert!(take_events_after(
            vec![event(7), event(6), event(5), event(4)],
            5,
            &mut kept
        ));
        assert_eq!(
            kept.iter().map(|e| e.event_id).collect::<Vec<_>>(),
            [9, 8, 7, 6]
        );
    }

    #[test]
    fn a_description_carries_the_run_start_time() {
        let mut response = described("run-1", "run-1");
        response
            .workflow_execution_info
            .as_mut()
            .unwrap()
            .start_time = Some(std::time::SystemTime::UNIX_EPOCH.into());
        assert_eq!(
            owner_description(response).unwrap().start_time,
            Some(std::time::SystemTime::UNIX_EPOCH)
        );
    }
}
