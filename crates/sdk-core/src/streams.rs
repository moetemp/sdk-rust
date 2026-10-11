//! Streams for lang: one process-wide stream service over the configured store, asking the
//! client's server about streams' owners.

use std::sync::Arc;
use temporalio_client::Connection;
use temporalio_common::protos::temporal::api::{
    common::v1::{WorkflowExecution, callback},
    enums::v1::StreamOwnerKind,
    history::v1::HistoryEvent,
    stream::v1::StreamReference,
    workflowservice::v1::{
        AttachStreamCallbackRequest, DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse, DetachStreamCallbackRequest,
        GetWorkflowExecutionHistoryReverseRequest, NotifyStreamRequest,
    },
};
use temporalio_streams::{
    CallerAttachment, NotifierClient, NotifyError, OwnerClient, OwnerDescription, OwnerError,
    StreamNotification, proto::ChainId,
};
pub use temporalio_streams::{StreamError, StreamResult, StreamService, StreamStore, proto};
use tonic::IntoRequest;

/// Connects to the store `config` names. The service asks `connection`'s server about streams'
/// owners, sends the stream notifications the config turns on to it, and its
/// [StreamService::store] goes to each Worker's `stream_store`.
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

    async fn attach(&self, attachment: CallerAttachment) -> Result<(), NotifyError> {
        self.connection
            .workflow_service()
            .attach_stream_callback(attach_request(attachment).into_request())
            .await
            .map(|_| ())
            .map_err(notify_error)
    }

    async fn detach(
        &self,
        chain: ChainId,
        topic: String,
        request_id: String,
    ) -> Result<(), NotifyError> {
        self.connection
            .workflow_service()
            .detach_stream_callback(detach_request(chain, topic, request_id).into_request())
            .await
            .map(|_| ())
            .map_err(notify_error)
    }
}

fn chain_reference(chain: &ChainId, topic: String) -> StreamReference {
    // The run chain's first run keys the notifier, so one notifier serves the stream across
    // Continue-as-New and a new chain on the same Workflow id gets its own.
    StreamReference {
        owner_kind: StreamOwnerKind::Workflow as i32,
        workflow_id: chain.workflow_id.clone(),
        run_id: chain.first_run_id.clone(),
        topic,
    }
}

fn attach_request(attachment: CallerAttachment) -> AttachStreamCallbackRequest {
    AttachStreamCallbackRequest {
        namespace: attachment.chain.namespace.clone(),
        stream_ref: Some(chain_reference(&attachment.chain, attachment.topic)),
        request_id: attachment.request_id,
        callback: Some(callback::Nexus {
            url: attachment.callback_url,
            header: attachment.callback_headers,
        }),
        operation_token: attachment.operation_token,
        start_time: Some(attachment.start_time.into()),
        ..Default::default()
    }
}

fn detach_request(
    chain: ChainId,
    topic: String,
    request_id: String,
) -> DetachStreamCallbackRequest {
    DetachStreamCallbackRequest {
        namespace: chain.namespace.clone(),
        stream_ref: Some(chain_reference(&chain, topic)),
        request_id,
        ..Default::default()
    }
}

fn notify_request(notification: StreamNotification) -> NotifyStreamRequest {
    NotifyStreamRequest {
        stream_ref: Some(chain_reference(&notification.chain, notification.topic)),
        namespace: notification.chain.namespace,
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

    fn chain() -> ChainId {
        ChainId {
            namespace: "default".to_string(),
            workflow_id: "owner-1".to_string(),
            first_run_id: "chain-1".to_string(),
        }
    }

    #[test]
    fn an_attach_names_the_chains_notifier_and_carries_the_start() {
        let start_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let request = attach_request(CallerAttachment {
            chain: chain(),
            topic: "tokens".to_string(),
            request_id: "attach-1".to_string(),
            callback_url: "temporal://system".to_string(),
            callback_headers: [("nexus-callback-token".to_string(), "abc".to_string())].into(),
            operation_token: "token".to_string(),
            start_time,
        });
        assert_eq!(request.namespace, "default");
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: "owner-1".to_string(),
                run_id: "chain-1".to_string(),
                topic: "tokens".to_string(),
            })
        );
        assert_eq!(request.request_id, "attach-1");
        let callback = request.callback.unwrap();
        assert_eq!(callback.url, "temporal://system");
        assert_eq!(callback.header["nexus-callback-token"], "abc");
        assert_eq!(request.operation_token, "token");
        assert_eq!(request.start_time.unwrap().seconds, 1_700_000_000);
    }

    #[test]
    fn a_detach_names_the_chains_notifier_and_the_attach() {
        let request = detach_request(chain(), "tokens".to_string(), "attach-1".to_string());
        assert_eq!(request.namespace, "default");
        assert_eq!(request.stream_ref.unwrap().run_id, "chain-1");
        assert_eq!(request.request_id, "attach-1");
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

    /// One HTTP request the test listener got: its `Nexus-Operation-State` and its body.
    type Delivery = (String, String);

    /// Answers every request with `200` and hands each one on, so a test sees what the server's
    /// callbacks deliver.
    fn listen(address: &str) -> std::sync::mpsc::Receiver<Delivery> {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind(address).unwrap();
        let (sender, deliveries) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let (mut state, mut length) = (String::new(), 0usize);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("nexus-operation-state:") {
                        state = value.trim().to_string();
                    }
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0; length];
                let _ = reader.read_exact(&mut body);
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
                if sender
                    .send((state, String::from_utf8_lossy(&body).to_string()))
                    .is_err()
                {
                    return;
                }
            }
        });
        deliveries
    }

    fn next_delivery(deliveries: &std::sync::mpsc::Receiver<Delivery>) -> Delivery {
        deliveries
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the server delivered nothing")
    }

    /// Needs a server with the stream notifier and progress on, at `STREAMS_NOTIFIER_SERVER`
    /// (such as `http://127.0.0.1:7861`), whose `callback.allowedAddresses` takes
    /// `127.0.0.1:7899`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_notifier_hears_appends_as_progress_and_the_close_as_the_result() {
        use temporalio_common::protos::temporal::api::{
            common::v1::{Payload, WorkflowType},
            taskqueue::v1::TaskQueue,
            workflowservice::v1::{
                StartWorkflowExecutionRequest, TerminateWorkflowExecutionRequest,
            },
        };
        let Ok(server) = std::env::var("STREAMS_NOTIFIER_SERVER") else {
            eprintln!("set STREAMS_NOTIFIER_SERVER to run the live notifier test");
            return;
        };
        let connection = Connection::connect(
            temporalio_client::ConnectionOptions::new(url::Url::parse(&server).unwrap()).build(),
        )
        .await
        .unwrap();
        let workflow_id = format!("cn-notifier-{}", uuid::Uuid::new_v4());
        // A running owner, which no Worker polls, so the stream stays open.
        let started = connection
            .workflow_service()
            .start_workflow_execution(
                StartWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_id: workflow_id.clone(),
                    workflow_type: Some(WorkflowType {
                        name: "owner".to_string(),
                    }),
                    task_queue: Some(TaskQueue {
                        name: "cn-notifier-nobody".to_string(),
                        ..Default::default()
                    }),
                    request_id: uuid::Uuid::new_v4().to_string(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();
        let deliveries = listen("127.0.0.1:7899");
        let service = connect_stream_service(
            proto::StreamStoreConfig {
                store: Some(proto::stream_store_config::Store::Memory(
                    proto::MemoryStoreConfig {},
                )),
                notify_on_append: true,
                ..Default::default()
            },
            connection.clone(),
        )
        .await
        .unwrap();
        let stream = proto::StreamAddress {
            namespace: "default".to_string(),
            owner_kind: proto::StreamOwnerKind::Workflow as i32,
            workflow_id: workflow_id.clone(),
            run_id: String::new(),
            topic: "tokens".to_string(),
        };
        let append = proto::AppendRequest {
            stream: Some(stream.clone()),
            producer: Some(proto::append_request::Producer::Named(
                proto::NamedProducer {
                    producer_id: "producer".to_string(),
                    attempt: 1,
                },
            )),
            sequence: 1,
            records: vec![proto::AppendRecord {
                kind: proto::StreamRecordKind::Data as i32,
                body: Some(Payload {
                    data: b"token".to_vec(),
                    ..Default::default()
                }),
                content_hash: vec![7; 32],
            }],
            // Any value names the batch, since this producer writes it once.
            digest: vec![9; 32],
        };
        use prost::Message;
        let attached = proto::AttachCallerResponse::decode(
            service
                .call(
                    "AttachCaller",
                    &proto::AttachCallerRequest {
                        stream: Some(stream.clone()),
                        request_id: "caller".to_string(),
                        callback_url: "http://127.0.0.1:7899/callback".to_string(),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let token =
            temporalio_streams::OperationToken::decode("default", &attached.operation_token)
                .unwrap();
        assert_eq!(token.chain_first_run_id, started.run_id);
        service
            .call("Append", &append.encode_to_vec())
            .await
            .unwrap();
        let (state, body) = next_delivery(&deliveries);
        assert_eq!(state, "running", "{body}");
        assert!(body.contains(r#""counter":"1""#), "{body}");

        service
            .call(
                "Close",
                &proto::CloseRequest {
                    stream: Some(stream),
                    result: Some(Payload {
                        metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
                        data: br#""summary""#.to_vec(),
                        ..Default::default()
                    }),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        let (state, body) = loop {
            // A progress retry may still arrive before the completion.
            let delivery = next_delivery(&deliveries);
            if delivery.0 != "running" {
                break delivery;
            }
        };
        assert_eq!(state, "succeeded", "{body}");
        assert!(body.contains("summary"), "{body}");

        connection
            .workflow_service()
            .terminate_workflow_execution(
                TerminateWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id,
                        run_id: started.run_id,
                    }),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();
    }
}
