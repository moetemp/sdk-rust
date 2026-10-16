//! Streams for lang: one process-wide stream service over the configured store, asking the
//! client's server about streams' owners.

use prost::Message;
use std::sync::Arc;
use temporalio_client::Connection;
use temporalio_common::protos::google::rpc::Status as RpcStatus;
use temporalio_common::protos::temporal::api::{
    common::v1::{WorkflowExecution, callback},
    enums::v1::StreamOwnerKind,
    stream::v1::{StreamReference, WorkflowWakeTarget},
    workflowservice::v1::{
        AttachStreamCallbackRequest, DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse, DetachStreamCallbackRequest, NotifyStreamRequest,
        PollStreamNotifierRequest, PollStreamNotifierResponse,
    },
};
use temporalio_streams::{
    CallerAttachment, CloseOutcome, NotifierClient, NotifierPoll, NotifyError, OwnerClient,
    OwnerDescription, OwnerError, StreamNotification, WorkflowAttachment, proto::ChainId,
};
pub use temporalio_streams::{StreamError, StreamResult, StreamService, StreamStore, proto};
use tonic::IntoRequest;

/// Connects to the store `config` names. The service asks `connection`'s server about streams'
/// owners, sends the stream notifications the config turns on to it, and attaches Workflow
/// readers there.
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

    async fn attach_workflow(&self, attachment: WorkflowAttachment) -> Result<(), NotifyError> {
        self.connection
            .workflow_service()
            .attach_stream_callback(attach_request(attachment).into_request())
            .await
            .map(|_| ())
            .map_err(notify_error)
    }

    async fn attach_caller(&self, attachment: CallerAttachment) -> Result<(), NotifyError> {
        self.connection
            .workflow_service()
            .attach_stream_callback(caller_attach_request(attachment).into_request())
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
            .detach_stream_callback(
                DetachStreamCallbackRequest {
                    namespace: chain.namespace.clone(),
                    stream_ref: Some(chain_reference(&chain, topic)),
                    request_id,
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .map(|_| ())
            .map_err(notify_error)
    }

    async fn poll(
        &self,
        chain: ChainId,
        topic: String,
        after_counter: i64,
    ) -> Result<Option<NotifierPoll>, NotifyError> {
        let answer = self
            .connection
            .workflow_service()
            .poll_stream_notifier(
                PollStreamNotifierRequest {
                    namespace: chain.namespace.clone(),
                    stream_ref: Some(chain_reference(&chain, topic)),
                    after_counter,
                    identity: self.connection.identity().to_string(),
                }
                .into_request(),
            )
            .await;
        match answer {
            Ok(response) => Ok(Some(poll_answer(response.into_inner()))),
            // The server answers at its own wait limit, before the client's deadline. A deadline
            // that passes anyway asks for the same re-read.
            Err(status) if status.code() == tonic::Code::DeadlineExceeded => Ok(None),
            Err(status) => Err(notify_error(status)),
        }
    }
}

fn poll_answer(response: PollStreamNotifierResponse) -> NotifierPoll {
    NotifierPoll {
        counter: response.counter,
        position: response.position,
        closed: response.closed,
        close_result: response.close_result,
        close_failure: response.close_failure,
    }
}

/// The notifier of `chain`'s stream on `topic`. The run chain's first run keys the notifier, so one
/// notifier serves the stream across Continue-as-New and a new chain on the same Workflow id gets
/// its own.
fn chain_reference(chain: &ChainId, topic: String) -> StreamReference {
    StreamReference {
        owner_kind: StreamOwnerKind::Workflow as i32,
        workflow_id: chain.workflow_id.clone(),
        run_id: chain.first_run_id.clone(),
        topic,
    }
}

fn attach_request(attachment: WorkflowAttachment) -> AttachStreamCallbackRequest {
    AttachStreamCallbackRequest {
        namespace: attachment.chain.namespace.clone(),
        stream_ref: Some(chain_reference(&attachment.chain, attachment.topic)),
        request_id: attachment.request_id,
        workflow_target: Some(WorkflowWakeTarget {
            workflow_id: attachment.workflow_id,
            run_id: attachment.run_id,
            subscription_id: attachment.subscription_id,
        }),
        ..Default::default()
    }
}

fn caller_attach_request(attachment: CallerAttachment) -> AttachStreamCallbackRequest {
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

fn notify_request(notification: StreamNotification) -> NotifyStreamRequest {
    NotifyStreamRequest {
        stream_ref: Some(chain_reference(&notification.chain, notification.topic)),
        namespace: notification.chain.namespace,
        position: notification.position,
        counter: notification.counter,
        close: notification.close.is_some(),
        close_result: notification.close.as_ref().and_then(CloseOutcome::result),
        close_failure: match notification.close {
            Some(CloseOutcome::Failure(failure)) => Some(failure),
            _ => None,
        },
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
    // A missing namespace says nothing about the Workflow. A failover or a namespace being
    // set up answers it for a while, and a chain must not close on it.
    if status.code() == tonic::Code::NotFound && !namespace_not_found(&status) {
        OwnerError::NotFound(status.message().to_string())
    } else {
        OwnerError::Failed(status.to_string())
    }
}

fn namespace_not_found(status: &tonic::Status) -> bool {
    RpcStatus::decode(status.details()).is_ok_and(|details| {
        details.details.iter().any(|detail| {
            detail
                .type_url
                .ends_with("/temporal.api.errordetails.v1.NamespaceNotFoundFailure")
        })
    })
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
    fn a_missing_namespace_is_a_failure_not_a_gone_owner() {
        let details = RpcStatus {
            code: tonic::Code::NotFound as i32,
            message: "namespace ns not found".to_string(),
            details: vec![prost_types::Any {
                type_url:
                    "type.googleapis.com/temporal.api.errordetails.v1.NamespaceNotFoundFailure"
                        .to_string(),
                value: Vec::new(),
            }],
        };
        let status = tonic::Status::with_details(
            tonic::Code::NotFound,
            "namespace ns not found",
            details.encode_to_vec().into(),
        );
        assert!(matches!(owner_error(status), OwnerError::Failed(_)));
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
            close: None,
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
    fn a_caller_attach_names_the_chains_notifier_and_carries_the_start() {
        let start_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let request = caller_attach_request(CallerAttachment {
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
        assert!(request.workflow_target.is_none(), "exactly one target");
    }

    #[test]
    fn an_attach_names_the_chains_notifier_and_wakes_the_reader() {
        let request = attach_request(WorkflowAttachment {
            chain: proto::ChainId {
                namespace: "ns".to_string(),
                workflow_id: "owner".to_string(),
                first_run_id: "chain-1".to_string(),
            },
            topic: "tokens".to_string(),
            request_id: "jetty/reader-run/1".to_string(),
            workflow_id: "reader".to_string(),
            run_id: "reader-run".to_string(),
            subscription_id: "1".to_string(),
        });
        assert_eq!(request.namespace, "ns");
        assert_eq!(
            request.stream_ref,
            Some(StreamReference {
                owner_kind: StreamOwnerKind::Workflow as i32,
                workflow_id: "owner".to_string(),
                run_id: "chain-1".to_string(),
                topic: "tokens".to_string(),
            })
        );
        assert_eq!(request.request_id, "jetty/reader-run/1");
        assert_eq!(
            request.workflow_target,
            Some(WorkflowWakeTarget {
                workflow_id: "reader".to_string(),
                run_id: "reader-run".to_string(),
                subscription_id: "1".to_string(),
            })
        );
        assert!(request.callback.is_none(), "exactly one target");
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

    /// One HTTP request the test listener got: its `Nexus-Operation-State` and its body.
    type Delivery = (String, String);

    /// Answers every request on `127.0.0.1:7899` with `200` and hands each one on, so a test sees
    /// what the server's callbacks deliver. One listener serves every live test, since the
    /// address can't be bound twice.
    fn deliveries() -> &'static std::sync::Mutex<std::sync::mpsc::Receiver<Delivery>> {
        static DELIVERIES: std::sync::OnceLock<
            std::sync::Mutex<std::sync::mpsc::Receiver<Delivery>>,
        > = std::sync::OnceLock::new();
        DELIVERIES.get_or_init(|| std::sync::Mutex::new(listen("127.0.0.1:7899")))
    }

    fn listen(address: &str) -> std::sync::mpsc::Receiver<Delivery> {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind(address).unwrap();
        let (sender, deliveries) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let (mut state, mut length, mut chunked) = (String::new(), 0usize, false);
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
                    if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
                        chunked = true;
                    }
                }
                let mut body = vec![];
                if chunked {
                    // A failed completion streams its failure in chunks.
                    loop {
                        let mut size = String::new();
                        if reader.read_line(&mut size).unwrap_or(0) == 0 {
                            break;
                        }
                        let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
                        let mut chunk = vec![0; size + 2];
                        let _ = reader.read_exact(&mut chunk);
                        if size == 0 {
                            break;
                        }
                        body.extend_from_slice(&chunk[..size]);
                    }
                } else {
                    body.resize(length, 0);
                    let _ = reader.read_exact(&mut body);
                }
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

    fn next_delivery() -> Delivery {
        deliveries()
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the server delivered nothing")
    }

    /// A running owner Workflow that no Worker polls, a caller attached to its stream on
    /// `tokens`, and a stream service that notifies through the live server.
    struct Live {
        connection: Connection,
        service: Arc<StreamService>,
        stream: proto::StreamAddress,
        workflow_id: String,
        run_id: String,
        /// The chain the caller's operation token names.
        token_first_run_id: String,
        // The live tests share the callback listener's address.
        _turn: tokio::sync::MutexGuard<'static, ()>,
    }

    static LIVE_TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Needs a server with the stream notifier and progress on, at `STREAMS_NOTIFIER_SERVER`
    /// (such as `http://127.0.0.1:7861`), whose `callback.allowedAddresses` takes
    /// `127.0.0.1:7899`. Answers `None` without one.
    async fn live() -> Option<Live> {
        use temporalio_common::protos::temporal::api::{
            common::v1::WorkflowType, taskqueue::v1::TaskQueue,
            workflowservice::v1::StartWorkflowExecutionRequest,
        };
        let Ok(server) = std::env::var("STREAMS_NOTIFIER_SERVER") else {
            eprintln!("set STREAMS_NOTIFIER_SERVER to run the live notifier test");
            return None;
        };
        let turn = LIVE_TURN.lock().await;
        let connection = Connection::connect(
            temporalio_client::ConnectionOptions::new(url::Url::parse(&server).unwrap()).build(),
        )
        .await
        .unwrap();
        let workflow_id = format!("cn-notifier-{}", uuid::Uuid::new_v4());
        // No Worker polls the owner, so the stream stays open.
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
        // An earlier live test may have left deliveries behind.
        while deliveries().lock().unwrap().try_recv().is_ok() {}
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
        Some(Live {
            connection,
            service,
            stream,
            workflow_id,
            run_id: started.run_id,
            token_first_run_id: token.chain_first_run_id,
            _turn: turn,
        })
    }

    impl Live {
        async fn append_one(&self) {
            use prost::Message;
            let append = proto::AppendRequest {
                stream: Some(self.stream.clone()),
                producer: Some(proto::append_request::Producer::Named(
                    proto::NamedProducer {
                        producer_id: "producer".to_string(),
                        attempt: 1,
                    },
                )),
                sequence: 1,
                records: vec![proto::AppendRecord {
                    kind: proto::StreamRecordKind::Data as i32,
                    body: Some(
                        temporalio_common::protos::temporal::api::common::v1::Payload {
                            data: b"token".to_vec(),
                            ..Default::default()
                        },
                    ),
                    content_hash: vec![7; 32],
                }],
                // Any value names the batch, since this producer writes it once.
                digest: vec![9; 32],
            };
            self.service
                .call("Append", &append.encode_to_vec())
                .await
                .unwrap();
            let (state, body) = next_delivery();
            assert_eq!(state, "running", "{body}");
            assert!(body.contains(r#""counter":"1""#), "{body}");
        }

        async fn close(&self, close: proto::CloseRequest) -> Delivery {
            use prost::Message;
            self.service
                .call("Close", &close.encode_to_vec())
                .await
                .unwrap();
            loop {
                // A progress retry may still arrive before the completion.
                let delivery = next_delivery();
                if delivery.0 != "running" {
                    return delivery;
                }
            }
        }

        async fn end(self) {
            use temporalio_common::protos::temporal::api::workflowservice::v1 as service;
            self.connection
                .workflow_service()
                .terminate_workflow_execution(
                    service::TerminateWorkflowExecutionRequest {
                        namespace: "default".to_string(),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: self.workflow_id,
                            run_id: self.run_id,
                        }),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_notifier_hears_appends_as_progress_and_the_close_as_the_result() {
        use temporalio_common::protos::temporal::api::common::v1::Payload;
        let Some(live) = live().await else { return };
        assert_eq!(live.token_first_run_id, live.run_id);
        live.append_one().await;
        let (state, body) = live
            .close(proto::CloseRequest {
                stream: Some(live.stream.clone()),
                result: Some(Payload {
                    metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
                    data: br#""summary""#.to_vec(),
                    ..Default::default()
                }),
                failure: None,
                closed_by: String::new(),
            })
            .await;
        assert_eq!(state, "succeeded", "{body}");
        assert!(body.contains("summary"), "{body}");
        live.end().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_close_with_a_failure_fails_the_callers() {
        use temporalio_common::protos::temporal::api::{
            failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
            workflowservice::v1::DescribeStreamNotifierRequest,
        };
        let Some(live) = live().await else { return };
        live.append_one().await;
        let (state, body) = live
            .close(proto::CloseRequest {
                stream: Some(live.stream.clone()),
                result: None,
                failure: Some(Failure {
                    message: "the agent gave up".to_string(),
                    failure_info: Some(FailureInfo::ApplicationFailureInfo(
                        ApplicationFailureInfo {
                            r#type: "AgentGaveUp".to_string(),
                            non_retryable: true,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                closed_by: String::new(),
            })
            .await;
        assert_eq!(state, "failed", "{body}");
        assert!(body.contains("the agent gave up"), "{body}");
        let described = live
            .connection
            .workflow_service()
            .describe_stream_notifier(
                DescribeStreamNotifierRequest {
                    namespace: "default".to_string(),
                    stream_ref: Some(StreamReference {
                        owner_kind: StreamOwnerKind::Workflow as i32,
                        workflow_id: live.workflow_id.clone(),
                        run_id: live.run_id.clone(),
                        topic: "tokens".to_string(),
                    }),
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();
        assert!(described.closed);
        assert_eq!(
            described.close_failure.map(|failure| failure.message),
            Some("the agent gave up".to_string())
        );
        live.end().await;
    }
}
