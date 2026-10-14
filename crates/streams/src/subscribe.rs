//! Workflow readers that sleep on a stream's notifier.
//!
//! A Workflow's reader attaches to the stream's notifier before its first read, so the notifier
//! wakes the Workflow on its next Workflow Task when the stream moves. The attach names the reader
//! by its run and its subscription id, which replay gives back, so a rerun attaches as the same
//! reader and the server keeps one callback.

use crate::{
    NotifierClient, NotifyError, StreamError, StreamResult, StreamStore, Streams,
    WorkflowAttachment,
    handle::address,
    proto::{
        StreamAddress, SubscribeRequest, SubscribeResponse, UnsubscribeRequest, UnsubscribeResponse,
    },
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// The longest subscription id the server takes.
const MAX_SUBSCRIPTION_ID_BYTES: usize = 256;

/// The subscriptions this process attached, so a reader's later reads send no attach.
#[derive(Default)]
pub(crate) struct Subscriptions {
    client: Option<Arc<dyn NotifierClient>>,
    attached: Mutex<HashMap<SubscriptionKey, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubscriptionKey {
    namespace: String,
    owner_workflow_id: String,
    owner_run_id: String,
    topic: String,
    request_id: String,
}

impl SubscriptionKey {
    fn new(stream: &StreamAddress, request_id: &str) -> Self {
        Self {
            namespace: stream.namespace.clone(),
            owner_workflow_id: stream.workflow_id.clone(),
            owner_run_id: stream.run_id.clone(),
            topic: stream.topic.clone(),
            request_id: request_id.to_string(),
        }
    }
}

/// The request id of a reader's attach. The server dedupes attaches by it, so it depends only on
/// what replay gives back.
fn request_id(run_id: &str, subscription_id: &str) -> String {
    format!("jetty/{run_id}/{subscription_id}")
}

fn check_subscription(run_id: &str, subscription_id: &str) -> StreamResult<()> {
    if run_id.is_empty() || subscription_id.is_empty() {
        return Err(StreamError::invalid_argument(
            "a subscription needs the reader's run id and its subscription id",
        ));
    }
    if subscription_id.len() > MAX_SUBSCRIPTION_ID_BYTES {
        return Err(StreamError::invalid_argument(format!(
            "a subscription id is at most {MAX_SUBSCRIPTION_ID_BYTES} bytes, not {}",
            subscription_id.len()
        )));
    }
    Ok(())
}

fn subscribe_error(error: NotifyError) -> StreamError {
    match error {
        NotifyError::Refused(message) => StreamError::refused(message),
        NotifyError::Unsupported(message) => StreamError::unsupported(message),
        NotifyError::Failed(message) => StreamError::storage(message),
    }
}

impl Subscriptions {
    pub(crate) fn new(client: Option<Arc<dyn NotifierClient>>) -> Self {
        Self {
            client,
            attached: Mutex::default(),
        }
    }

    fn client(&self) -> StreamResult<&Arc<dyn NotifierClient>> {
        self.client.as_ref().ok_or_else(|| {
            StreamError::unsupported("a Workflow reader needs a client connected to the server")
        })
    }

    /// Attaches the reader once per process. On an empty stream the attach also arms the stream,
    /// in the same store step that finds it empty, so the first append wakes the Workflow.
    pub(crate) async fn subscribe(
        &self,
        streams: &Streams,
        store: &dyn StreamStore,
        request: SubscribeRequest,
    ) -> StreamResult<SubscribeResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.workflow_id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a subscription needs the reader's Workflow id",
            ));
        }
        check_subscription(&request.run_id, &request.subscription_id)?;
        let request_id = request_id(&request.run_id, &request.subscription_id);
        let key = SubscriptionKey::new(stream, &request_id);
        if let Some(first_run_id) = self.attached.lock().unwrap().get(&key) {
            return Ok(SubscribeResponse {
                chain_first_run_id: first_run_id.clone(),
                attached: false,
            });
        }
        let client = self.client()?;
        // Fresh for the latest run, since the Workflow id may have moved on to a new chain since a
        // read warmed the cache, and an attach to the ended chain would never wake the reader.
        let chain = streams.chain_now(stream).await?;
        client
            .attach_workflow(WorkflowAttachment {
                chain: chain.clone(),
                topic: stream.topic.clone(),
                request_id,
                workflow_id: request.workflow_id,
                run_id: request.run_id,
                subscription_id: request.subscription_id,
            })
            .await
            .map_err(subscribe_error)?;
        // The reader's first read arms an empty stream too, so a failed arm costs nothing.
        if let Err(error) = store.newest_or_arm(&chain, &stream.topic).await {
            tracing::warn!(
                workflow_id = %chain.workflow_id,
                topic = %stream.topic,
                %error,
                "Could not arm the stream a Workflow reader subscribed to"
            );
        }
        self.attached
            .lock()
            .unwrap()
            .insert(key, chain.first_run_id.clone());
        Ok(SubscribeResponse {
            chain_first_run_id: chain.first_run_id,
            attached: true,
        })
    }

    /// Detaches the reader from the chain it attached to, or from the chain the stream names now
    /// when this process never attached it.
    pub(crate) async fn unsubscribe(
        &self,
        streams: &Streams,
        request: UnsubscribeRequest,
    ) -> StreamResult<UnsubscribeResponse> {
        let stream = address(request.stream.as_ref())?;
        check_subscription(&request.run_id, &request.subscription_id)?;
        let request_id = request_id(&request.run_id, &request.subscription_id);
        let client = self.client()?;
        let key = SubscriptionKey::new(stream, &request_id);
        let known = self.attached.lock().unwrap().get(&key).cloned();
        let chain = match known {
            Some(first_run_id) => crate::proto::ChainId {
                namespace: stream.namespace.clone(),
                workflow_id: stream.workflow_id.clone(),
                first_run_id,
            },
            None => streams.chain_now(stream).await?,
        };
        client
            .detach(chain, stream.topic.clone(), request_id)
            .await
            .map_err(subscribe_error)?;
        self.attached.lock().unwrap().remove(&key);
        Ok(UnsubscribeResponse {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStore, OwnerClient, OwnerDescription, OwnerError, StreamNotification, StreamService,
        StreamsOptions,
        proto::{ChainId, StoreAppendRequest, StreamFailureKind, StreamOwnerKind},
    };
    use prost::Message;
    use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;

    /// The owner's chain is `first_run_id`, which a test moves to a new chain.
    struct Owner(Mutex<String>);

    #[async_trait::async_trait]
    impl OwnerClient for Owner {
        async fn describe(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            let first_run_id = self.0.lock().unwrap().clone();
            Ok(OwnerDescription {
                run_id: first_run_id.clone(),
                first_run_id,
                status: WorkflowExecutionStatus::Running,
            })
        }
    }

    /// Records attaches and detaches, and answers each with `answer`.
    #[derive(Default)]
    struct Server {
        attached: Mutex<Vec<WorkflowAttachment>>,
        detached: Mutex<Vec<(ChainId, String, String)>>,
        answer: Mutex<Option<NotifyError>>,
    }

    #[async_trait::async_trait]
    impl NotifierClient for Server {
        async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
            Ok(())
        }

        async fn attach_workflow(&self, attachment: WorkflowAttachment) -> Result<(), NotifyError> {
            if let Some(error) = self.answer.lock().unwrap().clone() {
                return Err(error);
            }
            self.attached.lock().unwrap().push(attachment);
            Ok(())
        }

        async fn detach(
            &self,
            chain: ChainId,
            topic: String,
            request_id: String,
        ) -> Result<(), NotifyError> {
            self.detached
                .lock()
                .unwrap()
                .push((chain, topic, request_id));
            Ok(())
        }
    }

    struct Setup {
        service: StreamService,
        store: Arc<MemoryStore>,
        owner: Arc<Owner>,
        server: Arc<Server>,
    }

    fn setup() -> Setup {
        let store = Arc::new(MemoryStore::new());
        let owner = Arc::new(Owner(Mutex::new("chain-1".to_string())));
        let server = Arc::new(Server::default());
        let service = StreamService::new(store.clone(), owner.clone(), StreamsOptions::default())
            .with_subscriptions(server.clone());
        Setup {
            service,
            store,
            owner,
            server,
        }
    }

    fn stream() -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "owner".to_string(),
            run_id: String::new(),
            topic: "tokens".to_string(),
        }
    }

    fn subscribe_request(subscription_id: &str) -> SubscribeRequest {
        SubscribeRequest {
            stream: Some(stream()),
            workflow_id: "reader".to_string(),
            run_id: "reader-run".to_string(),
            subscription_id: subscription_id.to_string(),
        }
    }

    async fn subscribe(
        service: &StreamService,
        request: SubscribeRequest,
    ) -> Result<SubscribeResponse, crate::proto::StreamFailure> {
        service
            .call("Subscribe", &request.encode_to_vec())
            .await
            .map(|answer| SubscribeResponse::decode(answer.as_slice()).unwrap())
    }

    fn chain(first_run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "owner".to_string(),
            first_run_id: first_run_id.to_string(),
        }
    }

    /// The arm the next append on the stream reports.
    async fn arm_of_next_append(store: &MemoryStore, first_run_id: &str) -> Option<u64> {
        store
            .append(StoreAppendRequest {
                chain: Some(chain(first_run_id)),
                topic: "tokens".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence: 1,
                digest: vec![1],
                records: vec![b"record".to_vec()],
            })
            .await
            .unwrap()
            .arm
    }

    #[tokio::test]
    async fn a_subscription_attaches_the_workflow_target_once_per_process() {
        let setup = setup();
        let first = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(first.attached);
        assert_eq!(first.chain_first_run_id, "chain-1");
        let again = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(!again.attached, "a later read sends no attach");
        assert_eq!(again.chain_first_run_id, "chain-1");
        let attached = setup.server.attached.lock().unwrap().clone();
        assert_eq!(
            attached,
            [WorkflowAttachment {
                chain: chain("chain-1"),
                topic: "tokens".to_string(),
                request_id: "jetty/reader-run/1".to_string(),
                workflow_id: "reader".to_string(),
                run_id: "reader-run".to_string(),
                subscription_id: "1".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn readers_of_one_run_attach_apart() {
        let setup = setup();
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        subscribe(&setup.service, subscribe_request("2"))
            .await
            .unwrap();
        let ids: Vec<String> = setup
            .server
            .attached
            .lock()
            .unwrap()
            .iter()
            .map(|a| a.request_id.clone())
            .collect();
        assert_eq!(ids, ["jetty/reader-run/1", "jetty/reader-run/2"]);
    }

    #[tokio::test]
    async fn a_subscription_to_an_empty_stream_arms_it_for_the_first_append() {
        let setup = setup();
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(
            arm_of_next_append(&setup.store, "chain-1")
                .await
                .is_some_and(|arm| arm > 0),
            "the first append wakes the Workflow"
        );
    }

    #[tokio::test]
    async fn a_subscription_to_a_stream_with_records_does_not_arm() {
        let setup = setup();
        arm_of_next_append(&setup.store, "chain-1").await;
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        let landed = setup
            .store
            .append(StoreAppendRequest {
                chain: Some(chain("chain-1")),
                topic: "tokens".to_string(),
                producer_id: "other".to_string(),
                attempt: 1,
                sequence: 1,
                digest: vec![1],
                records: vec![b"record".to_vec()],
            })
            .await
            .unwrap();
        assert_eq!(landed.arm, Some(0), "the reader reads the records first");
    }

    #[tokio::test]
    async fn a_subscription_attaches_to_the_chain_that_holds_the_workflow_id_now() {
        let setup = setup();
        // A read warms the owner cache with the first chain.
        setup
            .service
            .call(
                "Latest",
                &crate::proto::LatestRequest {
                    stream: Some(stream()),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        *setup.owner.0.lock().unwrap() = "chain-2".to_string();
        let answer = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert_eq!(answer.chain_first_run_id, "chain-2");
    }

    #[tokio::test]
    async fn a_refused_attach_fails_the_subscription_and_is_not_remembered() {
        let setup = setup();
        *setup.server.answer.lock().unwrap() = Some(NotifyError::Refused(
            "the execution has 32 stream wake sources".to_string(),
        ));
        let error = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused as i32);
        *setup.server.answer.lock().unwrap() = None;
        let answer = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(answer.attached, "the next read tries the attach again");
    }

    #[tokio::test]
    async fn an_attach_that_may_land_later_fails_as_storage() {
        let setup = setup();
        *setup.server.answer.lock().unwrap() = Some(NotifyError::Failed("down".to_string()));
        let error = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage as i32);
    }

    #[tokio::test]
    async fn a_subscription_needs_its_reader() {
        let setup = setup();
        for request in [
            SubscribeRequest {
                workflow_id: String::new(),
                ..subscribe_request("1")
            },
            SubscribeRequest {
                run_id: String::new(),
                ..subscribe_request("1")
            },
            subscribe_request(""),
            subscribe_request(&"x".repeat(MAX_SUBSCRIPTION_ID_BYTES + 1)),
            SubscribeRequest {
                stream: None,
                ..subscribe_request("1")
            },
        ] {
            let error = subscribe(&setup.service, request).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument as i32);
        }
        assert!(setup.server.attached.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_subscription_without_a_client_is_unsupported() {
        let service = StreamService::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Owner(Mutex::new("chain-1".to_string()))),
            StreamsOptions::default(),
        );
        let error = subscribe(&service, subscribe_request("1"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported as i32);
    }

    #[tokio::test]
    async fn an_unsubscribe_detaches_from_the_chain_it_attached_to_and_forgets_it() {
        let setup = setup();
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        *setup.owner.0.lock().unwrap() = "chain-2".to_string();
        let unsubscribe = UnsubscribeRequest {
            stream: Some(stream()),
            run_id: "reader-run".to_string(),
            subscription_id: "1".to_string(),
        };
        setup
            .service
            .call("Unsubscribe", &unsubscribe.encode_to_vec())
            .await
            .unwrap();
        assert_eq!(
            setup.server.detached.lock().unwrap().clone(),
            [(
                chain("chain-1"),
                "tokens".to_string(),
                "jetty/reader-run/1".to_string()
            )]
        );
        let answer = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(answer.attached, "a new subscription attaches again");
        assert_eq!(answer.chain_first_run_id, "chain-2");
    }
}
