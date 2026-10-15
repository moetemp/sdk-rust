//! Readers that sleep on a stream's notifier.
//!
//! A Workflow's reader attaches to the stream's notifier before its first read, so the notifier
//! wakes the Workflow on its next Workflow Task when the stream moves. The attach names the reader
//! by its run and its subscription id, which replay gives back, so a rerun attaches as the same
//! reader and the server keeps one callback. A client reader has no task to wake, so it waits on
//! the notifier with a long poll.

use crate::{
    NotifierClient, NotifyError, OWNER_RECHECK, StreamError, StreamResult, Streams, StreamsOptions,
    WorkflowAttachment,
    handle::{Remembered, address},
    proto::{
        ChainId, StreamAddress, SubscribeRequest, SubscribeResponse, UnsubscribeRequest,
        UnsubscribeResponse, WaitForNotificationRequest, WaitForNotificationResponse,
    },
};
use std::{sync::Arc, time::Duration};

/// How long after it sends a poll Core reads the stream once more. The poll starts the stream's
/// notifier, and a record that lands later notifies it.
const REREAD_AFTER: Duration = Duration::from_millis(200);

/// The longest subscription id the server takes.
const MAX_SUBSCRIPTION_ID_BYTES: usize = 256;

/// The subscriptions this process attached, so a reader's later reads send no attach. A reader
/// that ends or a run that closes never detaches, so the oldest are forgotten. A reader this
/// process forgot attaches again, and the server dedupes that attach.
pub(crate) struct Subscriptions {
    client: Option<Arc<dyn NotifierClient>>,
    attached: Remembered<SubscriptionKey>,
}

impl Default for Subscriptions {
    fn default() -> Self {
        Self::new(None)
    }
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
/// what replay gives back. A later generation is a new attach, after the notifier dropped the
/// reader and the server would answer the old id as already done.
fn request_id(run_id: &str, subscription_id: &str, generation: u32) -> String {
    match generation {
        0 => format!("jetty/{run_id}/{subscription_id}"),
        generation => format!("jetty/{run_id}/{subscription_id}/{generation}"),
    }
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
            attached: Remembered::new(StreamsOptions::default().remembered),
        }
    }

    pub(crate) fn client(&self) -> StreamResult<&Arc<dyn NotifierClient>> {
        self.client.as_ref().ok_or_else(|| {
            StreamError::unsupported("a Workflow reader needs a client connected to the server")
        })
    }

    /// Attaches the reader once per process. The attach only announces the reader. Every append
    /// notifies, so the first one wakes the Workflow.
    pub(crate) async fn subscribe(
        &self,
        streams: &Streams,
        request: SubscribeRequest,
    ) -> StreamResult<SubscribeResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.workflow_id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a subscription needs the reader's Workflow id",
            ));
        }
        check_subscription(&request.run_id, &request.subscription_id)?;
        let request_id = request_id(
            &request.run_id,
            &request.subscription_id,
            request.generation,
        );
        let key = SubscriptionKey::new(stream, &request_id);
        if let Some(attached) = self.attached.get(&key) {
            let chain = ChainId {
                first_run_id: attached.first_run_id.clone(),
                ..streams.chain(stream).await?
            };
            return Ok(SubscribeResponse {
                newest_counter: streams.newest(stream, &chain).await?.0,
                chain_first_run_id: attached.first_run_id,
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
        self.attached.put(key, &chain.first_run_id, OWNER_RECHECK);
        // Read after the attach landed, so a record after this one notifies the reader.
        let newest_counter = streams.newest(stream, &chain).await?.0;
        Ok(SubscribeResponse {
            chain_first_run_id: chain.first_run_id,
            attached: true,
            newest_counter,
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
        let request_id = request_id(
            &request.run_id,
            &request.subscription_id,
            request.generation,
        );
        let client = self.client()?;
        let key = SubscriptionKey::new(stream, &request_id);
        let chain = match self.attached.get(&key) {
            Some(attached) => crate::proto::ChainId {
                namespace: stream.namespace.clone(),
                workflow_id: stream.workflow_id.clone(),
                first_run_id: attached.first_run_id,
                ..Default::default()
            },
            None => streams.chain_now(stream).await?,
        };
        client
            .detach(chain, stream.topic.clone(), request_id)
            .await
            .map_err(subscribe_error)?;
        self.attached.forget(&key);
        Ok(UnsubscribeResponse {})
    }

    /// Waits on the notifier of the chain the stream names. A wait that reached its limit answers
    /// `timed_out`, and the reader's next read checks the store itself, which covers a
    /// notification that a producer never sent.
    pub(crate) async fn wait(
        &self,
        streams: &Streams,
        request: WaitForNotificationRequest,
    ) -> StreamResult<WaitForNotificationResponse> {
        let stream = address(request.stream.as_ref())?;
        let client = self.client()?;
        let chain = streams.chain(stream).await?;
        let poll = client.poll(chain.clone(), stream.topic.clone(), request.after_counter);
        tokio::pin!(poll);
        // The poll starts the notifier, so a record from before it woke no one. One more read
        // once the poll is out finds it.
        let reread = async {
            tokio::time::sleep(REREAD_AFTER).await;
            streams.newest(stream, &chain).await
        };
        let polled = tokio::select! {
            polled = &mut poll => polled,
            newest = reread => match newest {
                Ok((counter, position)) if counter > request.after_counter => {
                    return Ok(WaitForNotificationResponse {
                        counter,
                        position,
                        ..Default::default()
                    });
                }
                _ => poll.await,
            },
        }
        .map_err(subscribe_error)?;
        let Some(polled) = polled else {
            return Ok(WaitForNotificationResponse {
                timed_out: true,
                ..Default::default()
            });
        };
        Ok(WaitForNotificationResponse {
            timed_out: !polled.closed && polled.counter <= request.after_counter,
            counter: polled.counter,
            position: polled.position,
            closed: polled.closed,
            close_result: polled.close_result,
            close_failure: polled.close_failure,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStore, NotifierPoll, OwnerClient, OwnerDescription, OwnerError, StreamNotification,
        StreamService, StreamsOptions,
        proto::{ChainId, StreamFailureKind, StreamOwnerKind},
    };
    use prost::Message;
    use std::sync::Mutex;
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
        polled: Mutex<Vec<(ChainId, String, i64)>>,
        poll_answer: Mutex<Option<Result<Option<NotifierPoll>, NotifyError>>>,
        /// How long the poll takes before it answers.
        poll_delay: Mutex<Duration>,
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

        async fn poll(
            &self,
            chain: ChainId,
            topic: String,
            after_counter: i64,
        ) -> Result<Option<NotifierPoll>, NotifyError> {
            self.polled
                .lock()
                .unwrap()
                .push((chain, topic, after_counter));
            let delay = *self.poll_delay.lock().unwrap();
            tokio::time::sleep(delay).await;
            self.poll_answer
                .lock()
                .unwrap()
                .clone()
                .expect("the test sets the poll's answer")
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
            ..Default::default()
        }
    }

    fn subscribe_request(subscription_id: &str) -> SubscribeRequest {
        SubscribeRequest {
            stream: Some(stream()),
            workflow_id: "reader".to_string(),
            run_id: "reader-run".to_string(),
            subscription_id: subscription_id.to_string(),
            generation: 0,
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
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_full_process_forgets_its_subscriptions_and_attaches_again() {
        let setup = setup();
        let subscriptions = Subscriptions {
            client: Some(setup.server.clone()),
            attached: Remembered::new(2),
        };
        let streams = Streams::new(
            setup.store.clone(),
            setup.owner.clone(),
            StreamsOptions::default(),
        );
        let subscribe = |id: &str| subscriptions.subscribe(&streams, subscribe_request(id));
        for id in ["1", "2", "3"] {
            assert!(subscribe(id).await.unwrap().attached);
        }
        assert!(!subscribe("3").await.unwrap().attached);
        assert!(
            subscribe("1").await.unwrap().attached,
            "the oldest was forgotten"
        );
        assert_eq!(setup.server.attached.lock().unwrap().len(), 4);
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
    async fn a_new_generation_attaches_again_after_the_notifier_dropped_the_reader() {
        let setup = setup();
        subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        let again = subscribe(
            &setup.service,
            SubscribeRequest {
                generation: 1,
                ..subscribe_request("1")
            },
        )
        .await
        .unwrap();
        assert!(again.attached, "a resubscribe sends a new attach");
        let attached = setup.server.attached.lock().unwrap().clone();
        let ids: Vec<&str> = attached.iter().map(|a| a.request_id.as_str()).collect();
        assert_eq!(ids, ["jetty/reader-run/1", "jetty/reader-run/1/1"]);
        assert!(
            attached.iter().all(|a| a.subscription_id == "1"),
            "the wakes still name the same reader"
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
            ..Default::default()
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

    async fn wait(
        setup: &Setup,
        after_counter: i64,
        answer: Result<Option<NotifierPoll>, NotifyError>,
    ) -> Result<WaitForNotificationResponse, crate::proto::StreamFailure> {
        *setup.server.poll_answer.lock().unwrap() = Some(answer);
        setup
            .service
            .call(
                "WaitForNotification",
                &WaitForNotificationRequest {
                    stream: Some(stream()),
                    after_counter,
                }
                .encode_to_vec(),
            )
            .await
            .map(|answer| WaitForNotificationResponse::decode(answer.as_slice()).unwrap())
    }

    /// Appends `count` records to the chain the owner names now.
    async fn append_records(setup: &Setup, count: i64) {
        use crate::StreamStore;
        let first_run_id = setup.owner.0.lock().unwrap().clone();
        setup
            .store
            .append(crate::proto::StoreAppendRequest {
                chain: Some(ChainId {
                    namespace: "ns".to_string(),
                    workflow_id: "owner".to_string(),
                    first_run_id,
                    ..Default::default()
                }),
                topic: "tokens".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence: 1,
                digest: vec![1],
                records: (0..count).map(|_| b"record".to_vec()).collect(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_subscription_names_the_newest_counter_it_found_after_the_attach() {
        let setup = setup();
        let empty = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert_eq!(empty.newest_counter, 0, "an empty stream");
        append_records(&setup, 2).await;
        // A record from before the attach woke no one, so the reader reads once more.
        let again = subscribe(&setup.service, subscribe_request("1"))
            .await
            .unwrap();
        assert!(!again.attached);
        assert_eq!(again.newest_counter, 2);
        let fresh = subscribe(&setup.service, subscribe_request("2"))
            .await
            .unwrap();
        assert!(fresh.attached);
        assert_eq!(fresh.newest_counter, 2);
    }

    #[tokio::test]
    async fn a_wait_answers_at_once_for_a_record_from_before_the_poll() {
        let setup = setup();
        append_records(&setup, 1).await;
        *setup.server.poll_delay.lock().unwrap() = Duration::from_secs(30);
        let started = tokio::time::Instant::now();
        let answer = wait(&setup, 0, Ok(None)).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(!answer.timed_out && !answer.closed);
        assert_eq!(answer.counter, 1);
        assert!(answer.position.starts_with("memory:"), "{answer:?}");
    }

    #[tokio::test]
    async fn a_wait_with_nothing_newer_waits_for_the_poll() {
        let setup = setup();
        append_records(&setup, 1).await;
        *setup.server.poll_delay.lock().unwrap() = Duration::from_millis(400);
        let answer = wait(
            &setup,
            1,
            Ok(Some(NotifierPoll {
                counter: 2,
                position: "cursor-2".to_string(),
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert_eq!((answer.counter, answer.position.as_str()), (2, "cursor-2"));
    }

    #[tokio::test]
    async fn a_wait_asks_the_notifier_of_the_streams_chain_for_a_counter_above_the_read() {
        let setup = setup();
        let answer = wait(
            &setup,
            41,
            Ok(Some(NotifierPoll {
                counter: 42,
                position: "cursor-42".to_string(),
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert_eq!(
            setup.server.polled.lock().unwrap().clone(),
            [(chain("chain-1"), "tokens".to_string(), 41)]
        );
        assert!(!answer.timed_out && !answer.closed);
        assert_eq!(
            (answer.counter, answer.position.as_str()),
            (42, "cursor-42")
        );
    }

    #[tokio::test]
    async fn a_wait_with_no_new_counter_times_out_for_a_re_read() {
        let setup = setup();
        let unchanged = wait(
            &setup,
            41,
            Ok(Some(NotifierPoll {
                counter: 41,
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert!(unchanged.timed_out);
        let deadline = wait(&setup, 41, Ok(None)).await.unwrap();
        assert!(deadline.timed_out && !deadline.closed);
    }

    #[tokio::test]
    async fn a_wait_on_a_closed_stream_carries_the_outcome() {
        let setup = setup();
        let failure = temporalio_common::protos::temporal::api::failure::v1::Failure {
            message: "the agent gave up".to_string(),
            ..Default::default()
        };
        let answer = wait(
            &setup,
            41,
            Ok(Some(NotifierPoll {
                counter: 41,
                closed: true,
                close_failure: Some(failure.clone()),
                ..Default::default()
            })),
        )
        .await
        .unwrap();
        assert!(answer.closed && !answer.timed_out);
        assert_eq!(answer.close_failure, Some(failure));
    }

    #[tokio::test]
    async fn a_wait_on_a_server_without_the_poll_is_unsupported() {
        let setup = setup();
        let error = wait(
            &setup,
            0,
            Err(NotifyError::Unsupported("unimplemented".to_string())),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported as i32);
    }
}
