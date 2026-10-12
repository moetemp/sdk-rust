//! The stream calls lang makes, served in process.

use crate::{
    DEFAULT_MAX_NOTIFIERS, Notifier, NotifierClient, NotifyingStore, OwnerClient, StreamError,
    StreamResult, StreamStore, Streams, StreamsOptions,
    nexus::{NexusCalls, NexusOptions},
    proto::{
        AppendRequest, AttachCallerRequest, CloseRequest, DeleteOwnerRequest, DetachCallerRequest,
        FlushNotificationsRequest, FlushNotificationsResponse, LatestRequest, NexusCall,
        NexusResumeRequest, ReadRequest, StreamFailure, StreamStoreConfig, stream_store_config,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};

/// How long a store keeps records when its configuration doesn't say.
const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// The most records one read answers with, as the stream service contract allows.
const MAX_READ_RECORDS: u32 = 1000;
/// The longest one read waits, as the stream service contract allows. A longer wait would hold a
/// store connection that other reads need.
const MAX_READ_WAIT: Duration = Duration::from_secs(60);

/// One process's stream store and the calls lang makes on it.
///
/// Lang builds one from a [StreamStoreConfig], hands [StreamService::store] to its Workers, and
/// sends its stream calls to [StreamService::call] as serialized `coresdk.streams` requests.
pub struct StreamService {
    store: Arc<dyn StreamStore>,
    streams: Arc<Streams>,
    nexus: NexusCalls,
    callers: Option<Arc<dyn NotifierClient>>,
    attach_progress: Option<Arc<Notifier>>,
}

impl std::fmt::Debug for StreamService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamService")
            .field("store", &self.store.name())
            .finish()
    }
}

impl StreamService {
    /// Connects to the store `config` names, asking `owner` about streams' owners. Refuses a
    /// config that turns notifications on, since there's no client to send them with.
    pub async fn connect(
        config: StreamStoreConfig,
        owner: Arc<dyn OwnerClient>,
    ) -> StreamResult<Self> {
        Self::connect_with_notifier(config, owner, None).await
    }

    /// Like [StreamService::connect], closing each stream's notifier through `notifier` after
    /// the store closes the stream, sending progress too when the config turns it on, and
    /// attaching and detaching stream operation callers through it.
    pub async fn connect_with_notifier(
        config: StreamStoreConfig,
        owner: Arc<dyn OwnerClient>,
        notifier: Option<Arc<dyn NotifierClient>>,
    ) -> StreamResult<Self> {
        let (store, retention): (Arc<dyn StreamStore>, Duration) = match config.store {
            Some(stream_store_config::Store::Memory(_)) => {
                (Arc::new(crate::MemoryStore::new()), DEFAULT_RETENTION)
            }
            Some(stream_store_config::Store::Redis(redis)) => connect_redis(redis).await?,
            None => {
                return Err(StreamError::refused(
                    "the stream store config names no store",
                ));
            }
        };
        if config.notify_on_append && notifier.is_none() {
            return Err(StreamError::refused(
                "stream notifications need a client to send them with",
            ));
        }
        let folding = notifier.as_ref().map(|client| {
            let max_notifiers = match config.max_notifiers {
                0 => DEFAULT_MAX_NOTIFIERS,
                max => max as usize,
            };
            Arc::new(Notifier::new(client.clone(), max_notifiers))
        });
        let store: Arc<dyn StreamStore> = match &folding {
            Some(folding) => Arc::new(
                NotifyingStore::new(store, folding.clone()).with_progress(config.notify_on_append),
            ),
            None => store,
        };
        let service = Self::new(store, owner, StreamsOptions::for_retention(retention));
        Ok(match notifier {
            Some(client) => service
                .with_callers(client)
                .with_attach_progress(folding.filter(|_| config.notify_on_append)),
            None => service,
        })
    }

    /// Serves calls on `store`.
    pub fn new(
        store: Arc<dyn StreamStore>,
        owner: Arc<dyn OwnerClient>,
        options: StreamsOptions,
    ) -> Self {
        let streams = Arc::new(Streams::new(store.clone(), owner, options));
        let nexus = NexusCalls::new(streams.clone(), NexusOptions::default());
        Self {
            store,
            streams,
            nexus,
            callers: None,
            attach_progress: None,
        }
    }

    /// Attaches and detaches stream operation callers through `client`.
    pub fn with_callers(mut self, client: Arc<dyn NotifierClient>) -> Self {
        self.callers = Some(client);
        self
    }

    /// After an attach, notifies the stream's newest record, so the new caller hears of the
    /// records already there. A notification before the first attach starts no notifier on the
    /// server, so without this a caller that only waits for progress hears nothing until the next
    /// append. It goes through `notifier`, the one that sends the appends' notifications, so it
    /// is retried and flushed as they are. `None` turns it off, as the config's progress setting
    /// does.
    pub fn with_attach_progress(mut self, notifier: Option<Arc<Notifier>>) -> Self {
        self.attach_progress = notifier;
        self
    }

    /// The store, for the Workers that stage and promote Workflow output in it.
    pub fn store(&self) -> Arc<dyn StreamStore> {
        self.store.clone()
    }

    /// Serves one call: `rpc` is a `StreamService` method name and `request` its serialized
    /// request. Answers with the serialized response, or the failure the call ended with.
    pub async fn call(&self, rpc: &str, request: &[u8]) -> Result<Vec<u8>, StreamFailure> {
        let response = match rpc {
            "Append" => self
                .streams
                .append(decode::<AppendRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "Read" => self
                .streams
                .read(capped(decode::<ReadRequest>(rpc, request)?))
                .await
                .map(|r| r.encode_to_vec()),
            "Latest" => self
                .streams
                .latest(decode::<LatestRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "Close" => self
                .streams
                .close(decode::<CloseRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "DeleteOwner" => self
                .streams
                .delete_owner(decode::<DeleteOwnerRequest>(rpc, request)?)
                .await
                .map(|r| r.encode_to_vec()),
            "FlushNotifications" => {
                decode::<FlushNotificationsRequest>(rpc, request)?;
                // A close retries until the server takes it, so a stopping process waits only
                // so long for one against a server that's down.
                let _ = tokio::time::timeout(crate::FLUSH_LIMIT, self.store.flush_notifications())
                    .await;
                Ok(FlushNotificationsResponse {}.encode_to_vec())
            }
            // A Nexus call answers with a step, its failure included, so lang hands the failure
            // to its Nexus handler as it is.
            "NexusAppend" => Ok(self
                .nexus
                .append(decode::<NexusCall>(rpc, request)?)
                .await
                .encode_to_vec()),
            "NexusRead" => Ok(self
                .nexus
                .read(decode::<NexusCall>(rpc, request)?)
                .await
                .encode_to_vec()),
            "NexusResume" => Ok(self
                .nexus
                .resume(decode::<NexusResumeRequest>(rpc, request)?)
                .await
                .encode_to_vec()),
            // Like the Nexus calls, a caller call answers its failure in the response.
            "AttachCaller" => Ok(crate::nexus::attach_caller(
                &self.streams,
                self.callers.as_ref(),
                self.attach_progress.as_ref(),
                decode::<AttachCallerRequest>(rpc, request)?,
            )
            .await
            .encode_to_vec()),
            "DetachCaller" => Ok(crate::nexus::detach_caller(
                self.callers.as_ref(),
                decode::<DetachCallerRequest>(rpc, request)?,
            )
            .await
            .encode_to_vec()),
            other => Err(StreamError::unsupported(format!(
                "the stream service has no call {other:?}"
            ))),
        };
        response.map_err(StreamFailure::from)
    }
}

/// `request` with its record count and wait cut to what Core allows.
fn capped(mut request: ReadRequest) -> ReadRequest {
    request.max_records = request.max_records.min(MAX_READ_RECORDS);
    let too_long = request
        .wait
        .and_then(|wait| Duration::try_from(wait).ok())
        .is_some_and(|wait| wait > MAX_READ_WAIT);
    if too_long {
        request.wait = MAX_READ_WAIT.try_into().ok();
    }
    request
}

fn decode<T: Message + Default>(rpc: &str, request: &[u8]) -> Result<T, StreamFailure> {
    T::decode(request).map_err(|error| {
        StreamError::refused(format!("the {rpc} request did not decode ({error})")).into()
    })
}

#[cfg(feature = "redis")]
async fn connect_redis(
    config: crate::proto::RedisStoreConfig,
) -> StreamResult<(Arc<dyn StreamStore>, Duration)> {
    let mut options = crate::RedisStoreOptions::new(String::new());
    options.urls = config.urls;
    options.cluster = config.cluster;
    if !config.key_prefix.is_empty() {
        options.key_prefix = config.key_prefix;
    }
    if let Some(retention) = config.retention {
        options.retention = duration(retention.seconds, retention.nanos, "retention")?;
    }
    if config.blocking_reads_per_node > 0 {
        options.blocking_reads_per_node = config.blocking_reads_per_node as usize;
    }
    if let Some(timeout) = config.response_timeout {
        options.response_timeout = duration(timeout.seconds, timeout.nanos, "response timeout")?;
    }
    let retention = options.retention;
    let store = crate::RedisStore::connect(options).await?;
    Ok((Arc::new(store), retention))
}

#[cfg(not(feature = "redis"))]
async fn connect_redis(
    _: crate::proto::RedisStoreConfig,
) -> StreamResult<(Arc<dyn StreamStore>, Duration)> {
    Err(StreamError::unsupported(
        "this build has no Redis store; build temporalio-streams with its redis feature",
    ))
}

#[cfg(feature = "redis")]
fn duration(seconds: i64, nanos: i32, name: &str) -> StreamResult<Duration> {
    match (u64::try_from(seconds), u32::try_from(nanos)) {
        (Ok(seconds), Ok(nanos)) if seconds > 0 || nanos > 0 => Ok(Duration::new(seconds, nanos)),
        _ => Err(StreamError::refused(format!(
            "the Redis store's {name} must be positive"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        OwnerDescription, OwnerError,
        proto::{
            AppendRecord, AppendResponse, LatestResponse, MemoryStoreConfig, NamedProducer,
            ReadResponse, StreamAddress, StreamFailureKind, StreamOwnerKind, StreamRecordKind,
            append_request, read_record,
        },
    };
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload, enums::v1::WorkflowExecutionStatus, history::v1::HistoryEvent,
    };

    /// Every Workflow's latest run is `run-1`, the first of its chain, and still running.
    struct Running;

    #[async_trait::async_trait]
    impl OwnerClient for Running {
        async fn describe(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            Ok(OwnerDescription {
                run_id: "run-1".to_string(),
                first_run_id: "run-1".to_string(),
                status: WorkflowExecutionStatus::Running,
                start_time: None,
            })
        }

        async fn history_after(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: i64,
        ) -> Result<Vec<HistoryEvent>, OwnerError> {
            Ok(vec![])
        }
    }

    async fn service() -> StreamService {
        StreamService::connect(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(MemoryStoreConfig {})),
                ..Default::default()
            },
            Arc::new(Running),
        )
        .await
        .unwrap()
    }

    fn address() -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "wf".to_string(),
            run_id: String::new(),
            topic: "out".to_string(),
        }
    }

    fn append(sequence: i64, values: &[&str]) -> Vec<u8> {
        AppendRequest {
            stream: Some(address()),
            producer: Some(append_request::Producer::Named(NamedProducer {
                producer_id: "p".to_string(),
                attempt: 1,
            })),
            sequence,
            digest: vec![sequence as u8; 32],
            records: values
                .iter()
                .map(|value| AppendRecord {
                    kind: StreamRecordKind::Data as i32,
                    body: Some(Payload {
                        data: value.as_bytes().to_vec(),
                        ..Default::default()
                    }),
                    content_hash: vec![value.as_bytes()[0]; 32],
                })
                .collect(),
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn an_append_reads_back_through_the_service() {
        let service = service().await;
        let appended = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a", "b"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let read = ReadResponse::decode(
            service
                .call(
                    "Read",
                    &ReadRequest {
                        stream: Some(address()),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let bodies: Vec<_> = read
            .records
            .iter()
            .map(|r| match &r.record {
                Some(read_record::Record::Stored(stored)) => stored.body.clone().unwrap().data,
                other => panic!("expected a stored record, got {other:?}"),
            })
            .collect();
        assert_eq!(bodies, [b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(read.cursor, appended.last_cursor);

        let latest = LatestResponse::decode(
            service
                .call(
                    "Latest",
                    &LatestRequest {
                        stream: Some(address()),
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(latest.cursor, appended.last_cursor);
    }

    fn read(request: ReadRequest) -> Vec<u8> {
        ReadRequest {
            stream: Some(address()),
            ..request
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn a_read_answers_at_most_a_thousand_records() {
        let service = service().await;
        let values: Vec<String> = (0..1001).map(|i| format!("v{i}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        service.call("Append", &append(1, &values)).await.unwrap();
        let answer = service
            .call(
                "Read",
                &read(ReadRequest {
                    max_records: 1_000_000,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let read = ReadResponse::decode(answer.as_slice()).unwrap();
        assert_eq!(read.records.len(), 1000);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_waits_at_most_a_minute() {
        let service = service().await;
        let started = tokio::time::Instant::now();
        service
            .call(
                "Read",
                &read(ReadRequest {
                    after: "$end".to_string(),
                    wait: Some(Duration::from_secs(3600).try_into().unwrap()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        assert!(
            started.elapsed() <= Duration::from_secs(61),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_failure_crosses_with_its_kind() {
        let service = service().await;
        service
            .call("Append", &append(1, &["a", "b"]))
            .await
            .unwrap();
        let failure = service
            .call("Append", &append(2, &["c"]))
            .await
            .unwrap_err();
        assert_eq!(
            failure.kind(),
            StreamFailureKind::ProducerStale,
            "{failure:?}"
        );
    }

    #[tokio::test]
    async fn an_unknown_call_or_a_bad_request_is_refused() {
        let service = service().await;
        let unknown = service.call("Truncate", &[]).await.unwrap_err();
        assert_eq!(unknown.kind(), StreamFailureKind::Unsupported);
        assert!(unknown.message.contains("Truncate"));
        let garbled = service.call("Append", &[0xff, 0xff]).await.unwrap_err();
        assert_eq!(garbled.kind(), StreamFailureKind::Refused);
        assert!(garbled.message.contains("did not decode"));
    }

    #[tokio::test]
    async fn a_delete_is_checked_before_it_reaches_the_store() {
        let service = service().await;
        let other_owner = crate::proto::DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Unspecified as i32,
            workflow_id: "wf".to_string(),
        };
        let failure = service
            .call("DeleteOwner", &other_owner.encode_to_vec())
            .await
            .unwrap_err();
        assert_eq!(failure.kind(), StreamFailureKind::Unsupported);
        let no_id = crate::proto::DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: String::new(),
        };
        let failure = service
            .call("DeleteOwner", &no_id.encode_to_vec())
            .await
            .unwrap_err();
        assert_eq!(failure.kind(), StreamFailureKind::Refused);
    }

    #[tokio::test]
    async fn a_config_that_names_no_store_is_refused() {
        let error = StreamService::connect(StreamStoreConfig::default(), Arc::new(Running))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused);
    }

    #[cfg(not(feature = "redis"))]
    #[tokio::test]
    async fn redis_needs_the_redis_feature() {
        let error = StreamService::connect(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Redis(Default::default())),
                ..Default::default()
            },
            Arc::new(Running),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
    }

    /// Records the notifications it was sent.
    #[derive(Default)]
    struct Recorder(std::sync::Mutex<Vec<crate::StreamNotification>>);

    #[async_trait::async_trait]
    impl NotifierClient for Recorder {
        async fn notify(
            &self,
            notification: crate::StreamNotification,
        ) -> Result<(), crate::NotifyError> {
            self.0.lock().unwrap().push(notification);
            Ok(())
        }
    }

    async fn service_notifying(notify_on_append: bool, recorder: Arc<Recorder>) -> StreamService {
        StreamService::connect_with_notifier(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(MemoryStoreConfig {})),
                notify_on_append,
                ..Default::default()
            },
            Arc::new(Running),
            Some(recorder),
        )
        .await
        .unwrap()
    }

    async fn flush(service: &StreamService) {
        let response = service
            .call(
                "FlushNotifications",
                &FlushNotificationsRequest {}.encode_to_vec(),
            )
            .await
            .unwrap();
        FlushNotificationsResponse::decode(response.as_slice()).unwrap();
    }

    #[tokio::test]
    async fn an_append_notifies_its_streams_notifier_once_notifications_are_on() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        let appended = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a", "b"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        flush(&service).await;
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].chain.first_run_id, "run-1",
            "the chain the append wrote to"
        );
        assert_eq!(sent[0].topic, "out");
        assert_eq!(sent[0].position, appended.last_cursor);
        assert_eq!(sent[0].counter, 2);
    }

    #[tokio::test]
    async fn notifications_are_off_by_default() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(false, recorder.clone()).await;
        service.call("Append", &append(1, &["a"])).await.unwrap();
        flush(&service).await;
        assert!(recorder.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn notifications_without_a_client_are_refused() {
        let error = StreamService::connect(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(MemoryStoreConfig {})),
                notify_on_append: true,
                ..Default::default()
            },
            Arc::new(Running),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Refused);
    }

    #[tokio::test]
    async fn a_close_reaches_the_notifier_with_notifications_off() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(false, recorder.clone()).await;
        service.call("Append", &append(1, &["a"])).await.unwrap();
        let result = Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        };
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: Some(result.clone()),
                    failure: None,
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        flush(&service).await;
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 1, "no progress, only the close");
        assert_eq!(sent[0].close_result(), Some(result));
    }

    /// Answers every notification like a server without the stream notifier.
    #[derive(Default)]
    struct Stock(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl NotifierClient for Stock {
        async fn notify(&self, _: crate::StreamNotification) -> Result<(), crate::NotifyError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(crate::NotifyError::Unsupported("unimplemented".to_string()))
        }
    }

    #[tokio::test]
    async fn a_server_without_the_notifier_costs_one_call() {
        let stock = Arc::new(Stock::default());
        let service = StreamService::connect_with_notifier(
            StreamStoreConfig {
                store: Some(stream_store_config::Store::Memory(MemoryStoreConfig {})),
                notify_on_append: true,
                ..Default::default()
            },
            Arc::new(Running),
            Some(stock.clone()),
        )
        .await
        .unwrap();
        service.call("Append", &append(1, &["a"])).await.unwrap();
        flush(&service).await;
        service.call("Append", &append(2, &["b"])).await.unwrap();
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: Some(Payload::default()),
                    failure: None,
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        flush(&service).await;
        assert_eq!(stock.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// The Workflow's chain is `run-1`. It runs until `ended` is set, then it's completed.
    #[derive(Default)]
    struct Ends(std::sync::atomic::AtomicBool);

    #[async_trait::async_trait]
    impl OwnerClient for Ends {
        async fn describe(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            let ended = self.0.load(std::sync::atomic::Ordering::SeqCst);
            Ok(OwnerDescription {
                run_id: "run-1".to_string(),
                first_run_id: "run-1".to_string(),
                status: if ended {
                    WorkflowExecutionStatus::Completed
                } else {
                    WorkflowExecutionStatus::Running
                },
                start_time: None,
            })
        }

        async fn history_after(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: i64,
        ) -> Result<Vec<HistoryEvent>, OwnerError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn a_readers_owner_check_right_after_the_final_task_sends_no_notifier_close() {
        let recorder = Arc::new(Recorder::default());
        let owner = Arc::new(Ends::default());
        let store = Arc::new(NotifyingStore::new(
            Arc::new(crate::MemoryStore::new()),
            Arc::new(Notifier::new(recorder.clone(), 10)),
        ));
        let service = StreamService::new(
            store,
            owner.clone(),
            StreamsOptions {
                owner_recheck: Duration::ZERO,
                owner_check_min: Duration::from_millis(1),
                owner_check_max: Duration::from_millis(1),
                ..Default::default()
            },
        );
        let appended = AppendResponse::decode(
            service
                .call("Append", &append(1, &["a"]))
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        flush(&service).await;
        // The server accepted the final task, and its Worker hasn't closed the topic yet.
        owner.0.store(true, std::sync::atomic::Ordering::SeqCst);
        let read = ReadResponse::decode(
            service
                .call(
                    "Read",
                    &ReadRequest {
                        stream: Some(address()),
                        after: appended.last_cursor,
                        wait: Some(Duration::from_millis(200).try_into().unwrap()),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert!(read.done, "the reader saw the owner end");
        let refused = service
            .call("Append", &append(2, &["b"]))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed as i32);
        flush(&service).await;
        assert!(
            recorder
                .0
                .lock()
                .unwrap()
                .iter()
                .all(|n| n.close_result().is_none()),
            "an empty close would beat the Workflow's result"
        );
    }

    #[tokio::test]
    async fn a_close_with_a_result_closes_the_store_and_then_the_notifier() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        service
            .call("Append", &append(1, &["a", "b"]))
            .await
            .unwrap();
        flush(&service).await;
        let result = Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        };
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: Some(result.clone()),
                    failure: None,
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].close_result(), Some(result));
        assert_eq!(sent[1].counter, 3, "above the newest record's counter");
        let refused = service
            .call("Append", &append(3, &["c"]))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed as i32);
    }

    #[tokio::test]
    async fn a_close_without_a_result_ends_the_stream_on_the_server_with_an_empty_result() {
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        service
            .call(
                "Close",
                &CloseRequest {
                    stream: Some(address()),
                    result: None,
                    failure: None,
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        flush(&service).await;
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        // A caller with a typed result decodes it as None, which an empty payload can't give.
        assert_eq!(sent[0].close_result(), Some(crate::empty_close_result()));
        assert_eq!(
            sent[0].close_result().unwrap().metadata["encoding"],
            b"binary/null"
        );
    }

    /// A store whose notifications never go out, as when the server is down and a close retries.
    struct NeverFlushes(crate::MemoryStore);

    #[async_trait::async_trait]
    impl StreamStore for NeverFlushes {
        fn name(&self) -> &str {
            self.0.name()
        }
        async fn append(
            &self,
            r: crate::proto::StoreAppendRequest,
        ) -> StreamResult<crate::proto::StoreAppendResponse> {
            self.0.append(r).await
        }
        async fn read(
            &self,
            r: crate::proto::StoreReadRequest,
        ) -> StreamResult<crate::proto::StoreReadResponse> {
            self.0.read(r).await
        }
        async fn latest(
            &self,
            r: crate::proto::StoreLatestRequest,
        ) -> StreamResult<crate::proto::StoreLatestResponse> {
            self.0.latest(r).await
        }
        async fn record_at(
            &self,
            c: &crate::proto::ChainId,
            t: &str,
            p: &str,
        ) -> StreamResult<Option<Vec<u8>>> {
            self.0.record_at(c, t, p).await
        }
        async fn stage(&self, b: crate::proto::StagedBatch) -> StreamResult<()> {
            self.0.stage(b).await
        }
        async fn promote(
            &self,
            s: &crate::proto::StageRef,
        ) -> StreamResult<crate::proto::PromoteResult> {
            self.0.promote(s).await
        }
        async fn abort(&self, s: &crate::proto::StageRef) -> StreamResult<()> {
            self.0.abort(s).await
        }
        async fn close_chain(&self, c: &crate::proto::ChainId) -> StreamResult<()> {
            self.0.close_chain(c).await
        }
        async fn close_topic(
            &self,
            c: &crate::proto::ChainId,
            t: &str,
            r: Option<Payload>,
        ) -> StreamResult<()> {
            self.0.close_topic(c, t, r).await
        }
        async fn pending_stages(
            &self,
            c: &crate::proto::ChainId,
        ) -> StreamResult<Vec<crate::proto::PendingStage>> {
            self.0.pending_stages(c).await
        }
        async fn delete_owner(
            &self,
            r: DeleteOwnerRequest,
        ) -> StreamResult<crate::proto::DeleteOwnerResponse> {
            self.0.delete_owner(r).await
        }
        async fn flush_notifications(&self) {
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_flush_waits_only_so_long_for_notifications_that_never_go_out() {
        let service = StreamService::new(
            Arc::new(NeverFlushes(crate::MemoryStore::new())),
            Arc::new(Running),
            StreamsOptions::default(),
        );
        let started = tokio::time::Instant::now();
        flush(&service).await;
        assert_eq!(
            started.elapsed(),
            crate::FLUSH_LIMIT,
            "a stopping process mustn't hang on a close retrying against a server that's down"
        );
    }

    #[tokio::test]
    async fn a_nexus_append_notifies_its_streams_notifier() {
        use crate::proto::{NexusCall, NexusStep, nexus_step};
        let recorder = Arc::new(Recorder::default());
        let service = service_notifying(true, recorder.clone()).await;
        let input = serde_json::json!({
            "stream": {"kind": "workflow", "workflow_id": "wf", "topic": "t"},
            "producer_id": "p",
            "attempt": 1,
            "sequence": 1,
            "finish": true,
        });
        let answer = service
            .call(
                "NexusAppend",
                &NexusCall {
                    input: serde_json::to_vec(&input).unwrap(),
                    namespace: "ns".to_string(),
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        let step = NexusStep::decode(answer.as_slice()).unwrap();
        let Some(nexus_step::Step::Output(output)) = step.step else {
            panic!("a finish needs no codec work, so it answers at once");
        };
        let output: serde_json::Value = serde_json::from_slice(&output).unwrap();
        flush(&service).await;
        let sent = recorder.0.lock().unwrap().clone();
        assert_eq!(
            sent.len(),
            1,
            "the Nexus append goes through the notifying store"
        );
        assert_eq!(sent[0].chain.first_run_id, "run-1");
        assert_eq!(sent[0].topic, "t");
        assert_eq!(sent[0].position, output["cursor"].as_str().unwrap());
    }

    #[tokio::test]
    async fn the_nexus_calls_go_through_the_one_call_entry() {
        use crate::proto::{NexusCall, NexusResumeRequest, NexusStep, nexus_step};
        let service = service().await;
        let input = serde_json::json!({
            "stream": {"kind": "workflow", "workflow_id": "wf", "topic": "t"},
            "producer_id": "p",
            "attempt": 1,
            "sequence": 1,
            "finish": true,
        });
        let call = NexusCall {
            input: serde_json::to_vec(&input).unwrap(),
            namespace: "ns".to_string(),
            ..Default::default()
        };
        let answer = service
            .call("NexusAppend", &call.encode_to_vec())
            .await
            .unwrap();
        let step = NexusStep::decode(answer.as_slice()).unwrap();
        let Some(nexus_step::Step::Output(output)) = step.step else {
            panic!("a finish needs no codec work, so it answers at once");
        };
        let output: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert!(output["cursor"].as_str().unwrap().starts_with("memory:"));
        // A resume of nothing is a step too, a failure lang hands its Nexus handler.
        let answer = service
            .call(
                "NexusResume",
                &NexusResumeRequest {
                    continuation: 99,
                    results: Vec::new(),
                }
                .encode_to_vec(),
            )
            .await
            .unwrap();
        let step = NexusStep::decode(answer.as_slice()).unwrap();
        assert!(matches!(step.step, Some(nexus_step::Step::Failure(_))));
        let refused = service.call("NexusRead", b"\xff").await.unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Refused as i32);
    }
}
