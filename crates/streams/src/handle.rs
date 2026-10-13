//! The stream calls lang makes, from the service protos down to a store.
//!
//! [Streams] resolves a stream's address to the run chain its store keys it by, checks the owner
//! where the contract says so, and mints and checks cursors. Stores only move records.

use crate::{
    ReadTarget, StreamError, StreamResult, StreamStore, WORKFLOW_OWNER_KIND, activity_producer_id,
    check_append_digest, mint_cursor,
    owner::{OwnerClient, OwnerDescription, OwnerError},
    proto::{
        AppendRequest, AppendResponse, ChainId, CloseRequest, CloseResponse, LatestRequest,
        LatestResponse, ReadRequest, ReadResponse, StoreAppendRequest, StoreLatestRequest,
        StreamAddress, StreamOwnerKind, append_request::Producer,
    },
    reader::{ReadState, read_with},
    stored_append_record, stream_hash,
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

/// The longest a producer writes without asking whether its owner's chain ended.
pub const OWNER_RECHECK: Duration = Duration::from_secs(60);

/// How the stream layer behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsOptions {
    /// How long a producer writes before it asks again whether its owner's chain ended. Nothing
    /// else ends the writes of a Workflow that never published itself.
    pub owner_recheck: Duration,
    /// How many producer attempts and chains to remember the last owner check of.
    pub remembered: usize,
}

impl Default for StreamsOptions {
    fn default() -> Self {
        Self {
            owner_recheck: OWNER_RECHECK,
            remembered: 10_000,
        }
    }
}

impl StreamsOptions {
    /// The options for a store that keeps records for `retention`. A close mark lives as long
    /// as the stream, so a producer asks again before a mark it could miss expires.
    pub fn for_retention(retention: Duration) -> Self {
        Self {
            owner_recheck: retention.min(OWNER_RECHECK),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OwnerKey {
    namespace: String,
    workflow_id: String,
    run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProducerKey {
    owner: OwnerKey,
    producer_id: String,
    attempt: i64,
}

#[derive(Debug, Clone)]
struct Checked {
    first_run_id: String,
    at: Instant,
}

/// Remembers when each key was last checked, forgetting the oldest when full.
#[derive(Debug)]
struct Remembered<K> {
    entries: Mutex<HashMap<K, Checked>>,
    limit: usize,
}

impl<K: std::hash::Hash + Eq + Clone> Remembered<K> {
    fn new(limit: usize) -> Self {
        Self {
            entries: Mutex::default(),
            limit: limit.max(1),
        }
    }

    fn get(&self, key: &K) -> Option<Checked> {
        self.entries.lock().unwrap().get(key).cloned()
    }

    fn put(&self, key: K, first_run_id: &str, fresh_for: Duration) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.limit && !entries.contains_key(&key) {
            entries.retain(|_, checked| checked.at.elapsed() < fresh_for);
            if entries.len() >= self.limit {
                entries.clear();
            }
        }
        entries.insert(
            key,
            Checked {
                first_run_id: first_run_id.to_string(),
                at: Instant::now(),
            },
        );
    }
}

/// The stream layer over one store.
pub struct Streams {
    store: Arc<dyn StreamStore>,
    owner: Arc<dyn OwnerClient>,
    options: StreamsOptions,
    producers: Remembered<ProducerKey>,
    chains: Remembered<OwnerKey>,
}

impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("store", &self.store.name())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl Streams {
    /// The stream layer over `store`, asking `owner` about owners.
    pub fn new(
        store: Arc<dyn StreamStore>,
        owner: Arc<dyn OwnerClient>,
        options: StreamsOptions,
    ) -> Self {
        Self {
            store,
            owner,
            producers: Remembered::new(options.remembered),
            chains: Remembered::new(options.remembered),
            options,
        }
    }

    /// Appends one batch for one producer attempt.
    ///
    /// A producer's first batch, and its first batch after [StreamsOptions::owner_recheck],
    /// asks whether the owner's chain ended. If it did, or Temporal no longer holds the owner,
    /// or a new chain took the Workflow id, the chain is marked closed and the batch is refused
    /// as closed.
    pub async fn append(&self, request: AppendRequest) -> StreamResult<AppendResponse> {
        let stream = address(request.stream.as_ref())?;
        let (producer_id, attempt) = match &request.producer {
            Some(Producer::Named(named)) => {
                if named.producer_id.is_empty() {
                    return Err(StreamError::invalid_argument("a producer needs an id"));
                }
                (named.producer_id.clone(), named.attempt)
            }
            Some(Producer::Activity(activity)) => {
                (activity_producer_id(activity), i64::from(activity.attempt))
            }
            None => {
                return Err(StreamError::invalid_argument(
                    "an append needs its producer",
                ));
            }
        };
        if attempt < 1 {
            return Err(StreamError::invalid_argument(format!(
                "a producer's attempt starts at one, not {attempt}"
            )));
        }
        if request.sequence < 1 {
            return Err(StreamError::invalid_argument(format!(
                "a producer's sequence starts at one, not {}",
                request.sequence
            )));
        }
        if request.records.is_empty() {
            return Err(StreamError::invalid_argument(
                "an append needs at least one record",
            ));
        }
        check_append_digest(&request.digest)?;
        let chain = self
            .producer_chain(ProducerKey {
                owner: owner_key(stream),
                producer_id: producer_id.clone(),
                attempt,
            })
            .await?;
        let records = request
            .records
            .iter()
            .enumerate()
            .map(|(index, record)| {
                stored_append_record(
                    &stream.topic,
                    &producer_id,
                    attempt,
                    request.sequence + index as i64,
                    record,
                )
                .map(|stored| stored.encode_to_vec())
            })
            .collect::<StreamResult<Vec<_>>>()?;
        let landed = self
            .store
            .append(StoreAppendRequest {
                chain: Some(chain),
                topic: stream.topic.clone(),
                producer_id,
                attempt,
                sequence: request.sequence,
                // Lang takes it before the codec, and only lang's own retries compare it, so the
                // store keeps it as given.
                digest: request.digest.clone(),
                records,
            })
            .await?;
        Ok(AppendResponse {
            first_cursor: self.cursor(stream, &landed.first_position),
            last_cursor: self.cursor(stream, &landed.last_position),
        })
    }

    /// Reads one page of a stream. The first call resolves the chain, and the state carries it.
    pub async fn read(&self, request: ReadRequest) -> StreamResult<ReadResponse> {
        let stream = address(request.stream.as_ref())?;
        let mut state = ReadState::decode_from(&request.state)?;
        if state.first_run_id.is_empty() {
            state.first_run_id = self.chain(stream).await?.first_run_id;
        }
        let target = ReadTarget {
            chain: chain_of(stream, &state.first_run_id),
            topic: stream.topic.clone(),
            stream_hash: self.hash(stream),
        };
        let mut response = read_with(&*self.store, &target, &request, &mut state).await?;
        response.state = state.encode_to_vec();
        Ok(response)
    }

    /// The cursor of a stream's newest record, empty when it holds none.
    pub async fn latest(&self, request: LatestRequest) -> StreamResult<LatestResponse> {
        let stream = address(request.stream.as_ref())?;
        let chain = self.chain(stream).await?;
        let position = self
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain),
                topic: stream.topic.clone(),
            })
            .await?
            .position;
        Ok(LatestResponse {
            cursor: match position.as_str() {
                "" => String::new(),
                position => self.cursor(stream, position),
            },
        })
    }

    /// Closes one stream, so producers' new batches are refused and reads end.
    pub async fn close(&self, request: CloseRequest) -> StreamResult<CloseResponse> {
        let stream = address(request.stream.as_ref())?;
        let chain = self.chain(stream).await?;
        self.store.close_topic(&chain, &stream.topic).await?;
        Ok(CloseResponse {})
    }

    /// The chain a producer writes on, asking about the owner when the producer is new or its
    /// last check is older than the recheck interval.
    async fn producer_chain(&self, key: ProducerKey) -> StreamResult<ChainId> {
        let recheck = self.options.owner_recheck;
        let stream = &key.owner;
        let chain = match self.producers.get(&key) {
            Some(checked) if checked.at.elapsed() < recheck => {
                return Ok(chain_of_key(stream, &checked.first_run_id));
            }
            Some(checked) => {
                let chain = chain_of_key(stream, &checked.first_run_id);
                self.refuse_if_ended(&chain).await?;
                chain
            }
            None if stream.run_id.is_empty() => {
                // The latest run names the chain and tells whether it ended, in one call.
                let latest = self.describe(stream, "").await.map_err(not_found)?;
                let chain = chain_of_key(stream, &first_run_id(stream, &latest)?);
                if latest.chain_ended() {
                    return Err(self.closed(&chain, "has closed").await);
                }
                chain
            }
            None => {
                let pinned = self
                    .describe(stream, &stream.run_id)
                    .await
                    .map_err(not_found)?;
                let chain = chain_of_key(stream, &first_run_id(stream, &pinned)?);
                self.refuse_if_ended(&chain).await?;
                chain
            }
        };
        self.producers.put(key, &chain.first_run_id, recheck);
        Ok(chain)
    }

    /// Marks the chain closed and refuses when its latest run ended, when Temporal no longer
    /// holds it, or when a new chain took its Workflow id.
    async fn refuse_if_ended(&self, chain: &ChainId) -> StreamResult<()> {
        let key = OwnerKey {
            namespace: chain.namespace.clone(),
            workflow_id: chain.workflow_id.clone(),
            run_id: String::new(),
        };
        match self.describe(&key, "").await {
            // The chain came from this Workflow, so History has since dropped it.
            Err(OwnerError::NotFound(_)) => Err(self.closed(chain, "is gone from History").await),
            Err(error) => Err(describe_failed(&error)),
            Ok(latest) if latest.chain_ended() || latest.first_run_id != chain.first_run_id => {
                Err(self.closed(chain, "has closed").await)
            }
            Ok(_) => Ok(()),
        }
    }

    async fn closed(&self, chain: &ChainId, how: &str) -> StreamError {
        if let Err(error) = self.store.close_chain(chain).await {
            return error;
        }
        StreamError::closed(format!(
            "the Workflow {:?} that owns this stream {how}",
            chain.workflow_id
        ))
    }

    /// The chain a stream address names now.
    async fn chain(&self, stream: &StreamAddress) -> StreamResult<ChainId> {
        let key = owner_key(stream);
        // A pinned run's chain never changes. The latest run's chain changes when a new chain
        // takes the Workflow id.
        if let Some(checked) = self.chains.get(&key)
            && (!key.run_id.is_empty() || checked.at.elapsed() < self.options.owner_recheck)
        {
            return Ok(chain_of(stream, &checked.first_run_id));
        }
        let described = self.describe(&key, &key.run_id).await.map_err(not_found)?;
        let first_run_id = first_run_id(&key, &described)?;
        self.chains
            .put(key, &first_run_id, self.options.owner_recheck);
        Ok(chain_of(stream, &first_run_id))
    }

    async fn describe(
        &self,
        owner: &OwnerKey,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        self.owner
            .describe(&owner.namespace, &owner.workflow_id, run_id)
            .await
    }

    fn hash(&self, stream: &StreamAddress) -> String {
        stream_hash(
            &stream.namespace,
            WORKFLOW_OWNER_KIND,
            &stream.workflow_id,
            &stream.topic,
        )
    }

    fn cursor(&self, stream: &StreamAddress, position: &str) -> String {
        mint_cursor(self.store.name(), &self.hash(stream), position)
    }
}

/// The address of a stream this release keeps, checked.
fn address(stream: Option<&StreamAddress>) -> StreamResult<&StreamAddress> {
    let stream =
        stream.ok_or_else(|| StreamError::invalid_argument("a stream call needs its stream"))?;
    if stream.owner_kind != StreamOwnerKind::Workflow as i32 {
        return Err(StreamError::unsupported(format!(
            "this release keeps streams of Workflows only, not owner kind {}",
            stream.owner_kind
        )));
    }
    if stream.namespace.is_empty() || stream.workflow_id.is_empty() {
        return Err(StreamError::invalid_argument(
            "a stream needs its namespace and its owner's Workflow id",
        ));
    }
    check_topic(&stream.topic)?;
    Ok(stream)
}

/// Refuses a topic no store keys reliably, by the rules Core's Workflow path also applies.
fn check_topic(topic: &str) -> StreamResult<()> {
    temporalio_common::streams::check_topic(topic).map_err(StreamError::invalid_argument)
}

fn owner_key(stream: &StreamAddress) -> OwnerKey {
    OwnerKey {
        namespace: stream.namespace.clone(),
        workflow_id: stream.workflow_id.clone(),
        run_id: stream.run_id.clone(),
    }
}

fn chain_of(stream: &StreamAddress, first_run_id: &str) -> ChainId {
    chain_of_key(&owner_key(stream), first_run_id)
}

fn chain_of_key(owner: &OwnerKey, first_run_id: &str) -> ChainId {
    ChainId {
        namespace: owner.namespace.clone(),
        workflow_id: owner.workflow_id.clone(),
        first_run_id: first_run_id.to_string(),
    }
}

fn first_run_id(owner: &OwnerKey, described: &OwnerDescription) -> StreamResult<String> {
    if described.first_run_id.is_empty() {
        // The Workflow's own output is keyed by the chain's first run, so an empty one would
        // split the stream in two without an error.
        return Err(StreamError::unsupported(format!(
            "Temporal did not report the first run id of Workflow {:?}, which streams are \
             keyed by",
            owner.workflow_id
        )));
    }
    Ok(described.first_run_id.clone())
}

fn not_found(error: OwnerError) -> StreamError {
    match error {
        OwnerError::NotFound(message) => StreamError::not_found(format!(
            "the Workflow that owns the stream was not found, and its stream is keyed by its \
             run chain: {message}"
        )),
        error => describe_failed(&error),
    }
}

fn describe_failed(error: &OwnerError) -> StreamError {
    // Callers catch stream errors only, and the owner check is part of the call.
    StreamError::storage(format!(
        "Temporal could not describe the stream's owner: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStore,
        proto::{
            ActivityProducer, AppendRecord, NamedProducer, StoreReadRequest, StreamFailureKind,
            StreamRecordKind,
        },
    };
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload, enums::v1::WorkflowExecutionStatus,
    };

    /// Temporal as the tests need it: the latest run of each Workflow id, and pinned runs.
    #[derive(Default)]
    struct Owners {
        latest: Mutex<HashMap<String, Result<OwnerDescription, OwnerError>>>,
        runs: Mutex<HashMap<String, OwnerDescription>>,
        describes: AtomicUsize,
    }

    impl Owners {
        fn run(&self, workflow_id: &str, run_id: &str, first_run_id: &str) {
            let described = OwnerDescription {
                run_id: run_id.to_string(),
                first_run_id: first_run_id.to_string(),
                status: WorkflowExecutionStatus::Running,
            };
            self.runs
                .lock()
                .unwrap()
                .insert(run_id.to_string(), described.clone());
            self.latest
                .lock()
                .unwrap()
                .insert(workflow_id.to_string(), Ok(described));
        }

        fn end(&self, workflow_id: &str, status: WorkflowExecutionStatus) {
            if let Some(Ok(latest)) = self.latest.lock().unwrap().get_mut(workflow_id) {
                latest.status = status;
            }
        }

        fn fail(&self, workflow_id: &str, error: OwnerError) {
            self.latest
                .lock()
                .unwrap()
                .insert(workflow_id.to_string(), Err(error));
        }

        fn describes(&self) -> usize {
            self.describes.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl OwnerClient for Owners {
        async fn describe(
            &self,
            _namespace: &str,
            workflow_id: &str,
            run_id: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            self.describes.fetch_add(1, Ordering::Relaxed);
            if !run_id.is_empty() {
                return self
                    .runs
                    .lock()
                    .unwrap()
                    .get(run_id)
                    .cloned()
                    .ok_or_else(|| OwnerError::NotFound(run_id.to_string()));
            }
            self.latest
                .lock()
                .unwrap()
                .get(workflow_id)
                .cloned()
                .unwrap_or_else(|| Err(OwnerError::NotFound(workflow_id.to_string())))
        }
    }

    struct Setup {
        store: Arc<MemoryStore>,
        owners: Arc<Owners>,
        streams: Streams,
    }

    fn setup(options: StreamsOptions) -> Setup {
        let store = Arc::new(MemoryStore::new());
        let owners = Arc::new(Owners::default());
        owners.run("wf", "run-1", "run-1");
        let streams = Streams::new(store.clone(), owners.clone(), options);
        Setup {
            store,
            owners,
            streams,
        }
    }

    fn stream(topic: &str) -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "wf".to_string(),
            run_id: String::new(),
            topic: topic.to_string(),
        }
    }

    fn data(value: &str) -> AppendRecord {
        AppendRecord {
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: value.as_bytes().to_vec(),
                ..Default::default()
            }),
            content_hash: Sha256::digest(value.as_bytes()).to_vec(),
        }
    }

    fn append(producer: &str, sequence: i64, value: &str) -> AppendRequest {
        AppendRequest {
            stream: Some(stream("out")),
            producer: Some(Producer::Named(NamedProducer {
                producer_id: producer.to_string(),
                attempt: 1,
            })),
            sequence,
            records: vec![data(value)],
            digest: Sha256::digest(value.as_bytes()).to_vec(),
        }
    }

    fn chain(first_run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: first_run_id.to_string(),
        }
    }

    async fn chain_closed(store: &MemoryStore, first_run_id: &str) -> bool {
        store
            .read(StoreReadRequest {
                chain: Some(chain(first_run_id)),
                topic: "out".to_string(),
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .closed
    }

    fn kind<T: std::fmt::Debug>(result: StreamResult<T>) -> StreamFailureKind {
        result.unwrap_err().kind
    }

    #[tokio::test]
    async fn an_append_lands_on_the_chain_of_the_latest_run() {
        let setup = setup(StreamsOptions::default());
        let landed = setup.streams.append(append("p", 1, "1")).await.unwrap();
        let read = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].cursor, landed.last_cursor);
        let latest = setup
            .streams
            .latest(LatestRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap();
        assert_eq!(latest.cursor, landed.last_cursor);
        // The record landed on the chain of the first run.
        let held = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(held.position, "0");
    }

    #[tokio::test]
    async fn an_activity_writes_as_its_id_at_its_run() {
        let setup = setup(StreamsOptions::default());
        let mut request = append("", 1, "1");
        request.producer = Some(Producer::Activity(ActivityProducer {
            workflow_id: "wf".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "fetch".to_string(),
            attempt: 2,
        }));
        setup.streams.append(request).await.unwrap();
        let read = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                ..Default::default()
            })
            .await
            .unwrap();
        let Some(crate::proto::read_record::Record::Stored(stored)) = &read.records[0].record
        else {
            panic!("no record");
        };
        assert_eq!(
            (stored.producer_id.as_str(), stored.attempt),
            ("fetch@run-1", 2)
        );
    }

    #[tokio::test]
    async fn requests_no_store_keeps_are_refused() {
        let setup = setup(StreamsOptions::default());
        let cases: Vec<(AppendRequest, StreamFailureKind)> = vec![
            (append("", 1, "1"), StreamFailureKind::InvalidArgument),
            (
                AppendRequest {
                    producer: Some(Producer::Named(NamedProducer {
                        producer_id: "p".to_string(),
                        attempt: 0,
                    })),
                    ..append("p", 1, "1")
                },
                StreamFailureKind::InvalidArgument,
            ),
            (append("p", 0, "1"), StreamFailureKind::InvalidArgument),
            (
                AppendRequest {
                    records: Vec::new(),
                    ..append("p", 1, "1")
                },
                StreamFailureKind::InvalidArgument,
            ),
            (
                AppendRequest {
                    producer: None,
                    ..append("p", 1, "1")
                },
                StreamFailureKind::InvalidArgument,
            ),
        ];
        for (request, expected) in cases {
            assert_eq!(kind(setup.streams.append(request).await), expected);
        }
        for topic in [
            "",
            "a\u{1f}b",
            &"t".repeat(temporalio_common::streams::MAX_TOPIC_BYTES + 1),
        ] {
            let request = AppendRequest {
                stream: Some(stream(topic)),
                ..append("p", 1, "1")
            };
            assert_eq!(
                kind(setup.streams.append(request).await),
                StreamFailureKind::InvalidArgument,
                "{topic:?}"
            );
        }
        // A topic counts bytes, so 256 two-byte characters are too many.
        assert!(check_topic(&"é".repeat(128)).is_ok());
        assert!(check_topic(&"é".repeat(129)).is_err());
        let mut other_kind = stream("out");
        other_kind.owner_kind = StreamOwnerKind::Unspecified as i32;
        assert_eq!(
            kind(
                setup
                    .streams
                    .latest(LatestRequest {
                        stream: Some(other_kind)
                    })
                    .await
            ),
            StreamFailureKind::Unsupported
        );
        assert_eq!(setup.owners.describes(), 0);
    }

    #[tokio::test]
    async fn a_stream_of_a_missing_workflow_is_not_found() {
        let setup = setup(StreamsOptions::default());
        let mut request = append("p", 1, "1");
        request.stream.as_mut().unwrap().workflow_id = "missing".to_string();
        assert_eq!(
            kind(setup.streams.append(request).await),
            StreamFailureKind::NotFound
        );
    }

    #[tokio::test]
    async fn rule_17_1_an_empty_first_run_id_is_unsupported() {
        let setup = setup(StreamsOptions::default());
        setup.owners.run("wf", "run-1", "");
        let error = setup
            .streams
            .latest(LatestRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
        assert!(error.message.contains("first run id"), "{error}");
    }

    #[tokio::test]
    async fn rule_17_2_a_describe_failure_arrives_as_a_storage_failure() {
        let setup = setup(StreamsOptions::default());
        setup
            .owners
            .fail("wf", OwnerError::Failed("unavailable".to_string()));
        let error = setup
            .streams
            .latest(LatestRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(error.message.contains("unavailable"), "{error}");
        assert_eq!(
            kind(setup.streams.append(append("p", 1, "1")).await),
            StreamFailureKind::Storage
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rule_19_1_a_long_lived_producer_rechecks_its_owner_within_a_minute() {
        // Nothing marks the chain of a Workflow that never published itself, so only the
        // producer's own recheck ends its writes.
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Terminated);
        tokio::time::advance(Duration::from_secs(59)).await;
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        assert_eq!(setup.owners.describes(), 1);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            kind(setup.streams.append(append("p", 3, "3")).await),
            StreamFailureKind::Closed
        );
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_producer_rechecks_its_owner_once_retention_passed() {
        // The close mark expires with the stream, so a producer can't wait longer than that.
        let setup = setup(StreamsOptions::for_retention(Duration::from_millis(300)));
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        tokio::time::advance(Duration::from_millis(301)).await;
        assert_eq!(
            kind(setup.streams.append(append("p", 2, "2")).await),
            StreamFailureKind::Closed
        );
    }

    #[tokio::test]
    async fn a_new_producer_closes_an_ended_chain_the_worker_missed() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Terminated);
        assert!(!chain_closed(&setup.store, "run-1").await);
        assert_eq!(
            kind(setup.streams.append(append("q", 1, "2")).await),
            StreamFailureKind::Closed
        );
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_producer_whose_workflow_id_was_reused_is_refused() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        // A new chain under the same id: the old chain is over even though the id's latest run
        // is running.
        setup.owners.run("wf", "run-9", "run-9");
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        assert_eq!(
            kind(setup.streams.append(append("p", 2, "2")).await),
            StreamFailureKind::Closed
        );
        assert!(chain_closed(&setup.store, "run-1").await);
        // A producer that starts now writes on the new chain.
        setup.streams.append(append("q", 1, "3")).await.unwrap();
        assert!(!chain_closed(&setup.store, "run-9").await);
    }

    #[tokio::test(start_paused = true)]
    async fn rule_19_2_an_owner_history_no_longer_holds_closes_the_stream() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .fail("wf", OwnerError::NotFound("workflow not found".to_string()));
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup.streams.append(append("p", 2, "2")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Closed);
        assert!(error.message.contains("gone from History"), "{error}");
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn rule_19_2_an_owner_check_that_fails_is_a_storage_failure() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .fail("wf", OwnerError::Failed("unavailable".to_string()));
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup.streams.append(append("p", 2, "2")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(error.message.contains("unavailable"), "{error}");
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_later_run_of_the_chain_keeps_producers_writing() {
        // A retried run, a cron run and a run that continued as new all keep the chain's first
        // run, so the chain is still open.
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.run("wf", "run-2", "run-1");
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[test]
    fn only_a_closed_run_that_did_not_continue_ends_its_chain() {
        use WorkflowExecutionStatus::*;
        let ended = |status| {
            OwnerDescription {
                run_id: String::new(),
                first_run_id: String::new(),
                status,
            }
            .chain_ended()
        };
        for status in [Completed, Failed, Canceled, Terminated, TimedOut] {
            assert!(ended(status), "{status:?}");
        }
        for status in [Running, ContinuedAsNew, Unspecified] {
            assert!(!ended(status), "{status:?}");
        }
    }

    #[tokio::test]
    async fn a_producer_pinned_to_a_run_writes_on_that_runs_chain() {
        let setup = setup(StreamsOptions::default());
        setup.owners.run("wf", "run-2", "run-1");
        let mut request = append("p", 1, "1");
        request.stream.as_mut().unwrap().run_id = "run-2".to_string();
        setup.streams.append(request).await.unwrap();
        let held = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(held.position, "0");
    }

    #[tokio::test]
    async fn a_read_resolves_its_chain_once() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let before = setup.owners.describes();
        let first = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        let next = setup
            .streams
            .read(ReadRequest {
                stream: Some(stream("out")),
                after: first.cursor.clone(),
                state: first.state.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(next.records.len(), 1);
        assert!(setup.owners.describes() <= before + 1);
    }

    #[tokio::test]
    async fn a_closed_stream_refuses_new_batches_but_answers_a_repeat() {
        let setup = setup(StreamsOptions::default());
        let landed = setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
            })
            .await
            .unwrap();
        assert_eq!(
            setup.streams.append(append("p", 1, "1")).await.unwrap(),
            landed
        );
        assert_eq!(
            kind(setup.streams.append(append("p", 2, "2")).await),
            StreamFailureKind::Closed
        );
        let mut other = append("p", 1, "1");
        other.stream.as_mut().unwrap().topic = "other".to_string();
        setup.streams.append(other).await.unwrap();
    }

    /// The digest the Python SDK's Redis provider took over `[{"n": 1}, {"n": 2}]` from producer
    /// `p`, attempt 1, sequence 1.
    const PYTHON_DIGEST: &str = "0499768af1856bc6fd19cf6cf90e28e971099f1680e8347d70bdb4b65273f30f";

    fn from_hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn a_retry_carrying_langs_digest_dedupes_and_the_store_keeps_it_unchanged() {
        let setup = setup(StreamsOptions::default());
        let mut request = append("p", 1, "1");
        request.records.push(data("2"));
        request.digest = from_hex(PYTHON_DIGEST);
        let first = setup.streams.append(request.clone()).await.unwrap();
        // A retry whose codec made other ciphertext still carries the same digest.
        let mut retry = request.clone();
        retry.records[0].body.as_mut().unwrap().data = b"other ciphertext".to_vec();
        assert_eq!(setup.streams.append(retry).await.unwrap(), first);
        // The store compares the bytes lang sent, so a store-level retry with them dedupes too.
        let landed = setup
            .store
            .append(StoreAppendRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence: 1,
                digest: from_hex(PYTHON_DIGEST),
                records: vec![b"\x1a\x03out".to_vec()],
            })
            .await
            .unwrap();
        assert_eq!(landed.first_position, "0");
        let mut divergent = request.clone();
        divergent.digest = vec![7; 32];
        assert_eq!(
            kind(setup.streams.append(divergent).await),
            StreamFailureKind::ProducerDivergent
        );
    }

    #[tokio::test]
    async fn an_append_without_langs_digest_is_refused_before_any_call() {
        let setup = setup(StreamsOptions::default());
        for digest in [Vec::new(), vec![1; 16]] {
            let mut request = append("p", 1, "1");
            request.digest = digest;
            assert_eq!(
                kind(setup.streams.append(request).await),
                StreamFailureKind::InvalidArgument
            );
        }
        assert_eq!(setup.owners.describes(), 0);
    }
}
