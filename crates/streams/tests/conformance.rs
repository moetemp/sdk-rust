//! The store contract, run against every store.
//!
//! Each case goes through [StreamStore] and [read_page] only, so a new store proves the contract
//! by passing this file. A store joins with one `conformance!` line and a [Case] that makes its
//! chains and drops its oldest records. Cases that need a Worker or the owner's History live with
//! the Worker and the owner checks.

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    BEGINNING, END, MemoryStore, NewestOrArm, Notifier, NotifierClient, NotifyError,
    NotifyingStore, OwnerClient, OwnerDescription, OwnerError, ReadTarget, StreamNotification,
    StreamResult, StreamStore, Streams, StreamsOptions, mint_cursor, proto::*, read_page,
    stored_append_record, stored_output_record, stream_hash,
};

#[async_trait::async_trait]
trait Case: Send + Sync {
    fn store(&self) -> Arc<dyn StreamStore>;

    /// A chain no other case uses.
    fn chain(&self) -> ChainId;

    /// Drops all but the newest `keep` records of a topic, as retention does.
    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize);

    /// Lets a quiet topic's records expire whole, as a log does after a retention with no
    /// write. What the store knows about the topic stays.
    async fn expire_log(&self, chain: &ChainId, topic: &str);
}

static CHAINS: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        CHAINS.fetch_add(1, Ordering::Relaxed)
    )
}

struct Memory(Arc<MemoryStore>);

#[async_trait::async_trait]
impl Case for Memory {
    fn store(&self) -> Arc<dyn StreamStore> {
        self.0.clone()
    }

    fn chain(&self) -> ChainId {
        ChainId {
            namespace: "default".to_string(),
            workflow_id: unique("conformance"),
            first_run_id: unique("run"),
        }
    }

    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
        self.0.truncate(chain, topic, keep);
    }

    async fn expire_log(&self, chain: &ChainId, topic: &str) {
        self.0.truncate(chain, topic, 0);
    }
}

async fn memory() -> Option<Memory> {
    Some(Memory(Arc::new(MemoryStore::new())))
}

/// Takes every notification and sends nothing.
struct Quiet;

#[async_trait::async_trait]
impl NotifierClient for Quiet {
    async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
        Ok(())
    }
}

/// Another case's store behind `NotifyingStore`, which every process with a client runs, so a
/// method the wrapper doesn't forward fails the contract.
struct Notifying<C> {
    inner: C,
    store: Arc<dyn StreamStore>,
}

async fn notifying<C: Case>(
    inner: impl std::future::Future<Output = Option<C>>,
) -> Option<Notifying<C>> {
    let inner = inner.await?;
    let store = Arc::new(NotifyingStore::new(
        inner.store(),
        Arc::new(Notifier::new(Arc::new(Quiet), 10)),
    ));
    Some(Notifying { inner, store })
}

#[async_trait::async_trait]
impl<C: Case> Case for Notifying<C> {
    fn store(&self) -> Arc<dyn StreamStore> {
        self.store.clone()
    }

    fn chain(&self) -> ChainId {
        self.inner.chain()
    }

    async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
        self.inner.drop_oldest(chain, topic, keep).await;
    }

    async fn expire_log(&self, chain: &ChainId, topic: &str) {
        self.inner.expire_log(chain, topic).await;
    }
}

#[cfg(feature = "redis")]
mod on_redis {
    use super::*;
    use redis::{AsyncCommands, Value};
    use temporalio_streams::{RedisStore, RedisStoreOptions};

    /// A connection of the test's own, to do to the keys what retention does.
    #[derive(Clone)]
    pub(crate) enum Raw {
        Single(redis::aio::MultiplexedConnection),
        Cluster(redis::cluster_async::ClusterConnection),
    }

    impl Raw {
        pub(crate) async fn connect(url: &str, cluster: bool) -> Self {
            if cluster {
                let client = redis::cluster::ClusterClient::new(vec![url]).unwrap();
                return Raw::Cluster(client.get_async_connection().await.unwrap());
            }
            let client = redis::Client::open(url).unwrap();
            Raw::Single(client.get_multiplexed_async_connection().await.unwrap())
        }

        pub(crate) async fn query<T: redis::FromRedisValue>(&mut self, cmd: &redis::Cmd) -> T {
            match self {
                Raw::Single(conn) => cmd.query_async(conn).await.unwrap(),
                Raw::Cluster(conn) => cmd.query_async(conn).await.unwrap(),
            }
        }

        pub(crate) async fn hset(&mut self, key: &str, field: &str, value: &str) {
            let _: () = match self {
                Raw::Single(conn) => conn.hset(key, field, value).await.unwrap(),
                Raw::Cluster(conn) => conn.hset(key, field, value).await.unwrap(),
            };
        }
    }

    /// The key layout, written out again so the tests check the store's.
    pub(crate) fn part(text: &str) -> String {
        text.bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                    (byte as char).to_string()
                }
                _ => format!("%{byte:02X}"),
            })
            .collect()
    }

    pub(crate) fn log_key(prefix: &str, chain: &ChainId, topic: &str) -> String {
        format!(
            "{}:{{{}:{}:{}}}:t:{}",
            part(prefix),
            part(&chain.namespace),
            part(&chain.workflow_id),
            part(&chain.first_run_id),
            part(topic)
        )
    }

    pub(crate) struct Redis {
        pub(crate) store: Arc<RedisStore>,
        pub(crate) raw: Raw,
        pub(crate) prefix: String,
        _user: Option<DocumentedUser>,
    }

    /// The rules of the documented `ACL SETUSER` line, after the user name.
    pub(crate) fn documented_rules() -> Vec<String> {
        let page = include_str!("../docs/redis.md");
        let lines: Vec<_> = page
            .lines()
            .filter_map(|line| line.strip_prefix("ACL SETUSER "))
            .collect();
        assert_eq!(
            lines.len(),
            1,
            "the Redis page should give one ACL SETUSER line"
        );
        lines[0]
            .split_whitespace()
            .skip(1)
            .map(str::to_string)
            .collect()
    }

    const DOCUMENTED_KEYS: &str = "~temporal-streams:{my-ns:*";

    /// A Redis user with exactly the documented rules, for one prefix and namespace. The page's
    /// key pattern and password are swapped for the test's own, and nothing else changes.
    pub(crate) struct DocumentedUser {
        admin_urls: Vec<String>,
        name: String,
        pub(crate) url: String,
    }

    impl DocumentedUser {
        pub(crate) async fn create(
            url: &str,
            prefix: &str,
            namespace: &str,
            extra: &[&str],
        ) -> Self {
            let rules = documented_rules();
            assert!(
                rules.iter().any(|rule| rule == DOCUMENTED_KEYS),
                "{rules:?}"
            );
            assert!(rules.iter().any(|rule| rule == ">secret"), "{rules:?}");
            let name = unique("streams-acl");
            let password = unique("pw");
            let keys = format!("~{}:{{{}:*", part(prefix), part(namespace));
            let mut setuser = redis::cmd("ACL");
            setuser.arg("SETUSER").arg(&name);
            for rule in &rules {
                match rule.as_str() {
                    DOCUMENTED_KEYS => setuser.arg(&keys),
                    ">secret" => setuser.arg(format!(">{password}")),
                    rule => setuser.arg(rule),
                };
            }
            for rule in extra {
                setuser.arg(*rule);
            }
            // An ACL user lives on one node, so a cluster needs it on every node.
            let admins = nodes(url).await;
            for admin in &admins {
                let mut admin = Raw::connect(admin, false).await;
                let _: () = admin.query(&setuser).await;
            }
            let address = url.trim_start_matches("redis://");
            Self {
                admin_urls: admins,
                url: format!("redis://{name}:{password}@{address}"),
                name,
            }
        }
    }

    impl Drop for DocumentedUser {
        fn drop(&mut self) {
            for url in &self.admin_urls {
                if let Ok(mut admin) =
                    redis::Client::open(url.as_str()).and_then(|client| client.get_connection())
                {
                    let _: redis::RedisResult<()> = redis::cmd("ACL")
                        .arg("DELUSER")
                        .arg(&self.name)
                        .query(&mut admin);
                }
            }
        }
    }

    /// Every node of the server at `url`: the server itself, or each node of a cluster.
    async fn nodes(url: &str) -> Vec<String> {
        let mut admin = Raw::connect(url, false).await;
        let info: String = admin.query(redis::cmd("INFO").arg("cluster")).await;
        if !info.contains("cluster_enabled:1") {
            return vec![url.to_string()];
        }
        let nodes: String = admin.query(redis::cmd("CLUSTER").arg("NODES")).await;
        nodes
            .lines()
            .filter_map(|line| line.split_whitespace().nth(1))
            .map(|address| format!("redis://{}/0", address.split('@').next().unwrap()))
            .collect()
    }

    pub(crate) async fn redis_as_documented_user(
        variable: &str,
        cluster: bool,
        extra: &[&str],
    ) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        let prefix = unique("conformance-acl");
        let user = DocumentedUser::create(&url, &prefix, "default", extra).await;
        let mut options = RedisStoreOptions::new(user.url.clone());
        options.key_prefix = prefix.clone();
        options.cluster = cluster;
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: Some(user),
        })
    }

    pub(crate) async fn redis(variable: &str, cluster: bool) -> Option<Redis> {
        let Ok(url) = std::env::var(variable) else {
            eprintln!("set {variable} to run the Redis store cases");
            return None;
        };
        // A prefix per case keeps cases apart in one Redis.
        let prefix = unique("conformance");
        let mut options = RedisStoreOptions::new(url.clone());
        options.cluster = cluster;
        options.key_prefix = prefix.clone();
        Some(Redis {
            store: Arc::new(RedisStore::connect(options).await.unwrap()),
            raw: Raw::connect(&url, cluster).await,
            prefix,
            _user: None,
        })
    }

    #[async_trait::async_trait]
    impl Case for Redis {
        fn store(&self) -> Arc<dyn StreamStore> {
            self.store.clone()
        }

        fn chain(&self) -> ChainId {
            ChainId {
                namespace: "default".to_string(),
                workflow_id: unique("conformance"),
                first_run_id: unique("run"),
            }
        }

        async fn drop_oldest(&self, chain: &ChainId, topic: &str, keep: usize) {
            // Trims as the append script does, watermark included.
            let mut raw = self.raw.clone();
            let log = log_key(&self.prefix, chain, topic);
            let entries: Vec<(String, Value)> = raw
                .query(redis::cmd("XRANGE").arg(&log).arg("-").arg("+"))
                .await;
            let Some(doomed) = entries.len().checked_sub(keep).filter(|&n| n > 0) else {
                return;
            };
            let _: i64 = raw
                .query(redis::cmd("XTRIM").arg(&log).arg("MAXLEN").arg(keep))
                .await;
            raw.hset(&format!("{log}:meta"), "trimmed", &entries[doomed - 1].0)
                .await;
        }

        async fn expire_log(&self, chain: &ChainId, topic: &str) {
            // The log key expires and its meta stays, as the tombstone.
            let mut raw = self.raw.clone();
            let _: i64 = raw
                .query(redis::cmd("DEL").arg(log_key(&self.prefix, chain, topic)))
                .await;
        }
    }
}

const OUT: &str = "out";
const OTHER: &str = "other";

/// One producer attempt on one topic, numbering its records as a producer in lang does.
struct Producer {
    store: Arc<dyn StreamStore>,
    chain: ChainId,
    topic: String,
    id: String,
    attempt: i64,
    sequence: i64,
}

impl Producer {
    fn new(case: &dyn Case, chain: &ChainId, topic: &str, id: &str, attempt: i64) -> Self {
        Self {
            store: case.store(),
            chain: chain.clone(),
            topic: topic.to_string(),
            id: id.to_string(),
            attempt,
            sequence: 1,
        }
    }

    fn request(&self, records: &[AppendRecord]) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(self.chain.clone()),
            topic: self.topic.clone(),
            producer_id: self.id.clone(),
            attempt: self.attempt,
            sequence: self.sequence,
            digest: digest(records),
            records: records
                .iter()
                .enumerate()
                .map(|(index, record)| {
                    stored_append_record(
                        &self.topic,
                        &self.id,
                        self.attempt,
                        self.sequence + index as i64,
                        record,
                    )
                    .unwrap()
                    .encode_to_vec()
                })
                .collect(),
        }
    }

    async fn send(&mut self, records: &[AppendRecord]) -> StreamResult<StoreAppendResponse> {
        let response = self.store.append(self.request(records)).await?;
        self.sequence += records.len() as i64;
        Ok(response)
    }

    async fn append(&mut self, values: &[&str]) -> StoreAppendResponse {
        let records: Vec<_> = values.iter().map(|value| data(value)).collect();
        self.send(&records).await.unwrap()
    }
}

/// A record as lang hands it over: the body already encoded, the hash taken before the codec.
fn data(value: &str) -> AppendRecord {
    encoded(value, value.as_bytes())
}

fn encoded(value: &str, ciphertext: &[u8]) -> AppendRecord {
    AppendRecord {
        kind: StreamRecordKind::Data as i32,
        body: Some(Payload {
            metadata: [("encoding".to_string(), b"binary/test".to_vec())].into(),
            data: ciphertext.to_vec(),
            ..Default::default()
        }),
        content_hash: hash(value),
    }
}

fn finish() -> AppendRecord {
    AppendRecord {
        kind: StreamRecordKind::Finish as i32,
        ..Default::default()
    }
}

fn hash(value: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(value.as_bytes()).to_vec()
}

/// Stands in for lang's batch digest, which a store keeps and compares as opaque bytes.
fn digest(records: &[AppendRecord]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for record in records {
        digest.update(record.kind.to_be_bytes());
        digest.update((record.content_hash.len() as u64).to_be_bytes());
        digest.update(&record.content_hash);
    }
    digest.finalize().to_vec()
}

fn target(chain: &ChainId, topic: &str) -> ReadTarget {
    ReadTarget {
        chain: chain.clone(),
        topic: topic.to_string(),
        stream_hash: stream_hash(&chain.namespace, "workflow", &chain.workflow_id, topic),
    }
}

fn cursor(case: &dyn Case, chain: &ChainId, topic: &str, position: &str) -> String {
    mint_cursor(
        case.store().name(),
        &target(chain, topic).stream_hash,
        position,
    )
}

async fn read(
    case: &dyn Case,
    chain: &ChainId,
    topic: &str,
    after: &str,
) -> StreamResult<ReadResponse> {
    read_with(case, chain, topic, after, Duration::ZERO, &[]).await
}

async fn read_with(
    case: &dyn Case,
    chain: &ChainId,
    topic: &str,
    after: &str,
    wait: Duration,
    state: &[u8],
) -> StreamResult<ReadResponse> {
    read_page(
        &*case.store(),
        &target(chain, topic),
        &ReadRequest {
            after: after.to_string(),
            wait: Some(wait.try_into().unwrap()),
            state: state.to_vec(),
            ..Default::default()
        },
    )
    .await
}

fn stored(record: &ReadRecord) -> &StreamRecord {
    match record.record.as_ref().unwrap() {
        read_record::Record::Stored(stored) => stored,
        other => panic!("not a stored record: {other:?}"),
    }
}

/// The body of each stored record, and `SUPERSEDED` for a supersession.
fn values(response: &ReadResponse) -> Vec<String> {
    response
        .records
        .iter()
        .map(|record| match record.record.as_ref().unwrap() {
            read_record::Record::Stored(stored) => match &stored.body {
                Some(body) => String::from_utf8(body.data.clone()).unwrap(),
                None => "FINISH".to_string(),
            },
            read_record::Record::Superseded(_) => "SUPERSEDED".to_string(),
        })
        .collect()
}

async fn latest(case: &dyn Case, chain: &ChainId, topic: &str) -> String {
    case.store()
        .latest(StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: topic.to_string(),
        })
        .await
        .unwrap()
        .position
}

fn staged(chain: &ChainId, run_id: &str, token: &str, records: &[(&str, &str)]) -> StagedBatch {
    StagedBatch {
        chain: Some(chain.clone()),
        run_id: run_id.to_string(),
        token: token.to_string(),
        history_floor_event_id: 3,
        records: records
            .iter()
            .map(|(topic, value)| {
                let record = data(value);
                StagedRecord {
                    topic: topic.to_string(),
                    record: stored_output_record(
                        topic,
                        record.kind,
                        record.body,
                        &record.content_hash,
                        run_id,
                    )
                    .unwrap()
                    .encode_to_vec(),
                }
            })
            .collect(),
    }
}

fn stage_ref(chain: &ChainId, token: &str, topics: &[&str]) -> StageRef {
    StageRef {
        chain: Some(chain.clone()),
        token: token.to_string(),
        topics: topics.iter().map(|topic| topic.to_string()).collect(),
    }
}

use temporalio_common::protos::temporal::api::{
    enums::v1::WorkflowExecutionStatus, history::v1::HistoryEvent,
};

/// Temporal for one chain whose Workflow is still running, for cases that go through [Streams],
/// with the History its first run holds.
struct RunningChain(ChainId, std::sync::Mutex<Vec<HistoryEvent>>);

#[async_trait::async_trait]
impl OwnerClient for RunningChain {
    async fn describe(
        &self,
        _namespace: &str,
        _workflow_id: &str,
        _run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: self.0.first_run_id.clone(),
            first_run_id: self.0.first_run_id.clone(),
            status: WorkflowExecutionStatus::Running,
            start_time: None,
        })
    }

    async fn history_after(
        &self,
        _namespace: &str,
        _workflow_id: &str,
        _run_id: &str,
        floor: i64,
    ) -> Result<Vec<HistoryEvent>, OwnerError> {
        Ok(self
            .1
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.event_id > floor)
            .cloned()
            .collect())
    }
}

fn streams_over(case: &dyn Case, chain: &ChainId) -> (Streams, Arc<RunningChain>) {
    let owner = Arc::new(RunningChain(chain.clone(), Default::default()));
    let streams = Streams::new(
        case.store(),
        owner.clone(),
        StreamsOptions {
            owner_check_min: Duration::from_millis(20),
            owner_check_max: Duration::from_millis(80),
            ..StreamsOptions::default()
        },
    );
    (streams, owner)
}

fn stream_read(chain: &ChainId, after: &str, state: &[u8], wait: Duration) -> ReadRequest {
    ReadRequest {
        stream: Some(StreamAddress {
            namespace: chain.namespace.clone(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: chain.workflow_id.clone(),
            run_id: String::new(),
            topic: OUT.to_string(),
        }),
        after: after.to_string(),
        state: state.to_vec(),
        wait: Some(wait.try_into().unwrap()),
        ..Default::default()
    }
}

/// The marker a Workflow Task writes when it commits the stage `token` made at `floor`.
fn marker(event_id: i64, token: &str, floor: i64) -> HistoryEvent {
    use temporalio_common::protos::{
        constants::EXTERNAL_STREAM_MARKER_NAME,
        coresdk::external_data::{
            ExternalOutputStreamManifest, ExternalStreamMarkerData,
            build_external_stream_marker_details,
        },
        temporal::api::{
            enums::v1::EventType,
            history::v1::{MarkerRecordedEventAttributes, history_event::Attributes},
        },
    };
    let data = ExternalStreamMarkerData {
        schema_version: 1,
        output: Some(ExternalOutputStreamManifest {
            stage_token: token.to_string(),
            history_floor_event_id: floor,
            ..Default::default()
        }),
        ..Default::default()
    };
    HistoryEvent {
        event_id,
        event_type: EventType::MarkerRecorded as i32,
        attributes: Some(Attributes::MarkerRecordedEventAttributes(
            MarkerRecordedEventAttributes {
                marker_name: EXTERNAL_STREAM_MARKER_NAME.to_string(),
                details: build_external_stream_marker_details(&data),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn kind(result: StreamResult<impl std::fmt::Debug>) -> StreamFailureKind {
    result.unwrap_err().kind
}

mod cases {
    use super::*;

    pub(crate) async fn append_read_roundtrip(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        producer.append(&["2"]).await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "2"]);
        let identity: Vec<_> = read
            .records
            .iter()
            .map(|record| {
                let stored = stored(record);
                (
                    stored.topic.as_str(),
                    stored.kind,
                    stored.producer_id.as_str(),
                    stored.attempt,
                    stored.sequence,
                )
            })
            .collect();
        let data = StreamRecordKind::Data as i32;
        assert_eq!(identity, [(OUT, data, "p", 1, 1), (OUT, data, "p", 1, 2)]);
    }

    pub(crate) async fn a_batch_lands_in_order_and_names_its_last_record(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let landed = producer.append(&["1", "2", "3"]).await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "2", "3"]);
        let sequences: Vec<_> = read.records.iter().map(|r| stored(r).sequence).collect();
        assert_eq!(sequences, [1, 2, 3]);
        assert_eq!(
            read.records[0].cursor,
            cursor(case, &chain, OUT, &landed.first_position)
        );
        assert_eq!(
            read.records[2].cursor,
            cursor(case, &chain, OUT, &landed.last_position)
        );
        assert_eq!(latest(case, &chain, OUT).await, landed.last_position);
    }

    pub(crate) async fn an_empty_append_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        assert_eq!(kind(producer.send(&[]).await), StreamFailureKind::Refused);
        assert_eq!(latest(case, &chain, OUT).await, "");
    }

    pub(crate) async fn an_append_without_a_digest_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let producer = Producer::new(case, &chain, OUT, "p", 1);
        let mut request = producer.request(&[data("1")]);
        request.digest.clear();
        assert_eq!(
            kind(case.store().append(request).await),
            StreamFailureKind::Refused
        );
    }

    pub(crate) async fn latest_is_empty_on_an_empty_topic(case: &dyn Case) {
        assert_eq!(latest(case, &case.chain(), OUT).await, "");
    }

    pub(crate) async fn a_cursor_resumes_strictly_after_its_record(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let first = producer.append(&["1"]).await;
        producer.append(&["2", "3"]).await;
        let after = cursor(case, &chain, OUT, &first.last_position);
        let read = read(case, &chain, OUT, &after).await.unwrap();
        assert_eq!(values(&read), ["2", "3"]);
        // A cursor a reader saw resumes the same way as one an append returned.
        let again = super::read(case, &chain, OUT, &read.records[0].cursor)
            .await
            .unwrap();
        assert_eq!(values(&again), ["3"]);
        assert_eq!(read.cursor, read.records[1].cursor);
    }

    pub(crate) async fn latest_positions_a_reader_at_the_end(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let end = cursor(case, &chain, OUT, &latest(case, &chain, OUT).await);
        producer.append(&["2"]).await;
        assert_eq!(values(&read(case, &chain, OUT, &end).await.unwrap()), ["2"]);
    }

    pub(crate) async fn end_reads_only_what_arrives_after_the_read_starts(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let mut later = Producer::new(case, &chain, OUT, "p", 1);
        later.sequence = 2;
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            later.append(&["2"]).await;
        });
        let read = read_with(case, &chain, OUT, END, Duration::from_secs(5), &[])
            .await
            .unwrap();
        writer.await.unwrap();
        assert_eq!(values(&read), ["2"]);
        let quiet = read_with(case, &chain, OUT, END, Duration::from_millis(200), &[])
            .await
            .unwrap();
        assert!(quiet.records.is_empty());
        assert_eq!(quiet.cursor, read.cursor);
    }

    pub(crate) async fn a_read_waits_no_longer_than_asked(case: &dyn Case) {
        let chain = case.chain();
        let started = tokio::time::Instant::now();
        let read = read_with(
            case,
            &chain,
            OUT,
            BEGINNING,
            Duration::from_millis(200),
            &[],
        )
        .await
        .unwrap();
        assert!(read.records.is_empty());
        assert_eq!(read.cursor, BEGINNING);
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(150), "{waited:?}");
        assert!(waited < Duration::from_secs(3), "{waited:?}");
    }

    pub(crate) async fn dropping_a_waiting_read_releases_it(case: &dyn Case) {
        let chain = case.chain();
        let parked = read_with(case, &chain, OUT, BEGINNING, Duration::from_secs(30), &[]);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), parked)
                .await
                .is_err()
        );
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1"]
        );
    }

    pub(crate) async fn beginning_starts_at_the_oldest_record_still_held(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let old = producer.append(&["1"]).await;
        producer.append(&["2", "3"]).await;
        case.drop_oldest(&chain, OUT, 2).await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["2", "3"]);
        // The dropped record's cursor still resumes, because nothing after it was dropped.
        let old = cursor(case, &chain, OUT, &old.last_position);
        assert_eq!(
            values(&super::read(case, &chain, OUT, &old).await.unwrap()),
            ["2", "3"]
        );
        // Once a record after it is gone too, the cursor is expired, which a reader can tell
        // apart from a cursor that was never valid here.
        case.drop_oldest(&chain, OUT, 1).await;
        assert_eq!(
            kind(super::read(case, &chain, OUT, &old).await),
            StreamFailureKind::Expired
        );
    }

    pub(crate) async fn a_reader_that_falls_behind_retention_is_told(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1", "2", "3"]).await;
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        let after_one = first.records[0].cursor.clone();
        case.drop_oldest(&chain, OUT, 1).await;
        // The reader got record 1, and record 2 is gone, so it must hear so.
        assert_eq!(
            kind(read_with(case, &chain, OUT, &after_one, Duration::ZERO, &first.state).await),
            StreamFailureKind::Expired
        );
    }

    pub(crate) async fn an_empty_read_from_the_beginning_learns_of_a_trim(case: &dyn Case) {
        // The read has delivered nothing, so it has no position, but records written after it
        // started and dropped before its next call are still lost to it.
        let chain = case.chain();
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert!(first.records.is_empty());
        assert_eq!(first.cursor, BEGINNING);
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        case.drop_oldest(&chain, OUT, 1).await;
        assert_eq!(
            kind(
                read_with(
                    case,
                    &chain,
                    OUT,
                    &first.cursor,
                    Duration::ZERO,
                    &first.state
                )
                .await
            ),
            StreamFailureKind::Expired
        );
        // A new read starts at the oldest record left, as a read from the beginning does.
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["2"]
        );
    }

    pub(crate) async fn rule_11_2_an_empty_read_goes_on_when_a_quiet_log_comes_back(
        case: &dyn Case,
    ) {
        // A log that expired comes back with its next write, and nothing the read could have
        // seen was lost.
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        case.expire_log(&chain, OUT).await;
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert!(first.records.is_empty());
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.sequence = 2;
        producer.append(&["2"]).await;
        let next = read_with(
            case,
            &chain,
            OUT,
            &first.cursor,
            Duration::ZERO,
            &first.state,
        )
        .await
        .unwrap();
        assert_eq!(values(&next), ["2"]);
    }

    pub(crate) async fn an_empty_read_from_the_beginning_with_nothing_trimmed_goes_on(
        case: &dyn Case,
    ) {
        let chain = case.chain();
        let first = read(case, &chain, OUT, BEGINNING).await.unwrap();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let next = read_with(
            case,
            &chain,
            OUT,
            &first.cursor,
            Duration::ZERO,
            &first.state,
        )
        .await
        .unwrap();
        assert_eq!(values(&next), ["1"]);
    }

    pub(crate) async fn a_retried_append_returns_the_original_positions(case: &dyn Case) {
        let chain = case.chain();
        let first = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        // A producer that lost the answer starts over with the same identity, as a restarted
        // process does.
        let retry = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2"])
            .await;
        assert_eq!(retry, first);
        assert_eq!(latest(case, &chain, OUT).await, first.last_position);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "2"]
        );
    }

    pub(crate) async fn a_retry_with_other_ciphertext_still_deduplicates(case: &dyn Case) {
        // A codec that encrypts with a fresh nonce makes a retry's bytes differ. The digest is
        // over the plaintext hashes, so the store still knows the batch.
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let first = producer.send(&[encoded("1", b"nonce-a")]).await.unwrap();
        let mut retry = Producer::new(case, &chain, OUT, "p", 1);
        let again = retry.send(&[encoded("1", b"nonce-b")]).await.unwrap();
        assert_eq!(again, first);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["nonce-a"]
        );
    }

    pub(crate) async fn a_divergent_retry_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let first = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        let mut retry = Producer::new(case, &chain, OUT, "p", 1);
        assert_eq!(
            kind(retry.send(&[data("different")]).await),
            StreamFailureKind::ProducerDivergent
        );
        assert_eq!(latest(case, &chain, OUT).await, first.last_position);
    }

    pub(crate) async fn a_sequence_below_the_newest_is_refused(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let newest = producer.append(&["2"]).await;
        // Even the same content is refused: only the newest batch is a retry.
        let mut stale = Producer::new(case, &chain, OUT, "p", 1);
        assert_eq!(
            kind(stale.send(&[data("1")]).await),
            StreamFailureKind::ProducerStale
        );
        assert_eq!(latest(case, &chain, OUT).await, newest.last_position);
    }

    pub(crate) async fn rule_9_1_a_batch_inside_the_held_batch_is_refused(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1", "2", "3"])
            .await;
        // A second writer of the same attempt, whose sequence sits inside the held batch.
        let mut inside = Producer::new(case, &chain, OUT, "p", 1);
        inside.sequence = 2;
        assert_eq!(
            kind(inside.send(&[data("9")]).await),
            StreamFailureKind::ProducerStale
        );
    }

    pub(crate) async fn rule_64_1_a_gap_in_the_sequence_is_accepted(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        producer.sequence = 10;
        producer.append(&["10"]).await;
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "10"]
        );
    }

    pub(crate) async fn producers_and_attempts_dedupe_apart(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "a", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OUT, "b", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OUT, "a", 2)
            .append(&["1"])
            .await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        let data: Vec<_> = read
            .records
            .iter()
            .filter_map(|record| match record.record.as_ref().unwrap() {
                read_record::Record::Stored(s) => Some((s.producer_id.as_str(), s.attempt)),
                read_record::Record::Superseded(_) => None,
            })
            .collect();
        assert_eq!(data, [("a", 1), ("b", 1), ("a", 2)]);
    }

    pub(crate) async fn a_new_attempt_supersedes_the_old_one(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "model", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OUT, "model", 2)
            .append(&["2"])
            .await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "SUPERSEDED", "2"]);
        let Some(read_record::Record::Superseded(superseded)) = &read.records[1].record else {
            panic!("no supersession");
        };
        assert_eq!(
            (
                superseded.producer_id.as_str(),
                superseded.previous_attempt,
                superseded.attempt
            ),
            ("model", 1, 2)
        );
        // The supersession sits at the cursor of the old attempt's last record. A read that
        // resumes there knows that attempt, so it reports the new one again.
        assert_eq!(read.records[1].cursor, read.records[0].cursor);
        let resumed = super::read(case, &chain, OUT, &read.records[1].cursor)
            .await
            .unwrap();
        assert_eq!(values(&resumed), ["SUPERSEDED", "2"]);
    }

    pub(crate) async fn a_read_resumed_before_a_new_attempt_reports_it(case: &dyn Case) {
        let chain = case.chain();
        let mut first = Producer::new(case, &chain, OUT, "model", 1);
        first.append(&["1"]).await;
        let delivered = first.append(&["2"]).await;
        // The reader stops here, then the producer's retry writes.
        Producer::new(case, &chain, OUT, "model", 2)
            .append(&["3"])
            .await;
        let after = cursor(case, &chain, OUT, &delivered.last_position);
        let resumed = read(case, &chain, OUT, &after).await.unwrap();
        assert_eq!(values(&resumed), ["SUPERSEDED", "3"]);
    }

    pub(crate) async fn a_record_that_does_not_parse_fails_with_its_cursor(case: &dyn Case) {
        let chain = case.chain();
        let producer = Producer::new(case, &chain, OUT, "p", 1);
        let mut request = producer.request(&[data("bad")]);
        request.records = vec![b"\xff not a record".to_vec()];
        let bad = case.store().append(request).await.unwrap();
        let mut next = Producer::new(case, &chain, OUT, "p", 1);
        next.sequence = 2;
        next.append(&["7"]).await;
        let error = read(case, &chain, OUT, BEGINNING).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        let bad = cursor(case, &chain, OUT, &bad.last_position);
        assert_eq!(error.cursor.as_deref(), Some(bad.as_str()));
        // Resuming past it is the caller's choice, and the next record reads.
        assert_eq!(values(&read(case, &chain, OUT, &bad).await.unwrap()), ["7"]);
    }

    pub(crate) async fn finish_is_a_record_of_its_own(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        producer.append(&["1"]).await;
        let finished = producer.send(&[finish()]).await.unwrap();
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        assert_eq!(values(&read), ["1", "FINISH"]);
        assert_eq!(
            stored(&read.records[1]).kind,
            StreamRecordKind::Finish as i32
        );
        assert_eq!(stored(&read.records[1]).producer_id, "p");
        assert_eq!(
            read.records[1].cursor,
            cursor(case, &chain, OUT, &finished.last_position)
        );
    }

    pub(crate) async fn topics_and_chains_are_apart(case: &dyn Case) {
        let chain = case.chain();
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, OTHER, "p", 1)
            .append(&["2"])
            .await;
        // A new chain on the same Workflow id gets streams of its own.
        let next = ChainId {
            first_run_id: format!("{}-next", chain.first_run_id),
            ..chain.clone()
        };
        Producer::new(case, &next, OUT, "p", 1).append(&["3"]).await;
        assert_eq!(
            values(&read(case, &chain, OTHER, BEGINNING).await.unwrap()),
            ["2"]
        );
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1"]
        );
        assert_eq!(
            values(&read(case, &next, OUT, BEGINNING).await.unwrap()),
            ["3"]
        );
    }

    pub(crate) async fn a_cursor_from_another_stream_or_store_is_refused(case: &dyn Case) {
        let one = case.chain();
        let other = case.chain();
        let landed = Producer::new(case, &one, OUT, "p", 1).append(&["1"]).await;
        Producer::new(case, &other, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &one, OTHER, "p", 1)
            .append(&["1"])
            .await;
        // Another Workflow's stream, and another topic of the same one, both hold a record at
        // that position. Neither may resume from it.
        let cursor = cursor(case, &one, OUT, &landed.last_position);
        for (chain, topic) in [(&other, OUT), (&one, OTHER)] {
            let error = read(case, chain, topic, &cursor).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::Cursor);
            assert!(error.message.contains("another stream"), "{error}");
        }
        for token in ["elsewhere:0000abcd:1", "not a cursor"] {
            assert_eq!(
                kind(read(case, &one, OUT, token).await),
                StreamFailureKind::Cursor
            );
        }
    }

    pub(crate) async fn a_closed_chain_refuses_new_batches_but_answers_a_repeat(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let landed = producer.append(&["1"]).await;
        case.store().close_chain(&chain).await.unwrap();
        let repeat = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert_eq!(repeat, landed);
        assert_eq!(
            kind(producer.send(&[data("2")]).await),
            StreamFailureKind::Closed
        );
        assert_eq!(
            kind(
                Producer::new(case, &chain, OTHER, "q", 1)
                    .send(&[data("1")])
                    .await
            ),
            StreamFailureKind::Closed
        );
        let read = case
            .store()
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: OUT.to_string(),
                max_records: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(read.closed);
        assert_eq!(read.records.len(), 1);
    }

    pub(crate) async fn a_refused_append_leaves_no_trace_of_its_topic(case: &dyn Case) {
        // Every store must answer a resume on a topic that never held a record the same way.
        let chain = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        case.store().close_chain(&chain).await.unwrap();
        assert_eq!(
            kind(
                Producer::new(case, &chain, OTHER, "p", 1)
                    .send(&[data("2")])
                    .await
            ),
            StreamFailureKind::Closed
        );
        let after = cursor(case, &chain, OTHER, &landed.last_position);
        assert_eq!(
            kind(read(case, &chain, OTHER, &after).await),
            StreamFailureKind::NotFound
        );
    }

    pub(crate) async fn a_closed_topic_leaves_the_others_open(case: &dyn Case) {
        let chain = case.chain();
        case.store().close_topic(&chain, OUT, None).await.unwrap();
        assert_eq!(
            kind(
                Producer::new(case, &chain, OUT, "p", 1)
                    .send(&[data("1")])
                    .await
            ),
            StreamFailureKind::Closed
        );
        Producer::new(case, &chain, OTHER, "p", 1)
            .append(&["1"])
            .await;
    }

    pub(crate) async fn a_closed_topic_answers_a_repeat_and_ends_reads(case: &dyn Case) {
        let chain = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        case.store().close_topic(&chain, OUT, None).await.unwrap();
        let repeat = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert_eq!(repeat, landed);
        for topic in [OUT, OTHER] {
            let read = case
                .store()
                .read(StoreReadRequest {
                    chain: Some(chain.clone()),
                    topic: topic.to_string(),
                    max_records: 10,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(read.closed, topic == OUT, "{topic}");
        }
    }

    pub(crate) async fn a_chain_lists_its_open_topics(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let open = |chain: ChainId| async move {
            let mut topics = case.store().open_topics(&chain).await.unwrap();
            topics.sort();
            topics
        };
        assert!(open(chain.clone()).await.is_empty());
        // A topic name that needs escaping in a key comes back as written.
        let odd = "a:b c/\u{e9}*";
        Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &chain, odd, "p", 1)
            .append(&["1"])
            .await;
        store
            .stage(staged(&chain, "run-1", "t1", &[("staged", "1")]))
            .await
            .unwrap();
        store
            .stage(staged(&chain, "run-1", "t2", &[("aborted", "1")]))
            .await
            .unwrap();
        store
            .abort(&stage_ref(&chain, "t2", &["aborted"]))
            .await
            .unwrap();
        assert_eq!(
            open(chain.clone()).await,
            [odd, "aborted", OUT, "staged"],
            "a staged topic is known once staged, and an abort doesn't forget it"
        );
        store
            .promote(&stage_ref(&chain, "t1", &["staged"]))
            .await
            .unwrap();
        store.close_topic(&chain, OUT, None).await.unwrap();
        assert_eq!(open(chain.clone()).await, [odd, "aborted", "staged"]);
        let other = case.chain();
        Producer::new(case, &other, OTHER, "p", 1)
            .append(&["1"])
            .await;
        assert_eq!(
            open(chain.clone()).await,
            [odd, "aborted", "staged"],
            "chains are apart"
        );
        store.close_chain(&chain).await.unwrap();
        assert_eq!(
            open(chain).await,
            [odd, "aborted", "staged"],
            "a chain close leaves its topics to be listed"
        );
    }

    pub(crate) async fn a_stage_is_invisible_until_promoted(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        store
            .stage(staged(
                &chain,
                "run-1",
                "t1",
                &[(OUT, "1"), (OTHER, "2"), (OUT, "3")],
            ))
            .await
            .unwrap();
        assert!(
            read(case, &chain, OUT, BEGINNING)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        assert_eq!(
            store.pending_stages(&chain).await.unwrap(),
            [PendingStage {
                token: "t1".to_string(),
                run_id: "run-1".to_string(),
                history_floor_event_id: 3,
                topics: vec![OUT.to_string(), OTHER.to_string()],
            }]
        );
        let promoted = store
            .promote(&stage_ref(&chain, "t1", &[OUT, OTHER]))
            .await
            .unwrap();
        assert_eq!(
            (promoted.outcome, promoted.records),
            (PromoteOutcome::Promoted as i32, 3)
        );
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "3"]
        );
        assert_eq!(
            values(&read(case, &chain, OTHER, BEGINNING).await.unwrap()),
            ["2"]
        );
        assert!(store.pending_stages(&chain).await.unwrap().is_empty());
        // The records become visible once.
        let again = store
            .promote(&stage_ref(&chain, "t1", &[OUT, OTHER]))
            .await
            .unwrap();
        assert_eq!(again.outcome, PromoteOutcome::Settled as i32);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "3"]
        );
    }

    pub(crate) async fn a_repeated_stage_holds_its_records_once(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let batch = staged(&chain, "run-1", "t1", &[(OUT, "1"), (OUT, "2")]);
        store.stage(batch.clone()).await.unwrap();
        store.stage(batch).await.unwrap();
        assert_eq!(store.pending_stages(&chain).await.unwrap().len(), 1);
        let promoted = store
            .promote(&stage_ref(&chain, "t1", &[OUT]))
            .await
            .unwrap();
        assert_eq!(promoted.records, 2);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1", "2"]
        );
    }

    pub(crate) async fn rule_11_1_a_read_ends_only_after_the_chains_last_stage_lands(
        case: &dyn Case,
    ) {
        // A close doesn't prove the chain's output is visible. A stage can still be pending.
        let store = case.store();
        let chain = case.chain();
        store
            .stage(staged(&chain, &chain.first_run_id, "t1", &[(OUT, "last")]))
            .await
            .unwrap();
        store.close_chain(&chain).await.unwrap();
        let (streams, _) = streams_over(case, &chain);
        let first = streams
            .read(stream_read(
                &chain,
                BEGINNING,
                &[],
                Duration::from_millis(200),
            ))
            .await
            .unwrap();
        assert!(first.records.is_empty());
        assert!(!first.done, "a read ended with a stage still pending");
        store
            .promote(&stage_ref(&chain, "t1", &[OUT]))
            .await
            .unwrap();
        let mut request = stream_read(&chain, &first.cursor, &first.state, Duration::from_secs(5));
        let mut delivered = Vec::new();
        for _ in 0..10 {
            let page = streams.read(request.clone()).await.unwrap();
            delivered.extend(values(&page));
            if page.done {
                assert_eq!(delivered, ["last"]);
                return;
            }
            request = stream_read(&chain, &page.cursor, &page.state, Duration::from_secs(5));
        }
        panic!("the read never ended, and delivered {delivered:?}");
    }

    pub(crate) async fn rule_12_1_a_read_that_ends_on_a_close_repairs_the_chain(case: &dyn Case) {
        // A Worker that stopped after the final commit leaves its stage pending, and only the
        // read that sees the close is there to promote it.
        let store = case.store();
        let chain = case.chain();
        let (streams, owner) = streams_over(case, &chain);
        let first = streams
            .read(stream_read(&chain, BEGINNING, &[], Duration::ZERO))
            .await
            .unwrap();
        assert!(first.records.is_empty() && !first.done);
        store
            .stage(staged(&chain, &chain.first_run_id, "t1", &[(OUT, "last")]))
            .await
            .unwrap();
        owner.1.lock().unwrap().push(marker(5, "t1", 3));
        store.close_chain(&chain).await.unwrap();
        let mut request = stream_read(&chain, &first.cursor, &first.state, Duration::from_secs(5));
        let mut delivered = Vec::new();
        for _ in 0..5 {
            let page = tokio::time::timeout(Duration::from_secs(10), streams.read(request.clone()))
                .await
                .expect("the read waited for a stage nothing would promote")
                .unwrap();
            delivered.extend(values(&page));
            if page.done {
                assert_eq!(delivered, ["last"]);
                return;
            }
            request = stream_read(&chain, &page.cursor, &page.state, Duration::from_secs(5));
        }
        panic!("the read never ended, and delivered {delivered:?}");
    }

    pub(crate) async fn an_aborted_stage_never_lands(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        store
            .stage(staged(&chain, "run-1", "t1", &[(OUT, "1")]))
            .await
            .unwrap();
        let reference = stage_ref(&chain, "t1", &[OUT]);
        store.abort(&reference).await.unwrap();
        store.abort(&reference).await.unwrap();
        assert!(store.pending_stages(&chain).await.unwrap().is_empty());
        let promoted = store.promote(&reference).await.unwrap();
        assert_eq!(promoted.outcome, PromoteOutcome::Settled as i32);
        assert_eq!(latest(case, &chain, OUT).await, "");
    }

    pub(crate) async fn a_promotion_naming_too_few_topics_writes_nothing(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        store
            .stage(staged(&chain, "run-1", "t1", &[(OUT, "1"), (OTHER, "2")]))
            .await
            .unwrap();
        let error = store
            .promote(&stage_ref(&chain, "t1", &[OUT]))
            .await
            .unwrap_err();
        assert!(error.message.contains(OTHER), "{error}");
        // A retry names the same topics, so the store refuses it for good.
        assert_eq!(error.kind, StreamFailureKind::Refused);
        assert_eq!(latest(case, &chain, OUT).await, "");
        assert_eq!(store.pending_stages(&chain).await.unwrap().len(), 1);
    }

    pub(crate) async fn a_promotion_lands_on_a_closed_chain(case: &dyn Case) {
        // A Workflow's committed output is never refused.
        let store = case.store();
        let chain = case.chain();
        store
            .stage(staged(&chain, "run-1", "t1", &[(OUT, "1")]))
            .await
            .unwrap();
        store.close_chain(&chain).await.unwrap();
        let promoted = store
            .promote(&stage_ref(&chain, "t1", &[OUT]))
            .await
            .unwrap();
        assert_eq!(promoted.outcome, PromoteOutcome::Promoted as i32);
        assert_eq!(
            values(&read(case, &chain, OUT, BEGINNING).await.unwrap()),
            ["1"]
        );
    }

    pub(crate) async fn rule_19_3_a_reset_run_writes_on_and_keeps_the_chain_open(case: &dyn Case) {
        // A reset run keeps the chain's first run, so its output follows the base run's on the
        // same streams, and producers still write.
        let store = case.store();
        let chain = case.chain();
        store
            .stage(staged(&chain, "base", "t1", &[(OUT, "1")]))
            .await
            .unwrap();
        store
            .promote(&stage_ref(&chain, "t1", &[OUT]))
            .await
            .unwrap();
        store
            .stage(staged(&chain, "reset", "t2", &[(OUT, "1")]))
            .await
            .unwrap();
        store
            .promote(&stage_ref(&chain, "t2", &[OUT]))
            .await
            .unwrap();
        Producer::new(case, &chain, OTHER, "p", 1)
            .append(&["2"])
            .await;
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        let runs: Vec<_> = read
            .records
            .iter()
            .map(|record| {
                let run = &stored(record).metadata[temporalio_common::streams::RUN_ID_KEY];
                String::from_utf8(run.data.clone()).unwrap()
            })
            .collect();
        assert_eq!(runs, ["base", "reset"]);
    }

    pub(crate) async fn deleting_an_owner_drops_every_chain_of_it(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let next = ChainId {
            first_run_id: format!("{}-next", chain.first_run_id),
            ..chain.clone()
        };
        let bystander = case.chain();
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        Producer::new(case, &next, OUT, "p", 1).append(&["1"]).await;
        store
            .stage(staged(&next, "run-2", "t1", &[(OUT, "2")]))
            .await
            .unwrap();
        Producer::new(case, &bystander, OUT, "p", 1)
            .append(&["1"])
            .await;
        let request = DeleteOwnerRequest {
            namespace: chain.namespace.clone(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: chain.workflow_id.clone(),
        };
        assert!(store.delete_owner(request.clone()).await.unwrap().deleted > 0);
        assert_eq!(store.delete_owner(request).await.unwrap().deleted, 0);
        assert_eq!(latest(case, &chain, OUT).await, "");
        assert!(store.pending_stages(&next).await.unwrap().is_empty());
        // Nothing is known about what followed the cursor any more.
        let after = cursor(case, &chain, OUT, &landed.last_position);
        assert_eq!(
            kind(read(case, &chain, OUT, &after).await),
            StreamFailureKind::NotFound
        );
        assert_eq!(
            values(&read(case, &bystander, OUT, BEGINNING).await.unwrap()),
            ["1"]
        );
    }
    /// A store read that asks to arm when it finds nothing, as a reader that sleeps on progress
    /// makes it.
    async fn arming_read(
        case: &dyn Case,
        chain: &ChainId,
        topic: &str,
        after: &str,
    ) -> StoreReadResponse {
        case.store()
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: topic.to_string(),
                after_position: after.to_string(),
                max_records: 10,
                arm: true,
                ..Default::default()
            })
            .await
            .unwrap()
    }

    pub(crate) async fn an_empty_read_that_asks_arms_and_the_next_append_reports_it(
        case: &dyn Case,
    ) {
        let chain = case.chain();
        let read = arming_read(case, &chain, OUT, "").await;
        assert!(read.records.is_empty());
        assert!(read.armed, "an empty read that asks arms");
        let landed = Producer::new(case, &chain, OUT, "p", 1)
            .append(&["1"])
            .await;
        assert!(landed.arm.is_some_and(|arm| arm > 0), "{landed:?}");
    }

    pub(crate) async fn an_append_with_nothing_armed_reports_zero(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        assert_eq!(producer.append(&["1"]).await.arm, Some(0));
        // A read that doesn't ask never arms.
        let read = read(case, &chain, OUT, BEGINNING).await.unwrap();
        let after = &read.records[0].cursor;
        let empty = read_with(case, &chain, OUT, after, Duration::ZERO, &read.state)
            .await
            .unwrap();
        assert!(empty.records.is_empty());
        assert_eq!(producer.append(&["2"]).await.arm, Some(0));
    }

    pub(crate) async fn an_arm_that_finds_a_newer_record_stays_unarmed(case: &dyn Case) {
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let first = producer.append(&["1"]).await;
        producer.append(&["2"]).await;
        let read = arming_read(case, &chain, OUT, &first.last_position).await;
        assert_eq!(read.records.len(), 1);
        assert!(!read.armed, "an answer with records never arms");
        assert_eq!(producer.append(&["3"]).await.arm, Some(0));
    }

    pub(crate) async fn a_disarm_of_an_older_generation_keeps_the_newer_arm(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        assert!(arming_read(case, &chain, OUT, "").await.armed);
        let landed = producer.append(&["1"]).await;
        let older = landed.arm.unwrap();
        assert!(
            arming_read(case, &chain, OUT, &landed.last_position)
                .await
                .armed
        );
        let landed = producer.append(&["2"]).await;
        let newer = landed.arm.unwrap();
        assert!(newer > older, "a new arm gets a higher generation");
        store.disarm(&chain, OUT, older).await.unwrap();
        assert_eq!(producer.append(&["3"]).await.arm, Some(newer));
        store.disarm(&chain, OUT, newer).await.unwrap();
        assert_eq!(producer.append(&["4"]).await.arm, Some(0));
        // A disarm of a topic that holds no arm changes nothing.
        store.disarm(&chain, OTHER, newer).await.unwrap();
    }

    pub(crate) async fn a_promotion_reports_each_topics_arm(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        assert!(arming_read(case, &chain, OUT, "").await.armed);
        store
            .stage(staged(&chain, "run-1", "t1", &[(OUT, "1"), (OTHER, "2")]))
            .await
            .unwrap();
        let promoted = store
            .promote(&stage_ref(&chain, "t1", &[OUT, OTHER]))
            .await
            .unwrap();
        assert_eq!(promoted.outcome(), PromoteOutcome::Promoted);
        assert!(promoted.arms[OUT] > 0, "{promoted:?}");
        assert_eq!(promoted.arms[OTHER], 0, "{promoted:?}");
    }

    pub(crate) async fn newest_or_arm_arms_only_an_empty_topic(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        let NewestOrArm::Armed(generation) = store.newest_or_arm(&chain, OUT).await.unwrap() else {
            panic!("an empty topic arms");
        };
        let mut producer = Producer::new(case, &chain, OUT, "p", 1);
        let landed = producer.append(&["1"]).await;
        assert_eq!(landed.arm, Some(generation));
        store.disarm(&chain, OUT, generation).await.unwrap();
        assert_eq!(
            store.newest_or_arm(&chain, OUT).await.unwrap(),
            NewestOrArm::Newest(landed.last_position)
        );
        assert_eq!(producer.append(&["2"]).await.arm, Some(0));
    }

    pub(crate) async fn a_closed_topic_never_arms(case: &dyn Case) {
        let store = case.store();
        let chain = case.chain();
        store.close_topic(&chain, OUT, None).await.unwrap();
        let read = arming_read(case, &chain, OUT, "").await;
        assert!(read.closed);
        assert!(!read.armed, "a closed answer never arms");
    }
}

macro_rules! conformance {
    ($case:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(case) = $case.await else { return };
                super::cases::$name(&case).await;
            }
        )*
    };
}

/// Every case but owner deletes, which need a rule of their own under the documented ACL.
macro_rules! store_cases {
    ($case:expr) => {
        conformance!($case;
            append_read_roundtrip,
            a_batch_lands_in_order_and_names_its_last_record,
            an_empty_append_is_refused,
            an_append_without_a_digest_is_refused,
            latest_is_empty_on_an_empty_topic,
            a_cursor_resumes_strictly_after_its_record,
            latest_positions_a_reader_at_the_end,
            end_reads_only_what_arrives_after_the_read_starts,
            a_read_waits_no_longer_than_asked,
            dropping_a_waiting_read_releases_it,
            beginning_starts_at_the_oldest_record_still_held,
            a_reader_that_falls_behind_retention_is_told,
            an_empty_read_from_the_beginning_learns_of_a_trim,
            an_empty_read_from_the_beginning_with_nothing_trimmed_goes_on,
            rule_11_2_an_empty_read_goes_on_when_a_quiet_log_comes_back,
            a_refused_append_leaves_no_trace_of_its_topic,
            a_retried_append_returns_the_original_positions,
            a_retry_with_other_ciphertext_still_deduplicates,
            a_divergent_retry_is_refused,
            a_sequence_below_the_newest_is_refused,
            rule_9_1_a_batch_inside_the_held_batch_is_refused,
            rule_64_1_a_gap_in_the_sequence_is_accepted,
            producers_and_attempts_dedupe_apart,
            a_new_attempt_supersedes_the_old_one,
            a_read_resumed_before_a_new_attempt_reports_it,
            a_record_that_does_not_parse_fails_with_its_cursor,
            finish_is_a_record_of_its_own,
            topics_and_chains_are_apart,
            a_cursor_from_another_stream_or_store_is_refused,
            a_closed_chain_refuses_new_batches_but_answers_a_repeat,
            a_closed_topic_leaves_the_others_open,
            a_closed_topic_answers_a_repeat_and_ends_reads,
            a_stage_is_invisible_until_promoted,
            a_repeated_stage_holds_its_records_once,
            an_aborted_stage_never_lands,
            rule_11_1_a_read_ends_only_after_the_chains_last_stage_lands,
            rule_12_1_a_read_that_ends_on_a_close_repairs_the_chain,
            a_promotion_naming_too_few_topics_writes_nothing,
            a_promotion_lands_on_a_closed_chain,
            rule_19_3_a_reset_run_writes_on_and_keeps_the_chain_open,
            // Also run by the documented ACL user, which has no `+scan`, so a chain end never
            // scans the keyspace.
            a_chain_lists_its_open_topics,
        );
    };
}

/// The arm a reader sets for the append notifications, for a store that arms.
macro_rules! arm_cases {
    ($case:expr) => {
        conformance!($case;
            an_empty_read_that_asks_arms_and_the_next_append_reports_it,
            an_append_with_nothing_armed_reports_zero,
            an_arm_that_finds_a_newer_record_stays_unarmed,
            a_disarm_of_an_older_generation_keeps_the_newer_arm,
            a_promotion_reports_each_topics_arm,
            newest_or_arm_arms_only_an_empty_topic,
            a_closed_topic_never_arms,
        );
    };
}

mod memory {
    store_cases!(super::memory());
    arm_cases!(super::memory());
    conformance!(super::memory(); deleting_an_owner_drops_every_chain_of_it);
}

mod notifying_memory {
    store_cases!(super::notifying(super::memory()));
    arm_cases!(super::notifying(super::memory()));
    conformance!(super::notifying(super::memory());
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod standalone_redis {
    store_cases!(super::on_redis::redis("STREAMS_REDIS_URL", false));
    arm_cases!(super::on_redis::redis("STREAMS_REDIS_URL", false));
    conformance!(super::on_redis::redis("STREAMS_REDIS_URL", false);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod cluster_redis {
    store_cases!(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true));
    arm_cases!(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true));
    // A Workflow id's chains hash to different slots, so the delete reaches every primary.
    conformance!(super::on_redis::redis("STREAMS_REDIS_CLUSTER_URL", true);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod notifying_redis {
    store_cases!(super::notifying(super::on_redis::redis(
        "STREAMS_REDIS_URL",
        false
    )));
    arm_cases!(super::notifying(super::on_redis::redis(
        "STREAMS_REDIS_URL",
        false
    )));
}

#[cfg(feature = "redis")]
mod documented_acl_redis {
    store_cases!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_URL",
        false,
        &[]
    ));
    arm_cases!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_URL",
        false,
        &[]
    ));
    // The page names the one rule a delete needs beyond the others.
    conformance!(super::on_redis::redis_as_documented_user("STREAMS_REDIS_URL", false, &["+scan"]);
        deleting_an_owner_drops_every_chain_of_it,
    );
}

#[cfg(feature = "redis")]
mod documented_acl_cluster_redis {
    // A cluster client asks the topology first, so the documented rules must allow it.
    store_cases!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_CLUSTER_URL",
        true,
        &[]
    ));
    arm_cases!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_CLUSTER_URL",
        true,
        &[]
    ));
    conformance!(super::on_redis::redis_as_documented_user(
        "STREAMS_REDIS_CLUSTER_URL",
        true,
        &["+scan"]
    );
        deleting_an_owner_drops_every_chain_of_it,
    );
}
