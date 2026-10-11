//! The store on the application's Redis.
//!
//! Records live in Redis and never pass through Temporal. The key layout, the stored values and
//! the Lua scripts are the ones the SDKs used before the store moved into Core, so a stream one
//! wrote stays readable and writable by the other.
//!
//! **Appends.** One script writes a whole batch, so a reader never sees part of one. The topic's
//! meta hash keeps one field per producer attempt: the newest batch's first sequence, its record
//! count, its first and last entry ids, and lang's digest of it in hex. The script refuses a
//! new batch once the chain or the topic is closed, in the same step as the write, so no batch
//! lands after a close returned. Only that topic check differs from the SDKs' script.
//!
//! **Retention.** Every script that writes a log trims it with `XTRIM MINID` to the retention and
//! slides the expiry of the log and its meta, so a stream dies a retention after its last write
//! and nobody cleans up when the Workflow closes. The meta outlives its log by a 30-day grace, as
//! the tombstone that tells a log that expired from one that never existed.
//!
//! **Stages.** A Workflow's own output is staged next to its logs and moved into them by one
//! script, so readers see a Workflow Task's records together or not at all.
//!
//! Redis 7.0 or later is required.

mod conn;
mod errors;
mod keys;
mod scripts;

use crate::{
    StreamError, StreamResult, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, PendingStage, PromoteOutcome,
        PromoteResult, StageRef, StagedBatch, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredRecord,
    },
};
use conn::{BlockingPool, Shared};
use errors::{Call, Mapped};
use keys::{ChainKeys, session_field};
use redis::{Cmd, Value, streams::StreamReadReply};
use std::{collections::HashMap, time::Duration};
use tokio::time::Instant;

const NAME: &str = "redis";
const RECORD_FIELD: &str = "r";
const TOMBSTONE_GRACE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const STAGE_SEPARATOR: char = '\x1f';
const CLOSED_FIELD: &str = "closed";

/// How to reach Redis and how long streams live there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisStoreOptions {
    /// One `redis://` or `rediss://` URL, or the seed nodes of a cluster. Credentials and TLS
    /// come from the URL.
    pub urls: Vec<String>,
    /// Whether `urls` name a Redis Cluster.
    pub cluster: bool,
    /// Prepended to every key, so streams can share a Redis with other data and an ACL can scope
    /// them.
    pub key_prefix: String,
    /// How long a stream keeps a record, and how long after its last write the stream lives.
    pub retention: Duration,
    /// How many reads may wait on one node at once. Each holds a connection of its own.
    pub blocking_reads_per_node: usize,
    /// How long a short call waits for its reply.
    pub response_timeout: Duration,
}

impl RedisStoreOptions {
    /// The defaults for one Redis at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            cluster: false,
            key_prefix: "temporal-streams".to_string(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            blocking_reads_per_node: 256,
            response_timeout: Duration::from_secs(5),
        }
    }
}

/// A [StreamStore] on Redis.
pub struct RedisStore {
    shared: Shared,
    blocking: BlockingPool,
    prefix: String,
    retention_ms: u64,
    grace_ms: u64,
}

impl std::fmt::Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("prefix", &self.prefix)
            .field("retention_ms", &self.retention_ms)
            .finish_non_exhaustive()
    }
}

impl RedisStore {
    /// Connects to Redis.
    ///
    /// Fails as refused when the options can't work, and as a storage failure when Redis can't
    /// be reached.
    pub async fn connect(options: RedisStoreOptions) -> StreamResult<Self> {
        let Some(seed) = options.urls.first() else {
            return Err(StreamError::refused("the Redis store needs a URL"));
        };
        if options.key_prefix.is_empty() {
            return Err(StreamError::refused("the key prefix must not be empty"));
        }
        let retention_ms = u64::try_from(options.retention.as_millis()).unwrap_or(u64::MAX);
        if retention_ms == 0 {
            return Err(StreamError::refused(format!(
                "the retention must be at least a millisecond, not {:?}",
                options.retention
            )));
        }
        let shared = Shared::connect(&options.urls, options.cluster, options.response_timeout)
            .await
            .mapped(Call::Read)?;
        let blocking = BlockingPool::new(
            seed,
            options.blocking_reads_per_node,
            options.cluster,
            options.response_timeout,
        )
        .mapped(Call::Read)?;
        Ok(Self {
            shared,
            blocking,
            prefix: options.key_prefix,
            retention_ms,
            grace_ms: TOMBSTONE_GRACE.as_millis() as u64,
        })
    }

    fn keys(&self, chain: Option<&ChainId>) -> StreamResult<ChainKeys> {
        let chain = chain
            .ok_or_else(|| StreamError::refused("a store call needs the chain it is about"))?;
        Ok(ChainKeys::new(&self.prefix, chain))
    }

    /// Reads the chain's close flag and the topic's, which share a slot on a cluster.
    fn closed(pipeline: &mut redis::Pipeline, keys: &ChainKeys, topic: &str) {
        pipeline
            .hget(keys.chain(), CLOSED_FIELD)
            .hget(keys.meta(topic), CLOSED_FIELD);
    }

    fn xread(
        keys: &ChainKeys,
        topic: &str,
        after: &str,
        count: u32,
        block: Option<Duration>,
    ) -> Cmd {
        let mut xread = redis::cmd("XREAD");
        xread.arg("COUNT").arg(count.max(1));
        if let Some(block) = block {
            xread.arg("BLOCK").arg(block.as_millis().max(1) as u64);
        }
        xread.arg("STREAMS").arg(keys.log(topic)).arg(after);
        xread
    }
}

/// A Redis stream id as its two numbers, so ids compare in log order.
fn entry(id: &str) -> StreamResult<(u64, u64)> {
    let (milliseconds, sequence) = id.split_once('-').unwrap_or((id, "0"));
    match (milliseconds.parse(), sequence.parse()) {
        (Ok(milliseconds), Ok(sequence)) => Ok((milliseconds, sequence)),
        _ => Err(StreamError::cursor(format!(
            "{id:?} is not a Redis stream id"
        ))),
    }
}

/// The records of an `XREAD` reply, in log order. An entry without a record reads as empty bytes,
/// which no reader takes for a record.
fn records(reply: Value) -> StreamResult<Vec<StoredRecord>> {
    let reply: Option<StreamReadReply> = redis::from_redis_value(reply).map_err(|error| {
        StreamError::storage(format!(
            "Redis answered XREAD with no stream entries: {error}"
        ))
    })?;
    Ok(reply
        .into_iter()
        .flat_map(|reply| reply.keys)
        .flat_map(|key| key.ids)
        .map(|id| StoredRecord {
            record: match id.map.get(RECORD_FIELD) {
                Some(Value::BulkString(bytes)) => bytes.clone(),
                _ => Vec::new(),
            },
            position: id.id,
        })
        .collect())
}

fn wait_of(request: &StoreReadRequest) -> Duration {
    request.wait.as_ref().map_or(Duration::ZERO, |wait| {
        Duration::new(
            u64::try_from(wait.seconds).unwrap_or(0),
            u32::try_from(wait.nanos).unwrap_or(0),
        )
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[async_trait::async_trait]
impl StreamStore for RedisStore {
    fn name(&self) -> &str {
        NAME
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        if request.records.is_empty() {
            return Err(StreamError::refused("an append needs at least one record"));
        }
        if request.digest.is_empty() {
            return Err(StreamError::refused("an append needs the batch's digest"));
        }
        let keys = self.keys(request.chain.as_ref())?;
        let mut script = scripts::APPEND_SCRIPT.prepare_invoke();
        script
            .key(keys.log(&request.topic))
            .key(keys.meta(&request.topic))
            .key(keys.chain())
            .arg(self.retention_ms)
            .arg(self.grace_ms)
            .arg(session_field(&request.producer_id, request.attempt))
            .arg(request.sequence)
            .arg(hex(&request.digest));
        for record in &request.records {
            script.arg(record.as_slice());
        }
        let (first_position, last_position): (String, String) = script
            .invoke_async(&mut self.shared.clone())
            .await
            .mapped(Call::Append)?;
        Ok(StoreAppendResponse {
            first_position,
            last_position,
        })
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        let keys = self.keys(request.chain.as_ref())?;
        let after = match request.after_position.as_str() {
            "" => "0-0".to_string(),
            position => {
                entry(position)?;
                position.to_string()
            }
        };
        let wait = wait_of(&request);
        let deadline = Instant::now() + wait;
        let mut pipeline = redis::pipe();
        pipeline.add_command(Self::xread(
            &keys,
            &request.topic,
            &after,
            request.max_records,
            None,
        ));
        Self::closed(&mut pipeline, &keys, &request.topic);
        let (reply, chain_closed, topic_closed): (Value, Option<String>, Option<String>) = pipeline
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        let mut found = records(reply)?;
        let mut closed = chain_closed.is_some() || topic_closed.is_some();
        if found.is_empty() && !wait.is_zero() {
            let xread = Self::xread(
                &keys,
                &request.topic,
                &after,
                request.max_records,
                Some(wait),
            );
            let blocked = self
                .blocking
                .query(&keys.log(&request.topic), &xread, deadline)
                .await
                .mapped(Call::Read)?;
            if let Some(reply) = blocked {
                found = records(reply)?;
                // Asked after the wait, so a close that came during it is seen.
                let mut pipeline = redis::pipe();
                Self::closed(&mut pipeline, &keys, &request.topic);
                let (chain_closed, topic_closed): (Option<String>, Option<String>) = pipeline
                    .query_async(&mut self.shared.clone())
                    .await
                    .mapped(Call::Read)?;
                closed = chain_closed.is_some() || topic_closed.is_some();
            }
        }
        Ok(StoreReadResponse {
            records: found,
            closed,
        })
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        let keys = self.keys(request.chain.as_ref())?;
        let newest: Vec<(String, Value)> = redis::cmd("XREVRANGE")
            .arg(keys.log(&request.topic))
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        Ok(StoreLatestResponse {
            position: newest
                .into_iter()
                .next()
                .map(|(id, _)| id)
                .unwrap_or_default(),
        })
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        entry(position)?;
        let keys = ChainKeys::new(&self.prefix, chain);
        let found: Vec<(String, HashMap<String, Value>)> = redis::cmd("XRANGE")
            .arg(keys.log(topic))
            .arg(position)
            .arg(position)
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        Ok(found
            .into_iter()
            .next()
            .map(|(_, fields)| match fields.get(RECORD_FIELD) {
                Some(Value::BulkString(bytes)) => bytes.clone(),
                _ => Vec::new(),
            }))
    }

    async fn stage(&self, batch: StagedBatch) -> StreamResult<()> {
        let keys = self.keys(batch.chain.as_ref())?;
        let mut topics: Vec<&str> = Vec::new();
        for record in &batch.records {
            if !topics.contains(&record.topic.as_str()) {
                topics.push(&record.topic);
            }
        }
        let floor = batch.history_floor_event_id.to_string();
        let description = [batch.run_id.as_str(), floor.as_str()]
            .into_iter()
            .chain(topics)
            .collect::<Vec<_>>()
            .join(&STAGE_SEPARATOR.to_string());
        let mut eval = redis::cmd("EVAL");
        eval.arg(scripts::STAGE)
            .arg(2)
            .arg(keys.stage(&batch.token))
            .arg(keys.pending())
            // Until promoted, a stage holds committed output, and crash repair can come after the
            // retention.
            .arg(self.retention_ms + self.grace_ms)
            .arg(&batch.token)
            .arg(description);
        for record in &batch.records {
            eval.arg(&record.topic).arg(record.record.as_slice());
        }
        // The script appends to the stage, so a repeat after a lost answer would hold the records
        // twice. Clearing the stage in the same transaction makes a repeat replace it. `EVAL`
        // and not `EVALSHA`, since a transaction can't load a script the server lost.
        redis::pipe()
            .atomic()
            .del(keys.stage(&batch.token))
            .ignore()
            .add_command(eval)
            .ignore()
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
    }

    async fn promote(&self, stage: &StageRef) -> StreamResult<PromoteResult> {
        let keys = self.keys(stage.chain.as_ref())?;
        let mut script = scripts::PROMOTE_SCRIPT.prepare_invoke();
        script.key(keys.stage(&stage.token)).key(keys.pending());
        for topic in &stage.topics {
            script.key(keys.log(topic)).key(keys.meta(topic));
        }
        script
            .arg(self.retention_ms)
            .arg(self.grace_ms)
            .arg(&stage.token);
        for topic in &stage.topics {
            script.arg(topic);
        }
        let added: i64 = script
            .invoke_async(&mut self.shared.clone())
            .await
            .map_err(|error| match error.code() {
                Some("STREAMS_TOPIC") => StreamError::storage(error.detail().unwrap_or_default()),
                _ => errors::stream_error(&error, Call::Write),
            })?;
        let (outcome, records) = match added {
            -1 => (PromoteOutcome::Lost, 0),
            0 => (PromoteOutcome::Settled, 0),
            added => (PromoteOutcome::Promoted, added as u32),
        };
        Ok(PromoteResult {
            outcome: outcome as i32,
            records,
        })
    }

    async fn abort(&self, stage: &StageRef) -> StreamResult<()> {
        let keys = self.keys(stage.chain.as_ref())?;
        redis::pipe()
            .atomic()
            .del(keys.stage(&stage.token))
            .hdel(keys.pending(), &stage.token)
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        let keys = ChainKeys::new(&self.prefix, chain);
        redis::pipe()
            .atomic()
            .hset(keys.chain(), CLOSED_FIELD, "1")
            .pexpire(keys.chain(), (self.retention_ms + self.grace_ms) as i64)
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        let meta = ChainKeys::new(&self.prefix, chain).meta(topic);
        // The meta is the topic's tombstone, so the mark lives as long as the topic is known.
        redis::pipe()
            .atomic()
            .hset(&meta, CLOSED_FIELD, "1")
            .pexpire(&meta, (self.retention_ms + self.grace_ms) as i64)
            .query_async::<()>(&mut self.shared.clone())
            .await
            .mapped(Call::Write)
    }

    async fn pending_stages(&self, chain: &ChainId) -> StreamResult<Vec<PendingStage>> {
        let keys = ChainKeys::new(&self.prefix, chain);
        let pending: HashMap<String, String> = redis::cmd("HGETALL")
            .arg(keys.pending())
            .query_async(&mut self.shared.clone())
            .await
            .mapped(Call::Read)?;
        pending
            .into_iter()
            .map(|(token, description)| {
                let mut parts = description.split(STAGE_SEPARATOR);
                let run_id = parts.next().unwrap_or_default().to_string();
                let floor = parts.next().and_then(|floor| floor.parse().ok());
                let Some(history_floor_event_id) = floor else {
                    return Err(StreamError::storage(format!(
                        "pending stage {token:?} has no History floor: {description:?}"
                    )));
                };
                Ok(PendingStage {
                    token,
                    run_id,
                    history_floor_event_id,
                    topics: parts.map(str::to_string).collect(),
                })
            })
            .collect()
    }

    async fn delete_owner(
        &self,
        _request: DeleteOwnerRequest,
    ) -> StreamResult<DeleteOwnerResponse> {
        Err(StreamError::unsupported(
            "the Redis store does not delete owners yet",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_ids_compare_in_log_order() {
        assert!(entry("1-2").unwrap() < entry("1-10").unwrap());
        assert!(entry("9-0").unwrap() < entry("10-0").unwrap());
        assert_eq!(entry("5").unwrap(), (5, 0));
        for bad in ["", "x-1", "1-x", "-1"] {
            assert!(entry(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_digest_is_stored_as_lowercase_hex() {
        assert_eq!(hex(&[0xab, 0x01]), "ab01");
    }

    async fn store(per_node: usize) -> Option<RedisStore> {
        let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
            eprintln!("set STREAMS_REDIS_URL to run the Redis store tests");
            return None;
        };
        let mut options = RedisStoreOptions::new(url);
        options.key_prefix = format!("pool-{}", std::process::id());
        options.blocking_reads_per_node = per_node;
        Some(RedisStore::connect(options).await.unwrap())
    }

    fn chain(workflow_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: workflow_id.to_string(),
            first_run_id: "run-1".to_string(),
        }
    }

    fn waiting_read(workflow_id: &str, wait: Duration) -> StoreReadRequest {
        StoreReadRequest {
            chain: Some(chain(workflow_id)),
            topic: "out".to_string(),
            wait: Some(wait.try_into().unwrap()),
            max_records: 10,
            ..Default::default()
        }
    }

    fn append(workflow_id: &str, sequence: i64) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(chain(workflow_id)),
            topic: "out".to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence,
            digest: vec![sequence as u8],
            records: vec![b"\x1a\x03out".to_vec()],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parked_readers_do_not_slow_appends() {
        let Some(store) = store(256).await else {
            return;
        };
        let store = std::sync::Arc::new(store);
        let run = format!("parked-{}", std::process::id());
        let readers: Vec<_> = (0..100)
            .map(|reader| {
                let store = store.clone();
                let workflow_id = format!("{run}-{reader}");
                tokio::spawn(async move {
                    store
                        .read(waiting_read(&workflow_id, Duration::from_secs(30)))
                        .await
                })
            })
            .collect();
        let parked = Instant::now() + Duration::from_secs(10);
        while store.blocking.in_use() < 100 {
            assert!(
                Instant::now() < parked,
                "{} parked",
                store.blocking.in_use()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut slowest = Duration::ZERO;
        for sequence in 1..=20 {
            let started = Instant::now();
            store.append(append(&run, sequence)).await.unwrap();
            slowest = slowest.max(started.elapsed());
        }
        // On a shared connection the first append would wait behind a parked XREAD for 30 s.
        assert!(
            slowest < Duration::from_secs(1),
            "slowest append {slowest:?}"
        );
        // A cancelled read drops its connection instead of handing it to the next read.
        for reader in &readers {
            reader.abort();
        }
        for reader in readers {
            assert!(reader.await.unwrap_err().is_cancelled());
        }
        assert_eq!(store.blocking.in_use(), 0);
    }

    #[tokio::test]
    async fn a_read_that_finds_the_pool_full_answers_empty_at_its_deadline() {
        let Some(store) = store(1).await else {
            return;
        };
        let store = std::sync::Arc::new(store);
        let run = format!("full-{}", std::process::id());
        let holder = {
            let store = store.clone();
            let run = run.clone();
            tokio::spawn(async move {
                store
                    .read(waiting_read(&format!("{run}-a"), Duration::from_secs(30)))
                    .await
            })
        };
        while store.blocking.in_use() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let started = Instant::now();
        let read = store
            .read(waiting_read(
                &format!("{run}-b"),
                Duration::from_millis(300),
            ))
            .await
            .unwrap();
        assert!(read.records.is_empty());
        assert!(started.elapsed() < Duration::from_secs(3));
        holder.abort();
    }
}
