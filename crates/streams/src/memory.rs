//! The in-process store, for tests.
//!
//! It holds everything in process memory, so nothing survives the process and it is no store for
//! production. It exists so the conformance suite runs the whole store contract without a server,
//! and to show in one file what a store owes. Positions are offsets that are never reused, so a
//! position keeps naming the same record after [MemoryStore::truncate] drops older ones.

use crate::{
    NewestOrArm, StreamError, StreamResult, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
        StoredRecord, StreamOwnerKind,
    },
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

const NAME: &str = "memory";

type ChainKey = (String, String, String);

fn chain_key(chain: &ChainId) -> ChainKey {
    (
        chain.namespace.clone(),
        chain.workflow_id.clone(),
        chain.first_run_id.clone(),
    )
}

/// The newest batch one producer attempt wrote.
#[derive(Debug, Clone)]
struct Held {
    sequence: i64,
    count: i64,
    first: u64,
    digest: Vec<u8>,
}

#[derive(Debug, Default)]
struct Topic {
    /// The offset of the first retained record.
    base: u64,
    records: VecDeque<Vec<u8>>,
    /// One entry per producer attempt, so the state stays bounded however long a producer writes.
    held: HashMap<(String, i64), Held>,
    closed: bool,
    /// The first close of the topic, which later closes keep.
    close: Option<StoredClose>,
}

impl Topic {
    fn head(&self) -> u64 {
        self.base + self.records.len() as u64
    }

    fn push(&mut self, record: Vec<u8>) -> u64 {
        self.records.push_back(record);
        self.head() - 1
    }

    fn at(&self, offset: u64) -> Option<&Vec<u8>> {
        offset
            .checked_sub(self.base)
            .and_then(|index| self.records.get(index as usize))
    }
}

#[derive(Debug, Default)]
struct Chain {
    topics: HashMap<String, Topic>,
    closed: bool,
    /// Kept apart from the topics, so an arm on a topic with no records leaves it unknown to
    /// reads and to the topic list.
    arms: HashMap<String, Arm>,
    /// Per topic, the position each positions-only reader holds the records after. `None` holds
    /// the whole topic.
    floors: HashMap<String, HashMap<String, Option<u64>>>,
}

/// A topic's arm: the generation a reader set, zero once cleared, and the last one handed out.
#[derive(Debug, Default)]
struct Arm {
    generation: u64,
    last: u64,
}

impl Chain {
    fn arm(&self, topic: &str) -> u64 {
        self.arms.get(topic).map_or(0, |arm| arm.generation)
    }

    fn set_arm(&mut self, topic: &str) -> u64 {
        let arm = self.arms.entry(topic.to_string()).or_default();
        arm.last += 1;
        arm.generation = arm.last;
        arm.generation
    }
}

/// A [StreamStore] in process memory. For tests only.
///
/// Share one instance between every client and Worker of a test, since two instances share
/// nothing.
#[derive(Debug)]
pub struct MemoryStore {
    chains: Mutex<HashMap<ChainKey, Chain>>,
    /// Bumped on every write, so a waiting read checks again.
    written: watch::Sender<u64>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            chains: Mutex::default(),
            written: watch::Sender::new(0),
        }
    }
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Drops all but the newest `keep` records of a topic, standing in for a store's retention.
    /// It keeps the records a positions-only reader holds, from the position it holds on.
    pub fn truncate(&self, chain: &ChainId, topic: &str, keep: usize) {
        let mut chains = self.chains.lock().unwrap();
        let Some(chain) = chains.get_mut(&chain_key(chain)) else {
            return;
        };
        let held = chain
            .floors
            .get(topic)
            .and_then(|holders| holders.values().map(|position| position.unwrap_or(0)).min());
        let Some(topic) = chain.topics.get_mut(topic) else {
            return;
        };
        let mut drop = topic.records.len().saturating_sub(keep);
        if let Some(held) = held {
            drop = drop.min(held.saturating_sub(topic.base) as usize);
        }
        topic.records.drain(..drop);
        topic.base += drop as u64;
    }

    fn wrote(&self) {
        self.written.send_modify(|version| *version += 1);
    }

    /// One look at what a read gets now: `None` when it should wait for more. With `arm`, a look
    /// that finds nothing arms the topic instead.
    fn read_now(
        &self,
        request: &StoreReadRequest,
        arm: bool,
    ) -> StreamResult<Option<StoreReadResponse>> {
        let mut chains = self.chains.lock().unwrap();
        let found = Self::look(&chains, request)?;
        if found.is_some() || !arm {
            return Ok(found);
        }
        // Under the lock the look held, so no append lands between finding nothing and the arm.
        chains
            .entry(chain_key(chain_of(request.chain.as_ref())?))
            .or_default()
            .set_arm(&request.topic);
        Ok(Some(StoreReadResponse {
            armed: true,
            ..Default::default()
        }))
    }

    fn look(
        chains: &HashMap<ChainKey, Chain>,
        request: &StoreReadRequest,
    ) -> StreamResult<Option<StoreReadResponse>> {
        let chain = chains.get(&chain_key(chain_of(request.chain.as_ref())?));
        let topic = chain.and_then(|chain| chain.topics.get(&request.topic));
        let closed = chain.is_some_and(|chain| chain.closed) || topic.is_some_and(|t| t.closed);
        let after = parse_position(&request.after_position)?;
        let Some(topic) = topic else {
            if after.is_some() {
                return Err(StreamError::not_found(format!(
                    "topic {:?} holds no records and none were dropped, so nothing is known \
                     about what followed position {}",
                    request.topic, request.after_position
                )));
            }
            return Ok(closed.then(|| StoreReadResponse {
                closed,
                ..Default::default()
            }));
        };
        let start = after.map_or(topic.base, |after| after + 1);
        if start > topic.head() {
            return Err(StreamError::cursor(format!(
                "position {} is one topic {:?} never held",
                request.after_position, request.topic
            )));
        }
        if start < topic.base {
            return Err(StreamError::expired(format!(
                "the records from offset {start} on topic {:?} were dropped by retention; the \
                 topic now starts at {}",
                request.topic, topic.base
            )));
        }
        let records: Vec<_> = (start..topic.head())
            .take(request.max_records.max(1) as usize)
            .map(|offset| StoredRecord {
                position: offset.to_string(),
                record: topic.at(offset).cloned().unwrap_or_default(),
            })
            .collect();
        Ok(
            (!records.is_empty() || closed).then_some(StoreReadResponse {
                records,
                closed,
                armed: false,
                close: topic.close.clone(),
            }),
        )
    }
}

fn chain_of(chain: Option<&ChainId>) -> StreamResult<&ChainId> {
    chain.ok_or_else(|| StreamError::refused("a store call needs the chain it is about"))
}

fn parse_position(position: &str) -> StreamResult<Option<u64>> {
    if position.is_empty() {
        return Ok(None);
    }
    position.parse().map(Some).map_err(|_| {
        StreamError::cursor(format!(
            "position {position:?} is not a position of the memory store"
        ))
    })
}

fn wait_of(request: &StoreReadRequest) -> Duration {
    request.wait.as_ref().map_or(Duration::ZERO, |wait| {
        Duration::new(
            u64::try_from(wait.seconds).unwrap_or(0),
            u32::try_from(wait.nanos).unwrap_or(0),
        )
    })
}

#[async_trait::async_trait]
impl StreamStore for MemoryStore {
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
        let mut chains = self.chains.lock().unwrap();
        let chain = chains
            .entry(chain_key(chain_of(request.chain.as_ref())?))
            .or_default();
        let chain_closed = chain.closed;
        let session = (request.producer_id.clone(), request.attempt);
        let existing = chain.topics.get(&request.topic);
        if let Some(held) = existing.and_then(|topic| topic.held.get(&session)) {
            if request.sequence == held.sequence {
                if request.digest != held.digest {
                    return Err(StreamError::new(
                        crate::proto::StreamFailureKind::ProducerDivergent,
                        format!(
                            "sequence {} was already written with different content",
                            request.sequence
                        ),
                    ));
                }
                let last = held.first + held.count as u64 - 1;
                return Ok(StoreAppendResponse {
                    first_position: held.first.to_string(),
                    last_position: last.to_string(),
                    arm: Some(chain.arm(&request.topic)),
                });
            }
            let next = held.sequence + held.count;
            if request.sequence < next {
                return Err(StreamError::new(
                    crate::proto::StreamFailureKind::ProducerStale,
                    format!(
                        "sequence {} is below the next one expected, {next}",
                        request.sequence
                    ),
                ));
            }
        }
        // Checked before the topic exists, so a refused batch leaves no trace of it.
        if chain_closed || existing.is_some_and(|topic| topic.closed) {
            let closed_by = existing
                .and_then(|topic| topic.close.as_ref())
                .map(|close| close.closed_by.clone())
                .unwrap_or_default();
            return Err(
                StreamError::closed("the Workflow that owns this stream has closed")
                    .with_closed_by(closed_by),
            );
        }
        let arm = chain.arm(&request.topic);
        let topic = chain.topics.entry(request.topic.clone()).or_default();
        let count = request.records.len() as i64;
        let mut positions = request.records.into_iter().map(|record| topic.push(record));
        let first = positions.next().unwrap_or_default();
        let last = positions.last().unwrap_or(first);
        topic.held.insert(
            session,
            Held {
                sequence: request.sequence,
                count,
                first,
                digest: request.digest,
            },
        );
        drop(chains);
        self.wrote();
        Ok(StoreAppendResponse {
            first_position: first.to_string(),
            last_position: last.to_string(),
            arm: Some(arm),
        })
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        let deadline = Instant::now() + wait_of(&request);
        let mut written = self.written.subscribe();
        loop {
            if let Some(response) = self.read_now(&request, false)? {
                return Ok(response);
            }
            if tokio::time::timeout_at(deadline, written.changed())
                .await
                .is_err()
            {
                return Ok(self.read_now(&request, request.arm)?.unwrap_or_default());
            }
        }
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        let chains = self.chains.lock().unwrap();
        let head = chains
            .get(&chain_key(chain_of(request.chain.as_ref())?))
            .and_then(|chain| chain.topics.get(&request.topic))
            .map_or(0, Topic::head);
        Ok(StoreLatestResponse {
            position: head
                .checked_sub(1)
                .map(|p| p.to_string())
                .unwrap_or_default(),
        })
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        let Some(offset) = parse_position(position)? else {
            return Ok(None);
        };
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .and_then(|chain| chain.topics.get(topic))
            .and_then(|topic| topic.at(offset))
            .cloned())
    }

    async fn newest_ever(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .and_then(|chain| chain.topics.get(topic))
            .and_then(|topic| topic.head().checked_sub(1))
            .map(|offset| offset.to_string())
            .unwrap_or_default())
    }

    async fn dropped_after(
        &self,
        chain: &ChainId,
        topic: &str,
        mark: &str,
    ) -> StreamResult<Option<String>> {
        let mark = parse_position(mark)?;
        let chains = self.chains.lock().unwrap();
        Ok(chains
            .get(&chain_key(chain))
            .and_then(|chain| chain.topics.get(topic))
            .and_then(|topic| topic.base.checked_sub(1))
            .filter(|dropped| mark.is_none_or(|mark| *dropped > mark))
            .map(|offset| offset.to_string()))
    }

    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        let position = parse_position(position)?;
        let mut chains = self.chains.lock().unwrap();
        let held = chains
            .entry(chain_key(chain))
            .or_default()
            .floors
            .entry(topic.to_string())
            .or_default()
            .entry(holder.to_string())
            .or_insert(position);
        *held = match (*held, position) {
            (Some(held), Some(position)) => Some(held.min(position)),
            _ => None,
        };
        Ok(())
    }

    /// The memory store has no clock for retention, so a release ends the hold at once.
    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        if let Some(holders) = self
            .chains
            .lock()
            .unwrap()
            .get_mut(&chain_key(chain))
            .and_then(|chain| chain.floors.get_mut(topic))
        {
            holders.remove(holder);
        }
        Ok(())
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.chains
            .lock()
            .unwrap()
            .entry(chain_key(chain))
            .or_default()
            .closed = true;
        self.wrote();
        Ok(())
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.close_topic_with(chain, topic, StoredClose::default())
            .await
            .map(drop)
    }

    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<StoredClose> {
        let stored = {
            let mut chains = self.chains.lock().unwrap();
            let topic = chains
                .entry(chain_key(chain))
                .or_default()
                .topics
                .entry(topic.to_string())
                .or_default();
            topic.closed = true;
            topic.close.get_or_insert(close).clone()
        };
        self.wrote();
        Ok(stored)
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        let mut chains = self.chains.lock().unwrap();
        if let Some(topic) = chains
            .get_mut(&chain_key(chain))
            .and_then(|chain| chain.topics.get_mut(topic))
        {
            topic.held.remove(&(producer_id.to_string(), attempt));
        }
        Ok(())
    }

    async fn disarm(&self, chain: &ChainId, topic: &str, generation: u64) -> StreamResult<()> {
        let mut chains = self.chains.lock().unwrap();
        if let Some(arm) = chains
            .get_mut(&chain_key(chain))
            .and_then(|chain| chain.arms.get_mut(topic))
            && arm.generation == generation
        {
            arm.generation = 0;
        }
        Ok(())
    }

    async fn newest_or_arm(&self, chain: &ChainId, topic: &str) -> StreamResult<NewestOrArm> {
        let mut chains = self.chains.lock().unwrap();
        let chain = chains.entry(chain_key(chain)).or_default();
        if let Some(held) = chain.topics.get(topic).filter(|t| !t.records.is_empty()) {
            return Ok(NewestOrArm::Newest((held.head() - 1).to_string()));
        }
        Ok(NewestOrArm::Armed(chain.set_arm(topic)))
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        if request.owner_kind != StreamOwnerKind::Workflow as i32 {
            return Err(StreamError::unsupported(format!(
                "this release keeps streams of Workflows only, not owner kind {}",
                request.owner_kind
            )));
        }
        let mut chains = self.chains.lock().unwrap();
        let mut deleted = 0;
        chains.retain(|(namespace, workflow_id, _), chain| {
            let owned = *namespace == request.namespace && *workflow_id == request.workflow_id;
            if owned {
                deleted += 1 + chain.topics.len() as u64;
            }
            !owned
        });
        Ok(DeleteOwnerResponse { deleted })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamFailureKind;

    fn chain() -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: "run-1".to_string(),
        }
    }

    fn append(sequence: i64, records: &[&[u8]]) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(chain()),
            topic: "out".to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence,
            digest: records.concat(),
            records: records.iter().map(|record| record.to_vec()).collect(),
        }
    }

    fn read_after(position: &str) -> StoreReadRequest {
        StoreReadRequest {
            chain: Some(chain()),
            topic: "out".to_string(),
            after_position: position.to_string(),
            max_records: 10,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_position_past_the_head_is_one_the_topic_never_held() {
        let store = MemoryStore::new();
        store.append(append(1, &[b"a"])).await.unwrap();
        let error = store.read(read_after("5")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
        assert!(error.message.contains("never held"), "{error}");
        let error = store.read(read_after("x")).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
    }

    #[tokio::test]
    async fn positions_survive_truncation() {
        let store = MemoryStore::new();
        store.append(append(1, &[b"a", b"b", b"c"])).await.unwrap();
        store.truncate(&chain(), "out", 1);
        let read = store.read(read_after("1")).await.unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].position, "2");
        assert_eq!(read.records[0].record, b"c");
        assert_eq!(
            store.record_at(&chain(), "out", "2").await.unwrap(),
            Some(b"c".to_vec())
        );
        assert_eq!(store.record_at(&chain(), "out", "0").await.unwrap(), None);
    }

    #[tokio::test]
    async fn another_owner_kind_is_unsupported() {
        let store = MemoryStore::new();
        let error = store
            .delete_owner(DeleteOwnerRequest {
                namespace: "ns".to_string(),
                owner_kind: StreamOwnerKind::Unspecified as i32,
                workflow_id: "wf".to_string(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Unsupported);
    }
}
