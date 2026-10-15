//! The stream calls lang makes, from the service protos down to a store.
//!
//! [Streams] resolves a stream's address to the run chain its store keys it by, checks the owner
//! where the contract says so, and mints and checks cursors. Stores only move records.

use crate::{
    BEGINNING, END, MAX_CLOSE_BYTES, ReadTarget, StreamError, StreamResult, StreamStore,
    activity_attempt, activity_producer_id, chain_owner, check_append_digest, cursor_position,
    mint_cursor,
    owner::{OwnerClient, OwnerDescription, OwnerError, OwnerId, OwnerStatus},
    proto::{
        ActivityProducer, AppendRequest, AppendResponse, ChainId, CloseRequest, CloseResponse,
        DeleteOwnerRequest, DeleteOwnerResponse, LatestRequest, LatestResponse, NamedProducer,
        ReadRangeRequest, ReadRequest, ReadResponse, ReleaseFloorRequest, ReleaseFloorResponse,
        RetireProducerRequest, RetireProducerResponse, StoreAppendRequest, StoreLatestRequest,
        StoredClose, StreamAddress, StreamFailureKind, StreamOwnerKind, append_request,
        read_record, retire_producer_request,
    },
    reader::{Attempts, ReadState, read_with},
    stored_append_record, stream_hash,
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

/// The longest `floor_holder` a read takes.
const MAX_FLOOR_HOLDER_BYTES: usize = 512;

/// The longest `closed_by` a close takes.
const MAX_CLOSED_BY_BYTES: usize = 256;

/// The longest a producer writes without asking whether its owner's chain ended.
pub const OWNER_RECHECK: Duration = Duration::from_secs(60);

/// How long a read waits by default for an owner that Temporal doesn't know yet.
pub const OWNER_START_GRACE: Duration = Duration::from_secs(60);

/// How the stream layer behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsOptions {
    /// How long a producer writes before it asks again whether its owner's chain ended. Nothing
    /// else ends the writes of a Workflow that never published itself.
    pub owner_recheck: Duration,
    /// How many owners, producer attempts and chains to remember the last owner check of.
    pub remembered: usize,
    /// How long an idle read waits before it first asks whether its owner ended.
    pub owner_check_min: Duration,
    /// The longest an idle read waits between two such questions.
    pub owner_check_max: Duration,
    /// How long after its first owner check a read takes a Workflow that Temporal doesn't know
    /// as one that has not started yet, and answers empty. Zero fails the read at once.
    pub owner_start_grace: Duration,
}

impl Default for StreamsOptions {
    fn default() -> Self {
        Self {
            owner_recheck: OWNER_RECHECK,
            remembered: 10_000,
            owner_check_min: Duration::from_millis(500),
            owner_check_max: Duration::from_secs(5),
            owner_start_grace: OWNER_START_GRACE,
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
    activity: bool,
    /// The Workflow id or the Activity id.
    id: String,
    run_id: String,
}

impl OwnerKey {
    fn owner(&self) -> OwnerId<'_> {
        if self.activity {
            OwnerId::Activity(&self.id)
        } else {
            OwnerId::Workflow(&self.id)
        }
    }

    /// The owner as error texts name it.
    fn label(&self) -> String {
        let kind = if self.activity {
            "Activity"
        } else {
            "Workflow"
        };
        format!("{kind} {:?}", self.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProducerKey {
    owner: OwnerKey,
    producer_id: String,
    attempt: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct Checked {
    pub(crate) first_run_id: String,
    at: Instant,
}

/// Remembers when each key was last checked, forgetting the oldest when full.
#[derive(Debug)]
pub(crate) struct Remembered<K> {
    entries: Mutex<HashMap<K, Checked>>,
    limit: usize,
}

impl<K: std::hash::Hash + Eq + Clone> Remembered<K> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            entries: Mutex::default(),
            limit: limit.max(1),
        }
    }

    pub(crate) fn get(&self, key: &K) -> Option<Checked> {
        self.entries.lock().unwrap().get(key).cloned()
    }

    pub(crate) fn forget(&self, key: &K) {
        self.entries.lock().unwrap().remove(key);
    }

    pub(crate) fn put(&self, key: K, first_run_id: &str, fresh_for: Duration) {
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

/// A read as [Streams::read_carried] answers it.
#[derive(Debug)]
pub(crate) struct CarriedRead {
    pub(crate) response: ReadResponse,
    pub(crate) state: ReadState,
    /// The attempts the read knew before the answer's first record.
    pub(crate) before: Attempts,
}

/// The stream layer over one store.
pub struct Streams {
    store: Arc<dyn StreamStore>,
    owner: Arc<dyn OwnerClient>,
    options: StreamsOptions,
    /// The last owner check per owner, which all its producers share.
    owners: Remembered<OwnerKey>,
    /// The chain each producer attempt began on.
    producers: Remembered<ProducerKey>,
    chains: Remembered<OwnerKey>,
    /// The chains carried read states named that a describe showed are the Workflow's own.
    carried_chains: Remembered<OwnerKey>,
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
            owners: Remembered::new(options.remembered),
            producers: Remembered::new(options.remembered),
            carried_chains: Remembered::new(options.remembered),
            chains: Remembered::new(options.remembered),
            options,
        }
    }

    /// Appends one batch for one producer attempt.
    ///
    /// The first batch for an owner, and the first after [StreamsOptions::owner_recheck], asks
    /// whether the owner's chain ended. All producers of the owner share that check. If it did, or
    /// Temporal no longer holds the owner, or a new chain took the owner id, the chain is marked
    /// closed and the batch is refused as closed. A standalone Activity run is a chain of one
    /// run, so a new run of the Activity id ends the earlier run's streams.
    pub async fn append(&self, request: AppendRequest) -> StreamResult<AppendResponse> {
        let stream = address(request.stream.as_ref())?;
        let (producer_id, attempt) = match &request.producer {
            Some(append_request::Producer::Named(named)) => producer(Some(named), None)?,
            Some(append_request::Producer::Activity(activity)) => producer(None, Some(activity))?,
            None => {
                return Err(StreamError::invalid_argument(
                    "an append needs its producer",
                ));
            }
        };
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

    /// Reads one page of a stream, waiting up to the request's `wait` for a record.
    ///
    /// The first call resolves the chain, and the state carries it. While nothing arrives, the
    /// read asks whether its owner ended, first after [StreamsOptions::owner_check_min] and
    /// then twice as long each time, up to [StreamsOptions::owner_check_max]. A record starts
    /// the interval over. So many readers parked on a quiet stream don't load the server. Once
    /// the owner ended, or the store marked the stream closed, the read delivers what is left
    /// and answers `done`. A read pinned to a run ends when that run closes, even if the chain
    /// continued as new. An empty answer carries the notification counter of its position as
    /// `wake_after`. A read whose owner Temporal doesn't know yet answers empty within
    /// [StreamsOptions::owner_start_grace], so it can start before its owner does.
    pub async fn read(&self, request: ReadRequest) -> StreamResult<ReadResponse> {
        let read = self.read_carried(request).await?;
        Ok(ReadResponse {
            state: read.state.encode_to_vec(),
            ..read.response
        })
    }

    /// [Streams::read], answering the state undecoded and the attempts the read knew before the
    /// answer's records, so a caller that keeps only some of them can hand back the state at its
    /// cut. The answer's `state` is left empty.
    pub(crate) async fn read_carried(&self, request: ReadRequest) -> StreamResult<CarriedRead> {
        let stream = address(request.stream.as_ref())?;
        let mut state = ReadState::decode_from(&request.state)?;
        let wait = request
            .wait
            .and_then(|wait| Duration::try_from(wait).ok())
            .unwrap_or_default();
        let deadline = Instant::now() + wait;
        if state.first_run_id.is_empty() {
            let Some(chain) = self.started_chain(stream, &mut state, deadline).await? else {
                // The owner has not started, so its stream holds nothing and its end is its start.
                let cursor = if request.after == END {
                    BEGINNING.to_string()
                } else {
                    request.after.clone()
                };
                return Ok(CarriedRead {
                    response: ReadResponse {
                        cursor,
                        ..Default::default()
                    },
                    before: state.attempts(),
                    state,
                });
            };
            state.first_run_id = chain.first_run_id;
            // Resolved here, so it is the Workflow's own, and the state it rides in back needs
            // no check.
            let key = OwnerKey {
                run_id: state.first_run_id.clone(),
                ..owner_key(stream)
            };
            self.carried_chains
                .put(key, &state.first_run_id, self.options.owner_recheck);
        } else {
            self.check_carried_chain(stream, &state.first_run_id)
                .await?;
        }
        let target = ReadTarget {
            chain: chain_of(stream, &state.first_run_id),
            topic: stream.topic.clone(),
            stream_hash: self.hash(stream),
        };
        if !request.floor_holder.is_empty() {
            self.hold_floor(&target, &request.after, &request.floor_holder)
                .await?;
        }
        let minimum = self.options.owner_check_min;
        let mut page_request = request.clone();
        let mut topic_close = None;
        let mut before;
        let response = loop {
            let now = unix_ms();
            if state.check_interval_ms == 0 {
                state.check_interval_ms = millis(minimum);
                state.next_check_ms = now + state.check_interval_ms;
            }
            let until_check = Duration::from_millis(state.next_check_ms.saturating_sub(now));
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = if state.owner_ended {
                Duration::ZERO
            } else {
                remaining.min(until_check)
            };
            page_request.wait = wait.try_into().ok();
            let read = read_with(&*self.store, &target, &page_request, &mut state).await?;
            before = read.before;
            let closed = read.closed;
            if read.close.is_some() {
                topic_close = read.close;
            }
            let mut page = read.response;
            page_request.after = page.cursor.clone();
            if !page.records.is_empty() {
                state.check_interval_ms = millis(minimum);
                state.next_check_ms = unix_ms() + state.check_interval_ms;
                break page;
            }
            if closed && !state.owner_ended {
                // One more pass after learning of the close, as after learning the owner ended.
                state.owner_ended = true;
                continue;
            }
            if state.owner_ended {
                page.done = true;
                page.wake_after = 0;
                // A reader can reach the end before a close wake does, so the end carries what the
                // stream closed with.
                if let Some(close) = topic_close.take() {
                    page.close_result = close.result;
                    page.close_failure = close.failure;
                    page.closed_by = close.closed_by;
                }
                break page;
            }
            if unix_ms() >= state.next_check_ms {
                state.owner_ended = self.owner_ended(stream, &target.chain).await?;
                state.check_interval_ms = (state.check_interval_ms * 2)
                    .clamp(millis(minimum), millis(self.options.owner_check_max));
                state.next_check_ms = unix_ms() + state.check_interval_ms;
                // One more pass after learning the owner ended, so a record that landed
                // between the read and the describe is delivered.
                if state.owner_ended {
                    continue;
                }
            }
            if Instant::now() >= deadline {
                break page;
            }
        };
        Ok(CarriedRead {
            response,
            state,
            before,
        })
    }

    /// The first run of the chain `stream` names now, as the last read resolved it.
    pub(crate) async fn first_run_of(&self, stream: &StreamAddress) -> StreamResult<String> {
        Ok(self.chain(stream).await?.first_run_id)
    }

    /// Refuses a carried state whose chain is not one of the Workflow's. A caller outside the
    /// process could otherwise point a read at another run's chain by editing its state. Asked
    /// once per chain in a process.
    async fn check_carried_chain(
        &self,
        stream: &StreamAddress,
        first_run_id: &str,
    ) -> StreamResult<()> {
        let key = OwnerKey {
            run_id: first_run_id.to_string(),
            ..owner_key(stream)
        };
        if self.carried_chains.get(&key).is_some() {
            return Ok(());
        }
        match self.describe(&key, first_run_id).await {
            Ok(described) if described.first_run_id != first_run_id => {
                return Err(StreamError::cursor(format!(
                    "the read state names run {first_run_id:?}, which doesn't start a run chain \
                     of Workflow {:?}",
                    stream.workflow_id
                )));
            }
            // A chain Temporal no longer holds leaves nothing another chain's reader could
            // reach, so its streams stay readable to the end of their retention.
            Ok(_) | Err(OwnerError::NotFound(_)) => {}
            Err(error) => return Err(describe_failed(&error)),
        }
        self.carried_chains
            .put(key, first_run_id, self.options.owner_recheck);
        Ok(())
    }

    /// Holds the records after `after` against retention for `holder`, a positions-only reader.
    async fn hold_floor(&self, target: &ReadTarget, after: &str, holder: &str) -> StreamResult<()> {
        if holder.len() > MAX_FLOOR_HOLDER_BYTES {
            return Err(StreamError::invalid_argument(format!(
                "a floor holder is at most {MAX_FLOOR_HOLDER_BYTES} bytes, not {}",
                holder.len()
            )));
        }
        let position = if after == END {
            self.store
                .latest(StoreLatestRequest {
                    chain: Some(target.chain.clone()),
                    topic: target.topic.clone(),
                })
                .await?
                .position
        } else {
            cursor_position(after, self.store.name(), &target.stream_hash)?
                .unwrap_or_default()
                .to_string()
        };
        self.store
            .hold_floor(&target.chain, &target.topic, holder, &position)
            .await
    }

    /// Reads again the exact range of an earlier answer and checks it against that answer's
    /// digest (RR-2). It runs the read that the answer came from, with the same input state and
    /// the start that read found, so it gives the same supersessions and stale marks, and it waits
    /// for nothing and checks no owner. Records with other content fail as a mismatch. Records the store no longer holds
    /// fail as lost (REC-01).
    pub async fn read_range(&self, request: ReadRangeRequest) -> StreamResult<ReadResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.chain_first_run_id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a replay read needs the chain its read read",
            ));
        }
        if request.after == END {
            return Err(StreamError::invalid_argument(
                "a replay read starts at the read's resolved start_cursor, not at $end",
            ));
        }
        let mut state = ReadState::decode_from(&request.state)?;
        if state.first_run_id.is_empty() {
            state.first_run_id = request.chain_first_run_id.clone();
        }
        if request.start_from_end {
            state.started = true;
        }
        let target = ReadTarget {
            chain: chain_of(stream, &request.chain_first_run_id),
            topic: stream.topic.clone(),
            stream_hash: self.hash(stream),
        };
        if !request.floor_holder.is_empty() {
            self.hold_floor(&target, &request.after, &request.floor_holder)
                .await?;
        }
        let expected = request.stored_records as usize;
        let lost = |message: String| {
            StreamError::new(
                StreamFailureKind::ReplayRangeLost,
                format!("REC-01: {message}"),
            )
        };
        let mismatch =
            |message: String| StreamError::new(StreamFailureKind::ReplayMismatch, message);
        let page = if expected == 0 {
            ReadResponse {
                cursor: request.after.clone(),
                records_digest: crate::reader::records_digest(&[]),
                chain_first_run_id: request.chain_first_run_id.clone(),
                start_cursor: request.after.clone(),
                ..Default::default()
            }
        } else {
            let page_request = ReadRequest {
                stream: Some(stream.clone()),
                after: request.after.clone(),
                max_records: request.stored_records,
                ..Default::default()
            };
            match read_with(&*self.store, &target, &page_request, &mut state).await {
                Ok(read) => read.response,
                Err(error)
                    if matches!(
                        error.kind,
                        StreamFailureKind::Expired | StreamFailureKind::NotFound
                    ) =>
                {
                    return Err(lost(error.message));
                }
                Err(error) => return Err(error),
            }
        };
        let stored = page
            .records
            .iter()
            .filter(|record| matches!(record.record, Some(read_record::Record::Stored(_))))
            .count();
        if stored < expected {
            return Err(lost(format!(
                "the store holds {stored} of the {expected} records after {:?} that the read got",
                request.after
            )));
        }
        if page.cursor != request.to || page.records_digest != request.records_digest {
            // A read from the beginning has no position to check, so a trim since the read shows
            // as other records, which end elsewhere or differ.
            if request.after.is_empty()
                && self
                    .store
                    .dropped_after(&target.chain, &target.topic, "")
                    .await?
                    .is_some()
            {
                return Err(lost(
                    "retention trimmed the start of the stream, which the read began at"
                        .to_string(),
                ));
            }
            if page.cursor != request.to {
                return Err(mismatch(format!(
                    "the replayed read ends at {:?}, not at {:?}",
                    page.cursor, request.to
                )));
            }
            return Err(mismatch(
                "the replayed records differ from the records the read got".to_string(),
            ));
        }
        Ok(ReadResponse {
            state: state.encode_to_vec(),
            ..page
        })
    }

    /// Ends a positions-only reader's hold after the store's release grace.
    pub async fn release_floor(
        &self,
        request: ReleaseFloorRequest,
    ) -> StreamResult<ReleaseFloorResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.chain_first_run_id.is_empty() || request.floor_holder.is_empty() {
            return Err(StreamError::invalid_argument(
                "a release needs the chain and the floor holder",
            ));
        }
        self.store
            .release_floor(
                &chain_of(stream, &request.chain_first_run_id),
                &stream.topic,
                &request.floor_holder,
            )
            .await?;
        Ok(ReleaseFloorResponse {})
    }

    /// The chain a read follows, or `None` when Temporal doesn't know its owner yet, within
    /// [StreamsOptions::owner_start_grace] of the read's first owner check. It asks again as
    /// an idle read does until `deadline`. After the grace the read fails as not found.
    async fn started_chain(
        &self,
        stream: &StreamAddress,
        state: &mut ReadState,
        deadline: Instant,
    ) -> StreamResult<Option<ChainId>> {
        let mut interval = self.options.owner_check_min;
        loop {
            let error = match self.chain(stream).await {
                Ok(chain) => {
                    state.owner_missing_since_ms = 0;
                    return Ok(Some(chain));
                }
                Err(error) if error.kind == StreamFailureKind::NotFound => error,
                Err(error) => return Err(error),
            };
            let now = unix_ms();
            if state.owner_missing_since_ms == 0 {
                state.owner_missing_since_ms = now;
            }
            let missing_for = now.saturating_sub(state.owner_missing_since_ms);
            if missing_for >= millis(self.options.owner_start_grace) {
                return Err(error);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            tokio::time::sleep(interval.min(remaining)).await;
            interval = (interval * 2).min(self.options.owner_check_max);
        }
    }

    /// Whether the owner a read follows ended. Following the chain, a run that continued as
    /// new is not the end, and a new chain on the Workflow id is.
    async fn owner_ended(&self, stream: &StreamAddress, chain: &ChainId) -> StreamResult<bool> {
        let key = owner_key(stream);
        match self.describe(&key, &key.run_id).await {
            Err(OwnerError::NotFound(_)) => {
                // The chain came from this owner, so Temporal has since dropped it, and nothing
                // more will be written for it.
                if key.run_id.is_empty() {
                    self.store.close_chain(chain).await?;
                }
                Ok(true)
            }
            Err(error) => Err(describe_failed(&error)),
            Ok(described) if !key.run_id.is_empty() => Ok(described.status != OwnerStatus::Running),
            Ok(latest) if !latest.chain_ended() && latest.first_run_id == chain.first_run_id => {
                Ok(false)
            }
            Ok(_) => {
                self.store.close_chain(chain).await?;
                Ok(true)
            }
        }
    }

    /// The store the handles serve.
    pub(crate) fn store(&self) -> &dyn StreamStore {
        self.store.as_ref()
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

    /// Closes one stream, so producers' new batches are refused and reads end. With a result or a
    /// failure, the stream's notifier closes too, and fails the call when the server doesn't take
    /// it, since the close completes every callback attached there.
    pub async fn close(&self, request: CloseRequest) -> StreamResult<CloseResponse> {
        let stream = address(request.stream.as_ref())?;
        if request.result.is_some() && request.failure.is_some() {
            return Err(StreamError::invalid_argument(
                "a close carries a result or a failure, not both",
            ));
        }
        if request.closed_by.len() > MAX_CLOSED_BY_BYTES {
            return Err(StreamError::invalid_argument(format!(
                "closed_by is at most {MAX_CLOSED_BY_BYTES} bytes, not {}",
                request.closed_by.len()
            )));
        }
        let has_outcome = request.result.is_some() || request.failure.is_some();
        let close = StoredClose {
            result: request.result,
            failure: request.failure,
            closed_by: request.closed_by,
        };
        if close.encoded_len() > MAX_CLOSE_BYTES {
            return Err(StreamError::invalid_argument(format!(
                "a close's outcome is at most {MAX_CLOSE_BYTES} bytes, not {}",
                close.encoded_len()
            )));
        }
        let chain = self.chain(stream).await?;
        if has_outcome {
            self.store
                .close_stream(&chain, &stream.topic, close)
                .await?
        } else {
            self.store
                .close_topic_with(&chain, &stream.topic, close)
                .await?;
        }
        Ok(CloseResponse {})
    }

    /// Drops one producer attempt's dedupe state on a stream, on the chain that attempt wrote
    /// on. It refuses nothing on a closed stream, and a stream or attempt with no state answers
    /// the same.
    ///
    /// Only safe once the attempt's last append returned: a retry of an earlier batch would land
    /// a second time. Nothing else is lost. A store compares a batch with its own attempt's
    /// state only, and readers find a newer attempt from the records.
    pub async fn retire_producer(
        &self,
        request: RetireProducerRequest,
    ) -> StreamResult<RetireProducerResponse> {
        let stream = address(request.stream.as_ref())?;
        let (producer_id, attempt) = match &request.producer {
            Some(retire_producer_request::Producer::Named(named)) => producer(Some(named), None)?,
            Some(retire_producer_request::Producer::Activity(activity)) => {
                producer(None, Some(activity))?
            }
            None => {
                return Err(StreamError::invalid_argument("a retire needs its producer"));
            }
        };
        let key = ProducerKey {
            owner: owner_key(stream),
            producer_id,
            attempt,
        };
        // The chain the attempt's appends resolved, even when a newer chain took the Workflow
        // id since, so the state goes from where it was written.
        let chain = match self.producers.get(&key) {
            Some(checked) => chain_of_key(&key.owner, &checked.first_run_id),
            None => self.chain(stream).await?,
        };
        self.store
            .retire_producer(&chain, &stream.topic, &key.producer_id, attempt)
            .await?;
        Ok(RetireProducerResponse {})
    }

    /// Deletes every stream of one owner, across all its chains. It asks Temporal nothing, so it
    /// works after the owner is gone. Delete only after the chain closed and its producers
    /// stopped, since a late append writes a fresh stream with no dedupe state and no close mark.
    pub async fn delete_owner(
        &self,
        request: DeleteOwnerRequest,
    ) -> StreamResult<DeleteOwnerResponse> {
        let (id, other) = match StreamOwnerKind::try_from(request.owner_kind) {
            Ok(StreamOwnerKind::Workflow) => (&request.workflow_id, &request.activity_id),
            Ok(StreamOwnerKind::Activity) => (&request.activity_id, &request.workflow_id),
            _ => return Err(unsupported_kind(request.owner_kind)),
        };
        if request.namespace.is_empty() || id.is_empty() {
            return Err(StreamError::invalid_argument(
                "a delete needs the namespace and the owner's id of its kind",
            ));
        }
        if !other.is_empty() {
            return Err(StreamError::invalid_argument(
                "a delete names the owner's id of its kind only",
            ));
        }
        self.store.delete_owner(request).await
    }

    /// The chain a producer writes on. A producer attempt stays on the chain it began on, and
    /// is refused once a new chain took the owner id.
    async fn producer_chain(&self, key: ProducerKey) -> StreamResult<ChainId> {
        let chain = self.owner_chain(&key.owner).await?;
        if let Some(began) = self.producers.get(&key)
            && began.first_run_id != chain.first_run_id
        {
            let began = chain_of_key(&key.owner, &began.first_run_id);
            return Err(self.closed(&began, "has closed").await);
        }
        self.producers
            .put(key, &chain.first_run_id, self.options.owner_recheck);
        Ok(chain)
    }

    /// The chain an owner writes on now, asking about the owner when no producer of it asked
    /// within the recheck interval.
    async fn owner_chain(&self, owner: &OwnerKey) -> StreamResult<ChainId> {
        let recheck = self.options.owner_recheck;
        let known = self.owners.get(owner);
        if let Some(checked) = &known
            && checked.at.elapsed() < recheck
        {
            return Ok(chain_of_key(owner, &checked.first_run_id));
        }
        let chain = if owner.run_id.is_empty() {
            // The latest run names the chain and tells whether it ended, in one call.
            let latest = self.describe(owner, "").await;
            if let (Err(OwnerError::NotFound(_)), Some(checked)) = (&latest, &known) {
                // The chain came from this owner, so Temporal has since dropped it.
                let gone = chain_of_key(owner, &checked.first_run_id);
                return Err(self.closed(&gone, gone_text(owner)).await);
            }
            let latest = latest.map_err(|error| not_found(owner, error))?;
            let chain = chain_of_key(owner, &first_run_id(owner, &latest)?);
            if latest.chain_ended() {
                return Err(self.closed(&chain, "has closed").await);
            }
            chain
        } else if let Some(checked) = known {
            // A pinned run's chain never changes, so only its end is left to check.
            let chain = chain_of_key(owner, &checked.first_run_id);
            self.refuse_if_ended(&chain).await?;
            chain
        } else {
            let pinned = self
                .describe(owner, &owner.run_id)
                .await
                .map_err(|error| not_found(owner, error))?;
            let chain = chain_of_key(owner, &first_run_id(owner, &pinned)?);
            // A running run is its chain's latest, so it answers the end check too.
            if pinned.status != OwnerStatus::Running {
                self.refuse_if_ended(&chain).await?;
            }
            chain
        };
        self.owners.put(owner.clone(), &chain.first_run_id, recheck);
        Ok(chain)
    }

    /// Marks the chain closed and refuses when its latest run ended, when Temporal no longer
    /// holds it, or when a new chain took its owner id.
    async fn refuse_if_ended(&self, chain: &ChainId) -> StreamResult<()> {
        let key = chain_owner_key(chain);
        match self.describe(&key, "").await {
            // The chain came from this owner, so Temporal has since dropped it.
            Err(OwnerError::NotFound(_)) => Err(self.closed(chain, gone_text(&key)).await),
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
            "the {} that owns this stream {how}",
            chain_owner_key(chain).label()
        ))
    }

    /// The run chain a checked stream address names now. A pinned run's chain comes from the
    /// owner cache, since it never changes. The latest run's chain comes from a fresh describe,
    /// for a caller that must not land on a chain that already ended.
    pub(crate) async fn chain_now(&self, stream: &StreamAddress) -> StreamResult<ChainId> {
        let key = owner_key(stream);
        if !key.run_id.is_empty() {
            return self.chain(stream).await;
        }
        let described = self
            .describe(&key, "")
            .await
            .map_err(|error| not_found(&key, error))?;
        let first_run_id = first_run_id(&key, &described)?;
        self.chains
            .put(key, &first_run_id, self.options.owner_recheck);
        Ok(chain_of(stream, &first_run_id))
    }

    /// The chain a stream address names now.
    pub(crate) async fn chain(&self, stream: &StreamAddress) -> StreamResult<ChainId> {
        let key = owner_key(stream);
        // A pinned run's chain never changes. The latest run's chain changes when a new chain
        // takes the owner id.
        if let Some(checked) = self.chains.get(&key)
            && (!key.run_id.is_empty() || checked.at.elapsed() < self.options.owner_recheck)
        {
            return Ok(chain_of(stream, &checked.first_run_id));
        }
        let described = self
            .describe(&key, &key.run_id)
            .await
            .map_err(|error| not_found(&key, error))?;
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
            .describe(&owner.namespace, owner.owner(), run_id)
            .await
    }

    fn hash(&self, stream: &StreamAddress) -> String {
        let chain = chain_of(stream, "");
        let (kind, id) = chain_owner(&chain);
        stream_hash(&stream.namespace, kind, id, &stream.topic)
    }

    fn cursor(&self, stream: &StreamAddress, position: &str) -> String {
        mint_cursor(self.store.name(), &self.hash(stream), position)
    }

    /// The notification counter and the cursor of the newest record of `stream` on `chain`, or
    /// zero and an empty cursor when it holds none.
    pub(crate) async fn newest(
        &self,
        stream: &StreamAddress,
        chain: &ChainId,
    ) -> StreamResult<(i64, String)> {
        let position = self
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain.clone()),
                topic: stream.topic.clone(),
            })
            .await?
            .position;
        if position.is_empty() {
            return Ok((0, String::new()));
        }
        let counter = crate::progress_counter(&position).unwrap_or(0);
        Ok((counter, self.cursor(stream, &position)))
    }
}

/// The address of a stream this release keeps, checked.
pub(crate) fn address(stream: Option<&StreamAddress>) -> StreamResult<&StreamAddress> {
    let stream =
        stream.ok_or_else(|| StreamError::invalid_argument("a stream call needs its stream"))?;
    match StreamOwnerKind::try_from(stream.owner_kind) {
        Ok(StreamOwnerKind::Workflow) => {
            if stream.namespace.is_empty() || stream.workflow_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream needs its namespace and its owner's Workflow id",
                ));
            }
            if !stream.activity_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream that a Workflow owns names no Activity id",
                ));
            }
        }
        Ok(StreamOwnerKind::Activity) => {
            if stream.namespace.is_empty() || stream.activity_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream needs its namespace and its owner's Activity id",
                ));
            }
            if !stream.workflow_id.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a stream that an Activity owns names no Workflow id",
                ));
            }
        }
        _ => return Err(unsupported_kind(stream.owner_kind)),
    }
    check_topic(&stream.topic)?;
    Ok(stream)
}

fn unsupported_kind(owner_kind: i32) -> StreamError {
    StreamError::unsupported(format!(
        "this release keeps streams of Workflows and standalone Activities only, not owner kind \
         {owner_kind}"
    ))
}

/// Refuses a topic no store keys reliably, by the rules Core's Workflow path also applies.
/// The producer id and attempt a named producer or an Activity attempt writes as.
fn producer(
    named: Option<&NamedProducer>,
    activity: Option<&ActivityProducer>,
) -> StreamResult<(String, i64)> {
    let (producer_id, attempt) = match (named, activity) {
        (Some(named), _) => {
            if named.producer_id.is_empty() {
                return Err(StreamError::invalid_argument("a producer needs an id"));
            }
            (named.producer_id.clone(), named.attempt)
        }
        (None, Some(activity)) => (activity_producer_id(activity), activity_attempt(activity)?),
        (None, None) => return Err(StreamError::invalid_argument("a call needs its producer")),
    };
    if attempt < 1 {
        return Err(StreamError::invalid_argument(format!(
            "a producer's attempt starts at one, not {attempt}"
        )));
    }
    Ok((producer_id, attempt))
}

fn check_topic(topic: &str) -> StreamResult<()> {
    temporalio_common::streams::check_topic(topic).map_err(StreamError::invalid_argument)
}

fn unix_ms() -> u64 {
    millis(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn owner_key(stream: &StreamAddress) -> OwnerKey {
    let activity = stream.owner_kind == StreamOwnerKind::Activity as i32;
    OwnerKey {
        namespace: stream.namespace.clone(),
        activity,
        id: if activity {
            stream.activity_id.clone()
        } else {
            stream.workflow_id.clone()
        },
        run_id: stream.run_id.clone(),
    }
}

/// The latest run of the owner of `chain`.
fn chain_owner_key(chain: &ChainId) -> OwnerKey {
    let activity = chain.owner_kind == StreamOwnerKind::Activity as i32;
    OwnerKey {
        namespace: chain.namespace.clone(),
        activity,
        id: chain_owner(chain).1.to_string(),
        run_id: String::new(),
    }
}

/// The chain of `stream`'s owner that starts at `first_run_id`.
pub(crate) fn chain_of(stream: &StreamAddress, first_run_id: &str) -> ChainId {
    chain_of_key(&owner_key(stream), first_run_id)
}

fn chain_of_key(owner: &OwnerKey, first_run_id: &str) -> ChainId {
    // A Workflow chain keeps the kind unset, as every chain from before the Activity kind does,
    // so the two compare equal.
    if owner.activity {
        ChainId {
            namespace: owner.namespace.clone(),
            first_run_id: first_run_id.to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: owner.id.clone(),
            ..Default::default()
        }
    } else {
        ChainId {
            namespace: owner.namespace.clone(),
            workflow_id: owner.id.clone(),
            first_run_id: first_run_id.to_string(),
            ..Default::default()
        }
    }
}

/// How a refusal says that Temporal no longer holds the owner.
fn gone_text(owner: &OwnerKey) -> &'static str {
    if owner.activity {
        "is gone from Temporal"
    } else {
        "is gone from History"
    }
}

fn first_run_id(owner: &OwnerKey, described: &OwnerDescription) -> StreamResult<String> {
    if described.first_run_id.is_empty() {
        // The owner's own output is keyed by the chain's first run, so an empty one would split
        // the stream in two without an error.
        return Err(StreamError::unsupported(format!(
            "Temporal did not report the first run id of {}, which streams are keyed by",
            owner.label()
        )));
    }
    Ok(described.first_run_id.clone())
}

fn not_found(owner: &OwnerKey, error: OwnerError) -> StreamError {
    match error {
        OwnerError::NotFound(message) => StreamError::not_found(format!(
            "the {} that owns the stream was not found, and its stream is keyed by its run \
             chain: {message}",
            owner.label()
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
            StreamRecordKind, append_request::Producer,
        },
    };
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload,
        enums::v1::{ActivityExecutionStatus, WorkflowExecutionStatus},
    };

    /// Temporal as the tests need it: the latest run of each owner, and pinned runs. A Workflow
    /// owner is keyed by its id, and an Activity owner by `activity/<id>`.
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
                status: OwnerStatus::Running,
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

        /// Starts run `run_id` of Activity `activity_id`, which is its own chain.
        fn activity_run(&self, activity_id: &str, run_id: &str) {
            self.run(&format!("activity/{activity_id}"), run_id, run_id);
        }

        fn end(&self, workflow_id: &str, status: WorkflowExecutionStatus) {
            self.end_as(workflow_id, OwnerStatus::of_workflow(status));
        }

        fn end_activity(&self, activity_id: &str, status: ActivityExecutionStatus) {
            let status = OwnerStatus::of_activity(status);
            let key = format!("activity/{activity_id}");
            if let Some(Ok(latest)) = self.latest.lock().unwrap().get(&key)
                && let Some(run) = self.runs.lock().unwrap().get_mut(&latest.run_id)
            {
                run.status = status;
            }
            self.end_as(&key, status);
        }

        fn end_as(&self, key: &str, status: OwnerStatus) {
            if let Some(Ok(latest)) = self.latest.lock().unwrap().get_mut(key) {
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
            owner: OwnerId<'_>,
            run_id: &str,
        ) -> Result<OwnerDescription, OwnerError> {
            self.describes.fetch_add(1, Ordering::Relaxed);
            let workflow_id = &match owner {
                OwnerId::Workflow(id) => id.to_string(),
                OwnerId::Activity(id) => format!("activity/{id}"),
            };
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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

    #[tokio::test(start_paused = true)]
    async fn a_new_producer_closes_an_ended_chain_the_worker_missed() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Terminated);
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        assert!(!chain_closed(&setup.store, "run-1").await);
        assert_eq!(
            kind(setup.streams.append(append("q", 1, "2")).await),
            StreamFailureKind::Closed
        );
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn the_producers_of_one_owner_share_its_check() {
        // A Workflow that writes each flush as a new producer asks about its owner once.
        let setup = setup(StreamsOptions::default());
        for producer in 0..50 {
            let request = append(&format!("__temporal_local_{producer}@run-1"), 1, "x");
            setup.streams.append(request).await.unwrap();
        }
        assert_eq!(setup.owners.describes(), 1);
    }

    #[tokio::test]
    async fn a_producer_pinned_to_a_running_run_asks_once() {
        let setup = setup(StreamsOptions::default());
        setup.owners.run("wf", "run-2", "run-1");
        for producer in ["p", "q"] {
            let mut request = append(producer, 1, "1");
            request.stream.as_mut().unwrap().run_id = "run-2".to_string();
            setup.streams.append(request).await.unwrap();
        }
        assert_eq!(setup.owners.describes(), 1);
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
                status: OwnerStatus::of_workflow(status),
            }
            .chain_ended()
        };
        for status in [Completed, Failed, Canceled, Terminated, TimedOut] {
            assert!(ended(status), "{status:?}");
        }
        for status in [Running, ContinuedAsNew, Unspecified, Paused] {
            assert!(!ended(status), "{status:?}");
        }
        assert_eq!(OwnerStatus::of_workflow(Paused), OwnerStatus::Running);
    }

    #[test]
    fn only_a_terminal_activity_status_ends_its_run() {
        use ActivityExecutionStatus::*;
        for status in [Completed, Failed, Canceled, Terminated, TimedOut] {
            assert_eq!(
                OwnerStatus::of_activity(status),
                OwnerStatus::Ended,
                "{status:?}"
            );
        }
        for status in [Running, Paused, Unspecified] {
            assert_eq!(
                OwnerStatus::of_activity(status),
                OwnerStatus::Running,
                "{status:?}"
            );
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
                result: None,
                failure: None,
                closed_by: Default::default(),
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

    #[tokio::test]
    async fn a_retire_drops_the_attempts_state_on_the_chain_it_wrote() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                ..Default::default()
            })
            .await
            .unwrap();
        let before = setup.owners.describes();
        let retire = |producer: Option<retire_producer_request::Producer>| RetireProducerRequest {
            stream: Some(stream("out")),
            producer,
        };
        let named = |attempt| {
            Some(retire_producer_request::Producer::Named(NamedProducer {
                producer_id: "p".to_string(),
                attempt,
            }))
        };
        // A closed stream refuses no retire, and a repeat answers the same.
        for _ in 0..2 {
            setup
                .streams
                .retire_producer(retire(named(1)))
                .await
                .unwrap();
        }
        // The append resolved the chain, so the retire asks Temporal nothing.
        assert_eq!(setup.owners.describes(), before);
        // With the state gone, the repeat is a new batch, which the closed stream refuses.
        assert_eq!(
            kind(setup.streams.append(append("p", 1, "1")).await),
            StreamFailureKind::Closed
        );
        for bad in [retire(None), retire(named(0))] {
            assert_eq!(
                kind(setup.streams.retire_producer(bad).await),
                StreamFailureKind::InvalidArgument
            );
        }
    }

    fn quick() -> StreamsOptions {
        StreamsOptions {
            owner_check_min: Duration::from_millis(20),
            owner_check_max: Duration::from_millis(80),
            ..StreamsOptions::default()
        }
    }

    fn read_after(after: &str, state: &[u8], wait: Duration) -> ReadRequest {
        ReadRequest {
            stream: Some(stream("out")),
            after: after.to_string(),
            state: state.to_vec(),
            wait: Some(wait.try_into().unwrap()),
            ..Default::default()
        }
    }

    /// A setup whose Workflow `wf` has not started yet, with an owner start grace of `grace`.
    fn before_owner_starts(grace: Duration) -> Setup {
        let store = Arc::new(MemoryStore::new());
        let owners = Arc::new(Owners::default());
        let options = StreamsOptions {
            owner_start_grace: grace,
            ..quick()
        };
        let streams = Streams::new(store.clone(), owners.clone(), options);
        Setup {
            store,
            owners,
            streams,
        }
    }

    #[tokio::test]
    async fn a_read_before_its_owner_starts_answers_empty_until_the_owner_writes() {
        let setup = before_owner_starts(Duration::from_secs(60));
        let empty = setup
            .streams
            .read(read_after(END, &[], Duration::ZERO))
            .await
            .unwrap();
        assert!(empty.records.is_empty() && !empty.done);
        assert_eq!(
            empty.cursor, BEGINNING,
            "the end of a stream that holds nothing is its start"
        );
        setup.owners.run("wf", "run-1", "run-1");
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let read = setup
            .streams
            .read(read_after(&empty.cursor, &empty.state, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1, "the first record is not missed");
    }

    #[tokio::test]
    async fn a_waiting_read_gets_the_first_record_of_an_owner_that_starts_later() {
        let setup = before_owner_starts(Duration::from_secs(60));
        let owners = setup.owners.clone();
        let streams = Arc::new(setup.streams);
        let writer = streams.clone();
        let late_start = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            owners.run("wf", "run-1", "run-1");
            writer.append(append("p", 1, "1")).await.unwrap();
        });
        let read = streams
            .read(read_after("", &[], Duration::from_secs(5)))
            .await
            .unwrap();
        late_start.await.unwrap();
        assert_eq!(read.records.len(), 1);
    }

    #[tokio::test]
    async fn a_read_of_an_owner_that_never_starts_is_not_found_after_the_grace() {
        let setup = before_owner_starts(Duration::from_millis(200));
        let empty = setup
            .streams
            .read(read_after("", &[], Duration::from_millis(50)))
            .await
            .unwrap();
        assert!(empty.records.is_empty());
        let error = setup
            .streams
            .read(read_after("", &empty.state, Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::NotFound);
        // The grace counts from the read's first owner check, so a new read waits again.
        assert!(
            setup
                .streams
                .read(read_after("", &[], Duration::ZERO))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn no_grace_fails_a_read_of_a_missing_owner_at_once() {
        let setup = before_owner_starts(Duration::ZERO);
        let error = setup
            .streams
            .read(read_after("", &[], Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::NotFound);
    }

    /// Reads until the read says it is done, or fails after `limit` calls.
    async fn read_to_end(streams: &Streams, wait: Duration) -> (usize, ReadResponse) {
        let mut delivered = 0;
        let mut last = read_after("", &[], wait);
        for _ in 0..50 {
            let page = streams.read(last.clone()).await.unwrap();
            delivered += page.records.len();
            if page.done {
                return (delivered, page);
            }
            last = read_after(&page.cursor, &page.state, wait);
        }
        panic!("the read never ended");
    }

    fn gave_up() -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        temporalio_common::protos::temporal::api::failure::v1::Failure {
            message: "the agent gave up".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_reader_that_reaches_the_end_sees_the_first_close() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                failure: Some(gave_up()),
                closed_by: "run-1".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                closed_by: "run-2".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let (delivered, end) = read_to_end(&setup.streams, Duration::from_millis(50)).await;
        assert_eq!(delivered, 1);
        assert!(end.done);
        assert_eq!(end.close_failure, Some(gave_up()));
        assert_eq!(end.close_result, None);
        assert_eq!(end.closed_by, "run-1", "the first close stays");
        let refused = setup.streams.append(append("q", 1, "2")).await.unwrap_err();
        assert_eq!(refused.kind, StreamFailureKind::Closed);
        assert_eq!(refused.closed_by.as_deref(), Some("run-1"));
    }

    #[tokio::test]
    async fn a_reader_whose_owner_ended_with_no_close_sees_no_outcome() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let (_, end) = read_to_end(&setup.streams, Duration::from_millis(50)).await;
        assert!(end.done);
        assert_eq!(
            (end.close_result, end.close_failure, end.closed_by.as_str()),
            (None, None, "")
        );
    }

    #[tokio::test]
    async fn a_close_too_large_to_keep_is_refused_and_closes_nothing() {
        let setup = setup(quick());
        for close in [
            CloseRequest {
                stream: Some(stream("out")),
                closed_by: "x".repeat(MAX_CLOSED_BY_BYTES + 1),
                ..Default::default()
            },
            CloseRequest {
                stream: Some(stream("out")),
                result: Some(Payload {
                    data: vec![0; MAX_CLOSE_BYTES],
                    ..Default::default()
                }),
                ..Default::default()
            },
        ] {
            let error = setup.streams.close(close).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
        }
        setup.streams.append(append("p", 1, "1")).await.unwrap();
    }

    /// The replay read that repeats the live `read` of `request` that answered `page`.
    fn replay_of(request: &ReadRequest, page: &ReadResponse) -> ReadRangeRequest {
        ReadRangeRequest {
            stream: Some(stream("out")),
            chain_first_run_id: page.chain_first_run_id.clone(),
            after: page.start_cursor.clone(),
            to: page.cursor.clone(),
            stored_records: page
                .records
                .iter()
                .filter(|r| matches!(r.record, Some(read_record::Record::Stored(_))))
                .count() as u32,
            state: request.state.clone(),
            records_digest: page.records_digest.clone(),
            floor_holder: String::new(),
            start_from_end: request.after == crate::END,
        }
    }

    fn attempt(producer: &str, attempt: i64, sequence: i64, value: &str) -> AppendRequest {
        AppendRequest {
            producer: Some(Producer::Named(NamedProducer {
                producer_id: producer.to_string(),
                attempt,
            })),
            ..append(producer, sequence, value)
        }
    }

    /// A read after the stream's first record, with its replay request.
    async fn first_page(setup: &Setup, holder: &str) -> (ReadRequest, ReadResponse) {
        let first = setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        // A newer attempt makes the read synthesize a supersession.
        setup.streams.append(attempt("p", 2, 1, "3")).await.unwrap();
        let request = ReadRequest {
            floor_holder: holder.to_string(),
            ..read_after(&first.last_cursor, &[], Duration::ZERO)
        };
        let page = setup.streams.read(request.clone()).await.unwrap();
        (request, page)
    }

    #[tokio::test]
    async fn a_replay_read_gives_the_records_the_read_got() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        assert!(
            page.records
                .iter()
                .any(|r| matches!(r.record, Some(read_record::Record::Superseded(_)))),
            "{page:?}"
        );
        assert_eq!(page.chain_first_run_id, "run-1");
        assert_eq!(page.start_cursor, request.after);
        assert_eq!(page.records_digest.len(), 32);
        // More records after the range change nothing in it.
        setup.streams.append(attempt("p", 2, 2, "4")).await.unwrap();
        let replayed = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap();
        assert_eq!(replayed.records, page.records);
        assert_eq!(replayed.cursor, page.cursor);
        assert_eq!(replayed.records_digest, page.records_digest);
    }

    #[tokio::test]
    async fn a_replay_read_of_a_read_from_the_end_gives_the_records_the_read_got() {
        let setup = setup(quick());
        setup.streams.append(attempt("p", 1, 1, "1")).await.unwrap();
        let request = read_after(crate::END, &[], Duration::from_secs(5));
        // The read finds its start at the end, then a newer attempt of the producer there lands.
        let (page, _) = tokio::join!(setup.streams.read(request.clone()), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            setup.streams.append(attempt("p", 2, 1, "2")).await.unwrap();
        });
        let page = page.unwrap();
        assert_eq!(page.records.len(), 1, "no supersession, {page:?}");
        let replay = replay_of(&request, &page);
        assert!(replay.start_from_end);
        let replayed = setup.streams.read_range(replay).await.unwrap();
        assert_eq!(replayed.records, page.records);
    }

    #[tokio::test]
    async fn a_replay_read_needs_no_owner() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        // History dropped the owner, as for a Replayer with only the store.
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let before = setup.owners.describes();
        setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap();
        assert_eq!(setup.owners.describes(), before);
    }

    #[tokio::test]
    async fn a_replay_read_with_other_records_is_a_mismatch() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        let mut other_digest = replay_of(&request, &page);
        other_digest.records_digest[0] ^= 1;
        let mut other_end = replay_of(&request, &page);
        other_end.stored_records -= 1;
        for replay in [other_digest, other_end] {
            let error = setup.streams.read_range(replay).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::ReplayMismatch, "{error}");
        }
    }

    #[tokio::test]
    async fn a_replay_read_of_records_retention_dropped_is_rec_01() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        setup.store.truncate(&chain("run-1"), "out", 1);
        let error = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::ReplayRangeLost, "{error}");
        assert!(error.message.starts_with("REC-01"), "{error}");
    }

    #[tokio::test]
    async fn a_replay_read_from_a_trimmed_start_is_rec_01() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.streams.append(append("p", 2, "2")).await.unwrap();
        let request = read_after("", &[], Duration::ZERO);
        let page = setup.streams.read(request.clone()).await.unwrap();
        assert_eq!(page.start_cursor, "");
        // Later records stand where the range began, so the replay finds as many as it wants.
        setup.store.truncate(&chain("run-1"), "out", 0);
        for sequence in 3..=4 {
            setup
                .streams
                .append(append("p", sequence, &sequence.to_string()))
                .await
                .unwrap();
        }
        let error = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::ReplayRangeLost, "{error}");
    }

    #[tokio::test]
    async fn a_floor_keeps_what_a_positions_only_reader_read_until_it_is_released() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "reader-run/1").await;
        setup.store.truncate(&chain("run-1"), "out", 1);
        let replayed = setup
            .streams
            .read_range(ReadRangeRequest {
                floor_holder: "reader-run/1".to_string(),
                ..replay_of(&request, &page)
            })
            .await
            .unwrap();
        assert_eq!(replayed.records, page.records, "the floor kept the range");
        setup
            .streams
            .release_floor(ReleaseFloorRequest {
                stream: Some(stream("out")),
                chain_first_run_id: "run-1".to_string(),
                floor_holder: "reader-run/1".to_string(),
            })
            .await
            .unwrap();
        setup.store.truncate(&chain("run-1"), "out", 1);
        let error = setup
            .streams
            .read_range(replay_of(&request, &page))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::ReplayRangeLost, "{error}");
    }

    #[tokio::test]
    async fn a_replay_read_needs_its_chain_and_a_resolved_start() {
        let setup = setup(quick());
        let (request, page) = first_page(&setup, "").await;
        for replay in [
            ReadRangeRequest {
                chain_first_run_id: String::new(),
                ..replay_of(&request, &page)
            },
            ReadRangeRequest {
                after: crate::END.to_string(),
                ..replay_of(&request, &page)
            },
        ] {
            let error = setup.streams.read_range(replay).await.unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument, "{error}");
        }
    }

    #[tokio::test]
    async fn an_empty_read_waits_for_a_counter_above_its_position() {
        let setup = setup(StreamsOptions::default());
        let empty = setup
            .streams
            .read(read_after("", &[], Duration::from_millis(20)))
            .await
            .unwrap();
        assert!(empty.records.is_empty() && !empty.done);
        assert_eq!(empty.wake_after, 0, "an empty stream wakes on any counter");
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let landed = setup.streams.append(append("p", 2, "2")).await.unwrap();
        let page = setup
            .streams
            .read(read_after(
                &landed.last_cursor,
                &[],
                Duration::from_millis(20),
            ))
            .await
            .unwrap();
        assert!(page.records.is_empty());
        let at = |cursor: &str| {
            crate::cursor_position(
                cursor,
                setup.store.name(),
                &setup.streams.hash(&stream("out")),
            )
            .unwrap()
            .unwrap()
            .to_string()
        };
        assert_eq!(
            Some(page.wake_after),
            crate::progress_counter(&at(&landed.last_cursor))
        );
        let next = setup.streams.append(append("p", 3, "3")).await.unwrap();
        assert!(
            crate::progress_counter(&at(&next.last_cursor)).unwrap() > page.wake_after,
            "the next record's notification wakes the reader"
        );
    }

    #[tokio::test]
    async fn a_read_that_finds_its_owner_ended_answers_done_with_no_wake_counter() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let page = setup
            .streams
            .read(read_after(END, &[], Duration::from_secs(5)))
            .await
            .unwrap();
        assert!(page.done);
        assert_eq!(page.wake_after, 0);
    }

    #[tokio::test]
    async fn a_read_ends_when_its_owner_chain_ends_and_marks_it_closed() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let (delivered, end) = read_to_end(&setup.streams, Duration::from_secs(5)).await;
        assert_eq!(delivered, 1);
        assert!(end.records.is_empty());
        // The read saw the chain end, and says so to later producers.
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_read_follows_a_chain_that_continued_as_new() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .end("wf", WorkflowExecutionStatus::ContinuedAsNew);
        setup.owners.run("wf", "run-2", "run-1");
        let page = setup
            .streams
            .read(read_after("", &[], Duration::from_millis(200)))
            .await
            .unwrap();
        let idle = setup
            .streams
            .read(read_after(
                &page.cursor,
                &page.state,
                Duration::from_millis(200),
            ))
            .await
            .unwrap();
        assert!(!idle.done);
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_read_pinned_to_a_run_ends_when_that_run_continues() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .owners
            .runs
            .lock()
            .unwrap()
            .get_mut("run-1")
            .unwrap()
            .status = OwnerStatus::ContinuedAsNew;
        setup.owners.run("wf", "run-2", "run-1");
        let mut request = read_after("", &[], Duration::from_secs(5));
        request.stream.as_mut().unwrap().run_id = "run-1".to_string();
        let mut done = false;
        for _ in 0..10 {
            let page = setup.streams.read(request.clone()).await.unwrap();
            if page.done {
                done = true;
                break;
            }
            request.after = page.cursor;
            request.state = page.state;
        }
        assert!(done);
        // It stopped with the first run, though the chain still runs.
        assert!(!chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn a_read_ends_when_a_new_chain_reuses_the_workflow_id() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        // The id's latest run is running, but it belongs to another chain.
        setup.owners.run("wf", "run-9", "run-9");
        let mut request = read_after(&first.cursor, &first.state, Duration::from_secs(5));
        let mut done = false;
        for _ in 0..10 {
            let page = setup.streams.read(request.clone()).await.unwrap();
            assert!(page.records.is_empty());
            if page.done {
                done = true;
                break;
            }
            request = read_after(&page.cursor, &page.state, Duration::from_secs(5));
        }
        assert!(done);
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn rule_20_3_a_read_ends_when_history_no_longer_holds_its_owner() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        setup
            .owners
            .fail("wf", OwnerError::NotFound("workflow not found".to_string()));
        let page = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_secs(5),
            ))
            .await
            .unwrap();
        assert!(page.done);
        assert!(chain_closed(&setup.store, "run-1").await);
    }

    #[tokio::test]
    async fn rule_17_2_an_owner_check_during_a_read_that_fails_is_a_storage_failure() {
        let setup = setup(quick());
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        setup
            .owners
            .fail("wf", OwnerError::Failed("unavailable".to_string()));
        let error = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_secs(5),
            ))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Storage);
        assert!(error.message.contains("unavailable"), "{error}");
    }

    #[tokio::test]
    async fn an_idle_read_backs_off_its_owner_checks() {
        let setup = setup(quick());
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        let before = setup.owners.describes();
        let idle = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_secs(1),
            ))
            .await
            .unwrap();
        // Fixed checks every 20 ms would be about 50, and doubling to 80 ms is about 15.
        let checks = setup.owners.describes() - before;
        assert!((8..=20).contains(&checks), "{checks} checks");
        let state = ReadState::decode_from(&idle.state).unwrap();
        assert_eq!(state.check_interval_ms, 80);
        // A record starts the interval over.
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let page = setup
            .streams
            .read(read_after(
                &idle.cursor,
                &idle.state,
                Duration::from_secs(1),
            ))
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        let state = ReadState::decode_from(&page.state).unwrap();
        assert_eq!(state.check_interval_ms, 20);
    }

    #[tokio::test]
    async fn a_closed_stream_ends_its_reads_without_asking_temporal() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        setup
            .streams
            .close(CloseRequest {
                stream: Some(stream("out")),
                result: None,
                failure: None,
                closed_by: Default::default(),
            })
            .await
            .unwrap();
        let before = setup.owners.describes();
        let (delivered, _) = read_to_end(&setup.streams, Duration::from_secs(5)).await;
        assert_eq!(delivered, 1);
        assert_eq!(setup.owners.describes(), before);
    }

    #[tokio::test]
    async fn an_owner_delete_asks_temporal_nothing() {
        let setup = setup(StreamsOptions::default());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let before = setup.owners.describes();
        let request = DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "wf".to_string(),
            ..Default::default()
        };
        assert!(
            setup
                .streams
                .delete_owner(request.clone())
                .await
                .unwrap()
                .deleted
                > 0
        );
        assert_eq!(setup.owners.describes(), before);
        for (field, expected) in [
            ("namespace", StreamFailureKind::InvalidArgument),
            ("workflow_id", StreamFailureKind::InvalidArgument),
            ("owner_kind", StreamFailureKind::Unsupported),
        ] {
            let mut bad = request.clone();
            match field {
                "namespace" => bad.namespace.clear(),
                "workflow_id" => bad.workflow_id.clear(),
                _ => bad.owner_kind = StreamOwnerKind::Unspecified as i32,
            }
            assert_eq!(
                kind(setup.streams.delete_owner(bad).await),
                expected,
                "{field}"
            );
        }
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

    /// A stream of standalone Activity `act`, whose run `act-run-1` is running.
    fn activity_stream(topic: &str) -> StreamAddress {
        StreamAddress {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "act".to_string(),
            topic: topic.to_string(),
            ..Default::default()
        }
    }

    fn activity_chain(run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            first_run_id: run_id.to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "act".to_string(),
            ..Default::default()
        }
    }

    fn activity_setup() -> Setup {
        let setup = setup(quick());
        setup.owners.activity_run("act", "act-run-1");
        setup
    }

    fn activity_append(sequence: i64, value: &str) -> AppendRequest {
        AppendRequest {
            stream: Some(activity_stream("out")),
            producer: Some(Producer::Activity(ActivityProducer {
                run_id: "act-run-1".to_string(),
                activity_id: "act".to_string(),
                attempt: 1,
                ..Default::default()
            })),
            ..append("", sequence, value)
        }
    }

    fn activity_read(after: &str, state: &[u8], wait: Duration) -> ReadRequest {
        ReadRequest {
            stream: Some(activity_stream("out")),
            ..read_after(after, state, wait)
        }
    }

    async fn read_activity_to_end(
        streams: &Streams,
        request: ReadRequest,
    ) -> (usize, ReadResponse) {
        let mut delivered = 0;
        let mut request = request;
        for _ in 0..50 {
            let page = streams.read(request.clone()).await.unwrap();
            delivered += page.records.len();
            if page.done {
                return (delivered, page);
            }
            request.after = page.cursor;
            request.state = page.state;
        }
        panic!("the read never ended");
    }

    async fn activity_chain_closed(store: &MemoryStore, run_id: &str) -> bool {
        store
            .read(StoreReadRequest {
                chain: Some(activity_chain(run_id)),
                topic: "out".to_string(),
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .closed
    }

    #[tokio::test]
    async fn an_activity_writes_its_own_stream_on_the_chain_of_its_run() {
        let setup = activity_setup();
        let landed = setup.streams.append(activity_append(1, "1")).await.unwrap();
        let held = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(activity_chain("act-run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(held.position, "0");
        let read = setup
            .streams
            .read(activity_read("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].cursor, landed.last_cursor);
        assert_eq!(read.chain_first_run_id, "act-run-1");
        // A Workflow with the same id owns other streams, and their cursors don't mix.
        setup.owners.run("act", "run-1", "run-1");
        let mut workflow = stream("out");
        workflow.workflow_id = "act".to_string();
        let workflow_read = setup
            .streams
            .read(ReadRequest {
                stream: Some(workflow.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(workflow_read.records.is_empty());
        let error = setup
            .streams
            .read(ReadRequest {
                stream: Some(workflow),
                after: landed.last_cursor,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor, "{error}");
    }

    #[tokio::test]
    async fn an_activity_address_names_the_activity_only() {
        let setup = activity_setup();
        let mut no_id = activity_stream("out");
        no_id.activity_id.clear();
        let mut with_workflow = activity_stream("out");
        with_workflow.workflow_id = "wf".to_string();
        let mut workflow_with_activity = stream("out");
        workflow_with_activity.activity_id = "act".to_string();
        for bad in [no_id, with_workflow, workflow_with_activity] {
            assert_eq!(
                kind(
                    setup
                        .streams
                        .latest(LatestRequest { stream: Some(bad) })
                        .await
                ),
                StreamFailureKind::InvalidArgument
            );
        }
        assert_eq!(setup.owners.describes(), 0);
    }

    #[tokio::test]
    async fn an_activity_read_ends_done_when_the_run_reaches_a_terminal_status() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup
            .owners
            .end_activity("act", ActivityExecutionStatus::Failed);
        let (delivered, end) = read_activity_to_end(
            &setup.streams,
            activity_read("", &[], Duration::from_secs(5)),
        )
        .await;
        assert_eq!(delivered, 1);
        assert!(end.done);
        assert!(activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test]
    async fn an_activity_read_that_retries_and_pauses_stays_open() {
        let setup = activity_setup();
        setup
            .owners
            .end_activity("act", ActivityExecutionStatus::Paused);
        let idle = setup
            .streams
            .read(activity_read("", &[], Duration::from_millis(200)))
            .await
            .unwrap();
        assert!(!idle.done);
        assert!(!activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test]
    async fn an_activity_read_before_the_run_exists_waits_the_grace() {
        let setup = before_owner_starts(Duration::from_secs(60));
        let empty = setup
            .streams
            .read(activity_read(END, &[], Duration::ZERO))
            .await
            .unwrap();
        assert!(empty.records.is_empty() && !empty.done);
        setup.owners.activity_run("act", "act-run-1");
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        let read = setup
            .streams
            .read(activity_read(&empty.cursor, &empty.state, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
        let late = before_owner_starts(Duration::ZERO);
        let error = late
            .streams
            .read(activity_read("", &[], Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::NotFound);
        assert!(error.message.contains("Activity \"act\""), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_activity_producer_is_refused_once_the_run_ended() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup
            .owners
            .end_activity("act", ActivityExecutionStatus::Completed);
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        let error = setup
            .streams
            .append(activity_append(2, "2"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Closed);
        assert!(error.message.contains("Activity \"act\""), "{error}");
        assert!(activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_run_of_the_activity_id_ends_the_earlier_runs_streams() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup.owners.activity_run("act", "act-run-2");
        tokio::time::advance(OWNER_RECHECK + Duration::from_secs(1)).await;
        assert_eq!(
            kind(setup.streams.append(activity_append(2, "2")).await),
            StreamFailureKind::Closed
        );
        assert!(activity_chain_closed(&setup.store, "act-run-1").await);
        // A producer of the new run writes on the new run's chain.
        let mut next = activity_append(1, "3");
        if let Some(Producer::Activity(activity)) = &mut next.producer {
            activity.run_id = "act-run-2".to_string();
        }
        setup.streams.append(next).await.unwrap();
        assert!(!activity_chain_closed(&setup.store, "act-run-2").await);
    }

    #[tokio::test]
    async fn an_activity_read_pinned_to_a_run_reads_that_run() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup.owners.activity_run("act", "act-run-2");
        let mut pinned = activity_read("", &[], Duration::ZERO);
        pinned.stream.as_mut().unwrap().run_id = "act-run-1".to_string();
        let page = setup.streams.read(pinned).await.unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.chain_first_run_id, "act-run-1");
    }

    #[tokio::test]
    async fn an_activity_read_follows_the_newest_run_at_its_start_until_a_newer_run() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        let first = setup
            .streams
            .read(activity_read("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(first.chain_first_run_id, "act-run-1");
        setup.owners.activity_run("act", "act-run-2");
        let (delivered, end) = read_activity_to_end(
            &setup.streams,
            activity_read(&first.cursor, &first.state, Duration::from_secs(5)),
        )
        .await;
        assert_eq!(delivered, 0);
        assert!(end.done);
        assert!(activity_chain_closed(&setup.store, "act-run-1").await);
    }

    #[tokio::test]
    async fn an_activity_owner_delete_removes_only_that_activitys_streams() {
        let setup = activity_setup();
        setup.streams.append(activity_append(1, "1")).await.unwrap();
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        let request = DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Activity as i32,
            activity_id: "act".to_string(),
            ..Default::default()
        };
        assert!(
            setup
                .streams
                .delete_owner(request.clone())
                .await
                .unwrap()
                .deleted
                > 0
        );
        let gone = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(activity_chain("act-run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(gone.position, "");
        let kept = setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain("run-1")),
                topic: "out".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(kept.position, "0");
        for bad in [
            DeleteOwnerRequest {
                activity_id: String::new(),
                ..request.clone()
            },
            DeleteOwnerRequest {
                workflow_id: "wf".to_string(),
                ..request.clone()
            },
        ] {
            assert_eq!(
                kind(setup.streams.delete_owner(bad).await),
                StreamFailureKind::InvalidArgument
            );
        }
    }

    /// The start of 2026-10-14 in Unix milliseconds.
    const DAY_START_MS: i64 = 1_791_936_000_000;

    /// Attempt `attempt` of Activity `fetch` of run `run-1` of `wf`, started `after_ms` past
    /// [DAY_START_MS], writing `value` at `sequence` on the Workflow's stream.
    fn timed(attempt: i32, after_ms: i64, sequence: i64, value: &str) -> AppendRequest {
        let mut activity = ActivityProducer {
            workflow_id: "wf".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "fetch".to_string(),
            attempt,
            ..Default::default()
        };
        let started = activity
            .attempt_started_time
            .get_or_insert_with(Default::default);
        let ms = DAY_START_MS + after_ms;
        started.seconds = ms / 1000;
        started.nanos = (ms % 1000) as i32 * 1_000_000;
        AppendRequest {
            producer: Some(Producer::Activity(activity)),
            ..append("", sequence, value)
        }
    }

    fn key(attempt: i64, after_ms: i64) -> i64 {
        (DAY_START_MS + after_ms) << 16 | attempt
    }

    /// The read's records, as each stored record's value and attempt, or a supersession's two
    /// attempts with an empty value.
    async fn shape(
        setup: &Setup,
        after: &str,
        state: &[u8],
    ) -> (Vec<(String, i64, i64, bool)>, ReadResponse) {
        let page = setup
            .streams
            .read(read_after(after, state, Duration::ZERO))
            .await
            .unwrap();
        let shape = page
            .records
            .iter()
            .map(|record| match &record.record {
                Some(read_record::Record::Stored(stored)) => (
                    String::from_utf8(stored.body.clone().unwrap().data).unwrap(),
                    stored.attempt,
                    0,
                    record.stale,
                ),
                Some(read_record::Record::Superseded(superseded)) => (
                    String::new(),
                    superseded.previous_attempt,
                    superseded.attempt,
                    record.stale,
                ),
                None => panic!("an empty record"),
            })
            .collect();
        (shape, page)
    }

    #[tokio::test]
    async fn a_timed_activity_attempt_writes_its_start_and_attempt_as_its_key() {
        let setup = setup(quick());
        setup.streams.append(timed(3, 0, 1, "a")).await.unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(records, [("a".to_string(), key(3, 0), 0, false)]);
        let mut late = timed(70_000, 0, 1, "b");
        if let Some(Producer::Activity(activity)) = &mut late.producer {
            activity.activity_id = "other".to_string();
        }
        setup.streams.append(late).await.unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(records[1].1, key(crate::MAX_KEYED_ACTIVITY_ATTEMPT, 0));
        let before_1970 = timed(1, -DAY_START_MS - 1000, 1, "c");
        assert_eq!(
            kind(setup.streams.append(before_1970).await),
            StreamFailureKind::InvalidArgument
        );
    }

    #[tokio::test]
    async fn an_activity_retry_supersedes_the_earlier_attempt() {
        let setup = setup(quick());
        setup.streams.append(timed(1, 0, 1, "a1")).await.unwrap();
        setup
            .streams
            .append(timed(2, 1_000, 1, "b1"))
            .await
            .unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(
            records,
            [
                ("a1".to_string(), key(1, 0), 0, false),
                (String::new(), key(1, 0), key(2, 1_000), false),
                ("b1".to_string(), key(2, 1_000), 0, false),
            ]
        );
    }

    #[tokio::test]
    async fn a_reset_activity_attempt_supersedes_with_no_divergence_and_no_drop() {
        let setup = setup(quick());
        for (attempt, after_ms) in [(1, 0), (2, 1_000), (3, 2_000)] {
            setup
                .streams
                .append(timed(attempt, after_ms, 1, &format!("attempt {attempt}")))
                .await
                .unwrap();
        }
        // A reset sets the Temporal attempt back to 1, and the reset attempt starts later.
        setup
            .streams
            .append(timed(1, 3_000, 1, "reset"))
            .await
            .unwrap();
        let (records, _) = shape(&setup, "", &[]).await;
        assert_eq!(
            records.last().unwrap(),
            &("reset".to_string(), key(1, 3_000), 0, false)
        );
        assert_eq!(
            records[records.len() - 2],
            (String::new(), key(3, 2_000), key(1, 3_000), false)
        );
        // With no start time the reset attempt would repeat attempt 1 with other content.
        let mut untimed = timed(1, 0, 1, "reset");
        if let Some(Producer::Activity(activity)) = &mut untimed.producer {
            activity.attempt_started_time = None;
        }
        setup.streams.append(untimed.clone()).await.unwrap();
        untimed.digest = vec![1; 32];
        assert_eq!(
            kind(setup.streams.append(untimed).await),
            StreamFailureKind::ProducerDivergent
        );
    }

    #[tokio::test]
    async fn an_activity_attempt_that_writes_after_a_newer_one_started_is_stale() {
        let setup = setup(quick());
        setup.streams.append(timed(1, 0, 1, "a1")).await.unwrap();
        setup
            .streams
            .append(timed(2, 1_000, 1, "b1"))
            .await
            .unwrap();
        let (_, page) = shape(&setup, "", &[]).await;
        // The first attempt timed out but keeps writing.
        setup.streams.append(timed(1, 0, 2, "a2")).await.unwrap();
        let (records, _) = shape(&setup, &page.cursor, &page.state).await;
        assert_eq!(records, [("a2".to_string(), key(1, 0), 0, true)]);
    }
}
