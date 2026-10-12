//! The stream calls lang makes, from the service protos down to a store.
//!
//! [Streams] resolves a stream's address to the run chain its store keys it by, checks the owner
//! where the contract says so, and mints and checks cursors. Stores only move records.

use crate::{
    CloseOutcome, ReadTarget, StreamError, StreamResult, StreamStore, WORKFLOW_OWNER_KIND,
    activity_producer_id, check_append_digest, mint_cursor,
    owner::{OwnerClient, OwnerDescription, OwnerError},
    proto::{
        AppendRequest, AppendResponse, ChainId, CloseRequest, CloseResponse, DeleteOwnerRequest,
        DeleteOwnerResponse, LatestRequest, LatestResponse, PromoteOutcome, ReadRequest,
        ReadResponse, StageRef, StoreAppendRequest, StoreLatestRequest, StreamAddress,
        StreamOwnerKind, append_request::Producer,
    },
    reader::{Attempts, ReadState, read_with},
    repair::{Decision, decide_token},
    stored_append_record, stream_hash,
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;
use tokio::time::Instant;

/// The longest a producer writes without asking whether its owner's chain ended.
pub const OWNER_RECHECK: Duration = Duration::from_secs(60);

/// Whether a repair takes a stage History couldn't decide lately as still undecided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recent {
    Trust,
    /// The owner ended or the stream closed, so History may hold the answer now.
    Ask,
}

/// How long a stage History couldn't decide is taken as still undecided without asking again.
const UNDECIDED_FOR: Duration = Duration::from_secs(5);

/// How the stream layer behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsOptions {
    /// How long a producer writes before it asks again whether its owner's chain ended. Nothing
    /// else ends the writes of a Workflow that never published itself.
    pub owner_recheck: Duration,
    /// How many producer attempts and chains to remember the last owner check of.
    pub remembered: usize,
    /// How long an idle read waits before it first asks whether its owner ended.
    pub owner_check_min: Duration,
    /// The longest an idle read waits between two such questions.
    pub owner_check_max: Duration,
}

impl Default for StreamsOptions {
    fn default() -> Self {
        Self {
            owner_recheck: OWNER_RECHECK,
            remembered: 10_000,
            owner_check_min: Duration::from_millis(500),
            owner_check_max: Duration::from_secs(5),
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
    producers: Remembered<ProducerKey>,
    chains: Remembered<OwnerKey>,
    /// The chains carried read states named that a describe showed are the Workflow's own.
    carried_chains: Remembered<OwnerKey>,
    /// Stages History couldn't decide lately, by token. Each new read would otherwise walk the
    /// staging run's History back to the floor again.
    undecided: Remembered<String>,
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
            undecided: Remembered::new(options.remembered),
            carried_chains: Remembered::new(options.remembered),
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
                    return Err(StreamError::refused("a producer needs an id"));
                }
                (named.producer_id.clone(), named.attempt)
            }
            Some(Producer::Activity(activity)) => {
                (activity_producer_id(activity), i64::from(activity.attempt))
            }
            None => return Err(StreamError::refused("an append needs its producer")),
        };
        if attempt < 1 {
            return Err(StreamError::refused(format!(
                "a producer's attempt starts at one, not {attempt}"
            )));
        }
        if request.sequence < 1 {
            return Err(StreamError::refused(format!(
                "a producer's sequence starts at one, not {}",
                request.sequence
            )));
        }
        if request.records.is_empty() {
            return Err(StreamError::refused("an append needs at least one record"));
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
    /// continued as new.
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
        let starting = state.first_run_id.is_empty();
        if starting {
            state.first_run_id = self.chain(stream).await?.first_run_id;
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
        if starting {
            self.repair(&target.chain, Recent::Trust).await;
        }
        let wait = request
            .wait
            .and_then(|wait| Duration::try_from(wait).ok())
            .unwrap_or_default();
        let deadline = Instant::now() + wait;
        let minimum = self.options.owner_check_min;
        let mut page_request = request.clone();
        // Set once the read found a stage of the chain still pending after the end, so it waits
        // for the promotion rather than reading as fast as it can.
        let mut awaiting_stage = false;
        let mut before;
        let response = loop {
            let now = unix_ms();
            if state.check_interval_ms == 0 {
                state.check_interval_ms = millis(minimum);
                state.next_check_ms = now + state.check_interval_ms;
            }
            let until_check = Duration::from_millis(state.next_check_ms.saturating_sub(now));
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = match (state.owner_ended, awaiting_stage) {
                (false, _) => remaining.min(until_check),
                (true, false) => Duration::ZERO,
                (true, true) => remaining.min(minimum),
            };
            page_request.wait = wait.try_into().ok();
            let read = read_with(&*self.store, &target, &page_request, &mut state).await?;
            before = read.before;
            let closed = read.closed;
            let mut page = read.response;
            page_request.after = page.cursor.clone();
            if !page.records.is_empty() {
                state.check_interval_ms = millis(minimum);
                state.next_check_ms = unix_ms() + state.check_interval_ms;
                break page;
            }
            if closed && !state.owner_ended {
                // One more pass after learning of the close, as after learning the owner ended,
                // with output a Worker committed but never promoted.
                state.owner_ended = true;
                self.repair(&target.chain, Recent::Ask).await;
                continue;
            }
            if state.owner_ended {
                // A close doesn't prove the chain's output is visible, since a promotion still
                // lands after it. The read ends once nothing of the chain is pending.
                if awaiting_stage {
                    self.repair(&target.chain, Recent::Trust).await;
                }
                if self.store.pending_stages(&target.chain).await?.is_empty() {
                    page.done = true;
                    break page;
                }
                awaiting_stage = true;
                if Instant::now() >= deadline {
                    break page;
                }
                continue;
            }
            if unix_ms() >= state.next_check_ms {
                state.owner_ended = self.owner_ended(stream, &target.chain).await?;
                state.check_interval_ms = (state.check_interval_ms * 2)
                    .clamp(millis(minimum), millis(self.options.owner_check_max));
                state.next_check_ms = unix_ms() + state.check_interval_ms;
                // One more pass after learning the owner ended, so a record that landed
                // between the read and the describe is delivered, and so is output a Worker
                // committed but never promoted.
                if state.owner_ended {
                    self.repair(&target.chain, Recent::Ask).await;
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

    /// Settles the stages a Worker left behind on `chain`, as History decides them. Best effort:
    /// a stage History has not decided stays for the next reader, and a failure only logs.
    async fn repair(&self, chain: &ChainId, recent: Recent) {
        if let Err(error) = self.try_repair(chain, recent).await {
            tracing::warn!(
                workflow_id = %chain.workflow_id,
                "Could not settle the pending stream stages of the Workflow: {error}"
            );
        }
    }

    async fn try_repair(&self, chain: &ChainId, recent: Recent) -> StreamResult<()> {
        let mut pending = self.store.pending_stages(chain).await?;
        let mut started: HashMap<String, Option<SystemTime>> = HashMap::new();
        for stage in &pending {
            if started.contains_key(&stage.run_id) {
                continue;
            }
            let start = match self
                .owner
                .describe(&chain.namespace, &chain.workflow_id, &stage.run_id)
                .await
            {
                Ok(described) => Some(described.start_time.unwrap_or(SystemTime::UNIX_EPOCH)),
                Err(OwnerError::NotFound(_)) => None,
                Err(error) => return Err(describe_failed(&error)),
            };
            started.insert(stage.run_id.clone(), start);
        }
        // Promotions append, so they go in commit order: by run, then by floor within the run.
        pending.sort_by(|a, b| {
            (started[&a.run_id], &a.run_id, a.history_floor_event_id).cmp(&(
                started[&b.run_id],
                &b.run_id,
                b.history_floor_event_id,
            ))
        });
        for stage in pending {
            if started[&stage.run_id].is_none() {
                // Retention may have removed the run, or a replica behind a failover may not have
                // it yet. Aborting could drop committed output, so the stage waits for its expiry
                // and holds back nothing.
                tracing::debug!(
                    token = %stage.token,
                    run_id = %stage.run_id,
                    "Leaving a stream stage whose run is not in History"
                );
                continue;
            }
            if recent == Recent::Trust
                && self
                    .undecided
                    .get(&stage.token)
                    .is_some_and(|checked| checked.at.elapsed() < UNDECIDED_FOR)
            {
                return Ok(());
            }
            let events = match self
                .owner
                .history_after(
                    &chain.namespace,
                    &chain.workflow_id,
                    &stage.run_id,
                    stage.history_floor_event_id,
                )
                .await
            {
                Ok(events) => events,
                Err(OwnerError::NotFound(_)) => continue,
                Err(error) => return Err(describe_failed(&error)),
            };
            let reference = StageRef {
                chain: Some(chain.clone()),
                token: stage.token.clone(),
                topics: stage.topics,
            };
            // A reader never holds an attempt in flight, so the same-floor rule always holds.
            let settled =
                match decide_token(&events, &stage.token, stage.history_floor_event_id, true) {
                    Decision::Promote => self.store.promote(&reference).await.map(|promoted| {
                        if promoted.outcome == PromoteOutcome::Lost as i32 {
                            tracing::warn!(
                                token = %stage.token,
                                workflow_id = %chain.workflow_id,
                                "Stream output the Workflow committed was dropped by retention \
                                 before it was promoted"
                            );
                        }
                    }),
                    Decision::Abort => self.store.abort(&reference).await,
                    // A stage History has not decided holds back the ones after it.
                    Decision::Unknown => {
                        self.undecided.put(stage.token.clone(), "", UNDECIDED_FOR);
                        return Ok(());
                    }
                };
            match settled {
                Ok(()) => {}
                // A stage the store refuses for good would hold back every later stage on every
                // read, so it is left to its expiry and the repair goes on.
                Err(error) if error.kind == crate::proto::StreamFailureKind::Refused => {
                    tracing::warn!(
                        token = %stage.token,
                        workflow_id = %chain.workflow_id,
                        "Leaving a stream stage the store refuses to settle: {error}"
                    )
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Whether the owner a read follows ended. Following the chain, a run that continued as
    /// new is not the end, and a new chain on the Workflow id is.
    async fn owner_ended(&self, stream: &StreamAddress, chain: &ChainId) -> StreamResult<bool> {
        let key = owner_key(stream);
        match self.describe(&key, &key.run_id).await {
            Err(OwnerError::NotFound(_)) => {
                // The chain came from this Workflow, so History has since dropped it, and
                // nothing more will be written for it.
                if key.run_id.is_empty() {
                    self.store.close_chain(chain).await?;
                }
                Ok(true)
            }
            Err(error) => Err(describe_failed(&error)),
            Ok(described) if !key.run_id.is_empty() => {
                Ok(described.status != WorkflowExecutionStatus::Running)
            }
            Ok(latest) if !latest.chain_ended() && latest.first_run_id == chain.first_run_id => {
                Ok(false)
            }
            Ok(_) => {
                self.store.close_chain(chain).await?;
                Ok(true)
            }
        }
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
    /// it, since the close completes every caller's operation.
    pub async fn close(&self, request: CloseRequest) -> StreamResult<CloseResponse> {
        let stream = address(request.stream.as_ref())?;
        let chain = self.chain(stream).await?;
        let outcome = match (request.result, request.failure) {
            (Some(_), Some(_)) => {
                return Err(StreamError::invalid_argument(
                    "a close carries a result or a failure, not both",
                ));
            }
            (Some(result), None) => Some(CloseOutcome::Result(result)),
            (None, Some(failure)) => Some(CloseOutcome::Failure(failure)),
            (None, None) => None,
        };
        match outcome {
            Some(outcome) => {
                self.store
                    .close_stream(&chain, &stream.topic, outcome)
                    .await?
            }
            None => self.store.close_topic(&chain, &stream.topic, None).await?,
        }
        Ok(CloseResponse {})
    }

    /// Deletes every stream of one owner, across all its chains. It asks Temporal nothing, so it
    /// works after the owner is gone. Delete only after the chain closed and its producers
    /// stopped, since a late append writes a fresh stream with no dedupe state and no close mark.
    pub async fn delete_owner(
        &self,
        request: DeleteOwnerRequest,
    ) -> StreamResult<DeleteOwnerResponse> {
        if request.owner_kind != StreamOwnerKind::Workflow as i32 {
            return Err(StreamError::unsupported(format!(
                "this release keeps streams of Workflows only, not owner kind {}",
                request.owner_kind
            )));
        }
        if request.namespace.is_empty() || request.workflow_id.is_empty() {
            return Err(StreamError::refused(
                "a delete needs the namespace and the owner's Workflow id",
            ));
        }
        self.store.delete_owner(request).await
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
    let stream = stream.ok_or_else(|| StreamError::refused("a stream call needs its stream"))?;
    if stream.owner_kind != StreamOwnerKind::Workflow as i32 {
        return Err(StreamError::unsupported(format!(
            "this release keeps streams of Workflows only, not owner kind {}",
            stream.owner_kind
        )));
    }
    if stream.namespace.is_empty() || stream.workflow_id.is_empty() {
        return Err(StreamError::refused(
            "a stream needs its namespace and its owner's Workflow id",
        ));
    }
    check_topic(&stream.topic)?;
    Ok(stream)
}

/// Refuses a topic no store keys reliably, by the rules Core's Workflow path also applies.
fn check_topic(topic: &str) -> StreamResult<()> {
    temporalio_common::streams::check_topic(topic).map_err(StreamError::refused)
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
    use crate::{
        proto::{StagedBatch, StagedRecord},
        repair::tests::{event, marker},
        stored_output_record,
    };
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload,
        enums::v1::{EventType, WorkflowExecutionStatus},
        history::v1::HistoryEvent,
    };

    /// Temporal as the tests need it: the latest run of each Workflow id, pinned runs, and the
    /// History of each run.
    #[derive(Default)]
    struct Owners {
        latest: Mutex<HashMap<String, Result<OwnerDescription, OwnerError>>>,
        runs: Mutex<HashMap<String, OwnerDescription>>,
        histories: Mutex<HashMap<String, Vec<HistoryEvent>>>,
        failing_runs: Mutex<Vec<String>>,
        described_runs: Mutex<Vec<String>>,
        describes: AtomicUsize,
        history_reads: AtomicUsize,
    }

    impl Owners {
        fn run(&self, workflow_id: &str, run_id: &str, first_run_id: &str) {
            let started = self.runs.lock().unwrap().len() as u64;
            let described = OwnerDescription {
                run_id: run_id.to_string(),
                first_run_id: first_run_id.to_string(),
                status: WorkflowExecutionStatus::Running,
                start_time: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(started)),
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
                self.described_runs.lock().unwrap().push(run_id.to_string());
                if self
                    .failing_runs
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|run| run == run_id)
                {
                    return Err(OwnerError::Failed("unavailable".to_string()));
                }
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

        async fn history_after(
            &self,
            _namespace: &str,
            _workflow_id: &str,
            run_id: &str,
            floor: i64,
        ) -> Result<Vec<HistoryEvent>, OwnerError> {
            self.history_reads.fetch_add(1, Ordering::Relaxed);
            let histories = self.histories.lock().unwrap();
            let events = histories
                .get(run_id)
                .ok_or_else(|| OwnerError::NotFound(run_id.to_string()))?;
            Ok(events
                .iter()
                .filter(|event| event.event_id > floor)
                .cloned()
                .collect())
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
            (append("", 1, "1"), StreamFailureKind::Refused),
            (
                AppendRequest {
                    producer: Some(Producer::Named(NamedProducer {
                        producer_id: "p".to_string(),
                        attempt: 0,
                    })),
                    ..append("p", 1, "1")
                },
                StreamFailureKind::Refused,
            ),
            (append("p", 0, "1"), StreamFailureKind::Refused),
            (
                AppendRequest {
                    records: Vec::new(),
                    ..append("p", 1, "1")
                },
                StreamFailureKind::Refused,
            ),
            (
                AppendRequest {
                    producer: None,
                    ..append("p", 1, "1")
                },
                StreamFailureKind::Refused,
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
                StreamFailureKind::Refused,
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
                start_time: None,
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
                result: None,
                failure: None,
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
            .status = WorkflowExecutionStatus::ContinuedAsNew;
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
            })
            .await
            .unwrap();
        let before = setup.owners.describes();
        let (delivered, _) = read_to_end(&setup.streams, Duration::from_secs(5)).await;
        assert_eq!(delivered, 1);
        assert_eq!(setup.owners.describes(), before);
    }

    /// A stage of `run_id` at `floor` that a Worker left behind, holding `value` on `out`.
    async fn left_behind(store: &MemoryStore, run_id: &str, token: &str, floor: i64, value: &str) {
        let record = data(value);
        store
            .stage(StagedBatch {
                chain: Some(chain("run-1")),
                run_id: run_id.to_string(),
                token: token.to_string(),
                history_floor_event_id: floor,
                records: vec![StagedRecord {
                    topic: "out".to_string(),
                    record: stored_output_record(
                        "out",
                        record.kind,
                        record.body,
                        &record.content_hash,
                        run_id,
                    )
                    .unwrap()
                    .encode_to_vec(),
                }],
            })
            .await
            .unwrap();
    }

    fn history(owners: &Owners, run_id: &str, events: Vec<HistoryEvent>) {
        owners
            .histories
            .lock()
            .unwrap()
            .insert(run_id.to_string(), events);
    }

    fn values(page: &ReadResponse) -> Vec<String> {
        page.records
            .iter()
            .filter_map(|record| match record.record.as_ref() {
                Some(crate::proto::read_record::Record::Stored(stored)) => {
                    Some(String::from_utf8(stored.body.clone().unwrap_or_default().data).unwrap())
                }
                _ => None,
            })
            .collect()
    }

    async fn pending(store: &MemoryStore) -> Vec<String> {
        store
            .pending_stages(&chain("run-1"))
            .await
            .unwrap()
            .into_iter()
            .map(|stage| stage.token)
            .collect()
    }

    #[tokio::test]
    async fn a_reader_promotes_output_a_stopped_worker_committed() {
        // The run finished, so no Worker will ever replay it, and only a reader finds the stage.
        let setup = setup(quick());
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        history(
            &setup.owners,
            "run-1",
            vec![
                event(4, EventType::WorkflowTaskCompleted),
                marker(5, "t1", 3),
            ],
        );
        let page = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1"]);
        assert!(pending(&setup.store).await.is_empty());
        // A second reader finds nothing left to repair and sees the record once.
        let again = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&again), ["1"]);
    }

    #[tokio::test]
    async fn rule_24_1_a_reader_aborts_a_stage_a_later_attempt_superseded() {
        // A transient attempt's failure is not in History, but the attempt that completed
        // carries every commit made at that floor.
        let setup = setup(quick());
        left_behind(&setup.store, "run-1", "orphan", 3, "0").await;
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        history(
            &setup.owners,
            "run-1",
            vec![
                event(4, EventType::WorkflowTaskCompleted),
                marker(5, "t1", 3),
            ],
        );
        let page = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1"]);
        assert!(pending(&setup.store).await.is_empty());
    }

    #[tokio::test]
    async fn rule_24_3_a_reader_repairs_pending_stages_in_commit_order() {
        let setup = setup(quick());
        setup.owners.run("wf", "run-2", "run-1");
        // A store keeps no order a reader can rely on, so the stages are left in reverse.
        left_behind(&setup.store, "run-2", "t4", 3, "4").await;
        left_behind(&setup.store, "run-1", "t3", 9, "3").await;
        left_behind(&setup.store, "run-1", "t2", 6, "2").await;
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        history(
            &setup.owners,
            "run-1",
            vec![marker(5, "t1", 3), marker(8, "t2", 6), marker(11, "t3", 9)],
        );
        history(&setup.owners, "run-2", vec![marker(5, "t4", 3)]);
        let page = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1", "2", "3", "4"]);
        // One describe per run, however many stages it left.
        let mut described = setup.owners.described_runs.lock().unwrap().clone();
        described.sort();
        assert_eq!(described, ["run-1", "run-2"]);
    }

    /// Takes every notification and sends nothing.
    struct Quiet;

    #[async_trait::async_trait]
    impl crate::NotifierClient for Quiet {
        async fn notify(&self, _: crate::StreamNotification) -> Result<(), crate::NotifyError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_reader_repairs_through_the_notifying_store() {
        // Every process with a client wraps its store, so repair must work through the wrapper.
        let setup = setup(quick());
        let notifying = Arc::new(crate::NotifyingStore::new(
            setup.store.clone(),
            Arc::new(crate::Notifier::new(Arc::new(Quiet), 10)),
        ));
        let streams = Streams::new(notifying, setup.owners.clone(), quick());
        left_behind(&setup.store, "run-1", "t2", 6, "2").await;
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        history(
            &setup.owners,
            "run-1",
            vec![marker(5, "t1", 3), marker(8, "t2", 6)],
        );
        let page = streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1", "2"]);
        assert!(pending(&setup.store).await.is_empty());
    }

    #[tokio::test]
    async fn rule_24_3_a_stage_history_has_not_decided_holds_back_the_ones_after_it() {
        let setup = setup(quick());
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        left_behind(&setup.store, "run-1", "t2", 6, "2").await;
        // A Local Activity may hold the first task open, so neither may land yet.
        history(
            &setup.owners,
            "run-1",
            vec![
                event(4, EventType::WorkflowTaskCompleted),
                marker(8, "t2", 6),
            ],
        );
        let page = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert!(page.records.is_empty());
        assert_eq!(pending(&setup.store).await.len(), 2);
    }

    #[tokio::test]
    async fn rule_24_4_a_reader_repairs_past_a_stage_whose_run_is_gone() {
        let setup = setup(quick());
        left_behind(&setup.store, "gone", "lost", 3, "0").await;
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        history(&setup.owners, "run-1", vec![marker(5, "t1", 3)]);
        let page = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1"]);
        // Not found can mean a replica behind a failover, so the stage waits for its expiry.
        assert_eq!(pending(&setup.store).await, ["lost"]);
    }

    #[tokio::test]
    async fn a_failed_repair_leaves_the_read_running() {
        let setup = setup(quick());
        setup.streams.append(append("p", 1, "1")).await.unwrap();
        left_behind(&setup.store, "run-x", "t1", 3, "0").await;
        setup
            .owners
            .failing_runs
            .lock()
            .unwrap()
            .push("run-x".to_string());
        let page = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1"]);
        assert_eq!(pending(&setup.store).await, ["t1"]);
    }

    #[tokio::test]
    async fn a_read_repairs_again_when_its_owner_ends() {
        let setup = setup(quick());
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        history(
            &setup.owners,
            "run-1",
            vec![event(4, EventType::WorkflowTaskStarted)],
        );
        let first = setup
            .streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert!(first.records.is_empty());
        // The Worker committed and stopped before promoting, then the run finished.
        history(
            &setup.owners,
            "run-1",
            vec![
                marker(5, "t1", 3),
                event(9, EventType::WorkflowExecutionCompleted),
            ],
        );
        setup.owners.end("wf", WorkflowExecutionStatus::Completed);
        let page = setup
            .streams
            .read(read_after(
                &first.cursor,
                &first.state,
                Duration::from_secs(5),
            ))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1"]);
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
        for (field, refused) in [
            ("namespace", StreamFailureKind::Refused),
            ("workflow_id", StreamFailureKind::Refused),
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
                refused,
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
                StreamFailureKind::Refused
            );
        }
        assert_eq!(setup.owners.describes(), 0);
    }

    /// A memory store that refuses for good to promote one stage.
    struct Refusing {
        inner: MemoryStore,
        token: String,
    }

    #[async_trait::async_trait]
    impl StreamStore for Refusing {
        fn name(&self) -> &str {
            self.inner.name()
        }
        async fn append(
            &self,
            r: crate::proto::StoreAppendRequest,
        ) -> StreamResult<crate::proto::StoreAppendResponse> {
            self.inner.append(r).await
        }
        async fn read(
            &self,
            r: crate::proto::StoreReadRequest,
        ) -> StreamResult<crate::proto::StoreReadResponse> {
            self.inner.read(r).await
        }
        async fn latest(
            &self,
            r: crate::proto::StoreLatestRequest,
        ) -> StreamResult<crate::proto::StoreLatestResponse> {
            self.inner.latest(r).await
        }
        async fn record_at(&self, c: &ChainId, t: &str, p: &str) -> StreamResult<Option<Vec<u8>>> {
            self.inner.record_at(c, t, p).await
        }
        async fn stage(&self, b: StagedBatch) -> StreamResult<()> {
            self.inner.stage(b).await
        }
        async fn promote(&self, s: &StageRef) -> StreamResult<crate::proto::PromoteResult> {
            if s.token == self.token {
                return Err(StreamError::refused(
                    "the stage names a topic the store can't key",
                ));
            }
            self.inner.promote(s).await
        }
        async fn abort(&self, s: &StageRef) -> StreamResult<()> {
            self.inner.abort(s).await
        }
        async fn close_chain(&self, c: &ChainId) -> StreamResult<()> {
            self.inner.close_chain(c).await
        }
        async fn close_topic(
            &self,
            c: &ChainId,
            t: &str,
            result: Option<crate::proto::Payload>,
        ) -> StreamResult<()> {
            self.inner.close_topic(c, t, result).await
        }
        async fn pending_stages(
            &self,
            c: &ChainId,
        ) -> StreamResult<Vec<crate::proto::PendingStage>> {
            self.inner.pending_stages(c).await
        }
        async fn delete_owner(
            &self,
            r: crate::proto::DeleteOwnerRequest,
        ) -> StreamResult<crate::proto::DeleteOwnerResponse> {
            self.inner.delete_owner(r).await
        }
    }

    #[tokio::test]
    async fn rule_12_2_repair_goes_past_a_stage_the_store_refuses_for_good() {
        let store = Arc::new(Refusing {
            inner: MemoryStore::new(),
            token: "bad".to_string(),
        });
        let owners = Arc::new(Owners::default());
        owners.run("wf", "run-1", "run-1");
        let streams = Streams::new(store.clone(), owners.clone(), quick());
        left_behind(&store.inner, "run-1", "bad", 3, "0").await;
        left_behind(&store.inner, "run-1", "t1", 6, "1").await;
        history(
            &owners,
            "run-1",
            vec![marker(5, "bad", 3), marker(8, "t1", 6)],
        );
        let page = streams
            .read(read_after("", &[], Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(values(&page), ["1"]);
        let left: Vec<_> = store
            .inner
            .pending_stages(&chain("run-1"))
            .await
            .unwrap()
            .into_iter()
            .map(|stage| stage.token)
            .collect();
        assert_eq!(left, ["bad"]);
    }

    #[tokio::test]
    async fn rule_12_3_an_undecided_stage_is_not_read_again_by_every_new_reader() {
        // A long Local Activity holds the task open, so History can't decide the stage yet.
        let setup = setup(quick());
        left_behind(&setup.store, "run-1", "t1", 3, "1").await;
        history(
            &setup.owners,
            "run-1",
            vec![event(4, EventType::WorkflowTaskStarted)],
        );
        for _ in 0..3 {
            setup
                .streams
                .read(read_after("", &[], Duration::ZERO))
                .await
                .unwrap();
        }
        assert_eq!(setup.owners.history_reads.load(Ordering::Relaxed), 1);
    }
}
