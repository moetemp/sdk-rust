//! Reading a stream: the producer attempts a read observes, and what it carries between calls.
//!
//! An Activity that streams half an answer and then fails leaves those records in the stream, and
//! its retry writes different ones. No store can undo the first half, so the reader is told that a
//! new attempt began and the application decides what to do. This runs over records the read
//! already observed, so it costs no round trip and every store reports a retry the same way.

use crate::{
    BEGINNING, END, StreamError, StreamResult, StreamStore, cursor_position, mint_cursor,
    progress_counter,
    proto::{
        ChainId, ReadRecord, ReadRequest, ReadResponse, StoreLatestRequest, StoreReadRequest,
        StoredClose, StreamRecord, StreamRecordKind, Supersession, read_record,
    },
};
use prost::Message;
use std::collections::HashMap;

/// The low bits of a Workflow producer's attempt, which count the reruns of one task. The high
/// bits name the last task that completed before it.
const WORKFLOW_RETRY_BITS: u32 = 16;

/// Whether a producer's `attempt` withdraws what its `previous` attempt wrote. A Workflow
/// producer (`@<run id>`) moves to a higher attempt with each completed task, and only a rerun
/// of the same task withdraws. Any other producer's higher attempt is a retry.
fn supersedes(producer_id: &str, previous: i64, attempt: i64) -> bool {
    !producer_id.starts_with('@')
        || previous >> WORKFLOW_RETRY_BITS == attempt >> WORKFLOW_RETRY_BITS
}

/// How many producers a read remembers the newest attempt of.
pub const MAX_TRACKED_PRODUCERS: usize = 1_000;

/// The most bytes the state a read carries takes. It crosses to lang on every call, and a
/// Workflow reader may keep it in History.
pub const MAX_STATE_BYTES: usize = 16 << 10;

/// How many records one call returns when the request leaves it to Core.
pub const DEFAULT_MAX_RECORDS: u32 = 100;

/// One stream, resolved to what its store keys it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadTarget {
    /// The run chain that owns the stream.
    pub chain: ChainId,
    /// The topic within the chain.
    pub topic: String,
    /// The stream hash its cursors carry, from [crate::stream_hash].
    pub stream_hash: String,
}

/// What a read carries from one call to the next. Lang passes it back as opaque bytes.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct ReadState {
    /// What the read knows of each producer, by producer id. One entry holds the attempt and the
    /// last sight, so a long producer id is stored once.
    #[prost(map = "string, message", tag = "1")]
    producers: HashMap<String, Tracked>,
    /// Set by the first call, so later calls don't look up the resume record again.
    #[prost(bool, tag = "2")]
    pub(crate) started: bool,
    /// The first run of the chain the read follows, so later calls don't resolve it again.
    #[prost(string, tag = "3")]
    pub(crate) first_run_id: String,
    /// Set once the read learned its owner ended, so it only drains what is left.
    #[prost(bool, tag = "4")]
    pub(crate) owner_ended: bool,
    /// How long the read waits between owner checks while nothing arrives.
    #[prost(uint64, tag = "5")]
    pub(crate) check_interval_ms: u64,
    /// When the next owner check is due, in milliseconds since the Unix epoch.
    #[prost(uint64, tag = "6")]
    pub(crate) next_check_ms: u64,
    /// The topic's newest position ever when a read that has delivered nothing started. Such a
    /// read has no position, so a record retention dropped after this mark was lost to it.
    #[prost(string, tag = "7")]
    watermark: String,
    #[prost(bool, tag = "8")]
    watermark_known: bool,
    #[prost(uint64, tag = "10")]
    clock: u64,
    /// When the read first found no owner, in milliseconds since the Unix epoch, or 0 once it
    /// found one. The owner start grace counts from it.
    #[prost(uint64, tag = "11")]
    pub(crate) owner_missing_since_ms: u64,
}

/// What a read knows of one producer.
#[derive(Clone, PartialEq, Message)]
struct Tracked {
    /// The newest attempt this read delivered.
    #[prost(int64, tag = "1")]
    attempt: i64,
    /// When this read last saw the producer, on the read's own clock, so the least recently
    /// seen one is forgotten first.
    #[prost(uint64, tag = "2")]
    seen: u64,
}

/// The producer attempts a read knows at one point of a page, so an answer cut short can hand back
/// the state as it stood after the records it kept.
#[derive(Debug, Clone, Default)]
pub(crate) struct Attempts {
    producers: HashMap<String, Tracked>,
    clock: u64,
}

/// One page as [read_with] answers it.
#[derive(Debug)]
pub(crate) struct Page {
    /// The records, with the answer's `state` left empty.
    pub(crate) response: ReadResponse,
    /// The store marked the chain or the topic closed.
    pub(crate) closed: bool,
    /// The topic's first close, when the store keeps one.
    pub(crate) close: Option<StoredClose>,
    /// The attempts the read knew before the page's first record.
    pub(crate) before: Attempts,
}

/// What one record tells the read about its producer's attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Observed {
    /// Nothing new: no producer attempt to compare, or the attempt the read already knows.
    Known,
    /// The producer's first attempt this read sees, or a newer one. A newer one carries the
    /// supersession to deliver before the record.
    Newer(Option<Supersession>),
    /// An older attempt wrote after this read delivered a newer one.
    Behind {
        /// The newest attempt the read delivered.
        newest: i64,
    },
}

impl ReadState {
    /// The state a caller passed back. One with more producers than a read remembers is no state
    /// Core returned, and trimming it would cost time on the caller's behalf.
    pub(crate) fn decode_from(state: &[u8]) -> StreamResult<Self> {
        let not_returned = |why: String| {
            StreamError::cursor(format!(
                "the read state is not one an earlier call of this read returned: {why}"
            ))
        };
        let state = Self::decode(state).map_err(|error| not_returned(error.to_string()))?;
        let producers = state.producers.len();
        if producers > MAX_TRACKED_PRODUCERS {
            return Err(not_returned(format!(
                "it names {producers} producers, above the {MAX_TRACKED_PRODUCERS} a read remembers"
            )));
        }
        Ok(state)
    }

    /// A first call's state that knows only the chain the read follows.
    pub(crate) fn of_chain(first_run_id: String) -> Self {
        Self {
            first_run_id,
            ..Self::default()
        }
    }

    /// The producers, least recently seen first.
    fn by_last_seen(&self) -> Vec<String> {
        let mut producers: Vec<(u64, &String)> = self
            .producers
            .iter()
            .map(|(producer, tracked)| (tracked.seen, producer))
            .collect();
        producers.sort_unstable();
        producers
            .into_iter()
            .map(|(_, producer)| producer.clone())
            .collect()
    }

    /// Keeps the state small however many producers a long chain had, since it crosses to lang
    /// on every call.
    fn forget_beyond(&mut self, limit: usize) {
        if self.producers.len() > limit {
            let excess = self.producers.len() - limit;
            for producer in self.by_last_seen().into_iter().take(excess) {
                self.producers.remove(&producer);
            }
        }
    }

    pub(crate) fn attempts(&self) -> Attempts {
        Attempts {
            producers: self.producers.clone(),
            clock: self.clock,
        }
    }

    /// The state as it stood after `kept`, the first records of a page that began with `before`.
    /// The rest of the state is this one's, since it doesn't depend on how far the page got.
    pub(crate) fn at_cut<'a>(
        &self,
        before: &Attempts,
        kept: impl IntoIterator<Item = &'a StreamRecord>,
    ) -> ReadState {
        let mut state = ReadState {
            producers: before.producers.clone(),
            clock: before.clock,
            ..self.clone()
        };
        for record in kept {
            state.observe(record);
        }
        state
    }

    /// Forgets the least recently seen producers until the encoded state takes at most `bytes`.
    /// A forgotten producer's next attempt reads without `SUPERSEDED`, as past the producer cap.
    pub(crate) fn fit_within(&mut self, bytes: usize) {
        let mut size = self.encoded_len();
        if size <= bytes {
            return;
        }
        for producer in self.by_last_seen() {
            // Map entries encode as repeated fields, so one producer's bytes are those of a
            // state that holds only that producer.
            let tracked = self.producers.remove(&producer).unwrap_or_default();
            size -= ReadState {
                producers: HashMap::from([(producer, tracked)]),
                ..ReadState::default()
            }
            .encoded_len();
            if size <= bytes {
                break;
            }
        }
    }

    fn observe(&mut self, record: &StreamRecord) -> Observed {
        // A producer that declares no attempt has no generation to compare.
        if record.producer_id.is_empty() || record.attempt <= 0 {
            return Observed::Known;
        }
        self.clock += 1;
        let tracked = self
            .producers
            .entry(record.producer_id.clone())
            .or_default();
        tracked.seen = self.clock;
        let seen = tracked.attempt;
        if record.attempt < seen {
            return Observed::Behind { newest: seen };
        }
        if record.attempt == seen {
            return Observed::Known;
        }
        tracked.attempt = record.attempt;
        self.forget_beyond(MAX_TRACKED_PRODUCERS);
        let superseded = seen > 0 && supersedes(&record.producer_id, seen, record.attempt);
        Observed::Newer(superseded.then(|| Supersession {
            topic: record.topic.clone(),
            producer_id: record.producer_id.clone(),
            previous_attempt: seen,
            attempt: record.attempt,
        }))
    }
}

/// Reads one page of a stream from `store`, as the `Read` call of `StreamService` answers it.
///
/// The read turns stored records into what lang delivers. A producer's newer attempt gets a
/// `SUPERSEDED` record before it, at the cursor of the record delivered before it, so a read that
/// resumes there reports the supersession again. A Workflow producer's attempt for a later task
/// gets none, since only a rerun of the same task withdraws records. A record from an attempt below the newest the
/// read delivered is still delivered, marked `stale`. The attempts seen ride in `state`, so this
/// holds across calls. A read that starts from a cursor without state looks up the record at the
/// cursor first, since that record was delivered before. Only that record's producer is known
/// then, and another producer's earlier attempts are not. A read remembers at most
/// [MAX_TRACKED_PRODUCERS] producers in at most [MAX_STATE_BYTES], and the next attempt of one it
/// forgot reads without `SUPERSEDED`.
///
/// A record that does not parse fails the call as `RECORD` with its cursor, so the caller resumes
/// past it on purpose. Skipping it would lose data no one hears of. The records before it in the
/// same page are returned first, and the next call fails.
pub async fn read_page(
    store: &dyn StreamStore,
    target: &ReadTarget,
    request: &ReadRequest,
) -> StreamResult<ReadResponse> {
    let mut state = ReadState::decode_from(&request.state)?;
    let mut response = read_with(store, target, request, &mut state)
        .await?
        .response;
    response.state = state.encode_to_vec();
    Ok(response)
}

/// [read_page] with the state already decoded, for a caller that keeps fields of its own in it.
/// The answer's `state` is left empty. Also answers whether the store marked the stream closed,
/// and the topic's first close when it was closed as a topic.
pub(crate) async fn read_with(
    store: &dyn StreamStore,
    target: &ReadTarget,
    request: &ReadRequest,
    state: &mut ReadState,
) -> StreamResult<Page> {
    let from_end = request.after == END;
    let position = if from_end {
        store
            .latest(StoreLatestRequest {
                chain: Some(target.chain.clone()),
                topic: target.topic.clone(),
            })
            .await?
            .position
    } else {
        cursor_position(&request.after, store.name(), &target.stream_hash)?
            .unwrap_or_default()
            .to_string()
    };
    let start = if position.is_empty() {
        BEGINNING.to_string()
    } else {
        mint_cursor(store.name(), &target.stream_hash, &position)
    };
    if !state.started && !from_end && !position.is_empty() {
        let at_cursor = store
            .record_at(&target.chain, &target.topic, &position)
            .await?;
        // A record the read can't parse was raised to the caller, who chose to resume past it.
        if let Some(record) = at_cursor.and_then(|bytes| StreamRecord::decode(&*bytes).ok()) {
            state.observe(&record);
        }
    }
    state.started = true;
    if position.is_empty() {
        if !state.watermark_known {
            state.watermark = store.newest_ever(&target.chain, &target.topic).await?;
            state.watermark_known = true;
        } else if let Some(dropped) = store
            .dropped_after(&target.chain, &target.topic, &state.watermark)
            .await?
        {
            return Err(StreamError::expired(format!(
                "records on topic {:?} were dropped by retention while this read was behind \
                 them, before it delivered any; the newest dropped one is {dropped}",
                target.topic
            )));
        }
    }

    let page = store
        .read(StoreReadRequest {
            chain: Some(target.chain.clone()),
            topic: target.topic.clone(),
            after_position: position.clone(),
            wait: request.wait,
            max_records: match request.max_records {
                0 => DEFAULT_MAX_RECORDS,
                max => max,
            },
        })
        .await?;
    let (closed, close) = (page.closed, page.close);
    let before = state.attempts();
    let mut records = Vec::with_capacity(page.records.len());
    let mut previous = start.clone();
    for stored in page.records {
        let cursor = mint_cursor(store.name(), &target.stream_hash, &stored.position);
        let record = match parse(&stored.record) {
            Ok(record) => record,
            Err(error) if records.is_empty() => {
                return Err(StreamError::record(
                    cursor.clone(),
                    format!("stream record at {cursor} could not be decoded: {error}"),
                ));
            }
            Err(_) => break,
        };
        let stale = match state.observe(&record) {
            Observed::Known => false,
            Observed::Newer(superseded) => {
                if let Some(superseded) = superseded {
                    records.push(ReadRecord {
                        cursor: previous.clone(),
                        record: Some(read_record::Record::Superseded(superseded)),
                        stale: false,
                    });
                }
                false
            }
            Observed::Behind { newest } => {
                tracing::warn!("{}", behind_message(&record, &previous, newest));
                true
            }
        };
        records.push(ReadRecord {
            cursor: cursor.clone(),
            record: Some(read_record::Record::Stored(record)),
            stale,
        });
        previous = cursor;
    }
    state.fit_within(MAX_STATE_BYTES);
    Ok(Page {
        response: ReadResponse {
            cursor: previous,
            state: Vec::new(),
            done: false,
            // An empty answer found nothing after `position`, so a notification counter above
            // that position's counter can only come from a newer record.
            wake_after: if records.is_empty() {
                progress_counter(&position).unwrap_or(0)
            } else {
                0
            },
            records_digest: records_digest(&records),
            records,
            chain_first_run_id: target.chain.first_run_id.clone(),
            start_cursor: start.clone(),
            ..Default::default()
        },
        closed,
        close,
        before,
    })
}

/// The digest of a read's answer that a replay read checks (RR-1). It covers what a reader acts
/// on and leaves out the encoded body, so a codec change keeps it. Each field has a length
/// prefix, so two answers can't give the same bytes.
pub(crate) fn records_digest(records: &[ReadRecord]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    let mut field = |bytes: &[u8]| {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    };
    for record in records {
        field(record.cursor.as_bytes());
        field(&[u8::from(record.stale)]);
        match &record.record {
            Some(read_record::Record::Stored(stored)) => {
                field(b"stored");
                field(&stored.kind.to_be_bytes());
                field(stored.producer_id.as_bytes());
                field(&stored.attempt.to_be_bytes());
                field(&stored.sequence.to_be_bytes());
                field(
                    stored
                        .metadata
                        .get(temporalio_common::streams::CONTENT_HASH_KEY)
                        .map_or(&[][..], |hash| hash.data.as_slice()),
                );
            }
            Some(read_record::Record::Superseded(superseded)) => {
                field(b"superseded");
                field(superseded.producer_id.as_bytes());
                field(&superseded.previous_attempt.to_be_bytes());
                field(&superseded.attempt.to_be_bytes());
            }
            None => field(b"none"),
        }
    }
    digest.finalize().to_vec()
}

/// A stored record as a reader delivers it. Fails on bytes that are no record, and on a kind no
/// writer stores, such as the `SUPERSEDED` that only readers synthesize.
///
/// Empty bytes are no record either. Every stored record names its topic, so it is never empty,
/// and a store hands over an entry it holds without a record as empty bytes.
fn parse(bytes: &[u8]) -> Result<StreamRecord, String> {
    if bytes.is_empty() {
        return Err("the entry holds no stream record".to_string());
    }
    let mut record = StreamRecord::decode(bytes).map_err(|error| error.to_string())?;
    match StreamRecordKind::try_from(record.kind) {
        Ok(StreamRecordKind::Data | StreamRecordKind::Finish) => {}
        Ok(StreamRecordKind::Unspecified) => record.kind = StreamRecordKind::Data as i32,
        Err(_) => return Err(format!("no writer stores a record of kind {}", record.kind)),
    }
    Ok(record)
}

/// Why a record from an older attempt is suspect. A lower attempt after a higher one means an
/// older attempt kept writing after a newer one started, such as an Activity attempt that timed
/// out but kept running. A consumer that reads it as the current answer shows a stale one.
fn behind_message(record: &StreamRecord, previous: &str, newest: i64) -> String {
    format!(
        "stream record on {:?} after {previous:?} is from attempt {} of producer {:?}, behind \
         attempt {newest}, which this reader already delivered: an older attempt wrote after a \
         newer one started",
        record.topic, record.attempt, record.producer_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::{
            DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
            StoreLatestResponse, StoreReadResponse, StoredRecord, StreamFailureKind,
        },
        stream_hash,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    /// One topic of positions `1`, `2` and so on, so the read logic is tested without a store.
    #[derive(Default)]
    struct Log {
        entries: Mutex<Vec<Vec<u8>>>,
        lookups: AtomicUsize,
    }

    impl Log {
        fn push(&self, bytes: Vec<u8>) -> String {
            let mut entries = self.entries.lock().unwrap();
            entries.push(bytes);
            entries.len().to_string()
        }

        fn write(&self, producer: &str, attempt: i64, data: &[u8]) -> String {
            self.push(data_record(producer, attempt, data).encode_to_vec())
        }
    }

    fn data_record(producer: &str, attempt: i64, data: &[u8]) -> StreamRecord {
        StreamRecord {
            topic: "out".to_string(),
            kind: StreamRecordKind::Data as i32,
            producer_id: producer.to_string(),
            attempt,
            sequence: 1,
            body: Some(Payload {
                data: data.to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[async_trait::async_trait]
    impl StreamStore for Log {
        fn name(&self) -> &str {
            "log"
        }

        async fn append(&self, _: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
            unreachable!()
        }

        async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
            let after: usize = request.after_position.parse().unwrap_or(0);
            let entries = self.entries.lock().unwrap();
            let records = entries
                .iter()
                .enumerate()
                .skip(after)
                .take(request.max_records as usize)
                .map(|(index, bytes)| StoredRecord {
                    position: (index + 1).to_string(),
                    record: bytes.clone(),
                })
                .collect();
            Ok(StoreReadResponse {
                records,
                ..Default::default()
            })
        }

        async fn latest(&self, _: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
            let entries = self.entries.lock().unwrap();
            Ok(StoreLatestResponse {
                position: match entries.len() {
                    0 => String::new(),
                    len => len.to_string(),
                },
            })
        }

        async fn record_at(
            &self,
            _: &ChainId,
            _: &str,
            position: &str,
        ) -> StreamResult<Option<Vec<u8>>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            let index: usize = position.parse().unwrap();
            Ok(self.entries.lock().unwrap().get(index - 1).cloned())
        }

        async fn close_chain(&self, _: &ChainId) -> StreamResult<()> {
            unreachable!()
        }

        async fn close_topic(&self, _: &ChainId, _: &str) -> StreamResult<()> {
            unreachable!()
        }

        async fn retire_producer(&self, _: &ChainId, _: &str, _: &str, _: i64) -> StreamResult<()> {
            unreachable!()
        }

        async fn delete_owner(&self, _: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
            unreachable!()
        }
    }

    fn target() -> ReadTarget {
        ReadTarget {
            chain: ChainId {
                namespace: "ns".to_string(),
                workflow_id: "wf".to_string(),
                first_run_id: "run-1".to_string(),
            },
            topic: "out".to_string(),
            stream_hash: stream_hash("ns", "workflow", "wf", "out"),
        }
    }

    fn cursor(position: &str) -> String {
        mint_cursor("log", &target().stream_hash, position)
    }

    async fn read(log: &Log, after: &str, state: &[u8]) -> StreamResult<ReadResponse> {
        read_page(
            log,
            &target(),
            &ReadRequest {
                after: after.to_string(),
                state: state.to_vec(),
                ..Default::default()
            },
        )
        .await
    }

    /// Each record as `(cursor, kind, attempt, stale)`, with kind 3 for a supersession.
    fn shape(response: &ReadResponse) -> Vec<(String, i32, i64, bool)> {
        response
            .records
            .iter()
            .map(|record| match record.record.as_ref().unwrap() {
                read_record::Record::Stored(stored) => (
                    record.cursor.clone(),
                    stored.kind,
                    stored.attempt,
                    record.stale,
                ),
                read_record::Record::Superseded(superseded) => {
                    (record.cursor.clone(), 3, superseded.attempt, record.stale)
                }
            })
            .collect()
    }

    fn superseded(response: &ReadResponse, index: usize) -> &Supersession {
        match response.records[index].record.as_ref().unwrap() {
            read_record::Record::Superseded(superseded) => superseded,
            other => panic!("not a supersession: {other:?}"),
        }
    }

    const DATA: i32 = StreamRecordKind::Data as i32;
    const SUPERSEDED: i32 = 3;

    fn observe(state: &mut ReadState, producer: &str, attempt: i64) -> Observed {
        state.observe(&data_record(producer, attempt, b""))
    }

    #[test]
    fn supersession_is_synthesized_from_observations() {
        let mut state = ReadState::default();
        assert_eq!(observe(&mut state, "model", 1), Observed::Newer(None));
        assert_eq!(observe(&mut state, "model", 1), Observed::Known);
        assert_eq!(
            observe(&mut state, "model", 2),
            Observed::Newer(Some(Supersession {
                topic: "out".to_string(),
                producer_id: "model".to_string(),
                previous_attempt: 1,
                attempt: 2,
            }))
        );
        assert_eq!(observe(&mut state, "model", 2), Observed::Known);
        // Without an id or an attempt there is no generation to compare.
        assert_eq!(observe(&mut state, "", 5), Observed::Known);
        assert_eq!(observe(&mut state, "other", 0), Observed::Known);
        assert_eq!(state.producers.len(), 1);
        assert_eq!(state.producers["model"].attempt, 2);
    }

    #[test]
    fn an_attempt_that_goes_backwards_is_reported() {
        let mut state = ReadState::default();
        observe(&mut state, "model", 2);
        assert_eq!(
            observe(&mut state, "model", 1),
            Observed::Behind { newest: 2 }
        );
        // The newest attempt stays, so a later record of attempt 2 is not new.
        assert_eq!(observe(&mut state, "model", 2), Observed::Known);
        let message = behind_message(&data_record("model", 1, b""), "log:x:3", 2);
        assert!(message.contains("attempt 1"), "{message}");
        assert!(message.contains("behind attempt 2"), "{message}");
        assert!(message.contains("\"model\""), "{message}");
    }

    #[test]
    fn rule_6_1_a_read_forgets_the_producer_it_saw_least_recently() {
        let mut state = ReadState::default();
        for producer in 0..MAX_TRACKED_PRODUCERS {
            observe(&mut state, &format!("p{producer}"), 1);
        }
        // Seen again, so `p0` is now the most recent and `p1` the least.
        observe(&mut state, "p0", 1);
        observe(&mut state, "new", 1);
        assert_eq!(state.producers.len(), MAX_TRACKED_PRODUCERS);
        assert!(state.producers.contains_key("p0"));
        assert!(!state.producers.contains_key("p1"));
        // A forgotten producer's next attempt reads as its first, without `SUPERSEDED`.
        assert_eq!(observe(&mut state, "p1", 2), Observed::Newer(None));
        assert!(matches!(
            observe(&mut state, "p0", 2),
            Observed::Newer(Some(_))
        ));
        // The bound holds across calls, since it rides in the state.
        let state = ReadState::decode_from(&state.encode_to_vec()).unwrap();
        assert_eq!(state.producers.len(), MAX_TRACKED_PRODUCERS);
    }

    /// A producer id as a Workflow's per-call Local Activity writes as.
    fn local_call(number: usize) -> String {
        format!("__temporal_local_{number}@0b0e5bd5-1c4b-4f4e-9d1e-6f3b7a8c9d0e")
    }

    #[test]
    fn a_producer_costs_its_id_once() {
        let mut state = ReadState::default();
        observe(&mut state, &local_call(0), 1_234_567_890_123);
        let one = state.encoded_len();
        // The id, the attempt and the last sight, with a few bytes of framing.
        assert!(one <= local_call(0).len() + 24, "{one}");
    }

    #[test]
    fn a_state_fits_its_budget_by_forgetting_the_least_recently_seen() {
        let mut state = ReadState::default();
        for producer in 0..MAX_TRACKED_PRODUCERS {
            observe(&mut state, &format!("{producer:0200}"), 1);
        }
        let started = std::time::Instant::now();
        state.fit_within(MAX_STATE_BYTES);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        let size = state.encoded_len();
        assert!(size <= MAX_STATE_BYTES, "{size}");
        // It forgot no more than it had to.
        let one = size / state.producers.len();
        assert!(size + one > MAX_STATE_BYTES, "{size}");
        let oldest_kept = MAX_TRACKED_PRODUCERS - state.producers.len();
        assert!(state.producers.contains_key(&format!("{oldest_kept:0200}")));
        assert!(
            !state
                .producers
                .contains_key(&format!("{:0200}", oldest_kept - 1))
        );
    }

    #[tokio::test]
    async fn a_read_hands_back_a_state_within_its_budget() {
        let log = Log::default();
        let mut state = Vec::new();
        let mut after = BEGINNING.to_string();
        // A Workflow that flushes as a new Local Activity call each time.
        for call in 0..MAX_TRACKED_PRODUCERS {
            log.write(&local_call(call), 1_234_567_890_123, b"x");
            if call % 100 == 99 {
                let page = read(&log, &after, &state).await.unwrap();
                assert!(page.state.len() <= MAX_STATE_BYTES, "{}", page.state.len());
                (after, state) = (page.cursor, page.state);
            }
        }
        let kept = ReadState::decode_from(&state).unwrap();
        assert!(
            kept.producers
                .contains_key(&local_call(MAX_TRACKED_PRODUCERS - 1))
        );
        assert!(!kept.producers.contains_key(&local_call(0)));
    }

    #[test]
    fn a_state_with_more_producers_than_a_read_remembers_is_refused() {
        let mut state = ReadState::default();
        for producer in 0..=MAX_TRACKED_PRODUCERS {
            state
                .producers
                .insert(format!("p{producer}"), Tracked::default());
        }
        let error = ReadState::decode_from(&state.encode_to_vec()).unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
    }

    #[test]
    fn a_workflow_producer_supersedes_only_within_one_task() {
        let task = |prefix: i64, retry: i64| (prefix << 16) | retry;
        let mut state = ReadState::default();
        assert_eq!(
            observe(&mut state, "@run", task(0, 1)),
            Observed::Newer(None)
        );
        // A completed task moves the prefix, so the next task goes on with no supersession.
        assert_eq!(
            observe(&mut state, "@run", task(3, 1)),
            Observed::Newer(None)
        );
        assert_eq!(
            observe(&mut state, "@run", task(3, 2)),
            Observed::Newer(Some(Supersession {
                topic: "out".to_string(),
                producer_id: "@run".to_string(),
                previous_attempt: task(3, 1),
                attempt: task(3, 2),
            }))
        );
        assert_eq!(
            observe(&mut state, "@run", task(9, 1)),
            Observed::Newer(None)
        );
        // An earlier task's record after a later one is still behind.
        assert_eq!(
            observe(&mut state, "@run", task(3, 2)),
            Observed::Behind { newest: task(9, 1) }
        );
        // Any other producer's higher attempt is a retry, whatever its bits hold.
        observe(&mut state, "model", task(0, 1));
        assert!(matches!(
            observe(&mut state, "model", task(3, 1)),
            Observed::Newer(Some(_))
        ));
    }

    #[test]
    fn a_repeat_of_the_current_attempt_is_not_reported() {
        let mut state = ReadState::default();
        observe(&mut state, "model", 1);
        assert_eq!(observe(&mut state, "model", 1), Observed::Known);
    }

    #[tokio::test]
    async fn a_new_attempt_supersedes_the_old_one() {
        let log = Log::default();
        log.write("model", 1, b"1");
        log.write("model", 2, b"2");
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        // The supersession sits at the cursor of the old attempt's last record.
        assert_eq!(
            shape(&read),
            [
                (cursor("1"), DATA, 1, false),
                (cursor("1"), SUPERSEDED, 2, false),
                (cursor("2"), DATA, 2, false),
            ]
        );
        assert_eq!(
            superseded(&read, 1),
            &Supersession {
                topic: "out".to_string(),
                producer_id: "model".to_string(),
                previous_attempt: 1,
                attempt: 2,
            }
        );
        assert_eq!(read.cursor, cursor("2"));
    }

    #[tokio::test]
    async fn rule_5_1_a_read_resumed_at_a_supersession_reports_it_again() {
        let log = Log::default();
        log.write("model", 1, b"1");
        log.write("model", 2, b"2");
        let resumed = read(&log, &cursor("1"), &[]).await.unwrap();
        assert_eq!(
            shape(&resumed),
            [
                (cursor("1"), SUPERSEDED, 2, false),
                (cursor("2"), DATA, 2, false),
            ]
        );
    }

    #[tokio::test]
    async fn rule_5_1_a_read_resumed_before_a_new_attempt_reports_it() {
        let log = Log::default();
        log.write("model", 1, b"1");
        let delivered = log.write("model", 1, b"2");
        // The reader stops here, then the producer's retry writes.
        log.write("model", 2, b"3");
        let resumed = read(&log, &cursor(&delivered), &[]).await.unwrap();
        assert_eq!(
            shape(&resumed),
            [
                (cursor("2"), SUPERSEDED, 2, false),
                (cursor("3"), DATA, 2, false),
            ]
        );
        assert_eq!(superseded(&resumed, 0).previous_attempt, 1);
    }

    #[tokio::test]
    async fn rule_5_1_only_the_producer_at_the_resume_cursor_is_known() {
        let log = Log::default();
        log.write("a", 1, b"1");
        log.write("b", 1, b"2");
        log.write("a", 2, b"3");
        let resumed = read(&log, &cursor("2"), &[]).await.unwrap();
        assert_eq!(shape(&resumed), [(cursor("3"), DATA, 2, false)]);
    }

    #[tokio::test]
    async fn rule_5_1_the_state_carries_attempts_across_calls() {
        let log = Log::default();
        log.write("a", 1, b"1");
        log.write("b", 1, b"2");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        log.write("a", 2, b"3");
        let next = read(&log, &first.cursor, &first.state).await.unwrap();
        // Producer `a` is not at the cursor, so only the state knows its first attempt.
        assert_eq!(
            shape(&next),
            [
                (cursor("2"), SUPERSEDED, 2, false),
                (cursor("3"), DATA, 2, false),
            ]
        );
        // A read with state never looks up the record at its cursor again.
        assert_eq!(log.lookups.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_read_that_resumes_looks_up_its_cursor_once() {
        let log = Log::default();
        log.write("a", 1, b"1");
        let first = read(&log, &cursor("1"), &[]).await.unwrap();
        assert!(first.records.is_empty());
        assert_eq!(first.cursor, cursor("1"));
        read(&log, &first.cursor, &first.state).await.unwrap();
        assert_eq!(log.lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn rule_11_1_a_record_from_an_older_attempt_is_marked_stale() {
        let log = Log::default();
        log.write("model", 2, b"2");
        log.write("model", 1, b"1");
        log.write("model", 2, b"3");
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        // Still delivered, because dropping it would hide what the store holds, but marked, so
        // the consumer can keep it out of the current answer.
        assert_eq!(
            shape(&read),
            [
                (cursor("1"), DATA, 2, false),
                (cursor("2"), DATA, 1, true),
                (cursor("3"), DATA, 2, false),
            ]
        );
    }

    #[tokio::test]
    async fn rule_11_1_a_stale_record_stays_stale_after_a_resume() {
        let log = Log::default();
        log.write("model", 2, b"2");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        log.write("model", 1, b"1");
        let next = read(&log, &first.cursor, &first.state).await.unwrap();
        assert_eq!(shape(&next), [(cursor("2"), DATA, 1, true)]);
    }

    #[tokio::test]
    async fn rule_5_2_a_record_that_does_not_parse_fails_with_its_cursor() {
        let log = Log::default();
        let bad = log.push(b"\xff not a record".to_vec());
        log.write("p", 1, b"7");
        let error = read(&log, BEGINNING, &[]).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        assert_eq!(error.cursor, Some(cursor(&bad)));
        assert!(error.message.contains(&cursor(&bad)), "{error}");
        // Resuming past it is the caller's choice, and the next record reads.
        let after = read(&log, &cursor(&bad), &[]).await.unwrap();
        assert_eq!(shape(&after), [(cursor("2"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn rule_5_2_records_before_a_bad_one_are_delivered_first() {
        let log = Log::default();
        log.write("p", 1, b"1");
        let bad = log.push(b"\xff".to_vec());
        log.write("p", 1, b"3");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(shape(&first), [(cursor("1"), DATA, 1, false)]);
        let error = read(&log, &first.cursor, &first.state).await.unwrap_err();
        assert_eq!(error.cursor, Some(cursor(&bad)));
        let after = read(&log, &cursor(&bad), &first.state).await.unwrap();
        assert_eq!(shape(&after), [(cursor("3"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn a_kind_no_writer_stores_fails_as_a_record_error() {
        let log = Log::default();
        let mut synthesized = data_record("p", 1, b"1");
        synthesized.kind = SUPERSEDED;
        let bad = log.push(synthesized.encode_to_vec());
        let error = read(&log, BEGINNING, &[]).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        assert_eq!(error.cursor, Some(cursor(&bad)));
        assert!(error.message.contains("kind 3"), "{error}");
    }

    #[tokio::test]
    async fn rule_20_4_an_entry_without_a_record_fails_with_its_cursor() {
        let log = Log::default();
        log.write("p", 1, b"1");
        let empty = log.push(Vec::new());
        log.write("p", 1, b"3");
        let first = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(shape(&first), [(cursor("1"), DATA, 1, false)]);
        let error = read(&log, &first.cursor, &first.state).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Record);
        assert_eq!(error.cursor, Some(cursor(&empty)));
        let after = read(&log, &cursor(&empty), &first.state).await.unwrap();
        assert_eq!(shape(&after), [(cursor("3"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn an_unspecified_kind_reads_as_data() {
        let log = Log::default();
        let mut unspecified = data_record("p", 1, b"1");
        unspecified.kind = StreamRecordKind::Unspecified as i32;
        log.push(unspecified.encode_to_vec());
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(shape(&read), [(cursor("1"), DATA, 1, false)]);
    }

    #[tokio::test]
    async fn a_read_from_the_end_resolves_its_cursor() {
        let log = Log::default();
        let empty = read(&log, END, &[]).await.unwrap();
        assert_eq!(empty.cursor, BEGINNING);
        log.write("model", 1, b"1");
        let resolved = read(&log, END, &[]).await.unwrap();
        assert!(resolved.records.is_empty());
        assert_eq!(resolved.cursor, cursor("1"));
        // The end is not a delivered record, so nothing is looked up.
        assert_eq!(log.lookups.load(Ordering::Relaxed), 0);
        log.write("model", 2, b"2");
        let next = read(&log, &resolved.cursor, &resolved.state).await.unwrap();
        assert_eq!(shape(&next), [(cursor("2"), DATA, 2, false)]);
    }

    #[tokio::test]
    async fn a_page_takes_the_default_size_when_the_request_leaves_it() {
        let log = Log::default();
        for n in 0..=DEFAULT_MAX_RECORDS {
            log.write("p", 1, n.to_string().as_bytes());
        }
        let read = read(&log, BEGINNING, &[]).await.unwrap();
        assert_eq!(read.records.len(), DEFAULT_MAX_RECORDS as usize);
    }

    #[tokio::test]
    async fn a_cursor_or_state_from_elsewhere_is_refused() {
        let log = Log::default();
        log.write("p", 1, b"1");
        let other = mint_cursor("log", &stream_hash("ns", "workflow", "wf", "other"), "1");
        let error = read(&log, &other, &[]).await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
        let error = read(&log, BEGINNING, b"\xff").await.unwrap_err();
        assert_eq!(error.kind, StreamFailureKind::Cursor);
        assert!(error.message.contains("read state"), "{error}");
    }
}
