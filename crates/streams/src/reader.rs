//! Reading a stream: the producer attempts a read observes, and what it carries between calls.
//!
//! An Activity that streams half an answer and then fails leaves those records in the stream, and
//! its retry writes different ones. No store can undo the first half, so the reader is told that a
//! new attempt began and the application decides what to do. This runs over records the read
//! already observed, so it costs no round trip and every store reports a retry the same way.

use crate::{
    BEGINNING, END, StreamError, StreamResult, StreamStore, cursor_position, mint_cursor,
    proto::{
        ChainId, ReadRecord, ReadRequest, ReadResponse, StoreLatestRequest, StoreReadRequest,
        StreamRecord, StreamRecordKind, Supersession, read_record,
    },
};
use prost::Message;
use std::collections::HashMap;

/// How many producers a read remembers the newest attempt of.
pub const MAX_TRACKED_PRODUCERS: usize = 1_000;

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
struct ReadState {
    /// The newest attempt this read delivered, per producer id.
    #[prost(map = "string, int64", tag = "1")]
    attempts: HashMap<String, i64>,
    /// Set by the first call, so later calls don't look up the resume record again.
    #[prost(bool, tag = "2")]
    started: bool,
    /// When this read last saw each producer in `attempts`, on the read's own clock, so the
    /// least recently seen one is forgotten first.
    #[prost(map = "string, uint64", tag = "9")]
    last_seen: HashMap<String, u64>,
    #[prost(uint64, tag = "10")]
    clock: u64,
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
    fn decode_from(state: &[u8]) -> StreamResult<Self> {
        Self::decode(state).map_err(|error| {
            StreamError::cursor(format!(
                "the read state is not one an earlier call of this read returned: {error}"
            ))
        })
    }

    /// Keeps the state small however many producers a long chain had, since it crosses to lang
    /// on every call.
    fn forget_beyond(&mut self, limit: usize) {
        while self.attempts.len() > limit {
            let oldest = self
                .attempts
                .keys()
                .min_by_key(|producer| self.last_seen.get(*producer).copied().unwrap_or(0))
                .cloned();
            let Some(oldest) = oldest else { break };
            self.attempts.remove(&oldest);
            self.last_seen.remove(&oldest);
        }
        self.last_seen
            .retain(|producer, _| self.attempts.contains_key(producer));
    }

    fn observe(&mut self, record: &StreamRecord) -> Observed {
        // A producer that declares no attempt has no generation to compare.
        if record.producer_id.is_empty() || record.attempt <= 0 {
            return Observed::Known;
        }
        self.clock += 1;
        self.last_seen
            .insert(record.producer_id.clone(), self.clock);
        let seen = self.attempts.get(&record.producer_id).copied().unwrap_or(0);
        if record.attempt < seen {
            return Observed::Behind { newest: seen };
        }
        if record.attempt == seen {
            return Observed::Known;
        }
        self.attempts
            .insert(record.producer_id.clone(), record.attempt);
        self.forget_beyond(MAX_TRACKED_PRODUCERS);
        Observed::Newer((seen > 0).then(|| Supersession {
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
/// resumes there reports the supersession again. A record from an attempt below the newest the
/// read delivered is still delivered, marked `stale`. The attempts seen ride in `state`, so this
/// holds across calls. A read that starts from a cursor without state looks up the record at the
/// cursor first, since that record was delivered before. Only that record's producer is known
/// then, and another producer's earlier attempts are not. A read remembers at most
/// [MAX_TRACKED_PRODUCERS] producers, and the next attempt of one it forgot reads without
/// `SUPERSEDED`.
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

    let page = store
        .read(StoreReadRequest {
            chain: Some(target.chain.clone()),
            topic: target.topic.clone(),
            after_position: position,
            wait: request.wait,
            max_records: match request.max_records {
                0 => DEFAULT_MAX_RECORDS,
                max => max,
            },
        })
        .await?;
    let mut records = Vec::with_capacity(page.records.len());
    let mut previous = start;
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
    Ok(ReadResponse {
        records,
        cursor: previous,
        state: state.encode_to_vec(),
        done: false,
    })
}

/// A stored record as a reader delivers it. Fails on bytes that are no record, and on a kind no
/// writer stores, such as the `SUPERSEDED` that only readers synthesize.
fn parse(bytes: &[u8]) -> Result<StreamRecord, String> {
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
                closed: false,
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
        assert_eq!(state.attempts, [("model".to_string(), 2)].into());
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
        assert_eq!(state.attempts.len(), MAX_TRACKED_PRODUCERS);
        assert!(state.attempts.contains_key("p0"));
        assert!(!state.attempts.contains_key("p1"));
        // A forgotten producer's next attempt reads as its first, without `SUPERSEDED`.
        assert_eq!(observe(&mut state, "p1", 2), Observed::Newer(None));
        assert!(matches!(
            observe(&mut state, "p0", 2),
            Observed::Newer(Some(_))
        ));
        // The bound holds across calls, since it rides in the state.
        let state = ReadState::decode_from(&state.encode_to_vec()).unwrap();
        assert_eq!(state.attempts.len(), MAX_TRACKED_PRODUCERS);
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
