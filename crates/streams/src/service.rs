//! The five Stream service operations on the Store, for Activities, clients and the Local
//! Activity that runs each Workflow call.

use crate::{
    Reason, StreamError, StreamResult,
    digest::batch_digest,
    epoch::withdraws,
    ids::{check_producer, check_stream_id},
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, ReadRecordsRequest, ReadRecordsResponse, StreamProducerKind, StreamReadStart,
        StreamRecord, StreamRef, StreamStatus, Supersession, stream_record::Item,
    },
    record::{caller_status, close_record, data_records, supersession},
    store::{
        Batch, Capabilities, CreateOutcome, FORMAT_VERSION, MAX_RECORD_BYTES, MAX_SEQUENCE,
        MAX_TTL, Owner, Store, StreamState, Withdrawal, owner_chain,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// The longest wait of one read, for new Records and for the create.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The maximum number of Records in one read.
pub const MAX_RECORDS: usize = 1000;
/// The maximum number of log entries that a read of the last Records scans.
const MAX_SCAN: usize = 10 * MAX_RECORDS;
/// The most entry bytes that a read of the last Records scans.
const MAX_SCAN_BYTES: usize = 64 * 1024 * 1024;
/// How often a read looks again for a Stream that nobody created yet.
const CREATE_POLL: Duration = Duration::from_millis(100);

/// The Stream service on one Store.
pub(crate) struct StreamService {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) metrics: Metrics,
    /// The backend name for the refs the service builds.
    pub(crate) backend: String,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// The Retention of a create, `None` for the Store default. A Store computes expiry times from it,
/// so a value past [MAX_TTL] would overflow them after the create's first write.
fn retention(value: Option<prost_types::Duration>) -> StreamResult<Option<Duration>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let out_of_range = || {
        StreamError::invalid(format!(
            "The Retention must be from 0 to {} s",
            MAX_TTL.as_secs()
        ))
    };
    let (Ok(seconds), Ok(nanos)) = (u64::try_from(value.seconds), u32::try_from(value.nanos))
    else {
        return Err(out_of_range());
    };
    if nanos >= 1_000_000_000 || Duration::new(seconds, nanos) > MAX_TTL {
        return Err(out_of_range());
    }
    Ok(duration(Some(value)))
}

fn proto_duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: value.as_secs() as i64,
        nanos: value.subsec_nanos() as i32,
    }
}

fn stream_id(stream: Option<&StreamRef>) -> StreamResult<&str> {
    let id = stream.map(|stream| stream.id.as_str()).unwrap_or_default();
    check_stream_id(id)?;
    Ok(id)
}

/// The `Superseded` facts of a create that withdraws a retry run's previous run. It has no epoch,
/// so a Reader withdraws only that run's Records.
fn run_supersession(owner: Option<&Producer>, withdraw_run: &str) -> Vec<u8> {
    let producer = Producer {
        epoch: None,
        ..owner.cloned().unwrap_or_default()
    };
    supersession(&producer, withdraw_run)
}

/// Checks a Producer that writes outside Workflow code.
fn outside_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    if producer.kind == StreamProducerKind::Workflow as i32 {
        return Err(StreamError::invalid(
            "A Workflow writes with the Producer of its Local Activity",
        ));
    }
    // Without its task, a re-dispatch in a later task has the same epoch as the first dispatch.
    let task = producer
        .epoch
        .map_or(0, |epoch| epoch.task_started_event_id);
    if producer.kind == StreamProducerKind::LocalActivity as i32 && task <= 0 {
        return Err(StreamError::invalid(
            "A Local Activity Producer's epoch must name the Workflow Task that dispatched it",
        ));
    }
    Ok(producer)
}

/// Checks the owner of a create, which must be a Workflow run with a ref of its run chain. A
/// Store stores that chain and checks every later create and close against it.
fn create_owner<'a>(owner: &'a Producer, stream: &StreamRef) -> StreamResult<&'a Producer> {
    check_producer(owner)?;
    if owner.kind != StreamProducerKind::Workflow as i32 {
        return Err(StreamError::invalid(
            "The owner of a create is a Workflow Producer",
        ));
    }
    if owner_chain(stream).is_empty() {
        return Err(StreamError::invalid(
            "A create with an owner needs a ref whose owner is the Workflow's run chain",
        ));
    }
    Ok(owner)
}

/// Checks the Producer of a close. A Workflow's close Activity closes as the run's Workflow
/// Producer, so a Workflow Producer may close.
fn close_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    Ok(producer)
}

impl StreamService {
    pub(crate) async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        check_stream_id(&request.stream_id)?;
        let retention = retention(request.retention)?;
        if request
            .r#ref
            .as_ref()
            .is_some_and(|stream| stream.id != request.stream_id)
        {
            return Err(StreamError::invalid(
                "The create's ref names another Stream than its stream_id",
            ));
        }
        let stream = request.r#ref.clone().unwrap_or_else(|| StreamRef {
            id: request.stream_id.clone(),
            ..Default::default()
        });
        // A Workflow's call from outside Workflow code, such as from a Local Activity, sets the
        // owner run. Then the Stream has the safety TTL of a Workflow-owned Stream.
        let owner = match &request.owner {
            Some(owner) => {
                create_owner(owner, &stream)?;
                self.store
                    .capabilities()
                    .require(Capabilities::SAFETY_TTL)?;
                Owner::Workflow
            }
            None => Owner::Client,
        };
        if !request.withdraw_run.is_empty() {
            // The `Superseded` item names its Producer, which here can only be the owner. Any other
            // caller could withdraw and fence an unrelated run, as on the append.
            let Some(owner) = &request.owner else {
                return Err(StreamError::invalid(
                    "A create with withdraw_run needs the owner run",
                ));
            };
            if request.withdraw_run == owner.run_id {
                return Err(StreamError::invalid("A run doesn't withdraw itself"));
            }
            self.store.capabilities().require(Capabilities::RUN_FENCE)?;
        }
        let bytes = run_supersession(request.owner.as_ref(), &request.withdraw_run);
        let withdrawal = Withdrawal {
            withdraw_run: &request.withdraw_run,
            supersession: &bytes,
            tasks: None,
        };
        let created = self
            .store
            .create(&request.namespace, &stream, retention, owner, &withdrawal)
            .await?;
        if created.flagged {
            self.metrics.superseded();
        }
        Ok(CreateStreamResponse {
            stream: Some(StreamRef {
                id: request.stream_id,
                owner: stream.owner.filter(|_| owner == Owner::Workflow),
                backend: self.backend.clone(),
            }),
            created: created.outcome == CreateOutcome::Created,
        })
    }

    pub(crate) async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        stream_id(request.stream.as_ref())?;
        let stream = request.stream.clone().unwrap_or_default();
        let producer = outside_producer(request.producer.as_ref())?;
        if !(1..=MAX_SEQUENCE).contains(&request.sequence) {
            return Err(StreamError::invalid(
                "The sequence must be from 1 to 2^53 - 1",
            ));
        }
        // Without the fence and the flag, a copy or a stale batch would reach Readers.
        self.store
            .capabilities()
            .require(Capabilities::PRODUCER_FENCE.union(Capabilities::WITHDRAWAL))?;
        if !request.withdraw_run.is_empty() {
            // Only a retry run's Local Activity withdraws its previous run, as only the owner's
            // create does. Any other Producer could withdraw and fence an unrelated run.
            if producer.kind != StreamProducerKind::LocalActivity as i32
                || request.withdraw_run == producer.run_id
            {
                return Err(StreamError::invalid(
                    "Only a Local Activity of a later run withdraws a run",
                ));
            }
            self.store.capabilities().require(Capabilities::RUN_FENCE)?;
        }
        let digest = batch_digest(&request.records);
        let records = data_records(producer, request.records)?;
        let bytes = supersession(producer, &request.withdraw_run);
        let batch = Batch {
            producer,
            sequence: request.sequence,
            records: &records,
            digest: &digest,
            withdrawal: Withdrawal {
                withdraw_run: &request.withdraw_run,
                supersession: &bytes,
                tasks: None,
            },
        };
        let appended = self.store.append(&request.namespace, &stream, &batch).await;
        let appended = self.count(appended)?;
        let (first_offset, last_offset) = appended.offsets.unwrap_or_default();
        Ok(AppendRecordsResponse {
            first_offset,
            last_offset,
        })
    }

    /// Counts a fence or a flag of an append on the metrics.
    pub(crate) fn count<T: Flagged>(&self, result: StreamResult<T>) -> StreamResult<T> {
        match &result {
            Ok(value) if value.flagged() => self.metrics.superseded(),
            Err(error) if error.reason == Reason::Fenced => self.metrics.fenced(),
            _ => {}
        }
        result
    }

    pub(crate) async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        stream_id(request.stream.as_ref())?;
        let stream = request.stream.clone().unwrap_or_default();
        let producer = close_producer(request.producer.as_ref())?;
        self.store.capabilities().require(Capabilities::CLOSE)?;
        let status = caller_status(request.status, request.failure.as_ref())?;
        let record = close_record(producer, request.result, request.failure, status);
        // Every read past the close returns the close Record again.
        if record.len() > MAX_RECORD_BYTES {
            return Err(StreamError::new(
                Reason::TooLarge,
                format!(
                    "The close Record has {} bytes, more than {MAX_RECORD_BYTES}",
                    record.len()
                ),
            ));
        }
        self.store
            .close(&request.namespace, &stream, producer, &record)
            .await?;
        Ok(CloseStreamResponse {})
    }

    pub(crate) async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let id = stream_id(request.stream.as_ref())?;
        let (state, _) = self.store.read(&request.namespace, id, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        // A closed Stream that expired has lost its close Record, and every read fails.
        let status = if state.expired() {
            StreamStatus::Expired
        } else if state.closed_at.is_some() {
            StreamStatus::Closed
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: state.newest,
            retention: Some(proto_duration(state.retention)),
        })
    }

    /// A long poll that returns the Records it found, the close Record, or nothing after `wait`.
    pub(crate) async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        let id = stream_id(request.stream.as_ref())?.to_string();
        let namespace = request.namespace.as_str();
        if request.from_offset < 0 {
            return Err(StreamError::invalid("The offset is below zero"));
        }
        let max_bytes = max_bytes(request.max_bytes)?;
        if request.last < 0 {
            return Err(StreamError::invalid("last must be 0 or more"));
        }
        if request.last > 0 {
            if request.wake_only || request.from_offset > 0 {
                return Err(StreamError::invalid(
                    "A read of the last Records takes no from_offset and no wake_only",
                ));
            }
            return self.read_last(request, max_bytes).await;
        }
        if request.wake_only || request.to_offset != 0 {
            return Err(StreamError::new(
                Reason::Unsupported,
                "The Store reads no to_offset or wake_only, which the native backend serves",
            ));
        }
        let max = match request.max_records {
            n if n <= 0 => MAX_RECORDS,
            n => (n as usize).min(MAX_RECORDS),
        };
        let started = Instant::now();
        let wait = duration(request.wait).unwrap_or_default().min(MAX_WAIT);
        let create_wait = duration(request.create_wait)
            .unwrap_or_default()
            .min(MAX_WAIT);
        let deadline = started + wait;

        let mut state = self.created(namespace, &id, started + create_wait).await?;
        if state.version > FORMAT_VERSION {
            return Err(StreamError::new(
                Reason::FormatTooNew,
                format!("format {} is newer than {FORMAT_VERSION}", state.version),
            ));
        }
        let from = match request.from_offset {
            0 if request.start == StreamReadStart::End as i32 => state.newest + 1,
            from => from,
        };
        if from > state.newest + 1 {
            let next = state.newest + 1;
            return Err(StreamError::new(
                Reason::Offset,
                format!(
                    "The offset {from} is past the newest offset + 1, {next}. A failover may have \
                     lost the newest Records. Read from {next}, or read again once newer Records \
                     pass the offset"
                ),
            ));
        }
        loop {
            check_expiry(&state, from)?;
            if let Some(page) = self
                .page(namespace, &id, &state, from, (max, max_bytes))
                .await?
            {
                return Ok(page);
            }
            if deadline <= Instant::now() {
                return Ok(ReadRecordsResponse {
                    next_offset: from,
                    ..Default::default()
                });
            }
            let after = (from - 1).max(state.newest);
            self.metrics.read_waits(true);
            let woke = self.store.wait_after(namespace, &id, after, deadline).await;
            self.metrics.read_waits(false);
            woke?;
            state = self.store.read(namespace, &id, 0, 0).await?.0;
        }
    }

    /// The newest `last` Records, oldest first, and the close Record of a closed Stream at the end.
    /// A Record that a newer `Superseded` item withdraws doesn't count. It never waits, and it
    /// stops after [MAX_SCAN] entries or [MAX_SCAN_BYTES] of them.
    async fn read_last(
        &self,
        request: ReadRecordsRequest,
        max_bytes: usize,
    ) -> StreamResult<ReadRecordsResponse> {
        let id = stream_id(request.stream.as_ref())?.to_string();
        let namespace = request.namespace.as_str();
        let count = (request.last as usize).min(MAX_RECORDS);
        let create_wait = duration(request.create_wait)
            .unwrap_or_default()
            .min(MAX_WAIT);
        let state = self
            .created(namespace, &id, Instant::now() + create_wait)
            .await?;
        if state.version > FORMAT_VERSION {
            return Err(StreamError::new(
                Reason::FormatTooNew,
                format!("format {} is newer than {FORMAT_VERSION}", state.version),
            ));
        }
        check_expiry(&state, 0)?;
        let retained = state.retained_from();
        let (mut picked, mut close) = (vec![], None);
        let mut withdrawals: Vec<Supersession> = vec![];
        let (mut bytes, mut scanned, mut scanned_bytes) = (0, 0, 0);
        let mut before = state.newest;
        'scan: while before >= retained
            && picked.len() < count
            && scanned < MAX_SCAN
            && scanned_bytes < MAX_SCAN_BYTES
        {
            let left = (count - picked.len()).clamp(16, MAX_RECORDS);
            let room = max_bytes
                .saturating_sub(bytes)
                .min(MAX_SCAN_BYTES.saturating_sub(scanned_bytes));
            let chunk = chunk_len(left, Some(room), (scanned_bytes, scanned));
            let entries = self.store.read_back(namespace, &id, before, chunk).await?;
            if entries.is_empty() {
                break;
            }
            for entry in entries {
                if entry.offset < retained {
                    break 'scan;
                }
                before = entry.offset - 1;
                scanned += 1;
                scanned_bytes += entry.record.as_ref().map_or(0, Vec::len)
                    + entry.supersession.as_ref().map_or(0, Vec::len);
                // An entry's `Superseded` item withdraws earlier Records, not the entry's own.
                if let Some(record) = entry.record {
                    let record = decode(&record, entry.offset)?;
                    let size = record.encoded_len();
                    // The close Record ends the page and doesn't count toward the byte cap.
                    if matches!(record.item, Some(Item::Close(_))) {
                        close = Some(record);
                    } else if !withdrawals.iter().any(|s| withdraws(s, &record)) {
                        // A Record over the cap is still returned alone, so that the read always
                        // returns a Record.
                        if !picked.is_empty() && bytes + size > max_bytes {
                            break 'scan;
                        }
                        bytes += size;
                        picked.push(record);
                        if picked.len() == count {
                            break 'scan;
                        }
                    }
                }
                if let Some(supersession) = entry.supersession {
                    withdrawals.push(
                        Supersession::decode(supersession.as_slice())
                            .map_err(|error| StreamError::storage(error.to_string()))?,
                    );
                }
            }
        }
        picked.reverse();
        picked.extend(close);
        Ok(ReadRecordsResponse {
            records: picked,
            next_offset: state.newest + 1,
            done: state.closed_at.is_some(),
            ..Default::default()
        })
    }

    /// The state of a Stream, after waiting up to `until` for its create.
    async fn created(
        &self,
        namespace: &str,
        id: &str,
        until: Instant,
    ) -> StreamResult<StreamState> {
        loop {
            let (state, _) = self.store.read(namespace, id, 0, 0).await?;
            if state.exists {
                return Ok(state);
            }
            let now = Instant::now();
            if now >= until {
                return Err(StreamError::new(
                    Reason::NotFound,
                    "Nobody created the Stream",
                ));
            }
            tokio::time::sleep(CREATE_POLL.min(until - now)).await;
        }
    }

    /// The Records from `from`, or `None` when there is none yet. A page that ends in the close
    /// Record is done. `(max, max_bytes)` caps the items and their bytes.
    async fn page(
        &self,
        namespace: &str,
        id: &str,
        state: &StreamState,
        from: i64,
        (max, max_bytes): (usize, usize),
    ) -> StreamResult<Option<ReadRecordsResponse>> {
        // A read past the close returns the close Record again, so that every read ends with it.
        if let Some(closed_at) = state.closed_at.filter(|at| from > *at) {
            let (_, entries) = self.store.read(namespace, id, closed_at, 1).await?;
            let mut records = vec![];
            for entry in entries {
                if let Some(bytes) = entry.record {
                    records.push(decode(&bytes, entry.offset)?);
                }
            }
            let page = ReadRecordsResponse {
                records,
                next_offset: from,
                done: true,
                ..Default::default()
            };
            return Ok(Some(page));
        }
        // A read from the start skips the Records that the Retention hides. A read from an offset
        // fails on them, since the Reader asked for those Records.
        let retained = state.retained_from();
        let mut start = if from > 0 { from } else { retained };
        let mut records = vec![];
        let mut next = from;
        let (mut done, mut full) = (false, false);
        let mut bytes = 0;
        // The bytes and the entries that the page read from the Store.
        let mut pulled = (0, 0);
        while records.len() < max && !done && !full {
            let room = (max_bytes != usize::MAX).then(|| max_bytes.saturating_sub(bytes));
            let chunk = chunk_len(max - records.len(), room, pulled);
            let (chunk_state, entries) = self.store.read(namespace, id, start, chunk).await?;
            // A trim after the state read leaves a gap before these entries.
            check_expiry(&chunk_state, if from > 0 { start } else { 0 })?;
            if entries.is_empty() {
                // Only an expired log has no entry at an offset up to `newest`.
                if from > 0 && start == from && from <= chunk_state.newest {
                    return Err(expired());
                }
                break;
            }
            for entry in entries {
                if entry.offset < retained {
                    if from > 0 {
                        return Err(expired());
                    }
                    next = entry.offset + 1;
                    start = next;
                    continue;
                }
                let mut items = vec![];
                if let Some(bytes) = entry.supersession {
                    let supersession = Supersession::decode(bytes.as_slice())
                        .map_err(|error| StreamError::storage(error.to_string()))?;
                    items.push(StreamRecord {
                        offset: entry.offset,
                        item: Some(Item::Superseded(supersession)),
                    });
                }
                if let Some(bytes) = entry.record {
                    let record = decode(&bytes, entry.offset)?;
                    done = matches!(record.item, Some(Item::Close(_)));
                    items.push(record);
                }
                // An entry over the caps is still returned alone, so that the read always makes
                // progress. The next read starts at the entry that didn't fit.
                let size: usize = items.iter().map(Message::encoded_len).sum();
                pulled = (pulled.0 + size, pulled.1 + 1);
                let over = bytes + size > max_bytes || records.len() + items.len() > max;
                if !records.is_empty() && over {
                    (done, full) = (false, true);
                    next = entry.offset;
                    break;
                }
                bytes += size;
                records.extend(items);
                next = entry.offset + 1;
                start = next;
                if done || records.len() >= max {
                    break;
                }
            }
        }
        if records.is_empty() {
            return Ok(None);
        }
        let page = ReadRecordsResponse {
            records,
            next_offset: next,
            done,
            ..Default::default()
        };
        Ok(Some(page))
    }
}

/// The number of entries of the next Store read, at most `left`. With a byte cap, the first read
/// takes one entry, and each later one takes what fits the `room` left at the average size so far.
fn chunk_len(left: usize, room: Option<usize>, (bytes, entries): (usize, usize)) -> usize {
    let Some(room) = room else {
        return left;
    };
    if entries == 0 {
        return 1;
    }
    let average = (bytes / entries).max(1);
    (room / average + 1).clamp(1, left.max(1))
}

fn decode(bytes: &[u8], offset: i64) -> StreamResult<StreamRecord> {
    let mut record = StreamRecord::decode(bytes)
        .map_err(|error| StreamError::storage(format!("A stored Record is bad: {error}")))?;
    record.offset = offset;
    Ok(record)
}

/// Fails a read whose Records the Retention or the expiry removed, so that the Reader always sees
/// the gap. A read from the start of the Stream has no gap.
///
/// A Tombstone fails every read. An open Stream that expired gets no more Records, so the Reader
/// would wait forever. A closed one lost its log and its close Record.
fn check_expiry(state: &StreamState, from: i64) -> StreamResult<()> {
    if state.expired() || (from > 0 && state.trimmed >= from) {
        return Err(expired());
    }
    Ok(())
}

/// The byte cap of a page, from a request's `max_bytes`. Zero is no cap.
fn max_bytes(value: i64) -> StreamResult<usize> {
    match value {
        0 => Ok(usize::MAX),
        n if n < 0 => Err(StreamError::invalid("max_bytes must be 0 or more")),
        n => Ok(usize::try_from(n).unwrap_or(usize::MAX)),
    }
}

fn expired() -> StreamError {
    StreamError::new(Reason::Expired, "The Records at the offset expired")
}

/// A Store answer that may have withdrawn Records.
pub(crate) trait Flagged {
    fn flagged(&self) -> bool;
}

impl Flagged for crate::store::Appended {
    fn flagged(&self) -> bool {
        self.flagged
    }
}
