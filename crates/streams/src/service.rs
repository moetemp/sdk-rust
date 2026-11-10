//! The five Stream service operations on the Store, for Activities, clients and the Local
//! Activity that runs each Workflow call.

use crate::{
    Reason, StreamError, StreamResult,
    digest::batch_digest,
    epoch::{ProducerKind, task_of},
    ids::{check_producer, check_stream_id},
    metrics::Metrics,
    offset::{offset_from_wire, offset_to_wire},
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, ReadRecordsRequest, ReadRecordsResponse, StreamClose, StreamIdConflictPolicy,
        StreamReadStart, StreamRecord, StreamRef, StreamStatus, stream_record::Item,
    },
    record::data_records,
    store::{
        Batch, Capabilities, ConflictPolicy, Create, CreateOutcome, FORMAT_VERSION, MAX_SEQUENCE,
        MAX_TTL, MIN_RETENTION, Store, StreamState,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// The longest wait of one read, for new Records and for the create.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The maximum number of Records in one read.
const MAX_RECORDS: usize = 1000;
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
            "The Retention must be 0 for the Store default, or from {} ms to {} s",
            MIN_RETENTION.as_millis(),
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
    let retention = duration(Some(value));
    if retention.is_some_and(|retention| retention < MIN_RETENTION) {
        return Err(out_of_range());
    }
    Ok(retention)
}

/// The conflict policy of a create. Unspecified is FAIL, as for a Workflow start.
fn conflict_policy(value: i32) -> StreamResult<ConflictPolicy> {
    match StreamIdConflictPolicy::try_from(value) {
        Ok(StreamIdConflictPolicy::Unspecified | StreamIdConflictPolicy::Fail) => {
            Ok(ConflictPolicy::Fail)
        }
        Ok(StreamIdConflictPolicy::UseExisting) => Ok(ConflictPolicy::UseExisting),
        Err(_) => Err(StreamError::invalid(format!(
            "The conflict policy {value} is unknown"
        ))),
    }
}

fn proto_duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: value.as_secs() as i64,
        nanos: value.subsec_nanos() as i32,
    }
}

fn stream_id(stream_ref: Option<&StreamRef>) -> StreamResult<&str> {
    let id = stream_ref
        .map(|stream_ref| stream_ref.id.as_str())
        .unwrap_or_default();
    check_stream_id(id)?;
    Ok(id)
}

/// Checks a Producer that writes outside Workflow code.
fn outside_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    let kind = ProducerKind::try_from(producer)?;
    if kind == ProducerKind::Workflow {
        return Err(StreamError::invalid(
            "A Workflow writes with the Producer of its Local Activity",
        ));
    }
    // Without its task, a re-dispatch in a later task has the same epoch as the first dispatch.
    let task = task_of(producer.kind.as_ref()).started_event_id;
    if kind == ProducerKind::LocalActivity && task <= 0 {
        return Err(StreamError::invalid(
            "A Local Activity Producer's epoch must name the Workflow Task that dispatched it",
        ));
    }
    Ok(producer)
}

/// Checks the Producer of a close.
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
        stream_id(request.stream_ref.as_ref())?;
        let stream = request.stream_ref.clone().unwrap_or_default();
        let retention = retention(request.retention)?;
        let conflict = conflict_policy(request.id_conflict_policy)?;
        let create = Create {
            retention,
            conflict,
            request_id: &request.request_id,
        };
        let created = self
            .store
            .create(&request.namespace, &stream, &create)
            .await?;
        Ok(CreateStreamResponse {
            r#ref: Some(StreamRef {
                id: stream.id,
                backend: self.backend.clone(),
            }),
            created: created.outcome == CreateOutcome::Created,
        })
    }

    pub(crate) async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        let writer = request.writer.unwrap_or_default();
        let producer = outside_producer(writer.producer.as_ref())?;
        if !(0..=MAX_SEQUENCE).contains(&request.sequence) {
            return Err(StreamError::invalid(
                "The sequence must be from 1 to 2^53 - 1, or zero for the next one",
            ));
        }
        // Without the fence, a stale batch would reach Readers.
        self.store
            .capabilities()
            .require(Capabilities::PRODUCER_FENCE)?;
        if request.sequence == 0 {
            // A call with no writer state sends no sequence. Only the Store can number the batch in
            // the same step as its dedupe check.
            self.store
                .capabilities()
                .require(Capabilities::NEXT_SEQUENCE)?;
        }
        let digest = batch_digest(&request.records);
        let records = data_records(producer, request.records)?;
        let batch = Batch {
            producer,
            sequence: request.sequence,
            records: &records,
            digest,
        };
        let appended = self.store.append(&request.namespace, &stream, &batch).await;
        let appended = self.count(appended)?;
        // An empty batch has no offset, which is the empty wire value.
        let (first_offset, last_offset) = appended.offsets.unwrap_or_default();
        Ok(AppendRecordsResponse {
            first_offset: offset_to_wire(first_offset),
            last_offset: offset_to_wire(last_offset),
        })
    }

    /// Counts a fence of an append on the metrics.
    fn count<T>(&self, result: StreamResult<T>) -> StreamResult<T> {
        if let Err(error) = &result
            && error.reason == Reason::Fenced
        {
            self.metrics.fenced();
        }
        result
    }

    pub(crate) async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        let writer = request.writer.unwrap_or_default();
        let producer = close_producer(writer.producer.as_ref())?;
        self.store.capabilities().require(Capabilities::CLOSE)?;
        self.store
            .close(&request.namespace, &stream, producer)
            .await?;
        Ok(CloseStreamResponse {})
    }

    pub(crate) async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let id = stream_id(request.r#ref.as_ref())?;
        let (state, _) = self.store.read(&request.namespace, id, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        // A closed Stream that expired has lost its log, and every read fails.
        let status = if state.expired() {
            StreamStatus::Expired
        } else if state.closed {
            StreamStatus::Closed
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: offset_to_wire(state.newest),
            retention: Some(proto_duration(state.retention)),
        })
    }

    /// A long poll that returns the Records it found, or nothing after `wait`. On a closed Stream
    /// with no item left, it returns `done` at once.
    pub(crate) async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        let id = stream_id(request.r#ref.as_ref())?.to_string();
        let namespace = request.namespace.as_str();
        // The wire from_offset is exclusive, so a read starts after it. The empty value is a read
        // from the start.
        let from_offset = offset_from_wire(&request.from_offset)?;
        if from_offset < 0 {
            return Err(StreamError::invalid("The offset is below zero"));
        }
        let max_bytes = max_bytes(request.max_bytes)?;
        if request.last < 0 {
            return Err(StreamError::invalid("last must be 0 or more"));
        }
        if request.last > 0 {
            if request.wake_only || from_offset > 0 {
                return Err(StreamError::invalid(
                    "A read of the last Records takes no from_offset and no wake_only",
                ));
            }
            return self.read_last(request, max_bytes).await;
        }
        if request.wake_only || !request.to_offset.is_empty() {
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
        // An exclusive from_offset reads after that Record, so the inclusive start is one past it.
        let from = match from_offset {
            0 if request.start == StreamReadStart::End as i32 => state.newest + 1,
            0 => 0,
            offset => offset + 1,
        };
        if from > state.newest + 1 {
            return Err(StreamError::new(
                Reason::OffsetTooHigh,
                "The offset is past the newest Record. A failover may have lost the newest \
                 Records. Resume from the newest offset, or read again once newer Records pass it",
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
                // An empty read resumes from where it started, which is one before the inclusive
                // start.
                return Ok(ReadRecordsResponse {
                    next_offset: offset_to_wire(from - 1),
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

    /// The newest `last` Records, oldest first, and `done` for a closed Stream. It never waits, and
    /// it stops after [MAX_SCAN] entries or [MAX_SCAN_BYTES] of them.
    async fn read_last(
        &self,
        request: ReadRecordsRequest,
        max_bytes: usize,
    ) -> StreamResult<ReadRecordsResponse> {
        let id = stream_id(request.r#ref.as_ref())?.to_string();
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
        let mut picked = vec![];
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
                scanned_bytes += entry.record.as_ref().map_or(0, Vec::len);
                if let Some(encoded) = entry.record {
                    let record = decode(&encoded, entry.offset)?;
                    let size = record.encoded_len();
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
        }
        picked.reverse();
        // The read resumes after the newest Record, which a close makes final.
        Ok(ReadRecordsResponse {
            records: picked,
            next_offset: offset_to_wire(state.newest),
            done: state.closed,
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

    /// The Records from `from`, or `None` when there is none yet and the Stream is open. A page
    /// of a closed Stream that gives its last item is done, also with no item. `(max, max_bytes)`
    /// caps the items and their bytes.
    async fn page(
        &self,
        namespace: &str,
        id: &str,
        state: &StreamState,
        from: i64,
        (max, max_bytes): (usize, usize),
    ) -> StreamResult<Option<ReadRecordsResponse>> {
        // A read from the start skips the Records that the log lost. A read from an offset fails on
        // them, since the Reader asked for those Records.
        let retained = state.retained_from();
        let mut start = if from > 0 { from } else { retained };
        let mut records = vec![];
        let mut full = false;
        let mut bytes = 0;
        // The bytes and the entries that the page read from the Store.
        let mut pulled = (0, 0);
        // Whether the Stream was closed, and its newest offset, at the newest Store read.
        let mut end = (state.closed, state.newest);
        // The closer and the status at the newest Store read, for the final close item.
        let mut closer = state.closer.clone();
        let mut close_status = state.close_status;
        while records.len() < max && !full {
            let room = (max_bytes != usize::MAX).then(|| max_bytes.saturating_sub(bytes));
            let chunk = chunk_len(max - records.len(), room, pulled);
            let (chunk_state, entries) = self.store.read(namespace, id, start, chunk).await?;
            // Records that the log lost after the state read leave a gap before these entries.
            check_expiry(&chunk_state, if from > 0 { start } else { 0 })?;
            end = (chunk_state.closed, chunk_state.newest);
            close_status = chunk_state.close_status;
            closer = chunk_state.closer;
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
                    start = entry.offset + 1;
                    continue;
                }
                let Some(encoded) = entry.record else {
                    start = entry.offset + 1;
                    continue;
                };
                let record = decode(&encoded, entry.offset)?;
                // An entry over the caps is still returned alone, so that the read always makes
                // progress. The next read starts at the entry that didn't fit.
                let size = record.encoded_len();
                pulled = (pulled.0 + size, pulled.1 + 1);
                let over = bytes + size > max_bytes || records.len() + 1 > max;
                if !records.is_empty() && over {
                    full = true;
                    break;
                }
                bytes += size;
                records.push(record);
                start = entry.offset + 1;
                if records.len() >= max {
                    break;
                }
            }
        }
        // A closed Stream takes no more appends, so no item is left past its newest offset.
        let done = end.0 && start > end.1;
        if records.is_empty() && !done {
            return Ok(None);
        }
        // The read that reaches the end of a closed Stream ends with its close item, at the offset
        // after the newest Record. A read that starts past the newest Record already saw it, so it
        // doesn't repeat it.
        if done && from <= end.1 {
            let producer = closer
                .as_deref()
                .map(Producer::decode)
                .transpose()
                .map_err(|error| StreamError::storage_corrupt(error.to_string()))?;
            records.push(StreamRecord {
                offset: offset_to_wire(end.1 + 1),
                item: Some(Item::Close(StreamClose {
                    producer,
                    status: close_status,
                })),
            });
        }
        // The resume is after the last Record returned, which is one before the inclusive start.
        let page = ReadRecordsResponse {
            records,
            next_offset: offset_to_wire(start - 1),
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
    let mut record = StreamRecord::decode(bytes).map_err(|error| {
        StreamError::storage_corrupt(format!("A stored Record is bad: {error}"))
    })?;
    record.offset = offset_to_wire(offset);
    Ok(record)
}

/// Fails a read whose Records the log lost or the expiry removed, so that the Reader always sees
/// the gap. A read from the start of the Stream has no gap.
///
/// A Tombstone fails every read. An open Stream that expired gets no more Records, so the Reader
/// would wait forever. A closed one lost its log.
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
