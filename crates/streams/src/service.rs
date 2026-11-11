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
        StreamReadStart, StreamRecord, StreamRef, StreamStatus, read_records_request::Position,
        stream_record::Item,
    },
    record::data_records,
    store::{
        Batch, Capabilities, ConflictPolicy, Create, CreateOutcome, FORMAT_VERSION, MAX_SEQUENCE,
        Store, StreamState,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// The longest wait of one read, for new Records and for the create.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The maximum number of Records in one read.
const MAX_RECORDS: usize = 1000;

/// The Stream service on one Store. A Store has no namespaces, so the service ignores a request's
/// `namespace`, which only the server's native Streams read.
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
        let conflict = conflict_policy(request.id_conflict_policy)?;
        // The backend config holds the Retention, so a create takes the Store default.
        let create = Create {
            retention: None,
            conflict,
            request_id: &request.request_id,
        };
        let created = self.store.create(&stream, &create).await?;
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
        let producer = outside_producer(request.producer.as_ref())?;
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
        let appended = self.store.append(&stream, &batch).await;
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
        let producer = close_producer(request.producer.as_ref())?;
        self.store.capabilities().require(Capabilities::CLOSE)?;
        self.store.close(&stream, producer).await?;
        Ok(CloseStreamResponse {})
    }

    pub(crate) async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        let (state, _) = self.store.read(&stream, 0, 0).await?;
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
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        // The wire from_offset is exclusive, so a read starts after it. The empty value is a read
        // from the start.
        let (from_offset, at_end) = match &request.position {
            Some(Position::FromOffset(offset)) => (offset_from_wire(offset)?, false),
            Some(Position::Start(start)) => (0, *start == StreamReadStart::End as i32),
            None => (0, false),
        };
        if from_offset < 0 {
            return Err(StreamError::invalid("The offset is below zero"));
        }
        let max_bytes = max_bytes(request.max_bytes)?;
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
        let deadline = started + wait;

        // A creator hands out the ref only after its create, so a read never waits for one.
        let (mut state, _) = self.store.read(&stream, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        if state.version > FORMAT_VERSION {
            return Err(StreamError::new(
                Reason::FormatTooNew,
                format!("format {} is newer than {FORMAT_VERSION}", state.version),
            ));
        }
        // An exclusive from_offset reads after that Record, so the inclusive start is one past it.
        let from = match from_offset {
            0 if at_end => state.newest + 1,
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
            if let Some(page) = self.page(&stream, &state, from, (max, max_bytes)).await? {
                return Ok(page);
            }
            if deadline <= Instant::now() {
                // An empty read resumes from where it started, which is one before the inclusive
                // start.
                return Ok(ReadRecordsResponse {
                    last_offset: offset_to_wire(from - 1),
                    ..Default::default()
                });
            }
            let after = (from - 1).max(state.newest);
            self.metrics.read_waits(true);
            let woke = self.store.wait_after(&stream, after, deadline).await;
            self.metrics.read_waits(false);
            woke?;
            state = self.store.read(&stream, 0, 0).await?.0;
        }
    }

    /// The Records from `from`, or `None` when there is none yet and the Stream is open. A page
    /// of a closed Stream that gives its last item is done, also with no item. `(max, max_bytes)`
    /// caps the items and their bytes.
    async fn page(
        &self,
        stream: &StreamRef,
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
            let (chunk_state, entries) = self.store.read(stream, start, chunk).await?;
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
                item: Some(Item::Close(StreamClose {
                    producer,
                    status: close_status,
                    offset: offset_to_wire(end.1 + 1),
                })),
            });
        }
        // The resume is after the last Record returned, which is one before the inclusive start.
        let page = ReadRecordsResponse {
            records,
            last_offset: offset_to_wire(start - 1),
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
    match record.item.as_mut() {
        Some(Item::Data(data)) => data.offset = offset_to_wire(offset),
        Some(Item::Close(close)) => close.offset = offset_to_wire(offset),
        None => {
            return Err(StreamError::storage_corrupt("A stored Record has no item"));
        }
    }
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
