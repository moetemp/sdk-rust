//! The Stream runtime, a plain object that lang's bridge builds per client.

use crate::{
    Reason, StreamError, StreamResult,
    digest::batch_digest,
    epoch::ProducerKind,
    ids::{check_producer, check_required, check_stream_id},
    memory::MemoryStore,
    metrics::Metrics,
    offset::offset_to_wire,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        MemoryStreamConfig, Producer, StreamIdConflictPolicy, StreamRef, StreamStatus,
    },
    record::{check_records, data_records},
    store::{
        Batch, Capabilities, ConflictPolicy, Create, CreateOutcome, DEFAULT_RETENTION,
        MAX_SEQUENCE, Store,
    },
    writer::Writers,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;

/// The Stream runtime on one Store. It serves the five Stream service operations and the writers.
/// A Store has no namespaces, so the runtime ignores a request's `namespace`. The SDK sets it, and
/// only the server's native Streams read it.
///
/// A bridge builds one for each client with a Stream backend, and that client's Workers share it.
/// A Store crate, such as `temporalio-streams-redis`, builds it with its own Store.
pub struct StreamRuntime {
    name: &'static str,
    /// The backend name for the refs the runtime builds.
    backend: String,
    pub(crate) store: Arc<dyn Store>,
    pub(crate) metrics: Metrics,
    pub(crate) writers: Writers,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRuntime")
            .field("name", &self.name)
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

pub(crate) fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
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

pub(crate) fn stream_id(stream_ref: Option<&StreamRef>) -> StreamResult<&str> {
    let id = stream_ref
        .map(|stream_ref| stream_ref.id.as_str())
        .unwrap_or_default();
    check_stream_id(id)?;
    Ok(id)
}

/// Checks a Producer that writes outside Workflow code.
pub(crate) fn outside_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    if ProducerKind::try_from(producer)? == ProducerKind::Workflow {
        return Err(StreamError::invalid(
            "A Workflow writes with the Producer of its Local Activity",
        ));
    }
    check_required(producer)?;
    Ok(producer)
}

/// Checks the Producer of a close.
fn close_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    check_required(producer)?;
    Ok(producer)
}

/// Appends one batch. The runtime and each writer send their batches through it, so a writer needs
/// no reference to the runtime.
pub(crate) async fn append(
    store: &dyn Store,
    metrics: &Metrics,
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
    check_records(&request.records)?;
    // Without the fence, a stale batch would reach Readers.
    store.capabilities().require(Capabilities::PRODUCER_FENCE)?;
    if request.sequence == 0 {
        // A call with no writer state sends no sequence. Only the Store can number the batch in
        // the same step as its dedupe check.
        store.capabilities().require(Capabilities::NEXT_SEQUENCE)?;
    }
    let digest = batch_digest(&request.records);
    let records = data_records(producer, request.records)?;
    let batch = Batch {
        producer,
        sequence: request.sequence,
        records: &records,
        digest,
    };
    let appended = store.append(&stream, &batch).await;
    if let Err(error) = &appended
        && error.reason == Reason::Fenced
    {
        metrics.fenced();
    }
    // An empty batch has no offset, which is the empty wire value.
    let (first_offset, last_offset) = appended?.offsets.unwrap_or_default();
    Ok(AppendRecordsResponse {
        first_offset: offset_to_wire(first_offset),
        last_offset: offset_to_wire(last_offset),
    })
}

impl StreamRuntime {
    /// Builds a runtime on `store`. `name` identifies the runtime in logs. `backend` is the name
    /// that lang registered the runtime under, which the refs that the runtime builds carry. The
    /// runtime records metrics on `meter`, lang's runtime meter, if set. A Store that connects
    /// later, such as a [crate::store::LazyStore], lets a bridge build the runtime with no async
    /// runtime.
    pub fn new(
        name: &'static str,
        backend: impl Into<String>,
        store: Arc<dyn Store>,
        meter: Option<TemporalMeter>,
    ) -> Arc<Self> {
        let meter = meter.unwrap_or_else(TemporalMeter::no_op);
        Arc::new(Self {
            name,
            backend: backend.into(),
            store,
            metrics: Metrics::new(&meter),
            writers: Writers::default(),
        })
    }

    /// A name that identifies the runtime in logs.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Creates a Stream, or gets it with USE_EXISTING.
    pub async fn create_stream(
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

    /// Appends one batch as one Producer. A writer keeps a Producer's epoch and `sequence` order
    /// for many batches.
    pub async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        append(&*self.store, &self.metrics, request).await
    }

    /// Closes a Stream. Its readers drain what is left, and later appends fail.
    pub async fn close_stream(
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

    /// Gets the status, the newest offset and the Retention of a Stream.
    pub async fn describe_stream(
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
}

/// The name that [StreamRuntime::name] gives for a memory runtime.
const MEMORY_RUNTIME_NAME: &str = "temporalio-streams-memory";

/// The Stream runtime on the memory Store, for tests. It serves the same calls with the same
/// rules as every other Store, and it keeps its Streams in this process only.
#[derive(Debug)]
pub struct MemoryStreamRuntime;

impl MemoryStreamRuntime {
    /// Builds a runtime from an encoded [MemoryStreamConfig]. Its field numbers are those of the
    /// same fields in the config of another Store, such as Redis, so a backend can pass that config
    /// too.
    pub fn from_config_bytes(
        config: &[u8],
        meter: Option<TemporalMeter>,
    ) -> StreamResult<Arc<StreamRuntime>> {
        let config = MemoryStreamConfig::decode(config)
            .map_err(|error| StreamError::invalid(format!("The config is bad: {error}")))?;
        let memory = Arc::new(MemoryStore::new(
            duration(config.retention).unwrap_or(DEFAULT_RETENTION),
            duration(config.last_write_ttl),
        ));
        Ok(StreamRuntime::new(
            MEMORY_RUNTIME_NAME,
            config.backend,
            memory,
            meter,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bad_memory_config_is_invalid() {
        let error = MemoryStreamRuntime::from_config_bytes(&[0xff], None).unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
    }
}
