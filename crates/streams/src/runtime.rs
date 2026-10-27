//! The Stream runtime, a plain object that lang's bridge builds per client and calls by name.

use crate::{
    Reason, StreamError, StreamResult,
    memory::MemoryStore,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, CloseStreamRequest, CloseWriterRequest, CloseWriterResponse,
        CloseWritersRequest, CloseWritersResponse, DescribeStreamRequest, FreeWriterRequest,
        FreeWriterResponse, MemoryStreamConfig, OpenWriterRequest, OpenWriterResponse,
        ReadRecordsRequest, WriteRecordsRequest, WriteRecordsResponse,
    },
    service::StreamService,
    store::{DEFAULT_RETENTION, DEFAULT_SAFETY_TTL, Store},
    writer::Writers,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use tokio::sync::OnceCell;

/// Connects the Store of a runtime.
///
/// The runtime connects on its first call that needs the Store. So a bridge can build the runtime
/// with no async runtime, and a Workflow that never publishes never connects. A failed connect is
/// tried again on the next call.
#[async_trait::async_trait]
pub trait StoreFactory: Send + Sync + 'static {
    /// Connects a Store.
    async fn connect(&self) -> StreamResult<Arc<dyn Store>>;
}

/// A Store that is already connected.
struct Connected(Arc<dyn Store>);

#[async_trait::async_trait]
impl StoreFactory for Connected {
    async fn connect(&self) -> StreamResult<Arc<dyn Store>> {
        Ok(self.0.clone())
    }
}

/// The state that the runtime's calls share.
pub(crate) struct Shared {
    name: &'static str,
    backend: String,
    metrics: Metrics,
    factory: Arc<dyn StoreFactory>,
    service: OnceCell<Arc<StreamService>>,
    writers: Writers,
}

impl Shared {
    /// The service, which connects its Store on its first use.
    pub(crate) async fn service(&self) -> StreamResult<&Arc<StreamService>> {
        self.service
            .get_or_try_init(|| async {
                let store = self.factory.connect().await?;
                Ok(Arc::new(StreamService {
                    store,
                    metrics: self.metrics.clone(),
                    backend: self.backend.clone(),
                }))
            })
            .await
    }
}

/// The Stream runtime on one Store.
///
/// A bridge builds one for each client with a Stream backend, and that client's Workers share it.
/// A Store crate, such as `temporalio-streams-redis`, builds it with its own Store.
pub struct StreamRuntime {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRuntime")
            .field("name", &self.shared.name)
            .field("backend", &self.shared.backend)
            .finish_non_exhaustive()
    }
}

impl StreamRuntime {
    /// Builds a runtime that connects its Store with `factory` on its first call that needs it.
    /// `name` identifies the runtime in logs. `backend` is the name that lang registered the
    /// runtime under, which the refs that the runtime builds carry. The runtime records metrics on
    /// `meter`, lang's runtime meter, if set.
    pub fn new(
        name: &'static str,
        backend: impl Into<String>,
        factory: Arc<dyn StoreFactory>,
        meter: Option<TemporalMeter>,
    ) -> Arc<Self> {
        let meter = meter.unwrap_or_else(TemporalMeter::no_op);
        let shared = Arc::new(Shared {
            name,
            backend: backend.into(),
            metrics: Metrics::new(&meter),
            factory,
            service: OnceCell::new(),
            writers: Writers::default(),
        });
        Arc::new(Self { shared })
    }

    /// Builds a runtime on a Store that is already connected.
    pub fn with_store(
        name: &'static str,
        backend: impl Into<String>,
        store: Arc<dyn Store>,
        meter: Option<TemporalMeter>,
    ) -> Arc<Self> {
        Self::new(name, backend, Arc::new(Connected(store)), meter)
    }

    /// A name that identifies the runtime in logs.
    pub fn name(&self) -> &str {
        self.shared.name
    }

    /// The backend name that the refs of the runtime carry.
    pub fn backend(&self) -> &str {
        &self.shared.backend
    }

    /// Releases one handle of a writer with no wait, as the `FreeWriter` method does. A bridge
    /// calls it from a finalizer, which has no async runtime to await on.
    pub fn free_writer(&self, writer_id: u64) -> StreamResult<()> {
        self.shared.writers.free(writer_id)
    }

    /// Runs one method on encoded input and returns the encoded output. The bridge raises an error
    /// with [StreamError::code] and [StreamError::details].
    pub async fn call(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        // `FreeWriter` needs no Store, so it works when the Store is down too.
        if method == "FreeWriter" {
            let request = decode::<FreeWriterRequest>(input)?;
            self.free_writer(request.writer_id)?;
            return Ok(FreeWriterResponse {}.encode_to_vec());
        }
        let service = self.shared.service().await?;
        let output = match method {
            "CreateStream" => service.create_stream(decode(input)?).await?.encode_to_vec(),
            "AppendRecords" => service
                .append_records(decode::<AppendRecordsRequest>(input)?)
                .await?
                .encode_to_vec(),
            "CloseStream" => service
                .close_stream(decode::<CloseStreamRequest>(input)?)
                .await?
                .encode_to_vec(),
            "ReadRecords" => service
                .read_records(decode::<ReadRecordsRequest>(input)?)
                .await?
                .encode_to_vec(),
            "DescribeStream" => service
                .describe_stream(decode::<DescribeStreamRequest>(input)?)
                .await?
                .encode_to_vec(),
            "OpenWriter" => {
                let request = decode::<OpenWriterRequest>(input)?;
                let writer_id = self.shared.writers.open(service, request).await?;
                OpenWriterResponse { writer_id }.encode_to_vec()
            }
            "WriteRecords" => {
                let request = decode::<WriteRecordsRequest>(input)?;
                let (first_offset, last_offset) = self
                    .shared
                    .writers
                    .write(request.writer_id, request.records)
                    .await?;
                WriteRecordsResponse {
                    first_offset,
                    last_offset,
                }
                .encode_to_vec()
            }
            "CloseWriters" => {
                decode::<CloseWritersRequest>(input)?;
                let failed = self.shared.writers.close_all().await;
                CloseWritersResponse {
                    failed: failed as i32,
                }
                .encode_to_vec()
            }
            "CloseWriter" => {
                let request = decode::<CloseWriterRequest>(input)?;
                let last_offset = self.shared.writers.close(request.writer_id).await?;
                CloseWriterResponse { last_offset }.encode_to_vec()
            }
            _ => {
                return Err(StreamError::new(
                    Reason::Unsupported,
                    format!("Unknown method {method}"),
                ));
            }
        };
        Ok(output)
    }
}

/// The name of the memory runtime, for a bridge that selects a runtime by name.
pub const MEMORY_RUNTIME_NAME: &str = "temporalio-streams-memory";

/// The Stream runtime on the memory Store, for tests. It serves the same calls with the same
/// rules as every other Store, and it keeps its Streams in this process only.
#[derive(Debug)]
pub struct MemoryStreamRuntime;

impl MemoryStreamRuntime {
    /// Builds a runtime on an empty memory Store.
    pub fn create(
        retention: Duration,
        safety_ttl: Duration,
        meter: Option<TemporalMeter>,
    ) -> Arc<StreamRuntime> {
        Self::with_store(Arc::new(MemoryStore::new(retention, safety_ttl)), meter)
    }

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
            duration(config.safety_ttl).unwrap_or(DEFAULT_SAFETY_TTL),
        ));
        Ok(StreamRuntime::with_store(
            MEMORY_RUNTIME_NAME,
            config.backend,
            memory,
            meter,
        ))
    }

    /// Builds a runtime on any Store, for tests of a Store that lacks some capabilities.
    #[doc(hidden)]
    pub fn with_store(store: Arc<dyn Store>, meter: Option<TemporalMeter>) -> Arc<StreamRuntime> {
        StreamRuntime::with_store(MEMORY_RUNTIME_NAME, String::new(), store, meter)
    }
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}
