//! The Stream runtime, a plain object that lang's bridge builds per client and calls by name.

use crate::{
    Reason, StreamError, StreamResult,
    memory::MemoryStore,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, CloseScopeRequest, CloseStreamRequest, CloseWriterRequest,
        CloseWriterResponse, CloseWritersRequest, CloseWritersResponse, DescribeStreamRequest,
        FreeResponse, FreeScopeRequest, FreeWriterRequest, FreeWriterResponse, MemoryStreamConfig,
        OpenWriterRequest, OpenWriterResponse, ReadRecordsRequest, ShutdownRequest,
        WorkflowAppendRequest, WriteRecordsRequest, WriteRecordsResponse,
    },
    scope::Scopes,
    service::StreamService,
    store::{DEFAULT_RETENTION, DEFAULT_SAFETY_TTL, Store},
    workflow::Pending,
    writer::{Holder, Writers},
};
use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
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
    pub(crate) writers: Writers,
    pub(crate) scopes: Scopes,
    pub(crate) pending: Pending,
    shut_down: AtomicBool,
    /// Set when a `Shutdown` ends every scope and writer. Then the `Free` methods do nothing.
    finished: AtomicBool,
}

impl Shared {
    pub(crate) fn is_shut_down(&self) -> bool {
        self.shut_down.load(Ordering::SeqCst)
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    /// A free of something that a running shutdown already took has nothing left to do.
    fn taken_by_shutdown(&self, freed: StreamResult<()>) -> StreamResult<()> {
        match freed {
            Err(error) if error.reason == Reason::UnknownWriter && self.is_shut_down() => Ok(()),
            freed => freed,
        }
    }

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
            scopes: Scopes::default(),
            pending: Pending::default(),
            shut_down: AtomicBool::new(false),
            finished: AtomicBool::new(false),
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
        if self.shared.is_finished() {
            return Ok(());
        }
        let freed = self.shared.writers.free(writer_id, Holder::Lang);
        self.shared.taken_by_shutdown(freed)
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Runs any method on encoded input and returns the encoded output. The bridge raises an error
    /// with [StreamError::code] and [StreamError::details].
    pub async fn call(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        self.check_open(method)?;
        // The methods of `call_sync` don't connect, so they work when the Store is down too.
        let service = || self.shared.service();
        let shared = &self.shared;
        let output = match method {
            "CreateStream" => service()
                .await?
                .create_stream(decode(input)?)
                .await?
                .encode_to_vec(),
            "AppendRecords" => service()
                .await?
                .append_records(decode::<AppendRecordsRequest>(input)?)
                .await?
                .encode_to_vec(),
            "CloseStream" => service()
                .await?
                .close_stream(decode::<CloseStreamRequest>(input)?)
                .await?
                .encode_to_vec(),
            "ReadRecords" => service()
                .await?
                .read_records(decode::<ReadRecordsRequest>(input)?)
                .await?
                .encode_to_vec(),
            "DescribeStream" => service()
                .await?
                .describe_stream(decode::<DescribeStreamRequest>(input)?)
                .await?
                .encode_to_vec(),
            "OpenWriter" => {
                let request = decode::<OpenWriterRequest>(input)?;
                let writers = &self.shared.writers;
                let writer_id = writers
                    .open(service().await?, request, Holder::Lang)
                    .await?;
                // A shutdown during the open has already closed every writer.
                if shared.is_shut_down() {
                    let _ = shared.writers.free(writer_id, Holder::Lang);
                    return Err(shut_down());
                }
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
                // The writers of scopes stay, since another Worker of the client may use them.
                let failed = self.shared.writers.close_all().await;
                CloseWritersResponse {
                    failed: failed as i32,
                }
                .encode_to_vec()
            }
            "CloseWriter" => {
                let request = decode::<CloseWriterRequest>(input)?;
                let last_offset = (self.shared.writers)
                    .close(request.writer_id, Holder::Lang)
                    .await?;
                CloseWriterResponse { last_offset }.encode_to_vec()
            }
            "OpenActivityScope" => shared
                .open_activity_scope(decode(input)?)
                .await?
                .encode_to_vec(),
            "Publish" => shared.publish(decode(input)?).await?.encode_to_vec(),
            "CloseStreamAs" => shared
                .close_stream_as(decode(input)?)
                .await?
                .encode_to_vec(),
            "CloseScope" => {
                let request = decode::<CloseScopeRequest>(input)?;
                shared.close_scope(request.scope_id).await?.encode_to_vec()
            }
            "WorkflowAppend" => shared
                .workflow_append(decode(input)?)
                .await?
                .encode_to_vec(),
            "WorkflowClose" => shared.workflow_close(decode(input)?).await?.encode_to_vec(),
            "Shutdown" => {
                decode::<ShutdownRequest>(input)?;
                shared.shut_down.store(true, Ordering::SeqCst);
                let closed = shared.close_everything().await;
                shared.finished.store(true, Ordering::SeqCst);
                closed.encode_to_vec()
            }
            _ => return self.call_sync(method, input),
        };
        Ok(output)
    }

    /// Runs a method that never waits, with no async runtime entered, so a finalizer can call it.
    /// `runtime.proto` marks these methods as synchronous or static.
    pub fn call_sync(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        // A finalizer can run after the shutdown, which already freed everything. Before that, a
        // free still works, so that lang can end a buffer whose pump is gone.
        if method.starts_with("Free") && self.shared.is_finished() {
            return Ok(vec![]);
        }
        self.check_open(method)?;
        let output = self.run_sync(method, input);
        match output {
            Err(error) if method.starts_with("Free") => {
                self.shared.taken_by_shutdown(Err(error)).map(|()| vec![])
            }
            output => output,
        }
    }

    fn run_sync(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        let shared = &self.shared;
        match method {
            "FreeWriter" => {
                let request = decode::<FreeWriterRequest>(input)?;
                self.free_writer(request.writer_id)?;
                Ok(FreeWriterResponse {}.encode_to_vec())
            }
            "OpenClientScope" => Ok(shared.open_client_scope(decode(input)?)?.encode_to_vec()),
            "WorkflowAppendPending" => Ok(shared
                .workflow_append_pending(&decode::<WorkflowAppendRequest>(input)?)?
                .encode_to_vec()),
            "FreeScope" => {
                let request = decode::<FreeScopeRequest>(input)?;
                shared.free_scope(request.scope_id)?;
                Ok(FreeResponse {}.encode_to_vec())
            }
            _ => crate::call_static(method, input),
        }
    }

    /// Refuses every method after `Shutdown`, except `Shutdown`, the `Free` methods and the static
    /// ones.
    fn check_open(&self, method: &str) -> StreamResult<()> {
        // A dropped `Shutdown` leaves work that the next one does.
        if !self.shared.is_shut_down()
            || method.starts_with("Free")
            || crate::sdk::is_static(method)
            || method == "Shutdown"
        {
            return Ok(());
        }
        Err(shut_down())
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

pub(crate) fn shut_down() -> StreamError {
    StreamError::new(Reason::Unsupported, "The Stream runtime shut down")
}

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}
