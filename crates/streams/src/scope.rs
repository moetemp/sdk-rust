//! Producer scopes. A scope is the Producer of one Activity attempt or one client, and the writers
//! and buffers that write as it, with one writer for each Stream. Lang keeps only the scope id.

use crate::{
    Reason, StreamError, StreamResult, derive_producer, ids,
    proto::{
        CloseScopeResponse, CloseStreamAsRequest, CloseStreamResponse, DeriveProducerRequest,
        OpenActivityScopeRequest, OpenClientScopeRequest, OpenScopeResponse, OpenWriterRequest,
        Producer, ProducerEpoch, ProducerPurpose, PublishRequest, PublishResponse,
        ShutdownResponse, StreamProducerKind, StreamRef, WorkflowTaskEpoch,
    },
    runtime::Shared,
    writer::Holder,
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub(crate) struct Scope {
    namespace: String,
    producer: Producer,
    previous_completed_task: Option<WorkflowTaskEpoch>,
    failed_tasks: Vec<i64>,
    /// An Activity attempt's scope, which doesn't close a Stream.
    activity: bool,
    writers: Mutex<ScopeWriters>,
    /// Allows one open at a time for each Stream, so that two first publishes share one writer.
    opening: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

#[derive(Default)]
struct ScopeWriters {
    by_stream: HashMap<String, u64>,
    buffers: Vec<u64>,
    /// Set when the scope's close or free starts. Then the scope takes no new buffer.
    closing: bool,
    /// Set by the close or free of the scope. A writer that opens after it is released at once.
    ended: bool,
}

impl Scope {
    fn new(
        namespace: String,
        producer: Producer,
        previous_completed_task: Option<WorkflowTaskEpoch>,
        failed_tasks: Vec<i64>,
        activity: bool,
    ) -> Self {
        Self {
            namespace,
            producer,
            previous_completed_task,
            failed_tasks,
            activity,
            writers: Mutex::default(),
            opening: Mutex::default(),
        }
    }

    fn cached(&self, stream_id: &str) -> StreamResult<Option<u64>> {
        let writers = self.writers.lock();
        if writers.ended {
            return Err(unknown_scope());
        }
        Ok(writers.by_stream.get(stream_id).copied())
    }

    /// The scope's writer for `stream`, which the first call opens.
    pub(crate) async fn writer(&self, shared: &Shared, stream: &StreamRef) -> StreamResult<u64> {
        if let Some(id) = self.cached(&stream.id)? {
            return Ok(id);
        }
        let opening = self.opening_lock(&stream.id);
        let _opening = opening.lock().await;
        if let Some(id) = self.cached(&stream.id)? {
            return Ok(id);
        }
        let request = OpenWriterRequest {
            namespace: self.namespace.clone(),
            stream: Some(stream.clone()),
            producer: Some(self.producer.clone()),
            previous_completed_task: self.previous_completed_task,
            failed_tasks: self.failed_tasks.clone(),
            ..Default::default()
        };
        let service = shared.service().await?;
        let id = shared.writers.open(service, request, Holder::Scope).await?;
        let mut writers = self.writers.lock();
        if writers.ended {
            drop(writers);
            shared.writers.free(id, Holder::Scope)?;
            return Err(unknown_scope());
        }
        if let Some(existing) = writers.by_stream.get(&stream.id).copied() {
            drop(writers);
            // An open that waited on an older lock got there first, so this handle goes.
            shared.writers.free(id, Holder::Scope)?;
            return Ok(existing);
        }
        writers.by_stream.insert(stream.id.clone(), id);
        Ok(id)
    }

    pub(crate) fn add_buffer(&self, id: u64) -> StreamResult<()> {
        let mut writers = self.writers.lock();
        if writers.closing {
            return Err(unknown_scope());
        }
        writers.buffers.push(id);
        Ok(())
    }

    pub(crate) fn remove_buffer(&self, id: u64) {
        self.writers.lock().buffers.retain(|buffer| *buffer != id);
    }

    /// Starts the close of the scope and returns its first buffer, which the caller ends.
    fn first_buffer(&self) -> Option<u64> {
        let mut writers = self.writers.lock();
        writers.closing = true;
        writers.buffers.first().copied()
    }

    /// Starts the close of the scope and returns its buffers, which the caller frees.
    fn take_buffers(&self) -> Vec<u64> {
        let mut writers = self.writers.lock();
        writers.closing = true;
        std::mem::take(&mut writers.buffers)
    }

    /// Ends the scope and returns its writers, which the caller frees.
    fn end(&self) -> Vec<u64> {
        let mut writers = self.writers.lock();
        writers.ended = true;
        self.opening.lock().clear();
        writers.by_stream.drain().map(|(_, id)| id).collect()
    }

    fn opening_lock(&self, stream_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut opening = self.opening.lock();
        opening.entry(stream_id.to_string()).or_default().clone()
    }

    /// Ends the scope and takes one of its writers, which the caller closes.
    fn take_writer(&self) -> Option<u64> {
        let mut writers = self.writers.lock();
        writers.ended = true;
        let stream = writers.by_stream.keys().next()?.clone();
        writers.by_stream.remove(&stream)
    }
}

/// Frees what a scope still holds when the call that opens or ends it fails or is dropped. A
/// shutdown puts its scope back in the table instead, so that the next shutdown ends it.
struct Leftovers<'a> {
    shared: &'a Shared,
    scope: &'a Scope,
    requeue: Option<(u64, Arc<Scope>)>,
    armed: bool,
}

impl Drop for Leftovers<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        match self.requeue.take() {
            Some((id, scope)) => {
                self.shared.scopes.table.lock().insert(id, scope);
            }
            None => self.shared.release(self.scope),
        }
    }
}

fn unknown_scope() -> StreamError {
    StreamError::new(Reason::UnknownWriter, "No scope has the id")
}

pub(crate) struct Scopes {
    next: AtomicU64,
    table: Mutex<HashMap<u64, Arc<Scope>>>,
    /// The runtime's random key. A client scope with no name takes `<key>-<n>`.
    client_key: String,
    clients: AtomicU64,
}

impl Default for Scopes {
    fn default() -> Self {
        Self {
            next: AtomicU64::new(0),
            table: Mutex::default(),
            client_key: random_key(),
            clients: AtomicU64::new(0),
        }
    }
}

/// Random, so that two processes, or a forked child and its parent, never share a client Producer.
fn random_key() -> String {
    let half = |salt: u64| {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        hasher.write_u128(now.as_nanos());
        hasher.write_u32(std::process::id());
        hasher.write_u64(salt);
        hasher.finish()
    };
    format!("{:016x}{:016x}", half(1), half(2))
}

impl Scopes {
    fn insert(&self, scope: Scope) -> (u64, Arc<Scope>) {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let scope = Arc::new(scope);
        self.table.lock().insert(id, scope.clone());
        (id, scope)
    }

    pub(crate) fn get(&self, id: u64) -> StreamResult<Arc<Scope>> {
        self.table
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(unknown_scope)
    }

    fn remove(&self, id: u64) -> StreamResult<Arc<Scope>> {
        self.table.lock().remove(&id).ok_or_else(unknown_scope)
    }

    fn take_any(&self) -> Option<(u64, Arc<Scope>)> {
        let mut table = self.table.lock();
        let id = *table.keys().next()?;
        table.remove(&id).map(|scope| (id, scope))
    }
}

impl Shared {
    pub(crate) async fn open_activity_scope(
        &self,
        request: OpenActivityScopeRequest,
    ) -> StreamResult<OpenScopeResponse> {
        let context = request
            .context
            .ok_or_else(|| StreamError::invalid("The request has no Activity context"))?;
        let attempt = context.attempt;
        let derived = derive_producer(DeriveProducerRequest {
            context: Some(context),
            purpose: ProducerPurpose::ActivityWriter as i32,
        })?;
        let producer = derived.producer.unwrap_or_default();
        let scope = Scope::new(
            request.namespace,
            producer.clone(),
            derived.previous_completed_task,
            derived.failed_tasks,
            true,
        );
        if attempt > 1 {
            let mut leftovers = Leftovers {
                shared: self,
                scope: &scope,
                requeue: None,
                armed: true,
            };
            let mut seen = HashSet::new();
            for stream in request.streams.iter().filter(|s| seen.insert(s.id.clone())) {
                // The writer's open ignores a missing, closed or expired Stream.
                scope.writer(self, stream).await?;
            }
            leftovers.armed = false;
        }
        // Lang gets no id from a dropped call, so the scope joins the table only at the end.
        let scope_id = self.admit(scope)?;
        Ok(OpenScopeResponse {
            scope_id,
            producer: Some(producer),
        })
    }

    pub(crate) fn open_client_scope(
        &self,
        request: OpenClientScopeRequest,
    ) -> StreamResult<OpenScopeResponse> {
        let (id, attempt) = if request.name.is_empty() {
            let n = self.scopes.clients.fetch_add(1, Ordering::Relaxed) + 1;
            let key = format!("{}-{n}", self.scopes.client_key);
            (ids::client_producer_id(&request.identity, &key), 1)
        } else if request.epoch < 1 {
            return Err(StreamError::invalid(
                "A named Producer's epoch is 1 or more",
            ));
        } else {
            (ids::named_client_producer_id(&request.name), request.epoch)
        };
        let producer = Producer {
            kind: StreamProducerKind::Client as i32,
            id,
            epoch: Some(ProducerEpoch {
                attempt,
                ..Default::default()
            }),
            run_id: String::new(),
        };
        ids::check_producer(&producer)?;
        let scope_id = self.admit(Scope::new(
            request.namespace,
            producer.clone(),
            None,
            vec![],
            false,
        ))?;
        Ok(OpenScopeResponse {
            scope_id,
            producer: Some(producer),
        })
    }

    /// Adds a scope to the table. A shutdown that already drained the table frees it instead.
    fn admit(&self, scope: Scope) -> StreamResult<u64> {
        let (scope_id, scope) = self.scopes.insert(scope);
        if self.is_shut_down() {
            if self.scopes.remove(scope_id).is_ok() {
                self.release(&scope);
            }
            return Err(crate::runtime::shut_down());
        }
        Ok(scope_id)
    }

    pub(crate) async fn publish(&self, request: PublishRequest) -> StreamResult<PublishResponse> {
        let scope = self.scopes.get(request.scope_id)?;
        let stream = request
            .stream
            .ok_or_else(|| StreamError::invalid("The request has no Stream"))?;
        let writer = scope.writer(self, &stream).await?;
        let (first_offset, last_offset) = self.writers.write(writer, request.records).await?;
        Ok(PublishResponse {
            first_offset,
            last_offset,
        })
    }

    pub(crate) async fn close_stream_as(
        &self,
        request: CloseStreamAsRequest,
    ) -> StreamResult<CloseStreamResponse> {
        let scope = self.scopes.get(request.scope_id)?;
        if scope.activity {
            return Err(StreamError::new(
                Reason::NotOwner,
                "An Activity doesn't close a Stream. Its Workflow or a client closes it",
            ));
        }
        let mut close = request
            .close
            .ok_or_else(|| StreamError::invalid("The request has no close"))?;
        let stream_id = close
            .stream
            .as_ref()
            .map(|s| s.id.clone())
            .unwrap_or_default();
        // An open of the Stream waits, so that it can't share the writer while the close ends it.
        let opening = scope.opening_lock(&stream_id);
        let _opening = opening.lock().await;
        // Closing the writer first puts this Producer's Records before the close Record.
        let writer = scope.writers.lock().by_stream.remove(&stream_id);
        if let Some(writer) = writer {
            self.writers.close(writer, Holder::Scope).await?;
        }
        close.namespace = scope.namespace.clone();
        close.producer = Some(scope.producer.clone());
        let closed = self.service().await?.close_stream(close).await;
        scope.opening.lock().remove(&stream_id);
        closed
    }

    pub(crate) async fn close_scope(&self, scope_id: u64) -> StreamResult<CloseScopeResponse> {
        let scope = self.scopes.remove(scope_id)?;
        Ok(self.end_scope(&scope, None).await)
    }

    /// Flushes and frees the scope's buffers first, since their writes need its writers.
    async fn end_scope(
        &self,
        scope: &Scope,
        requeue: Option<(u64, Arc<Scope>)>,
    ) -> CloseScopeResponse {
        let mut leftovers = Leftovers {
            shared: self,
            scope,
            requeue,
            armed: true,
        };
        let mut response = CloseScopeResponse::default();
        while let Some(buffer) = scope.first_buffer() {
            let failed = self.end_buffer(buffer).await;
            scope.remove_buffer(buffer);
            if let Some(failure) = failed {
                response.failed_buffers += 1;
                response.failure.get_or_insert(failure);
            }
        }
        while let Some(writer) = scope.take_writer() {
            if let Err(error) = self.writers.close(writer, Holder::Scope).await {
                response.failed_writers += 1;
                response.failure.get_or_insert_with(|| error.failure());
            }
        }
        leftovers.armed = false;
        response
    }

    pub(crate) fn free_scope(&self, scope_id: u64) -> StreamResult<()> {
        let scope = self.scopes.remove(scope_id)?;
        self.release(&scope);
        Ok(())
    }

    /// Releases what the scope holds, with no wait.
    fn release(&self, scope: &Scope) {
        for buffer in scope.take_buffers() {
            // Lang may have freed the buffer already.
            let _ = self.buffers.free(buffer);
        }
        for writer in scope.end() {
            let _ = self.writers.free(writer, Holder::Scope);
        }
    }

    /// Closes every scope, then every writer that lang opened with `OpenWriter`.
    pub(crate) async fn close_everything(&self) -> ShutdownResponse {
        let mut response = ShutdownResponse::default();
        // One scope at a time, so that a dropped shutdown leaves the rest for the next one.
        while let Some((id, scope)) = self.scopes.take_any() {
            let requeue = Some((id, scope.clone()));
            let closed = self.end_scope(&scope, requeue).await;
            response.failed_writers += closed.failed_writers;
            response.failed_buffers += closed.failed_buffers;
        }
        response.failed_writers += self.writers.close_all().await as i32;
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStreamRuntime, StreamRuntime,
        memory::MemoryStore,
        proto::{
            ActivityContext, AppendRecord, CloseScopeRequest, CloseScopeResponse,
            CreateStreamRequest, FreeBufferRequest, FreeScopeRequest, OpenBufferRequest,
            OpenBufferResponse, ShutdownRequest,
        },
        testing::support::Flaky,
    };
    use prost::Message;
    use std::time::Duration;

    #[test]
    fn each_runtime_has_its_own_client_key() {
        let (a, b) = (Scopes::default(), Scopes::default());
        assert_eq!(a.client_key.len(), 32);
        assert_ne!(a.client_key, b.client_key);
    }

    /// A runtime on a Store whose appends fail while it is down, with the Streams `a` and `b`.
    async fn flaky() -> (Arc<Flaky>, Arc<StreamRuntime>) {
        let memory = MemoryStore::new(Duration::from_secs(3600), Duration::from_secs(7200));
        let store = Arc::new(Flaky::new(Arc::new(memory)));
        let ext = MemoryStreamRuntime::with_store(store.clone(), None);
        for stream in ["a", "b"] {
            let create = CreateStreamRequest {
                namespace: "ns".into(),
                stream_id: stream.into(),
                ..Default::default()
            };
            ext.call("CreateStream", &create.encode_to_vec())
                .await
                .unwrap();
        }
        (store, ext)
    }

    fn stream(id: &str) -> Option<StreamRef> {
        Some(StreamRef {
            id: id.into(),
            ..Default::default()
        })
    }

    fn publish(scope_id: u64, id: &str) -> Vec<u8> {
        PublishRequest {
            scope_id,
            stream: stream(id),
            records: vec![AppendRecord::default()],
        }
        .encode_to_vec()
    }

    fn retry_scope() -> Vec<u8> {
        retry_scope_on(&["a"])
    }

    /// The open of attempt 2 of an Activity, which opens a writer on each of `streams`.
    fn retry_scope_on(streams: &[&str]) -> Vec<u8> {
        OpenActivityScopeRequest {
            namespace: "ns".into(),
            context: Some(ActivityContext {
                in_workflow: true,
                workflow_run_id: "r1".into(),
                activity_id: "a".into(),
                attempt: 2,
                ..Default::default()
            }),
            streams: streams.iter().map(|id| stream(id).unwrap()).collect(),
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn a_dropped_scope_close_frees_its_writers() {
        let (store, ext) = flaky().await;
        let open = OpenClientScopeRequest {
            namespace: "ns".into(),
            ..Default::default()
        };
        let opened = OpenScopeResponse::decode(
            ext.call_sync("OpenClientScope", &open.encode_to_vec())
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        for id in ["a", "b"] {
            ext.call("Publish", &publish(opened.scope_id, id))
                .await
                .unwrap();
        }
        // A write in flight on each writer makes the close wait on the first one it closes.
        store.down.store(true, Ordering::SeqCst);
        let writes: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|id| {
                let (ext, input) = (ext.clone(), publish(opened.scope_id, id));
                tokio::spawn(async move { ext.call("Publish", &input).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let close = CloseScopeRequest {
            scope_id: opened.scope_id,
        };
        let input = close.encode_to_vec();
        let dropped =
            tokio::time::timeout(Duration::from_millis(200), ext.call("CloseScope", &input));
        assert!(dropped.await.is_err());
        assert_eq!(ext.shared().writers.open_count(), 0);
        store.down.store(false, Ordering::SeqCst);
        for write in writes {
            let _ = write.await.unwrap();
        }
    }

    /// A client scope with a writer on `a` and on `b`, each with a write that waits while the
    /// Store is down, so that a close of the scope waits on the first writer that it closes.
    async fn scope_with_blocked_writes(
        store: &Flaky,
        ext: &Arc<StreamRuntime>,
    ) -> (u64, Vec<tokio::task::JoinHandle<StreamResult<Vec<u8>>>>) {
        let open = OpenClientScopeRequest {
            namespace: "ns".into(),
            ..Default::default()
        };
        let opened = OpenScopeResponse::decode(
            ext.call_sync("OpenClientScope", &open.encode_to_vec())
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        for id in ["a", "b"] {
            ext.call("Publish", &publish(opened.scope_id, id))
                .await
                .unwrap();
        }
        store.down.store(true, Ordering::SeqCst);
        let writes = ["a", "b"]
            .into_iter()
            .map(|id| {
                let (ext, input) = (ext.clone(), publish(opened.scope_id, id));
                tokio::spawn(async move { ext.call("Publish", &input).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50)).await;
        (opened.scope_id, writes)
    }

    #[tokio::test]
    async fn close_writers_leaves_the_writers_of_a_scope_that_is_closing() {
        let (store, ext) = flaky().await;
        let (scope_id, writes) = scope_with_blocked_writes(&store, &ext).await;
        let close = tokio::spawn({
            let ext = ext.clone();
            let input = CloseScopeRequest { scope_id }.encode_to_vec();
            async move { ext.call("CloseScope", &input).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let close_writers = tokio::spawn({
            let ext = ext.clone();
            async move { ext.call("CloseWriters", &[]).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        store.down.store(false, Ordering::SeqCst);
        let closed = CloseScopeResponse::decode(close.await.unwrap().unwrap().as_slice()).unwrap();
        assert_eq!(closed, CloseScopeResponse::default());
        close_writers.await.unwrap().unwrap();
        for write in writes {
            write.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn a_shutdown_leaves_the_writers_of_a_scope_that_is_closing() {
        let (store, ext) = flaky().await;
        let (scope_id, writes) = scope_with_blocked_writes(&store, &ext).await;
        let close = tokio::spawn({
            let ext = ext.clone();
            let input = CloseScopeRequest { scope_id }.encode_to_vec();
            async move { ext.call("CloseScope", &input).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let shutdown = ShutdownRequest {}.encode_to_vec();
        ext.call("Shutdown", &shutdown).await.unwrap();
        store.down.store(false, Ordering::SeqCst);
        let closed = CloseScopeResponse::decode(close.await.unwrap().unwrap().as_slice()).unwrap();
        assert_eq!(closed, CloseScopeResponse::default());
        for write in writes {
            write.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn a_free_of_what_a_running_shutdown_took_answers_ok() {
        let (store, ext) = flaky().await;
        let (scope_id, writes) = scope_with_blocked_writes(&store, &ext).await;
        let shutdown = tokio::spawn({
            let ext = ext.clone();
            async move {
                ext.call("Shutdown", &ShutdownRequest {}.encode_to_vec())
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let free = FreeScopeRequest { scope_id }.encode_to_vec();
        ext.call_sync("FreeScope", &free).unwrap();
        ext.free_writer(u64::MAX).unwrap();
        store.down.store(false, Ordering::SeqCst);
        shutdown.await.unwrap().unwrap();
        for write in writes {
            let _ = write.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_dropped_shutdown_runs_again() {
        let (store, ext) = flaky().await;
        let (_, writes) = scope_with_blocked_writes(&store, &ext).await;
        let shutdown = ShutdownRequest {}.encode_to_vec();
        let dropped =
            tokio::time::timeout(Duration::from_millis(200), ext.call("Shutdown", &shutdown));
        assert!(dropped.await.is_err());
        store.down.store(false, Ordering::SeqCst);
        ext.call("Shutdown", &shutdown).await.unwrap();
        assert!(ext.shared().scopes.table.lock().is_empty());
        assert_eq!(ext.shared().writers.open_count(), 0);
        for write in writes {
            let _ = write.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_freed_buffer_leaves_its_scope() {
        let (_, ext) = flaky().await;
        let open = OpenClientScopeRequest::default().encode_to_vec();
        let scope_id =
            OpenScopeResponse::decode(ext.call_sync("OpenClientScope", &open).unwrap().as_slice())
                .unwrap()
                .scope_id;
        for _ in 0..3 {
            let open = OpenBufferRequest {
                scope_id,
                stream: stream("a"),
                ..Default::default()
            };
            let opened = OpenBufferResponse::decode(
                ext.call_sync("OpenBuffer", &open.encode_to_vec())
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let free = FreeBufferRequest {
                buffer_id: opened.buffer_id,
            };
            ext.call_sync("FreeBuffer", &free.encode_to_vec()).unwrap();
        }
        let scope = ext.shared().scopes.get(scope_id).unwrap();
        assert!(scope.writers.lock().buffers.is_empty());
    }

    #[tokio::test]
    async fn a_dropped_activity_scope_open_leaves_no_scope() {
        let (store, ext) = flaky().await;
        // The writer on `a` opens, and the one on `b` waits while the Store is down.
        store.down_after.store(1, Ordering::SeqCst);
        let input = retry_scope_on(&["a", "b"]);
        let dropped = tokio::time::timeout(
            Duration::from_millis(200),
            ext.call("OpenActivityScope", &input),
        );
        assert!(dropped.await.is_err());
        assert!(ext.shared().scopes.table.lock().is_empty());
        assert_eq!(ext.shared().writers.open_count(), 0);
    }

    #[tokio::test]
    async fn close_writers_leaves_the_writers_of_a_scope_that_is_opening() {
        let (store, ext) = flaky().await;
        store.down_after.store(1, Ordering::SeqCst);
        let open = tokio::spawn({
            let ext = ext.clone();
            async move {
                ext.call("OpenActivityScope", &retry_scope_on(&["a", "b"]))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        ext.call("CloseWriters", &[]).await.unwrap();
        store.down.store(false, Ordering::SeqCst);
        let opened = OpenScopeResponse::decode(open.await.unwrap().unwrap().as_slice()).unwrap();
        ext.call("Publish", &publish(opened.scope_id, "a"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn an_open_that_ends_after_the_shutdown_is_refused() {
        let (store, ext) = flaky().await;
        store.down.store(true, Ordering::SeqCst);
        let open = tokio::spawn({
            let ext = ext.clone();
            async move { ext.call("OpenActivityScope", &retry_scope()).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let shutdown = ShutdownRequest {}.encode_to_vec();
        ext.call("Shutdown", &shutdown).await.unwrap();
        store.down.store(false, Ordering::SeqCst);
        let error = open.await.unwrap().unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        assert!(ext.shared().scopes.table.lock().is_empty());
        assert_eq!(ext.shared().writers.open_count(), 0);
    }
}
