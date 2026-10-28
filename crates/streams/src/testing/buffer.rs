//! Buffered publishers, with and without lang's codec pump, through the runtime's byte call, as
//! lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{
    case,
    support::{Flaky, Needs, stores_for},
};
use crate::{
    MemoryStreamRuntime, StreamError, StreamRuntime,
    proto::{
        AppendRecord, BufferBatch, BufferFlushRequest, BufferFlushResponse, BufferNextBatchRequest,
        BufferPushRequest, BufferPushResponse, BufferRecords, BufferWriteRequest,
        BufferWriteResponse, CloseScopeRequest, CloseScopeResponse, CloseStreamAsRequest,
        CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
        FreeBufferRequest, FreeResponse, OpenBufferRequest, OpenBufferResponse,
        OpenClientScopeRequest, OpenScopeResponse, ReadRecordsRequest, ReadRecordsResponse,
        ShutdownRequest, ShutdownResponse, StreamErrorReason, StreamRef,
        buffer_write_request::Body, stream_record::Item,
    },
};
use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone)]
struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
}

async fn case_on(ext: Arc<StreamRuntime>) -> Case {
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let case = Case {
        ext,
        namespace: format!("ns-{now}-{n}"),
        stream: format!("s-{now}-{n}"),
    };
    let _: CreateStreamResponse = case
        .call(
            "CreateStream",
            CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_id: case.stream.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    case
}

async fn cases() -> Vec<Case> {
    let mut cases = vec![];
    for ext in super::support::stream_runtimes() {
        cases.push(case_on(ext).await);
    }
    cases
}

fn millis(ms: u64) -> Option<prost_types::Duration> {
    let duration = Duration::from_millis(ms);
    Some(prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

fn records(texts: &[&str]) -> Vec<AppendRecord> {
    texts
        .iter()
        .map(|text| AppendRecord {
            body: Some(Payload {
                data: text.as_bytes().to_vec(),
                ..Default::default()
            }),
        })
        .collect()
}

impl Case {
    async fn call<Req: Message, Resp: Message + Default>(
        &self,
        method: &str,
        request: Req,
    ) -> Result<Resp, StreamError> {
        let bytes = self.ext.call(method, &request.encode_to_vec()).await?;
        Ok(Resp::decode(bytes.as_slice()).unwrap())
    }

    fn call_sync<Req: Message, Resp: Message + Default>(
        &self,
        method: &str,
        request: Req,
    ) -> Result<Resp, StreamError> {
        let bytes = self.ext.call_sync(method, &request.encode_to_vec())?;
        Ok(Resp::decode(bytes.as_slice()).unwrap())
    }

    fn stream_ref(&self) -> StreamRef {
        StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        }
    }

    fn scope(&self) -> u64 {
        let opened: OpenScopeResponse = self
            .call_sync(
                "OpenClientScope",
                OpenClientScopeRequest {
                    namespace: self.namespace.clone(),
                    identity: "me".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        opened.scope_id
    }

    fn buffer(&self, scope_id: u64, encoded: bool, delay_ms: u64, max_batch: i32) -> u64 {
        let opened: OpenBufferResponse = self
            .call_sync(
                "OpenBuffer",
                OpenBufferRequest {
                    scope_id,
                    stream: Some(self.stream_ref()),
                    max_delay: millis(delay_ms),
                    max_batch,
                    encoded,
                },
            )
            .unwrap();
        opened.buffer_id
    }

    fn push(&self, buffer_id: u64, texts: &[&str]) -> Result<(), StreamError> {
        let _: BufferPushResponse = self.call_sync(
            "BufferPush",
            BufferPushRequest {
                buffer_id,
                records: records(texts),
            },
        )?;
        Ok(())
    }

    async fn flush(&self, buffer_id: u64) -> BufferFlushResponse {
        self.call("BufferFlush", BufferFlushRequest { buffer_id })
            .await
            .unwrap()
    }

    async fn next_batch(&self, buffer_id: u64) -> BufferBatch {
        self.call("BufferNextBatch", BufferNextBatchRequest { buffer_id })
            .await
            .unwrap()
    }

    async fn write(&self, buffer_id: u64, batch_id: u64, body: Body) {
        let _: BufferWriteResponse = self
            .call(
                "BufferWrite",
                BufferWriteRequest {
                    buffer_id,
                    batch_id,
                    body: Some(body),
                },
            )
            .await
            .unwrap();
    }

    /// Lang's pump, with a codec that writes each body in upper case. Returns the batch count.
    async fn pump(&self, buffer_id: u64, batches: Arc<AtomicUsize>) {
        loop {
            let batch = self.next_batch(buffer_id).await;
            if batch.done {
                return;
            }
            batches.fetch_add(1, Ordering::SeqCst);
            let mut records = batch.records;
            for record in &mut records {
                record.body.as_mut().unwrap().data.make_ascii_uppercase();
            }
            let body = Body::Records(BufferRecords { records });
            self.write(buffer_id, batch.batch_id, body).await;
        }
    }

    async fn close_scope(&self, scope_id: u64) -> CloseScopeResponse {
        self.call("CloseScope", CloseScopeRequest { scope_id })
            .await
            .unwrap()
    }

    async fn close_stream(&self) {
        let other = self.scope();
        let _: CloseStreamResponse = self
            .call(
                "CloseStreamAs",
                CloseStreamAsRequest {
                    scope_id: other,
                    close: Some(CloseStreamRequest {
                        stream: Some(self.stream_ref()),
                        ..Default::default()
                    }),
                },
            )
            .await
            .unwrap();
    }

    async fn texts(&self) -> Vec<String> {
        let read: ReadRecordsResponse = self
            .call(
                "ReadRecords",
                ReadRecordsRequest {
                    namespace: self.namespace.clone(),
                    stream: Some(self.stream_ref()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        read.records
            .into_iter()
            .filter_map(|record| match record.item {
                Some(Item::Data(data)) => Some(String::from_utf8(data.body.unwrap().data).unwrap()),
                _ => None,
            })
            .collect()
    }

    /// The texts once the Stream has `count` of them, within a few seconds.
    async fn texts_when(&self, count: usize) -> Vec<String> {
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let texts = self.texts().await;
            if texts.len() >= count || Instant::now() > until {
                return texts;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// An encoded buffer writes its values in one batch at most `max_delay` after the first one, and
/// a flush returns the last offset.
async fn an_encoded_buffer_writes_its_batch_after_the_delay() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), true, 200, 0);
        case.push(buffer, &["a", "b"]).unwrap();
        case.push(buffer, &["c"]).unwrap();
        assert!(case.texts().await.is_empty());
        assert_eq!(case.texts_when(3).await, ["a", "b", "c"]);
        case.push(buffer, &["d"]).unwrap();
        let flushed = case.flush(buffer).await;
        assert!(flushed.last_offset > 0 && flushed.failure.is_none());
        assert_eq!(case.texts().await, ["a", "b", "c", "d"]);
    }
}

/// A full batch goes at once, with no wait for the delay.
async fn a_full_batch_goes_at_once() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), true, 60_000, 2);
        case.push(buffer, &["1", "2", "3"]).unwrap();
        assert_eq!(case.texts_when(2).await[..2], ["1", "2"]);
        assert_eq!(case.flush(buffer).await.unwritten, 0);
        assert_eq!(case.texts().await, ["1", "2", "3"]);
    }
}

/// Lang's pump runs its codec once on each batch, and a flush makes the batch due at once.
async fn a_pump_runs_the_codec_once_per_batch() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), false, 60_000, 0);
        let batches = Arc::new(AtomicUsize::new(0));
        let pump = tokio::spawn({
            let (case, batches) = (case.clone(), batches.clone());
            async move { case.pump(buffer, batches).await }
        });
        case.push(buffer, &["a", "b", "c"]).unwrap();
        let flushed = case.flush(buffer).await;
        assert!(flushed.last_offset > 0);
        assert_eq!(case.texts().await, ["A", "B", "C"]);
        assert_eq!(batches.load(Ordering::SeqCst), 1);
        let _: FreeResponse = case
            .call_sync("FreeBuffer", FreeBufferRequest { buffer_id: buffer })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), pump)
            .await
            .unwrap()
            .unwrap();
    }
}

/// After a failed batch, the buffer writes nothing more. Each later push fails, and a flush gives
/// the failure and the unwritten count.
async fn a_failed_batch_fails_every_later_call() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), true, 50, 0);
        case.close_stream().await;
        case.push(buffer, &["a", "b"]).unwrap();
        let flushed = case.flush(buffer).await;
        assert_eq!(flushed.failure.unwrap().reason(), StreamErrorReason::Closed);
        assert_eq!((flushed.unwritten, flushed.last_offset), (2, 0));
        let error = case.push(buffer, &["c"]).unwrap_err();
        assert_eq!(error.reason.token(), "stream_closed");
    }
}

/// A codec failure fails the buffer with its message.
async fn a_codec_failure_fails_the_buffer() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), false, 60_000, 0);
        case.push(buffer, &["a"]).unwrap();
        let flush = case.flush(buffer);
        let pump = async {
            let batch = case.next_batch(buffer).await;
            let body = Body::CodecError("the key is gone".into());
            case.write(buffer, batch.batch_id, body).await;
            assert!(case.next_batch(buffer).await.done);
        };
        let (flushed, ()) = tokio::join!(flush, pump);
        let failure = flushed.failure.unwrap();
        assert_eq!(failure.reason(), StreamErrorReason::Invalid);
        assert!(failure.message.contains("the key is gone"));
        assert!(case.texts().await.is_empty());
    }
}

/// The close of a scope writes its buffers' values first. It reports a buffer failure that no
/// push or flush gave lang yet.
async fn a_scope_close_flushes_its_buffers() {
    for case in cases().await {
        let scope = case.scope();
        let buffer = case.buffer(scope, true, 60_000, 0);
        case.push(buffer, &["a", "b"]).unwrap();
        let closed = case.close_scope(scope).await;
        assert_eq!(closed, CloseScopeResponse::default());
        assert_eq!(case.texts().await, ["a", "b"]);
        let error = case.push(buffer, &["c"]).unwrap_err();
        assert_eq!(error.reason.token(), "stream_unknown_writer");

        let scope = case.scope();
        let buffer = case.buffer(scope, true, 60_000, 0);
        case.push(buffer, &["late"]).unwrap();
        case.close_stream().await;
        let closed = case.close_scope(scope).await;
        assert_eq!(closed.failed_buffers, 1);
        assert_eq!(closed.failure.unwrap().reason(), StreamErrorReason::Closed);
    }
}

/// A buffer failure that lang already got doesn't fail the scope's close again.
async fn a_reported_buffer_failure_doesnt_count_at_the_scope_close() {
    for case in cases().await {
        let scope = case.scope();
        let buffer = case.buffer(scope, true, 50, 0);
        case.close_stream().await;
        case.push(buffer, &["a"]).unwrap();
        assert!(case.flush(buffer).await.failure.is_some());
        assert_eq!(case.close_scope(scope).await.failed_buffers, 0);
    }
}

/// The shutdown writes every buffer's values. A second runtime on the same Store reads them, since
/// the first one refuses every call after it.
async fn the_shutdown_writes_every_buffer() {
    for target in stores_for(Needs::CALLS).await {
        let case = case_on(MemoryStreamRuntime::with_store(target.store.clone(), None)).await;
        let buffer = case.buffer(case.scope(), true, 60_000, 0);
        case.push(buffer, &["a"]).unwrap();
        let shut: ShutdownResponse = case.call("Shutdown", ShutdownRequest {}).await.unwrap();
        assert_eq!((shut.failed_writers, shut.failed_buffers), (0, 0));
        let reader = Case {
            ext: MemoryStreamRuntime::with_store(target.store, None),
            ..case
        };
        assert_eq!(reader.texts().await, ["a"]);
    }
}

/// The runtime refuses a pump call on an encoded buffer, and a write of a batch that isn't in
/// flight or that a write already took.
async fn the_pump_calls_are_checked() {
    for case in cases().await {
        let scope = case.scope();
        let encoded = case.buffer(scope, true, 60_000, 0);
        let next = case.call::<_, BufferBatch>(
            "BufferNextBatch",
            BufferNextBatchRequest { buffer_id: encoded },
        );
        let error = tokio::time::timeout(Duration::from_secs(5), next).await;
        assert_eq!(error.unwrap().unwrap_err().reason.token(), "stream_invalid");
        let buffer = case.buffer(scope, false, 0, 0);
        case.push(buffer, &["a"]).unwrap();
        let batch = case.next_batch(buffer).await;
        let write = |batch_id| BufferWriteRequest {
            buffer_id: buffer,
            batch_id,
            body: Some(Body::Records(BufferRecords {
                records: records(&["a"]),
            })),
        };
        let wrong = case
            .call::<_, BufferWriteResponse>("BufferWrite", write(batch.batch_id + 1))
            .await;
        assert_eq!(wrong.unwrap_err().reason.token(), "stream_invalid");
        let _: BufferWriteResponse = case
            .call("BufferWrite", write(batch.batch_id))
            .await
            .unwrap();
        let again = case
            .call::<_, BufferWriteResponse>("BufferWrite", write(batch.batch_id))
            .await;
        assert_eq!(again.unwrap_err().reason.token(), "stream_invalid");
        assert_eq!(case.texts().await, ["a"]);
    }
}

/// The shutdown waits for lang's pump to write the values of its buffer.
async fn the_shutdown_waits_for_lang_s_pump() {
    for target in stores_for(Needs::CALLS).await {
        let case = case_on(MemoryStreamRuntime::with_store(target.store.clone(), None)).await;
        let buffer = case.buffer(case.scope(), false, 60_000, 0);
        let pump = tokio::spawn({
            let case = case.clone();
            async move { case.pump(buffer, Arc::default()).await }
        });
        case.push(buffer, &["a"]).unwrap();
        let shutdown = case.call::<_, ShutdownResponse>("Shutdown", ShutdownRequest {});
        let shut = tokio::time::timeout(Duration::from_secs(5), shutdown).await;
        assert_eq!(shut.unwrap().unwrap().failed_buffers, 0);
        tokio::time::timeout(Duration::from_secs(5), pump)
            .await
            .unwrap()
            .unwrap();
        let reader = Case {
            ext: MemoryStreamRuntime::with_store(target.store, None),
            ..case
        };
        assert_eq!(reader.texts().await, ["A"]);
    }
}

/// A `BufferWrite` of the one value `a`, for batch `batch_id`.
fn write_a(buffer_id: u64, batch_id: u64) -> BufferWriteRequest {
    BufferWriteRequest {
        buffer_id,
        batch_id,
        body: Some(Body::Records(BufferRecords {
            records: records(&["a"]),
        })),
    }
}

/// A `BufferWrite` that a cancel drops while the Store is down still writes its batch once the
/// Store is back, so a flush doesn't wait for it forever. A freed buffer gives its pump `done`.
async fn a_dropped_write_still_settles_its_batch() {
    for (store, case) in flaky_cases(-1).await {
        let buffer = case.buffer(case.scope(), false, 0, 0);
        case.push(buffer, &["a"]).unwrap();
        let batch = case.next_batch(buffer).await;
        store.down.store(true, Ordering::SeqCst);
        let write =
            case.call::<_, BufferWriteResponse>("BufferWrite", write_a(buffer, batch.batch_id));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), write)
                .await
                .is_err()
        );
        store.down.store(false, Ordering::SeqCst);
        let flushed = tokio::time::timeout(Duration::from_secs(5), case.flush(buffer)).await;
        assert!(flushed.unwrap().last_offset > 0);
        assert_eq!(case.texts().await, ["a"]);
        let _: FreeResponse = case
            .call_sync("FreeBuffer", FreeBufferRequest { buffer_id: buffer })
            .unwrap();
        assert!(case.next_batch(buffer).await.done);
    }
}

/// A write that panics, with its `BufferWrite` dropped, still fails its batch, so a flush ends.
async fn a_panicking_write_fails_its_batch() {
    for (store, case) in flaky_cases(-1).await {
        let buffer = case.buffer(case.scope(), false, 0, 0);
        case.push(buffer, &["a"]).unwrap();
        let batch = case.next_batch(buffer).await;
        store.panic_next.store(true, Ordering::SeqCst);
        let write =
            case.call::<_, BufferWriteResponse>("BufferWrite", write_a(buffer, batch.batch_id));
        // The write's first poll spawns the write task, and the select drops the call at once.
        tokio::select! {
            biased;
            _ = write => panic!("the write answered in its first poll"),
            _ = std::future::ready(()) => {}
        }
        let flushed = tokio::time::timeout(Duration::from_secs(5), case.flush(buffer)).await;
        let flushed = flushed.unwrap();
        assert_eq!(
            flushed.failure.unwrap().reason(),
            StreamErrorReason::Storage
        );
        assert_eq!(flushed.unwritten, 1);
    }
}

/// The runtime's own pump fails a batch whose write panics, so a flush ends.
async fn an_encoded_pump_whose_write_panics_fails_its_batch() {
    for (store, case) in flaky_cases(-1).await {
        let buffer = case.buffer(case.scope(), true, 60_000, 0);
        store.panic_next.store(true, Ordering::SeqCst);
        case.push(buffer, &["a"]).unwrap();
        let flushed = tokio::time::timeout(Duration::from_secs(5), case.flush(buffer)).await;
        let flushed = flushed.unwrap();
        assert_eq!(
            flushed.failure.unwrap().reason(),
            StreamErrorReason::Storage
        );
        assert_eq!(flushed.unwritten, 1);
    }
}

/// A free still works while a shutdown runs, so lang can end a buffer whose pump is gone.
async fn a_free_ends_a_shutdown_whose_pump_is_gone() {
    for (_, case) in flaky_cases(-1).await {
        let buffer = case.buffer(case.scope(), false, 60_000, 0);
        case.push(buffer, &["a"]).unwrap();
        let shutdown = tokio::spawn({
            let case = case.clone();
            async move {
                case.call::<_, ShutdownResponse>("Shutdown", ShutdownRequest {})
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _: FreeResponse = case
            .call_sync("FreeBuffer", FreeBufferRequest { buffer_id: buffer })
            .unwrap();
        let shut = tokio::time::timeout(Duration::from_secs(5), shutdown).await;
        let shut = shut.unwrap().unwrap().unwrap();
        assert_eq!(shut.failed_buffers, 1);
    }
}

/// A runtime on each backend under test, on a fault-injecting Store over the backend's Store that
/// takes `appends` appends before it refuses the later ones, with the case's Stream created.
async fn flaky_cases(appends: i64) -> Vec<(Arc<Flaky>, Case)> {
    let mut cases = vec![];
    for target in stores_for(Needs::CALLS).await {
        let store = Arc::new(Flaky {
            appends_left: AtomicI64::new(appends),
            ..Flaky::new(target.store)
        });
        let case = case_on(MemoryStreamRuntime::with_store(store.clone(), None)).await;
        cases.push((store, case));
    }
    cases
}

/// A batch that the writer splits, and whose second part fails, counts only that part as
/// unwritten, and gives the first part's last offset.
async fn a_partly_written_batch_counts_only_the_rest() {
    for (_, case) in flaky_cases(1).await {
        let buffer = case.buffer(case.scope(), true, 60_000, 0);
        let big = "x".repeat(1_400_000);
        case.push(buffer, &[&big, &big, &big]).unwrap();
        let flushed = case.flush(buffer).await;
        assert_eq!(flushed.failure.unwrap().reason(), StreamErrorReason::Closed);
        assert_eq!(flushed.unwritten, 1);
        assert!(flushed.last_offset > 0);
        assert_eq!(case.texts().await.len(), 2);
    }
}

/// A dropped scope close frees the buffers that it didn't reach.
async fn a_dropped_scope_close_frees_its_buffers() {
    for (store, case) in flaky_cases(-1).await {
        let scope = case.scope();
        let (first, second) = (
            case.buffer(scope, true, 60_000, 0),
            case.buffer(scope, true, 60_000, 0),
        );
        case.push(first, &["a"]).unwrap();
        case.push(second, &["b"]).unwrap();
        // The first buffer's write retries while the Store is down, so the close waits on it.
        store.down.store(true, Ordering::SeqCst);
        let close =
            case.call::<_, CloseScopeResponse>("CloseScope", CloseScopeRequest { scope_id: scope });
        assert!(
            tokio::time::timeout(Duration::from_millis(300), close)
                .await
                .is_err()
        );
        for buffer in [first, second] {
            let error = case.push(buffer, &["c"]).unwrap_err();
            assert_eq!(error.reason.token(), "stream_unknown_writer");
        }
        store.down.store(false, Ordering::SeqCst);
    }
}

/// A huge delay counts as a year, so the batch math doesn't overflow and a flush still writes.
async fn a_huge_delay_doesnt_overflow() {
    for case in cases().await {
        let opened: OpenBufferResponse = case
            .call_sync(
                "OpenBuffer",
                OpenBufferRequest {
                    scope_id: case.scope(),
                    stream: Some(case.stream_ref()),
                    max_delay: Some(prost_types::Duration {
                        seconds: i64::MAX,
                        nanos: 0,
                    }),
                    encoded: true,
                    ..Default::default()
                },
            )
            .unwrap();
        case.push(opened.buffer_id, &["a"]).unwrap();
        let flushed = tokio::time::timeout(Duration::from_secs(5), case.flush(opened.buffer_id));
        assert!(flushed.await.unwrap().last_offset > 0);
    }
}

/// A batch that a dropped pump took comes again on the next `BufferNextBatch`, with its id and
/// values.
async fn a_batch_that_a_dropped_pump_took_comes_again() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), false, 0, 0);
        case.push(buffer, &["a"]).unwrap();
        let taken = case.next_batch(buffer).await;
        let again = tokio::time::timeout(Duration::from_secs(5), case.next_batch(buffer)).await;
        let again = again.unwrap();
        assert_eq!(
            (again.batch_id, again.records),
            (taken.batch_id, taken.records)
        );
        let records = records(&["a"]);
        let body = Body::Records(BufferRecords { records });
        case.write(buffer, taken.batch_id, body).await;
        assert!(case.flush(buffer).await.last_offset > 0);
        assert_eq!(case.texts().await, ["a"]);
    }
}

/// A write must give one Record for each value of its batch. Another count fails the buffer, so a
/// flush doesn't wait for that batch.
async fn a_write_with_another_count_fails_the_buffer() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), false, 0, 0);
        case.push(buffer, &["a"]).unwrap();
        let batch = case.next_batch(buffer).await;
        let write = |texts: &[&str]| BufferWriteRequest {
            buffer_id: buffer,
            batch_id: batch.batch_id,
            body: Some(Body::Records(BufferRecords {
                records: records(texts),
            })),
        };
        let error = case
            .call::<_, BufferWriteResponse>("BufferWrite", write(&["a", "b"]))
            .await
            .unwrap_err();
        assert_eq!(error.reason.token(), "stream_invalid");
        let flushed = tokio::time::timeout(Duration::from_secs(5), case.flush(buffer)).await;
        let flushed = flushed.unwrap();
        assert_eq!(
            flushed.failure.unwrap().reason(),
            StreamErrorReason::Invalid
        );
        assert_eq!(flushed.unwritten, 1);
        assert!(case.texts().await.is_empty());
    }
}

/// A push fails once the scope's close flushes the buffer, so the close drops no value.
async fn a_push_during_the_scope_close_fails() {
    for (store, case) in flaky_cases(-1).await {
        let scope = case.scope();
        let buffer = case.buffer(scope, true, 60_000, 0);
        case.push(buffer, &["a"]).unwrap();
        store.down.store(true, Ordering::SeqCst);
        let close = tokio::spawn({
            let case = case.clone();
            async move {
                case.call::<_, CloseScopeResponse>(
                    "CloseScope",
                    CloseScopeRequest { scope_id: scope },
                )
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let error = case.push(buffer, &["b"]).unwrap_err();
        assert_eq!(error.reason.token(), "stream_unknown_writer");
        store.down.store(false, Ordering::SeqCst);
        let closed = close.await.unwrap().unwrap();
        assert_eq!(closed, CloseScopeResponse::default());
        assert_eq!(case.texts().await, ["a"]);
    }
}

/// A dropped shutdown puts back the scope that it was closing, and the next one writes its
/// buffer's values.
async fn a_dropped_shutdown_writes_the_buffers_next_time() {
    for (store, case) in flaky_cases(-1).await {
        // One value a batch, so that `b` waits behind `a` while the Store is down.
        let buffer = case.buffer(case.scope(), true, 60_000, 1);
        store.down.store(true, Ordering::SeqCst);
        case.push(buffer, &["a", "b"]).unwrap();
        let shutdown = case.call::<_, ShutdownResponse>("Shutdown", ShutdownRequest {});
        assert!(
            tokio::time::timeout(Duration::from_millis(300), shutdown)
                .await
                .is_err()
        );
        store.down.store(false, Ordering::SeqCst);
        let shut: ShutdownResponse = case.call("Shutdown", ShutdownRequest {}).await.unwrap();
        assert_eq!((shut.failed_writers, shut.failed_buffers), (0, 0));
        let reader = Case {
            ext: MemoryStreamRuntime::with_store(store, None),
            ..case
        };
        assert_eq!(reader.texts().await, ["a", "b"]);
    }
}

/// A free counts a batch that lang's pump took but didn't write as unwritten.
async fn a_free_counts_a_taken_batch_as_unwritten() {
    for case in cases().await {
        let buffer = case.buffer(case.scope(), false, 0, 0);
        case.push(buffer, &["a", "b"]).unwrap();
        case.next_batch(buffer).await;
        let flush = tokio::spawn({
            let case = case.clone();
            async move { case.flush(buffer).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _: FreeResponse = case
            .call_sync("FreeBuffer", FreeBufferRequest { buffer_id: buffer })
            .unwrap();
        let flushed = tokio::time::timeout(Duration::from_secs(5), flush).await;
        let flushed = flushed.unwrap().unwrap();
        assert_eq!(flushed.unwritten, 2);
        assert!(flushed.failure.is_some());
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(an_encoded_buffer_writes_its_batch_after_the_delay),
    case!(a_full_batch_goes_at_once),
    case!(a_pump_runs_the_codec_once_per_batch),
    case!(a_failed_batch_fails_every_later_call),
    case!(a_codec_failure_fails_the_buffer),
    case!(a_scope_close_flushes_its_buffers),
    case!(a_reported_buffer_failure_doesnt_count_at_the_scope_close),
    case!(the_shutdown_writes_every_buffer),
    case!(the_pump_calls_are_checked),
    case!(the_shutdown_waits_for_lang_s_pump),
    case!(a_dropped_write_still_settles_its_batch),
    case!(a_partly_written_batch_counts_only_the_rest),
    case!(a_dropped_scope_close_frees_its_buffers),
    case!(a_huge_delay_doesnt_overflow),
    case!(a_batch_that_a_dropped_pump_took_comes_again),
    case!(a_write_with_another_count_fails_the_buffer),
    case!(a_push_during_the_scope_close_fails),
    case!(a_dropped_shutdown_writes_the_buffers_next_time),
    case!(a_free_counts_a_taken_batch_as_unwritten),
    case!(a_panicking_write_fails_its_batch),
    case!(a_free_ends_a_shutdown_whose_pump_is_gone),
    case!(an_encoded_pump_whose_write_panics_fails_its_batch),
];
