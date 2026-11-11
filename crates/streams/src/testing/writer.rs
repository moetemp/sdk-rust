//! The writers of Activities and clients, through the runtime's typed calls.
//!
//! Every Store runs the suite through its own runtime.

use super::{
    RuntimeConfig, case,
    support::{Needs, item_offset, stores_for},
};
use crate::{
    StreamError, StreamResult, StreamRuntime, StreamWriter,
    ids::{activity_producer_id, client_producer_id, named_client_producer_id},
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecord, ClientEpoch, ClientProducer,
        CloseStreamRequest, CreateStreamRequest, OpenWriterRequest, Producer, ReadRecordsRequest,
        ReadRecordsResponse, StreamErrorDetails, StreamRef, WriteRecordsRequest, producer::Kind,
        stream_record::Item,
    },
    store::{
        Appended, Batch, Capabilities, Closed, Create, Created, Entry, MAX_BATCH_RECORDS, Store,
        StreamState,
    },
};
use futures_util::future::join_all;
use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use tokio::sync::Notify;
use tonic::Code;

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
}

async fn cases() -> Vec<Case> {
    cases_with(&RuntimeConfig::default()).await
}

/// A case on each backend under test, on a runtime with `config`, with its Stream created.
async fn cases_with(config: &RuntimeConfig) -> Vec<Case> {
    let mut cases = vec![];
    for ext in super::support::config_runtimes(config) {
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
        assert!(case.create().await.created);
        cases.push(case);
    }
    cases
}

fn client() -> Producer {
    Producer {
        id: client_producer_id("me", "p"),
        kind: Some(Kind::Client(ClientProducer {
            epoch: Some(ClientEpoch { attempt: 1 }),
        })),
    }
}

fn activity(attempt: i32) -> Producer {
    Producer {
        id: activity_producer_id("r1", "a", "s"),
        kind: Some(Kind::WorkflowActivity(ActivityProducer {
            run_id: "r1".to_string(),
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: Some(Timestamp {
                    seconds: 10 * attempt as i64,
                    nanos: 0,
                }),
                attempt,
            }),
        })),
    }
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

fn reason(error: &StreamError) -> String {
    let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
    assert_eq!(details.reason(), error.reason.proto());
    error.reason.token().to_string()
}

/// The internal offset of a wire value, for the suite's assertions.
fn off(wire: &[u8]) -> i64 {
    crate::offset::offset_from_wire(wire).unwrap()
}

fn millis(ms: u64) -> Option<prost_types::Duration> {
    Some(prost_types::Duration {
        seconds: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    })
}

/// An awaited publish. The Records are in the Store when it returns.
async fn write(writer: &StreamWriter, texts: &[&str]) -> Result<(i64, i64), StreamError> {
    let request = WriteRecordsRequest {
        records: records(texts),
    };
    let written = writer.write(request).await?;
    Ok((off(&written.first_offset), off(&written.last_offset)))
}

/// Closes the writer, and gives the offset of the newest Record it wrote.
async fn close(writer: &StreamWriter) -> Result<i64, StreamError> {
    Ok(off(&writer.close().await?.last_offset))
}

impl Case {
    fn stream_ref(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        })
    }

    async fn create(&self) -> crate::proto::CreateStreamResponse {
        let request = CreateStreamRequest {
            namespace: self.namespace.clone(),
            stream_ref: self.stream_ref(),
            ..Default::default()
        };
        self.ext.create_stream(request).await.unwrap()
    }

    fn open_request(&self, producer: Producer) -> OpenWriterRequest {
        OpenWriterRequest {
            namespace: self.namespace.clone(),
            r#ref: self.stream_ref(),
            producer: Some(producer),
            ..Default::default()
        }
    }

    async fn open(
        &self,
        producer: Producer,
        change: impl Fn(&mut OpenWriterRequest),
    ) -> StreamWriter {
        let mut request = self.open_request(producer);
        change(&mut request);
        self.ext.open_writer(request).await.unwrap()
    }

    /// Closes the Stream as the client Producer.
    async fn close_stream(&self) {
        let request = CloseStreamRequest {
            namespace: self.namespace.clone(),
            r#ref: self.stream_ref(),
            producer: Some(client()),
            ..Default::default()
        };
        self.ext.close_stream(request).await.unwrap();
    }

    async fn read(&self, wait_ms: u64) -> ReadRecordsResponse {
        let request = ReadRecordsRequest {
            namespace: self.namespace.clone(),
            r#ref: self.stream_ref(),
            wait: millis(wait_ms),
            ..Default::default()
        };
        self.ext.read_records(request).await.unwrap()
    }

    async fn visible(&self) -> Vec<String> {
        self.read(0)
            .await
            .records
            .into_iter()
            .filter_map(|record| match record.item {
                Some(Item::Data(data)) => Some(String::from_utf8(data.body.unwrap().data).unwrap()),
                _ => None,
            })
            .collect()
    }
}

async fn a_write_is_in_the_store_when_it_answers_its_offsets() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let (first, last) = write(&writer, &["a", "b"]).await.unwrap();
        assert!(0 < first && first < last);
        let read = case.read(0).await;
        let offsets: Vec<i64> = read.records.iter().map(|r| off(item_offset(r))).collect();
        assert_eq!(offsets, [first, last]);
        let (one, same) = write(&writer, &["c"]).await.unwrap();
        assert_eq!(one, same);
        assert!(one > last);
        assert_eq!(write(&writer, &[]).await.unwrap(), (0, 0));
        assert_eq!(case.visible().await, ["a", "b", "c"]);
        assert_eq!(close(&writer).await.unwrap(), one);
    }
}

async fn writes_split_at_the_batch_size_and_keep_one_sequence() {
    for case in cases().await {
        let writer = case.open(client(), |r| r.max_batch_size = 2).await;
        let (first, last) = write(&writer, &["1", "2", "3"]).await.unwrap();
        write(&writer, &["4", "5"]).await.unwrap();
        let read = case.read(0).await;
        assert_eq!(case.visible().await, ["1", "2", "3", "4", "5"]);
        assert_eq!(off(item_offset(&read.records[0])), first);
        assert_eq!(off(item_offset(&read.records[2])), last);
    }
}

async fn a_batch_size_over_the_store_limit_writes_in_more_batches() {
    for case in cases().await {
        let writer = case.open(client(), |r| r.max_batch_size = 5000).await;
        let texts: Vec<String> = (0..=MAX_BATCH_RECORDS).map(|i| i.to_string()).collect();
        let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
        // The Store refuses one batch of this many, so the write goes in two.
        let (first, last) = write(&writer, &texts).await.unwrap();
        assert!(first < last);
    }
}

async fn a_failed_write_fails_every_later_write_and_the_close() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        case.close_stream().await;
        let error = write(&writer, &["late"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
        assert_eq!(
            reason(&write(&writer, &["x"]).await.unwrap_err()),
            "stream_closed"
        );
        assert_eq!(reason(&close(&writer).await.unwrap_err()), "stream_closed");
    }
}

/// An Activity retry opens its writer on a closed or expired Stream, so a retry that only reads
/// the Stream still runs. Its first write fails with the real reason.
async fn an_activity_retry_opens_on_a_closed_or_expired_stream() {
    for case in cases().await {
        let first = case.open(activity(1), |_| {}).await;
        write(&first, &["a1"]).await.unwrap();
        case.close_stream().await;
        let retry = case.open(activity(2), |_| {}).await;
        let error = write(&retry, &["a2"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_closed");
    }
    let short = RuntimeConfig {
        retention: Some(Duration::from_millis(300)),
        ..Default::default()
    };
    for case in cases_with(&short).await {
        let first = case.open(activity(1), |_| {}).await;
        write(&first, &["a1"]).await.unwrap();
        case.close_stream().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let retry = case.open(activity(2), |_| {}).await;
        let error = write(&retry, &["a2"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
    }
}

/// A closed writer refuses every later write as closed.
async fn a_closed_writer_refuses_writes() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        write(&writer, &["last"]).await.unwrap();
        assert!(close(&writer).await.unwrap() > 0);
        let error = write(&writer, &["x"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
        assert_eq!(case.visible().await, ["last"]);
    }
}

async fn a_write_splits_big_records_by_bytes_and_refuses_an_oversize_one() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let big = "x".repeat(1_400_000);
        write(&writer, &[&big, &big, &big, "end"]).await.unwrap();
        assert_eq!(case.visible().await.len(), 4);
        let huge = "y".repeat(1_700_000);
        let error = write(&writer, &[&huge]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::InvalidArgument, "stream_too_large")
        );
        // A refusal before the Store leaves the writer usable.
        write(&writer, &["ok"]).await.unwrap();
    }
}

async fn a_reopen_of_a_closed_writer_goes_on_with_its_sequence() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        write(&writer, &["1"]).await.unwrap();
        close(&writer).await.unwrap();
        drop(writer);
        let again = case.open(client(), |_| {}).await;
        write(&again, &["2"]).await.unwrap();
        assert_eq!(case.visible().await, ["1", "2"]);
    }
}

/// The writers of one Producer epoch share a lane, so their batches reach the Store in `sequence`
/// order, also when they write at once. A closed writer doesn't stop the other one, and an open
/// after both are gone writes on a new lane.
async fn the_writers_of_one_producer_epoch_share_a_lane() {
    for case in cases().await {
        let a = case.open(client(), |_| {}).await;
        let b = case.open(client(), |_| {}).await;
        let texts: Vec<String> = (0..10).map(|i| i.to_string()).collect();
        let writes = texts.iter().enumerate().map(|(i, text)| {
            let writer = if i % 2 == 0 { &a } else { &b };
            async move { write(writer, &[text.as_str()]).await }
        });
        for written in join_all(writes).await {
            written.unwrap();
        }
        close(&a).await.unwrap();
        write(&b, &["b"]).await.unwrap();
        drop((a, b));
        let c = case.open(client(), |_| {}).await;
        write(&c, &["c"]).await.unwrap();
        assert_eq!(case.visible().await.len(), 12);
    }
}

async fn a_named_client_with_a_higher_epoch_fences_its_earlier_self() {
    for case in cases().await {
        let named = |attempt| Producer {
            id: named_client_producer_id("ingest-1"),
            kind: Some(Kind::Client(ClientProducer {
                epoch: Some(ClientEpoch { attempt }),
            })),
        };
        let old = case.open(named(1), |_| {}).await;
        write(&old, &["old"]).await.unwrap();
        let new = case.open(named(2), |_| {}).await;
        write(&new, &["new"]).await.unwrap();
        let error = write(&old, &["late"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
        // A fence blocks the old epoch's later writes but keeps its earlier Records.
        assert_eq!(case.visible().await, ["old", "new"]);
    }
}

/// A Store over another one, whose appends fail. It can write an append and lose its answer, as on
/// a dropped connection, and it refuses every append while it is down. It can also refuse the next
/// append as corrupt Store data. It can hold the next append until a release, and it counts the
/// appends that reach the inner Store.
struct Flaky {
    inner: Arc<dyn Store>,
    lose_next: AtomicBool,
    down: AtomicBool,
    corrupt_next: AtomicBool,
    hold_next: AtomicBool,
    /// Notified when the held append arrives.
    held: Notify,
    /// Lets the held append go on.
    release: Notify,
    appends: AtomicUsize,
}

impl Flaky {
    /// A Store that is up, over `inner`.
    fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner,
            lose_next: AtomicBool::new(false),
            down: AtomicBool::new(false),
            corrupt_next: AtomicBool::new(false),
            hold_next: AtomicBool::new(false),
            held: Notify::new(),
            release: Notify::new(),
            appends: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Store for Flaky {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn create(&self, stream_ref: &StreamRef, create: &Create<'_>) -> StreamResult<Created> {
        self.inner.create(stream_ref, create).await
    }

    async fn append(&self, stream_ref: &StreamRef, batch: &Batch<'_>) -> StreamResult<Appended> {
        if self.hold_next.swap(false, Ordering::SeqCst) {
            self.held.notify_one();
            self.release.notified().await;
        }
        if self.down.load(Ordering::SeqCst) {
            return Err(StreamError::storage_unavailable("the Store is down"));
        }
        if self.corrupt_next.swap(false, Ordering::SeqCst) {
            return Err(StreamError::storage_corrupt("a stored field is bad"));
        }
        self.appends.fetch_add(1, Ordering::SeqCst);
        let appended = self.inner.append(stream_ref, batch).await?;
        if self.lose_next.swap(false, Ordering::SeqCst) {
            return Err(StreamError::storage_unavailable("the answer was lost"));
        }
        Ok(appended)
    }

    async fn read(
        &self,
        stream_ref: &StreamRef,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        self.inner.read(stream_ref, from, count).await
    }

    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: tokio::time::Instant,
    ) -> StreamResult<bool> {
        self.inner.wait_after(stream_ref, after, deadline).await
    }

    async fn close(&self, stream_ref: &StreamRef, producer: &Producer) -> StreamResult<Closed> {
        self.inner.close(stream_ref, producer).await
    }
}

/// A case on each backend under test, on a [Flaky] Store over the backend's Store, with its Stream
/// created.
async fn flaky_cases(lose_next: bool) -> Vec<(Arc<Flaky>, Case)> {
    let mut cases = vec![];
    for target in stores_for(Needs::CALLS).await {
        let store = Arc::new(Flaky {
            lose_next: AtomicBool::new(lose_next),
            ..Flaky::new(target.store)
        });
        let case = Case {
            ext: StreamRuntime::new("flaky", "", store.clone(), None),
            namespace: format!("ns-{}", target.stream),
            stream: target.stream,
        };
        case.create().await;
        cases.push((store, case));
    }
    cases
}

async fn a_write_whose_answer_is_lost_goes_in_once() {
    for (_, case) in flaky_cases(true).await {
        let writer = case.open(client(), |_| {}).await;
        let (first, last) = write(&writer, &["a", "b"]).await.unwrap();
        let read = case.read(0).await;
        assert_eq!(case.visible().await, ["a", "b"]);
        assert_eq!(
            (
                off(item_offset(&read.records[0])),
                off(item_offset(&read.records[1]))
            ),
            (first, last)
        );
        write(&writer, &["c"]).await.unwrap();
        assert_eq!(case.visible().await, ["a", "b", "c"]);
    }
}

/// A write that fails in a Store outage fails alone, and the next write succeeds once the Store is
/// back.
async fn a_writer_writes_again_after_a_store_outage() {
    for (store, case) in flaky_cases(false).await {
        let writer = case
            .open(client(), |r| r.max_retry_duration = millis(100))
            .await;
        write(&writer, &["a"]).await.unwrap();
        store.down.store(true, Ordering::SeqCst);
        let error = write(&writer, &["lost"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::Unavailable, "stream_storage_unavailable")
        );
        store.down.store(false, Ordering::SeqCst);
        write(&writer, &["b"]).await.unwrap();
        assert_eq!(case.visible().await, ["a", "b"]);
        assert!(close(&writer).await.unwrap() > 0);
    }
}

/// A write that meets corrupt Store data fails at once, since a retry reads the same data. The
/// writer then fails every later write.
async fn a_write_on_corrupt_store_data_fails_with_no_retry() {
    for (store, case) in flaky_cases(false).await {
        let writer = case.open(client(), |_| {}).await;
        store.corrupt_next.store(true, Ordering::SeqCst);
        let error = write(&writer, &["a"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::DataLoss, "stream_storage_corrupt")
        );
        assert!(case.visible().await.is_empty());
        let again = write(&writer, &["b"]).await.unwrap_err();
        assert_eq!(reason(&again), "stream_storage_corrupt");
    }
}

/// A write that waits on the lane behind another writer's retries, while its own writer closes,
/// is refused as closed and writes nothing.
async fn a_write_that_waits_behind_a_close_is_refused() {
    for (store, case) in flaky_cases(false).await {
        let writer = case.open(client(), |_| {}).await;
        let other = case
            .open(client(), |r| r.max_retry_duration = millis(400))
            .await;
        store.down.store(true, Ordering::SeqCst);
        let busy = write(&other, &["busy"]);
        let late = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            write(&writer, &["late"]).await
        };
        let closing = async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            close(&writer).await
        };
        let (busy, late, closed) = tokio::join!(busy, late, closing);
        assert_eq!(reason(&busy.unwrap_err()), "stream_storage_unavailable");
        let late = late.unwrap_err();
        assert_eq!(
            (reason(&late).as_str(), late.message.as_str()),
            ("stream_closed", "The writer closed while the write waited")
        );
        assert_eq!(closed.unwrap(), 0);
        store.down.store(false, Ordering::SeqCst);
        assert!(case.visible().await.is_empty());
    }
}

/// The writers of one epoch send their batches in call order. A write waits on the lane while an
/// earlier write of the epoch is in the Store, so its higher `sequence` never arrives first.
async fn the_writers_of_one_epoch_send_their_batches_in_call_order() {
    for (store, case) in flaky_cases(false).await {
        let a = case.open(client(), |_| {}).await;
        let b = case.open(client(), |_| {}).await;
        store.hold_next.store(true, Ordering::SeqCst);
        let first = write(&a, &["first"]);
        let second = async {
            store.held.notified().await;
            write(&b, &["second"]).await
        };
        let release = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            store.release.notify_one();
        };
        let (first, second, ()) = tokio::join!(first, second, release);
        let (first, second) = (first.unwrap(), second.unwrap());
        assert!(first.1 < second.0, "{first:?} {second:?}");
        assert_eq!(case.visible().await, ["first", "second"]);
    }
}

/// A closed writer refuses a write as closed, also after it failed. Its close still fails with the
/// writer's error.
async fn a_closed_writer_refuses_writes_as_closed_after_a_failure() {
    for case in cases().await {
        let named = |attempt| Producer {
            id: named_client_producer_id("ingest-2"),
            kind: Some(Kind::Client(ClientProducer {
                epoch: Some(ClientEpoch { attempt }),
            })),
        };
        let old = case.open(named(1), |_| {}).await;
        let new = case.open(named(2), |_| {}).await;
        write(&new, &["new"]).await.unwrap();
        assert_eq!(
            reason(&write(&old, &["late"]).await.unwrap_err()),
            "stream_fenced"
        );
        assert_eq!(reason(&close(&old).await.unwrap_err()), "stream_fenced");
        assert_eq!(
            reason(&write(&old, &["again"]).await.unwrap_err()),
            "stream_closed"
        );
    }
}

/// A write that waits on the lane behind a write that fails the writer fails with that error, also
/// when it has no Record.
async fn an_empty_write_behind_a_failing_write_fails_with_it() {
    for (store, case) in flaky_cases(false).await {
        let writer = case.open(client(), |_| {}).await;
        store.hold_next.store(true, Ordering::SeqCst);
        store.corrupt_next.store(true, Ordering::SeqCst);
        let failing = write(&writer, &["a"]);
        let empty = async {
            store.held.notified().await;
            write(&writer, &[]).await
        };
        let release = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            store.release.notify_one();
        };
        let (failing, empty, ()) = tokio::join!(failing, empty, release);
        assert_eq!(reason(&failing.unwrap_err()), "stream_storage_corrupt");
        assert_eq!(reason(&empty.unwrap_err()), "stream_storage_corrupt");
    }
}

/// The first open of a retry epoch sends the empty batch that fences the earlier epoch, and a
/// second open of the same epoch sends none.
async fn a_retry_epoch_sends_its_empty_batch_once() {
    for (store, case) in flaky_cases(false).await {
        let _first = case.open(activity(2), |_| {}).await;
        assert_eq!(store.appends.load(Ordering::SeqCst), 1);
        let _second = case.open(activity(2), |_| {}).await;
        assert_eq!(store.appends.load(Ordering::SeqCst), 1);
    }
}

/// An open whose empty batch fails, or whose future is dropped while it sends, leaves the epoch
/// unfenced. So an open of the same epoch that waited on the lane sends the empty batch itself.
async fn an_open_after_a_failed_or_cancelled_fence_sends_it() {
    for (store, case) in flaky_cases(false).await {
        store.down.store(true, Ordering::SeqCst);
        let failed = case.ext.open_writer(OpenWriterRequest {
            max_retry_duration: millis(100),
            ..case.open_request(activity(2))
        });
        let waited = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            case.ext.open_writer(case.open_request(activity(2))).await
        };
        let back = async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            store.down.store(false, Ordering::SeqCst);
        };
        let (failed, waited, ()) = tokio::join!(failed, waited, back);
        assert_eq!(reason(&failed.unwrap_err()), "stream_storage_unavailable");
        let _waited = waited.unwrap();
        assert_eq!(store.appends.load(Ordering::SeqCst), 1);
        let earlier = case.open(activity(1), |_| {}).await;
        let error = write(&earlier, &["late"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
    }
    for (store, case) in flaky_cases(false).await {
        store.hold_next.store(true, Ordering::SeqCst);
        let held = case.ext.open_writer(case.open_request(activity(2)));
        let cancelled = tokio::time::timeout(Duration::from_millis(100), held);
        let waited = async {
            store.held.notified().await;
            case.ext.open_writer(case.open_request(activity(2))).await
        };
        let (cancelled, waited) = tokio::join!(cancelled, waited);
        assert!(cancelled.is_err());
        let _waited = waited.unwrap();
        assert_eq!(store.appends.load(Ordering::SeqCst), 1);
        let earlier = case.open(activity(1), |_| {}).await;
        let error = write(&earlier, &["late"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
    }
}

/// An open of a writer whose Producer lacks a required field fails as invalid.
async fn a_writer_of_a_producer_with_a_missing_required_field_does_not_open() {
    for case in cases().await {
        let request = case.open_request(Producer {
            kind: Some(Kind::Client(ClientProducer::default())),
            ..client()
        });
        let error = case.ext.open_writer(request).await.unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// A write with a Record that has no body fails as invalid before the Store, and the writer goes
/// on.
async fn a_write_of_a_record_with_no_body_is_refused_and_the_writer_goes_on() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let request = WriteRecordsRequest {
            records: vec![AppendRecord::default()],
        };
        let error = writer.write(request).await.unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        write(&writer, &["ok"]).await.unwrap();
        assert_eq!(case.visible().await, ["ok"]);
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(a_write_is_in_the_store_when_it_answers_its_offsets),
    case!(writes_split_at_the_batch_size_and_keep_one_sequence),
    case!(a_batch_size_over_the_store_limit_writes_in_more_batches),
    case!(a_failed_write_fails_every_later_write_and_the_close),
    case!(an_activity_retry_opens_on_a_closed_or_expired_stream),
    case!(a_closed_writer_refuses_writes),
    case!(a_write_splits_big_records_by_bytes_and_refuses_an_oversize_one),
    case!(a_reopen_of_a_closed_writer_goes_on_with_its_sequence),
    case!(the_writers_of_one_producer_epoch_share_a_lane),
    case!(a_named_client_with_a_higher_epoch_fences_its_earlier_self),
    case!(a_write_whose_answer_is_lost_goes_in_once),
    case!(a_writer_writes_again_after_a_store_outage),
    case!(a_write_on_corrupt_store_data_fails_with_no_retry),
    case!(a_write_that_waits_behind_a_close_is_refused),
    case!(a_writer_of_a_producer_with_a_missing_required_field_does_not_open),
    case!(a_write_of_a_record_with_no_body_is_refused_and_the_writer_goes_on),
    case!(the_writers_of_one_epoch_send_their_batches_in_call_order),
    case!(a_closed_writer_refuses_writes_as_closed_after_a_failure),
    case!(an_empty_write_behind_a_failing_write_fails_with_it),
    case!(a_retry_epoch_sends_its_empty_batch_once),
    case!(an_open_after_a_failed_or_cancelled_fence_sends_it),
];
