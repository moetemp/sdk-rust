//! The writers of Activities and clients, through the runtime's byte call, as lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{
    RuntimeConfig, case,
    support::{Needs, stores_for},
};
use crate::{
    MemoryStreamRuntime, StreamError, StreamResult, StreamRuntime,
    ids::{activity_producer_id, client_producer_id, named_client_producer_id},
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecord, ClientEpoch, ClientProducer,
        CloseStreamRequest, CloseWriterRequest, CloseWriterResponse, CreateStreamRequest,
        CreateStreamResponse, OpenWriterRequest, OpenWriterResponse, Producer, ReadRecordsRequest,
        ReadRecordsResponse, StreamErrorDetails, StreamRef, StreamWriter, WriteRecordsRequest,
        WriteRecordsResponse, producer::Kind, stream_record::Item,
    },
    store::{
        Appended, Batch, Capabilities, Closed, Create, Created, Entry, MAX_BATCH_RECORDS, Store,
        StreamState,
    },
};
use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
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
        let created: CreateStreamResponse = case
            .call(
                "CreateStream",
                CreateStreamRequest {
                    namespace: case.namespace.clone(),
                    stream_ref: Some(StreamRef {
                        id: case.stream.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(created.created);
        cases.push(case);
    }
    cases
}

/// The writer of a write by `producer`.
fn writer_for(producer: Producer) -> Option<StreamWriter> {
    Some(StreamWriter {
        producer: Some(producer),
    })
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

impl Case {
    async fn call<Req: Message, Resp: Message + Default>(
        &self,
        method: &str,
        request: Req,
    ) -> Result<Resp, StreamError> {
        let bytes = self.ext.call(method, &request.encode_to_vec()).await?;
        Ok(Resp::decode(bytes.as_slice()).unwrap())
    }

    async fn open(&self, producer: Producer, change: impl Fn(&mut OpenWriterRequest)) -> u64 {
        let mut request = OpenWriterRequest {
            r#ref: Some(StreamRef {
                id: self.stream.clone(),
                ..Default::default()
            }),
            writer: writer_for(producer),
            ..Default::default()
        };
        change(&mut request);
        let opened: OpenWriterResponse = self.call("OpenWriter", request).await.unwrap();
        opened.writer_id
    }

    /// An awaited publish. The Records are in the Store when it returns.
    async fn write(&self, writer_id: u64, texts: &[&str]) -> Result<(i64, i64), StreamError> {
        let written: WriteRecordsResponse = self
            .call(
                "WriteRecords",
                WriteRecordsRequest {
                    writer_id,
                    records: records(texts),
                },
            )
            .await?;
        Ok((off(&written.first_offset), off(&written.last_offset)))
    }

    async fn read(&self, wait_ms: u64) -> ReadRecordsResponse {
        self.call(
            "ReadRecords",
            ReadRecordsRequest {
                namespace: self.namespace.clone(),
                r#ref: Some(StreamRef {
                    id: self.stream.clone(),
                    ..Default::default()
                }),
                wait: millis(wait_ms),
                ..Default::default()
            },
        )
        .await
        .unwrap()
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
        let (first, last) = case.write(writer, &["a", "b"]).await.unwrap();
        assert!(0 < first && first < last);
        let read = case.read(0).await;
        let offsets: Vec<i64> = read.records.iter().map(|r| off(&r.offset)).collect();
        assert_eq!(offsets, [first, last]);
        let (one, same) = case.write(writer, &["c"]).await.unwrap();
        assert_eq!(one, same);
        assert!(one > last);
        assert_eq!(case.write(writer, &[]).await.unwrap(), (0, 0));
        assert_eq!(case.visible().await, ["a", "b", "c"]);
        let closed: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        assert_eq!(off(&closed.last_offset), one);
    }
}

async fn writes_split_at_the_batch_size_and_keep_one_sequence() {
    for case in cases().await {
        let writer = case.open(client(), |r| r.max_batch_size = 2).await;
        let (first, last) = case.write(writer, &["1", "2", "3"]).await.unwrap();
        case.write(writer, &["4", "5"]).await.unwrap();
        let read = case.read(0).await;
        assert_eq!(case.visible().await, ["1", "2", "3", "4", "5"]);
        assert_eq!(off(&read.records[0].offset), first);
        assert_eq!(off(&read.records[2].offset), last);
        // Opening the same Producer epoch again returns the same writer, so it keeps the sequence.
        assert_eq!(case.open(client(), |_| {}).await, writer);
    }
}

async fn a_batch_size_over_the_store_limit_writes_in_more_batches() {
    for case in cases().await {
        let writer = case.open(client(), |r| r.max_batch_size = 5000).await;
        let texts: Vec<String> = (0..=MAX_BATCH_RECORDS).map(|i| i.to_string()).collect();
        let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
        // The Store refuses one batch of this many, so the write goes in two.
        let (first, last) = case.write(writer, &texts).await.unwrap();
        assert!(first < last);
    }
}

async fn a_failed_write_fails_every_later_write_and_the_close() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: Some(StreamRef {
                id: case.stream.clone(),
                ..Default::default()
            }),
            writer: writer_for(client()),
            ..Default::default()
        };
        let _: crate::proto::CloseStreamResponse = case.call("CloseStream", close).await.unwrap();
        let error = case.write(writer, &["late"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
        assert_eq!(
            reason(&case.write(writer, &["x"]).await.unwrap_err()),
            "stream_closed"
        );
        let error = case
            .call::<_, CloseWriterResponse>("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_closed");
    }
}

/// An Activity retry opens its writer on a closed or expired Stream, so a retry that only reads
/// the Stream still runs. Its first write fails with the real reason.
async fn an_activity_retry_opens_on_a_closed_or_expired_stream() {
    for case in cases().await {
        let first = case.open(activity(1), |_| {}).await;
        case.write(first, &["a1"]).await.unwrap();
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: Some(StreamRef {
                id: case.stream.clone(),
                ..Default::default()
            }),
            writer: writer_for(client()),
            ..Default::default()
        };
        let _: crate::proto::CloseStreamResponse = case.call("CloseStream", close).await.unwrap();
        let retry = case.open(activity(2), |_| {}).await;
        let error = case.write(retry, &["a2"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_closed");
    }
    let short = RuntimeConfig {
        retention: Some(Duration::from_millis(300)),
        ..Default::default()
    };
    for case in cases_with(&short).await {
        let first = case.open(activity(1), |_| {}).await;
        case.write(first, &["a1"]).await.unwrap();
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: Some(StreamRef {
                id: case.stream.clone(),
                ..Default::default()
            }),
            writer: writer_for(client()),
            ..Default::default()
        };
        let _: crate::proto::CloseStreamResponse = case.call("CloseStream", close).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let retry = case.open(activity(2), |_| {}).await;
        let error = case.write(retry, &["a2"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
    }
}

async fn a_closed_writer_is_gone() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        case.write(writer, &["last"]).await.unwrap();
        let closed: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        assert!(off(&closed.last_offset) > 0);
        let error = case.write(writer, &["x"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::NotFound, "stream_unknown_writer")
        );
    }
}

async fn a_write_splits_big_records_by_bytes_and_refuses_an_oversize_one() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        let big = "x".repeat(1_400_000);
        case.write(writer, &[&big, &big, &big, "end"])
            .await
            .unwrap();
        assert_eq!(case.visible().await.len(), 4);
        let huge = "y".repeat(1_700_000);
        let error = case.write(writer, &[&huge]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::InvalidArgument, "stream_too_large")
        );
        // A refusal before the Store leaves the writer usable.
        case.write(writer, &["ok"]).await.unwrap();
    }
}

async fn a_reopen_of_a_closed_writer_goes_on_with_its_sequence() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        case.write(writer, &["1"]).await.unwrap();
        let _: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        let again = case.open(client(), |_| {}).await;
        assert_ne!(again, writer);
        case.write(again, &["2"]).await.unwrap();
        assert_eq!(case.visible().await, ["1", "2"]);
    }
}

async fn the_opens_of_one_producer_share_a_writer_until_the_last_one_lets_go() {
    for case in cases().await {
        let a = case.open(client(), |_| {}).await;
        let b = case.open(client(), |_| {}).await;
        assert_eq!(a, b);
        case.write(a, &["1"]).await.unwrap();
        let _: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: a })
            .await
            .unwrap();
        // The other reference still writes, on the same sequence.
        case.write(b, &["2"]).await.unwrap();
        case.ext.free_writer(b).unwrap();
        let error = case.write(b, &["3"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_unknown_writer");
        assert_eq!(case.visible().await, ["1", "2"]);
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
        case.write(old, &["old"]).await.unwrap();
        let new = case.open(named(2), |_| {}).await;
        case.write(new, &["new"]).await.unwrap();
        let error = case.write(old, &["late"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
        // A fence blocks the old epoch's later writes but keeps its earlier Records.
        assert_eq!(case.visible().await, ["old", "new"]);
    }
}

async fn closing_every_writer_drops_them_all() {
    for case in cases().await {
        let a = case.open(client(), |_| {}).await;
        let b = case.open(activity(1), |_| {}).await;
        case.write(a, &["a"]).await.unwrap();
        case.write(b, &["b"]).await.unwrap();
        let closed: crate::proto::CloseWritersResponse = case
            .call("CloseWriters", crate::proto::CloseWritersRequest {})
            .await
            .unwrap();
        assert_eq!(closed.failed, 0);
        assert!(case.write(a, &["x"]).await.is_err());
        assert!(case.write(b, &["x"]).await.is_err());
    }
}

/// A Store over another one, whose appends fail. It can write an append and lose its answer, as on
/// a dropped connection, and it refuses every append while it is down. It can also refuse the next
/// append as corrupt Store data.
struct Flaky {
    inner: Arc<dyn Store>,
    lose_next: AtomicBool,
    down: AtomicBool,
    corrupt_next: AtomicBool,
}

impl Flaky {
    /// A Store that is up, over `inner`.
    fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner,
            lose_next: AtomicBool::new(false),
            down: AtomicBool::new(false),
            corrupt_next: AtomicBool::new(false),
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
        if self.down.load(Ordering::SeqCst) {
            return Err(StreamError::storage_unavailable("the Store is down"));
        }
        if self.corrupt_next.swap(false, Ordering::SeqCst) {
            return Err(StreamError::storage_corrupt("a stored field is bad"));
        }
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
            ext: MemoryStreamRuntime::with_store(store.clone(), None),
            namespace: format!("ns-{}", target.stream),
            stream: target.stream,
        };
        let _: CreateStreamResponse = case
            .call(
                "CreateStream",
                CreateStreamRequest {
                    namespace: case.namespace.clone(),
                    stream_ref: Some(StreamRef {
                        id: case.stream.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        cases.push((store, case));
    }
    cases
}

async fn a_write_whose_answer_is_lost_goes_in_once() {
    for (_, case) in flaky_cases(true).await {
        let writer = case.open(client(), |_| {}).await;
        let (first, last) = case.write(writer, &["a", "b"]).await.unwrap();
        let read = case.read(0).await;
        assert_eq!(case.visible().await, ["a", "b"]);
        assert_eq!(
            (off(&read.records[0].offset), off(&read.records[1].offset)),
            (first, last)
        );
        case.write(writer, &["c"]).await.unwrap();
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
        case.write(writer, &["a"]).await.unwrap();
        store.down.store(true, Ordering::SeqCst);
        let error = case.write(writer, &["lost"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::Unavailable, "stream_storage_unavailable")
        );
        store.down.store(false, Ordering::SeqCst);
        case.write(writer, &["b"]).await.unwrap();
        assert_eq!(case.visible().await, ["a", "b"]);
        let closed: CloseWriterResponse = case
            .call("CloseWriter", CloseWriterRequest { writer_id: writer })
            .await
            .unwrap();
        assert!(off(&closed.last_offset) > 0);
    }
}

/// A write that meets corrupt Store data fails at once, since a retry reads the same data. The
/// writer then fails every later write.
async fn a_write_on_corrupt_store_data_fails_with_no_retry() {
    for (store, case) in flaky_cases(false).await {
        let writer = case.open(client(), |_| {}).await;
        store.corrupt_next.store(true, Ordering::SeqCst);
        let error = case.write(writer, &["a"]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::DataLoss, "stream_storage_corrupt")
        );
        assert!(case.visible().await.is_empty());
        let again = case.write(writer, &["b"]).await.unwrap_err();
        assert_eq!(reason(&again), "stream_storage_corrupt");
    }
}

async fn a_write_that_waits_behind_a_close_is_refused() {
    for case in cases().await {
        let writer = case.open(client(), |_| {}).await;
        case.write(writer, &["a"]).await.unwrap();
        let close = case.call::<_, CloseWriterResponse>(
            "CloseWriter",
            CloseWriterRequest { writer_id: writer },
        );
        let write = async {
            tokio::task::yield_now().await;
            case.write(writer, &["late"]).await
        };
        let (closed, written) = tokio::join!(close, write);
        closed.unwrap();
        assert_eq!(reason(&written.unwrap_err()), "stream_unknown_writer");
        assert_eq!(case.visible().await, ["a"]);
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(a_write_is_in_the_store_when_it_answers_its_offsets),
    case!(writes_split_at_the_batch_size_and_keep_one_sequence),
    case!(a_batch_size_over_the_store_limit_writes_in_more_batches),
    case!(a_failed_write_fails_every_later_write_and_the_close),
    case!(an_activity_retry_opens_on_a_closed_or_expired_stream),
    case!(a_closed_writer_is_gone),
    case!(a_write_splits_big_records_by_bytes_and_refuses_an_oversize_one),
    case!(a_reopen_of_a_closed_writer_goes_on_with_its_sequence),
    case!(the_opens_of_one_producer_share_a_writer_until_the_last_one_lets_go),
    case!(a_named_client_with_a_higher_epoch_fences_its_earlier_self),
    case!(closing_every_writer_drops_them_all),
    case!(a_write_whose_answer_is_lost_goes_in_once),
    case!(a_writer_writes_again_after_a_store_outage),
    case!(a_write_on_corrupt_store_data_fails_with_no_retry),
    case!(a_write_that_waits_behind_a_close_is_refused),
];
