//! Readers with resume points and withdrawn offsets, through the runtime's byte call, as lang
//! sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::case;
use crate::{
    StreamError, StreamRuntime,
    proto::{
        ActivityContext, AppendRecord, CloseStreamAsRequest, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, FreeReaderRequest,
        FreeResponse, OpenActivityScopeRequest, OpenClientScopeRequest, OpenReaderRequest,
        OpenReaderResponse, OpenScopeResponse, Producer, ProducerEpoch, PublishRequest,
        PublishResponse, ReadNextRequest, ReaderItem, ReaderPage, StreamProducerKind,
        StreamReadStart, StreamRef, Supersession, WithdrawsRequest, WithdrawsResponse,
        stream_record::Item,
    },
};
use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
}

fn unique() -> String {
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{now}-{n}")
}

/// A case on each backend, with its Stream not created yet.
fn uncreated() -> Vec<Case> {
    super::support::stream_runtimes()
        .into_iter()
        .map(|ext| {
            let unique = unique();
            Case {
                ext,
                namespace: format!("ns-{unique}"),
                stream: format!("s-{unique}"),
            }
        })
        .collect()
}

async fn cases() -> Vec<Case> {
    let cases = uncreated();
    for case in &cases {
        case.create().await;
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

fn activity(attempt: i32) -> ActivityContext {
    ActivityContext {
        in_workflow: true,
        workflow_id: "wf".into(),
        workflow_run_id: "r1".into(),
        activity_id: "a".into(),
        attempt,
        current_attempt_scheduled_time: Some(Timestamp {
            seconds: 10 * attempt as i64,
            nanos: 0,
        }),
        ..Default::default()
    }
}

fn texts(page: &ReaderPage) -> Vec<String> {
    page.items
        .iter()
        .filter_map(|item| match &item.record.as_ref().unwrap().item {
            Some(Item::Data(data)) => {
                Some(String::from_utf8(data.body.clone().unwrap().data).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn offset(item: &ReaderItem) -> i64 {
    item.record.as_ref().unwrap().offset
}

fn is_superseded(item: &ReaderItem) -> bool {
    matches!(
        item.record.as_ref().unwrap().item,
        Some(Item::Superseded(_))
    )
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

    async fn create(&self) {
        let _: CreateStreamResponse = self
            .call(
                "CreateStream",
                CreateStreamRequest {
                    namespace: self.namespace.clone(),
                    stream_id: self.stream.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    fn client(&self) -> u64 {
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

    async fn activity(&self, attempt: i32) -> u64 {
        let opened: OpenScopeResponse = self
            .call(
                "OpenActivityScope",
                OpenActivityScopeRequest {
                    namespace: self.namespace.clone(),
                    context: Some(activity(attempt)),
                    streams: vec![self.stream_ref()],
                },
            )
            .await
            .unwrap();
        opened.scope_id
    }

    async fn publish(&self, scope_id: u64, texts: &[&str]) -> PublishResponse {
        self.call(
            "Publish",
            PublishRequest {
                scope_id,
                stream: Some(self.stream_ref()),
                records: texts
                    .iter()
                    .map(|text| AppendRecord {
                        body: Some(Payload {
                            data: text.as_bytes().to_vec(),
                            ..Default::default()
                        }),
                    })
                    .collect(),
            },
        )
        .await
        .unwrap()
    }

    fn open(&self, change: impl FnOnce(&mut OpenReaderRequest)) -> Result<u64, StreamError> {
        let mut request = OpenReaderRequest {
            namespace: self.namespace.clone(),
            stream: Some(self.stream_ref()),
            ..Default::default()
        };
        change(&mut request);
        let opened: OpenReaderResponse = self.call_sync("OpenReader", request)?;
        Ok(opened.reader_id)
    }

    async fn next(&self, reader_id: u64, max_wait_ms: u64) -> Result<ReaderPage, StreamError> {
        self.call(
            "ReadNext",
            ReadNextRequest {
                reader_id,
                max_wait: millis(max_wait_ms),
            },
        )
        .await
    }
}

/// Each item carries the resume point after it. A read with no wait ends at the newest Record,
/// which the page after the last Record tells.
async fn each_item_carries_its_resume_point() {
    for case in cases().await {
        let client = case.client();
        let written = case.publish(client, &["a", "b", "c"]).await;
        let reader = case.open(|_| {}).unwrap();
        let page = case.next(reader, 1000).await.unwrap();
        assert_eq!(texts(&page), ["a", "b", "c"]);
        for item in &page.items {
            assert_eq!(item.next_offset, offset(item) + 1);
        }
        assert_eq!(page.next_offset, written.last_offset + 1);
        let after = case.next(reader, 1000).await.unwrap();
        assert!(after.items.is_empty() && after.done);
        assert_eq!(after.next_offset, page.next_offset);
        assert!(case.next(reader, 1000).await.unwrap().done);
    }
}

/// A Superseded item lists the offsets of the Records that this read gave and it withdraws. Its
/// resume point is its own offset.
async fn a_superseded_item_lists_what_it_withdraws_from_this_read() {
    for case in cases().await {
        let first = case.activity(1).await;
        case.publish(first, &["a1", "a2"]).await;
        let reader = case.open(|r| r.wait_new_records = true).unwrap();
        let page = case.next(reader, 1000).await.unwrap();
        let shown: Vec<i64> = page.items.iter().map(offset).collect();
        assert_eq!(shown.len(), 2);
        let retry = case.activity(2).await;
        case.publish(retry, &["b"]).await;
        let mut items = vec![];
        while items.len() < 2 {
            let page = case.next(reader, 1000).await.unwrap();
            assert!(!page.done);
            items.extend(page.items);
        }
        assert!(is_superseded(&items[0]));
        assert_eq!(items[0].withdrawn_offsets, shown);
        assert_eq!(items[0].next_offset, offset(&items[0]));
        assert!(items[1].withdrawn_offsets.is_empty());
    }
}

/// A read with `last` starts with the newest Records. With a wait it then goes on after them.
async fn a_last_read_starts_with_the_newest_records() {
    for case in cases().await {
        let client = case.client();
        case.publish(client, &["1", "2", "3", "4"]).await;
        let reader = case.open(|r| r.last = 2).unwrap();
        let page = case.next(reader, 1000).await.unwrap();
        assert_eq!(texts(&page), ["3", "4"]);
        assert!(page.done);
        let reader = case
            .open(|r| {
                r.last = 1;
                r.wait_new_records = true;
            })
            .unwrap();
        assert_eq!(texts(&case.next(reader, 1000).await.unwrap()), ["4"]);
        case.publish(client, &["5"]).await;
        assert_eq!(texts(&case.next(reader, 1000).await.unwrap()), ["5"]);
    }
}

/// A read from END gives only new Records. A quiet read returns an empty page at `max_wait`, with
/// the resume point after the newest Record.
async fn an_end_read_gives_only_new_records() {
    for case in cases().await {
        let client = case.client();
        let old = case.publish(client, &["old"]).await;
        let reader = case
            .open(|r| {
                r.start = StreamReadStart::End as i32;
                r.wait_new_records = true;
            })
            .unwrap();
        let started = Instant::now();
        let quiet = case.next(reader, 300).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(250));
        assert!(quiet.items.is_empty() && !quiet.done);
        assert_eq!(quiet.next_offset, old.last_offset + 1);
        case.publish(client, &["new"]).await;
        assert_eq!(texts(&case.next(reader, 1000).await.unwrap()), ["new"]);
    }
}

/// A read waits for the Stream's create only as long as `create_wait`.
async fn a_read_waits_for_the_create_up_to_create_wait() {
    for case in uncreated() {
        let reader = case.open(|_| {}).unwrap();
        let error = case.next(reader, 1000).await.unwrap_err();
        assert_eq!(error.reason.token(), "stream_not_found");
        let reader = case
            .open(|r| {
                r.wait_new_records = true;
                r.create_wait = millis(5000);
            })
            .unwrap();
        let write = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            case.create().await;
            let client = case.client();
            case.publish(client, &["x"]).await;
        };
        let (page, ()) = tokio::join!(case.next(reader, 4000), write);
        assert_eq!(texts(&page.unwrap()), ["x"]);
    }
}

/// The create wait starts at the first `ReadNext` that returns, not at the open or at a dropped
/// call.
async fn the_create_wait_starts_at_the_first_read() {
    for case in uncreated() {
        let reader = case
            .open(|r| {
                r.wait_new_records = true;
                r.create_wait = millis(1000);
            })
            .unwrap();
        tokio::select! {
            biased;
            _ = case.next(reader, 3000) => panic!("the read answered in its first poll"),
            _ = std::future::ready(()) => {}
        }
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let write = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            case.create().await;
            let client = case.client();
            case.publish(client, &["x"]).await;
        };
        let (page, ()) = tokio::join!(case.next(reader, 3000), write);
        assert_eq!(texts(&page.unwrap()), ["x"]);
    }
}

/// The close ends the read, as its last item.
async fn the_close_ends_the_read() {
    for case in cases().await {
        let client = case.client();
        case.publish(client, &["a"]).await;
        let _: CloseStreamResponse = case
            .call(
                "CloseStreamAs",
                CloseStreamAsRequest {
                    scope_id: client,
                    close: Some(CloseStreamRequest {
                        stream: Some(case.stream_ref()),
                        ..Default::default()
                    }),
                },
            )
            .await
            .unwrap();
        let reader = case.open(|r| r.wait_new_records = true).unwrap();
        let page = case.next(reader, 1000).await.unwrap();
        assert_eq!(texts(&page), ["a"]);
        let last = page.items.last().unwrap();
        assert!(matches!(
            last.record.as_ref().unwrap().item,
            Some(Item::Close(_))
        ));
        assert!(page.done);
        assert_eq!(page.next_offset, offset(last) + 1);
    }
}

/// A dropped `ReadNext` leaves the reader as it was.
async fn a_dropped_read_leaves_the_reader_as_it_was() {
    for case in cases().await {
        let reader = case.open(|r| r.wait_new_records = true).unwrap();
        let dropped = tokio::time::timeout(Duration::from_millis(100), case.next(reader, 5000));
        assert!(dropped.await.is_err());
        let client = case.client();
        let written = case.publish(client, &["a"]).await;
        let page = case.next(reader, 1000).await.unwrap();
        assert_eq!(texts(&page), ["a"]);
        assert_eq!(page.next_offset, written.last_offset + 1);
    }
}

/// Bad options are refused at the open, and a freed reader is gone.
async fn bad_options_are_refused_and_a_freed_reader_is_gone() {
    for case in cases().await {
        let bad: [fn(&mut OpenReaderRequest); 4] = [
            |r| r.last = -1,
            |r| r.from_offset = -1,
            |r| {
                r.last = 2;
                r.from_offset = 5;
            },
            |r| {
                r.last = 2;
                r.start = StreamReadStart::End as i32;
            },
        ];
        for change in bad {
            assert_eq!(
                case.open(change).unwrap_err().reason.token(),
                "stream_invalid"
            );
        }
        // A huge wait counts as a year, so it doesn't overflow.
        let huge = Some(prost_types::Duration {
            seconds: i64::MAX,
            nanos: 0,
        });
        let reader = case.open(|r| r.create_wait = huge).unwrap();
        let page: ReaderPage = case
            .call(
                "ReadNext",
                ReadNextRequest {
                    reader_id: reader,
                    max_wait: huge,
                },
            )
            .await
            .unwrap();
        assert!(page.done);
        let reader = case.open(|_| {}).unwrap();
        let _: FreeResponse = case
            .call_sync("FreeReader", FreeReaderRequest { reader_id: reader })
            .unwrap();
        let error = case.next(reader, 100).await.unwrap_err();
        assert_eq!(error.reason.token(), "stream_unknown_writer");
    }
}

/// The static `Withdraws` applies the withdrawal rule to a Producer of an earlier read.
async fn the_static_withdrawal_rule_matches_the_reader() {
    for case in cases().await {
        let producer = |attempt| Producer {
            kind: StreamProducerKind::Activity as i32,
            id: crate::ids::activity_producer_id("r1", "a", ""),
            epoch: Some(ProducerEpoch {
                attempt,
                ..Default::default()
            }),
            run_id: "r1".into(),
        };
        let supersession = |kind: StreamProducerKind| Supersession {
            producer_kind: kind as i32,
            producer_id: crate::ids::activity_producer_id("r1", "a", ""),
            epoch: producer(2).epoch,
            run_id: "r1".into(),
            ..Default::default()
        };
        for (kind, attempt, expected) in [
            (StreamProducerKind::Activity, 1, true),
            (StreamProducerKind::Activity, 2, false),
            (StreamProducerKind::Client, 1, false),
        ] {
            let answer: WithdrawsResponse = case
                .call_sync(
                    "Withdraws",
                    WithdrawsRequest {
                        supersession: Some(supersession(kind)),
                        producer: Some(producer(attempt)),
                    },
                )
                .unwrap();
            assert_eq!(answer.withdraws, expected, "{kind:?} {attempt}");
        }
        let error = crate::call_static("Withdraws", &WithdrawsRequest::default().encode_to_vec())
            .unwrap_err();
        assert_eq!(error.reason.token(), "stream_invalid");
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(each_item_carries_its_resume_point),
    case!(a_superseded_item_lists_what_it_withdraws_from_this_read),
    case!(a_last_read_starts_with_the_newest_records),
    case!(an_end_read_gives_only_new_records),
    case!(a_read_waits_for_the_create_up_to_create_wait),
    case!(the_create_wait_starts_at_the_first_read),
    case!(the_close_ends_the_read),
    case!(a_dropped_read_leaves_the_reader_as_it_was),
    case!(bad_options_are_refused_and_a_freed_reader_is_gone),
    case!(the_static_withdrawal_rule_matches_the_reader),
];
