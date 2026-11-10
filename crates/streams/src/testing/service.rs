//! The Stream service operations through the runtime's byte call, as lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{case, support::Needs};

use crate::{
    StreamError, StreamRuntime,
    ids::{client_producer_id, named_client_producer_id},
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecord, AppendRecordsRequest, AppendRecordsResponse,
        ClientEpoch, ClientProducer, CloseStreamRequest, CloseStreamResponse, CreateStreamRequest,
        CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse, LocalActivityEpoch,
        LocalActivityProducer, Producer, ReadRecordsRequest, ReadRecordsResponse,
        StreamCloseStatus, StreamErrorDetails, StreamIdConflictPolicy, StreamReadStart, StreamRef,
        StreamStatus, StreamWriter, WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
        stream_record::Item,
    },
};
use prost::Message;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use tonic::Code;

struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
}

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn unique() -> String {
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{now}-{n}")
}

/// A fresh Stream on each backend that gives `needs`.
async fn cases_for(needs: Needs) -> Vec<Case> {
    super::support::runtimes(needs)
        .await
        .into_iter()
        .map(|runtime| {
            let unique = unique();
            Case {
                ext: runtime.ext,
                namespace: runtime.namespace.unwrap_or(format!("ns-{unique}")),
                stream: format!("s-{unique}"),
            }
        })
        .collect()
}

async fn call<Req: Message, Resp: Message + Default>(
    ext: &StreamRuntime,
    method: &str,
    request: Req,
) -> Result<Resp, StreamError> {
    let bytes = ext.call(method, &request.encode_to_vec()).await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
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

/// The wire value of an internal offset.
fn wire(offset: i64) -> Vec<u8> {
    crate::offset::offset_to_wire(offset)
}

/// The writer of a write by `producer`.
fn writer_for(producer: Producer) -> Option<StreamWriter> {
    Some(StreamWriter {
        producer: Some(producer),
    })
}

fn client(attempt: i32) -> Producer {
    Producer {
        id: client_producer_id("me", "p"),
        kind: Some(Kind::Client(ClientProducer {
            epoch: Some(ClientEpoch { attempt }),
        })),
    }
}

/// A client Producer that a test names, at `attempt`. Every backend accepts it as sent.
fn named(name: &str, attempt: i32) -> Producer {
    Producer {
        id: named_client_producer_id(name),
        ..client(attempt)
    }
}

/// The Producer of run `run` in its first Workflow Task, as its close Activity sends it.
fn workflow(run: &str) -> Producer {
    Producer {
        id: crate::ids::workflow_producer_id(run),
        kind: Some(Kind::Workflow(WorkflowProducer {
            run_id: run.to_string(),
            epoch: Some(WorkflowTaskEpoch::default()),
        })),
    }
}

/// The fields of a create that uses an existing Stream.
fn use_existing() -> CreateStreamRequest {
    CreateStreamRequest {
        id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
        ..Default::default()
    }
}

fn record(text: &str) -> AppendRecord {
    AppendRecord {
        body: Some(Payload {
            data: text.as_bytes().to_vec(),
            ..Default::default()
        }),
    }
}

fn millis(ms: u64) -> Option<prost_types::Duration> {
    let duration = Duration::from_millis(ms);
    Some(prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

impl Case {
    fn stream_ref(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        })
    }

    async fn create(
        &self,
        retention: Option<prost_types::Duration>,
    ) -> Result<CreateStreamResponse, StreamError> {
        self.create_with(CreateStreamRequest {
            retention,
            ..Default::default()
        })
        .await
    }

    /// Creates the Stream with the fields of `request`, in the case's namespace and ref.
    async fn create_with(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, StreamError> {
        let request = CreateStreamRequest {
            namespace: self.namespace.clone(),
            stream_ref: self.stream_ref(),
            ..request
        };
        call(&self.ext, "CreateStream", request).await
    }

    async fn append(
        &self,
        producer: Producer,
        sequence: i64,
        records: Vec<AppendRecord>,
    ) -> Result<AppendRecordsResponse, StreamError> {
        call(
            &self.ext,
            "AppendRecords",
            AppendRecordsRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
                writer: writer_for(producer),
                sequence,
                records,
                ..Default::default()
            },
        )
        .await
    }

    async fn read(&self, request: ReadRecordsRequest) -> Result<ReadRecordsResponse, StreamError> {
        call(
            &self.ext,
            "ReadRecords",
            ReadRecordsRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
                ..request
            },
        )
        .await
    }

    async fn describe(&self) -> Result<DescribeStreamResponse, StreamError> {
        call(
            &self.ext,
            "DescribeStream",
            DescribeStreamRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
            },
        )
        .await
    }

    async fn close(&self, producer: Producer) -> Result<CloseStreamResponse, StreamError> {
        call(
            &self.ext,
            "CloseStream",
            CloseStreamRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
                writer: writer_for(producer),
                ..Default::default()
            },
        )
        .await
    }
}

fn texts(response: &ReadRecordsResponse) -> Vec<String> {
    response
        .records
        .iter()
        .map(|record| match &record.item {
            Some(Item::Data(data)) => String::from_utf8(data.body.clone().unwrap().data).unwrap(),
            Some(Item::Close(_)) => "<close>".to_string(),
            None => "?".to_string(),
        })
        .collect()
}

/// A create that uses a Stream that exists returns it with `created` false, whatever its
/// Retention. A Retention outside a proto Duration's range, or under 1 ms, fails.
async fn a_create_returns_the_stream_that_exists_whatever_its_retention() {
    for case in cases_for(Needs::CALLS).await {
        for (seconds, nanos) in [(i64::MAX, 0), (-1, 0), (0, 999_999)] {
            let retention = Some(prost_types::Duration { seconds, nanos });
            let error = case.create(retention).await.unwrap_err();
            assert_eq!(reason(&error), "stream_invalid");
        }
        assert!(case.create(None).await.unwrap().created);
        for retention in [None, millis(1000)] {
            let request = CreateStreamRequest {
                retention,
                ..use_existing()
            };
            assert!(!case.create_with(request).await.unwrap().created);
        }
    }
}

/// A create of an open Stream fails as `already_exists`, unless it uses the Stream or it is a
/// retry of the create that made the Stream. Such a retry has the same request id, and it gets
/// `created` true with either policy. A create needs a ref with an id.
async fn a_create_of_an_open_stream_fails_unless_it_uses_it_or_retries() {
    for case in cases_for(Needs::CALLS).await {
        let first = CreateStreamRequest {
            request_id: "create-1".to_string(),
            ..Default::default()
        };
        assert!(case.create_with(first.clone()).await.unwrap().created);
        let other = CreateStreamRequest {
            request_id: "create-2".to_string(),
            ..Default::default()
        };
        for request in [other.clone(), CreateStreamRequest::default()] {
            let error = case.create_with(request).await.unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::AlreadyExists, "stream_already_exists")
            );
        }
        let used = CreateStreamRequest {
            request_id: other.request_id,
            ..use_existing()
        };
        assert!(!case.create_with(used).await.unwrap().created);
        let retry = CreateStreamRequest {
            request_id: first.request_id.clone(),
            ..use_existing()
        };
        for request in [first, retry] {
            assert!(case.create_with(request).await.unwrap().created);
        }
        for stream_ref in [None, Some(StreamRef::default())] {
            let request = CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_ref,
                ..Default::default()
            };
            let error = call::<_, CreateStreamResponse>(&case.ext, "CreateStream", request)
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_invalid");
        }
    }
}

/// A create of a closed Stream fails as closed, before its conflict policy and before a retry of
/// the create that made the Stream.
async fn a_create_of_a_closed_stream_fails() {
    for case in cases_for(Needs::CALLS).await {
        let first = CreateStreamRequest {
            request_id: "create-1".to_string(),
            ..Default::default()
        };
        case.create_with(first.clone()).await.unwrap();
        case.close(client(1)).await.unwrap();
        for request in [first, CreateStreamRequest::default()] {
            let error = case.create_with(request).await.unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::FailedPrecondition, "stream_closed")
            );
        }
    }
}

/// A close writes no Record, so the describe's last offset stays at the newest Record. A second
/// close of the closed Stream succeeds, and a later append fails as closed.
async fn a_close_writes_no_record_and_a_second_close_succeeds() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let written = case.append(client(1), 1, vec![record("a")]).await.unwrap();
        case.close(client(1)).await.unwrap();
        case.close(client(1)).await.unwrap();
        let error = case
            .append(client(1), 2, vec![record("b")])
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_closed");
        let describe = DescribeStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: case.stream_ref(),
        };
        let described: DescribeStreamResponse =
            call(&case.ext, "DescribeStream", describe).await.unwrap();
        assert_eq!(described.status, StreamStatus::Closed as i32);
        assert_eq!(described.last_offset, written.last_offset);
    }
}

/// A create of a closed Stream whose Retention passed fails as expired while its Tombstone is
/// there, before the closed check and before `already_exists`.
async fn a_create_of_an_expired_stream_fails() {
    for case in cases_for(Needs::SHORT_TIMES).await {
        case.create(millis(300)).await.unwrap();
        case.close(client(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let error = case.create(None).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::NotFound, "stream_expired")
        );
    }
}

/// A Producer's epoch has a case, its id must match that kind, a Workflow appends only through a
/// Local Activity, and a Local Activity's epoch names the Workflow Task that dispatched it.
async fn a_producer_must_match_its_kind_and_not_be_a_workflow() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let activity = Kind::Activity(ActivityProducer {
            run_id: String::new(),
            epoch: Some(ActivityEpoch::default()),
        });
        for kind in [Some(activity), None] {
            let wrong = Producer { kind, ..client(1) };
            let error = case.append(wrong, 1, vec![record("x")]).await.unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::InvalidArgument, "stream_invalid")
            );
        }
        let error = case.append(workflow("r1"), 1, vec![]).await.unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        let no_task = Producer {
            id: crate::ids::local_activity_producer_id("r1", "call-1"),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: "r1".to_string(),
                epoch: Some(LocalActivityEpoch {
                    task: None,
                    attempt: 1,
                }),
            })),
        };
        let error = case
            .append(no_task, 1, vec![record("x")])
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// An append with no sequence takes the next one of its Producer's epoch, as a call with no writer
/// state sends it. Nothing dedupes it, so its retry writes the batch again. A negative sequence
/// fails.
async fn an_append_with_no_sequence_takes_the_next_one_of_its_epoch() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let first = case.append(client(1), 0, vec![record("a")]).await.unwrap();
        let retry = case.append(client(1), 0, vec![record("a")]).await.unwrap();
        assert!(
            retry.first_offset > first.last_offset,
            "{first:?} {retry:?}"
        );
        let resend = case.append(client(1), 2, vec![record("a")]).await.unwrap();
        assert_eq!(resend, retry);
        let error = case
            .append(client(1), -1, vec![record("b")])
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// A read is exclusive from its offset, a read from the end gives only new Records, an offset past
/// the newest fails with `stream_offset_too_high`, and one that decodes below zero is invalid.
async fn a_read_is_exclusive_from_its_offset_and_get_offset_reads_only_new_records() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let first = case
            .append(client(1), 1, vec![record("1"), record("2")])
            .await
            .unwrap();
        let all = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&all), ["1", "2"]);
        assert_eq!(all.records[0].offset, first.first_offset);
        assert_eq!(all.next_offset, first.last_offset);
        let from = case
            .read(ReadRecordsRequest {
                from_offset: first.first_offset.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&from), ["2"]);
        let newest = case.describe().await.unwrap().last_offset;
        let none = case
            .read(ReadRecordsRequest {
                from_offset: newest.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(none.records.is_empty());
        assert_eq!(none.next_offset, newest);
        let end = case
            .read(ReadRecordsRequest {
                start: StreamReadStart::End as i32,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(end.next_offset, newest);
        let error = case
            .read(ReadRecordsRequest {
                from_offset: wire(off(&newest) + 1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::OutOfRange, "stream_offset_too_high")
        );
        let error = case
            .read(ReadRecordsRequest {
                // An 8-byte value with the top bit set decodes to a negative offset.
                from_offset: vec![0x80, 0, 0, 0, 0, 0, 0, 0],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// A read waits for the Stream's create only as long as `create_wait`.
async fn a_read_waits_for_the_create_only_as_long_as_create_wait() {
    for case in cases_for(Needs::CALLS).await {
        let error = case.read(ReadRecordsRequest::default()).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::NotFound, "stream_not_found")
        );
        let started = Instant::now();
        let error = case
            .read(ReadRecordsRequest {
                create_wait: millis(200),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::NotFound);
        assert!(started.elapsed() >= Duration::from_millis(200));
        let ext = case.ext.clone();
        let (namespace, stream) = (case.namespace.clone(), case.stream.clone());
        let creator = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let request = CreateStreamRequest {
                namespace,
                stream_ref: Some(StreamRef {
                    id: stream,
                    ..Default::default()
                }),
                ..Default::default()
            };
            call::<_, CreateStreamResponse>(&ext, "CreateStream", request).await
        });
        let read = case
            .read(ReadRecordsRequest {
                create_wait: millis(3000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(read.records.is_empty());
        creator.await.unwrap().unwrap();
    }
}

/// A quiet read ends empty after its wait, and a long poll wakes on an append.
async fn a_quiet_read_ends_empty_after_its_wait_and_a_long_poll_wakes_on_append() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let first = case.append(client(1), 1, vec![record("1")]).await.unwrap();
        let from = first.last_offset;
        let started = Instant::now();
        let quiet = case
            .read(ReadRecordsRequest {
                from_offset: from.clone(),
                wait: millis(200),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(quiet.records.is_empty());
        assert_eq!(quiet.next_offset, from);
        assert!(!quiet.done);
        let ext = case.ext.clone();
        let (namespace, stream) = (case.namespace.clone(), case.stream.clone());
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let request = AppendRecordsRequest {
                namespace,
                r#ref: Some(StreamRef {
                    id: stream,
                    ..Default::default()
                }),
                writer: writer_for(client(1)),
                sequence: 2,
                records: vec![record("2")],
                ..Default::default()
            };
            call::<_, AppendRecordsResponse>(&ext, "AppendRecords", request).await
        });
        let woke = case
            .read(ReadRecordsRequest {
                from_offset: from,
                wait: millis(5000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&woke), ["2"]);
        writer.await.unwrap().unwrap();
    }
}

/// A read that gives the last item of a closed Stream is done, and so is every later read, which
/// gives no item. A close refuses later appends.
async fn a_close_ends_every_read_with_done() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        case.append(client(1), 1, vec![record("1")]).await.unwrap();
        case.close(client(1)).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["1", "<close>"]);
        assert!(read.done);
        let again = case
            .read(ReadRecordsRequest {
                from_offset: read.next_offset.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(again.records.is_empty() && again.done);
        assert_eq!(again.next_offset, read.next_offset);
        assert_eq!(
            case.describe().await.unwrap().status,
            StreamStatus::Closed as i32
        );
        let error = case
            .append(client(1), 2, vec![record("2")])
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
    }
}

/// A read of a closed Stream ends with a StreamClose item, at the offset after the newest Record.
/// Its status is CLOSED, and its Producer is the one that closed the Stream.
async fn a_closed_read_ends_with_a_close_item() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let written = case.append(client(1), 1, vec![record("1")]).await.unwrap();
        case.close(client(1)).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert!(read.done);
        let last = read.records.last().expect("a close item");
        assert_eq!(off(&last.offset), off(&written.last_offset) + 1);
        let Some(Item::Close(close)) = &last.item else {
            panic!("not a close item: {:?}", last.item);
        };
        assert_eq!(close.status, StreamCloseStatus::Closed as i32);
        assert_eq!(close.producer, Some(client(1)));
    }
}

/// An unknown method fails with UNIMPLEMENTED.
async fn an_unknown_method_is_unimplemented() {
    for case in cases_for(Needs::CALLS).await {
        let error = case.ext.call("Nope", &[]).await.unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);
    }
}

/// A read of a closed Stream whose Retention passed fails as expired, from any offset, instead
/// of waiting out its wait. A last read fails the same way.
async fn a_read_of_an_expired_closed_stream_fails_instead_of_waiting() {
    for case in cases_for(Needs::SHORT_TIMES).await {
        case.create(millis(300)).await.unwrap();
        let first = case.append(client(1), 1, vec![record("1")]).await.unwrap();
        case.close(client(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let describe = case.describe().await.unwrap();
        assert_eq!(describe.status, StreamStatus::Expired as i32);
        for request in [
            ReadRecordsRequest::default(),
            ReadRecordsRequest {
                from_offset: first.first_offset,
                ..Default::default()
            },
            ReadRecordsRequest {
                from_offset: describe.last_offset,
                ..Default::default()
            },
        ] {
            let started = Instant::now();
            let error = case
                .read(ReadRecordsRequest {
                    wait: millis(1500),
                    ..request
                })
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_expired");
            assert!(started.elapsed() < Duration::from_secs(1));
        }
        let last = case
            .read(ReadRecordsRequest {
                last: 5,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&last), "stream_expired");
    }
}

/// Creates the case's Stream with a 300 ms Retention. While it is open, its meta stays and only
/// its log expires.
async fn create_short(case: &Case) {
    case.create(millis(300)).await.unwrap();
}

/// A read from an offset that an idle open Stream's expired log took fails as expired at once,
/// before the next write and after it. A read from the start skips the gap.
async fn a_read_behind_an_expired_log_fails_before_and_after_the_next_write() {
    for case in cases_for(Needs::SHORT_TIMES).await {
        create_short(&case).await;
        let first = case
            .append(client(1), 1, vec![record("a"), record("b")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let behind = ReadRecordsRequest {
            from_offset: first.first_offset.clone(),
            wait: millis(1500),
            ..Default::default()
        };
        let started = Instant::now();
        let error = case.read(behind.clone()).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
        assert!(started.elapsed() < Duration::from_secs(1));
        case.append(client(1), 2, vec![record("c")]).await.unwrap();
        let error = case.read(behind).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["c"]);
    }
}

/// A close after an idle open Stream's log expired keeps the gap, so a read from an offset that the
/// log took fails as expired.
async fn a_close_after_the_log_expired_keeps_the_gap() {
    for case in cases_for(Needs::SHORT_TIMES).await {
        create_short(&case).await;
        let first = case
            .append(client(1), 1, vec![record("a"), record("b")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        case.close(client(1)).await.unwrap();
        let behind = ReadRecordsRequest {
            from_offset: first.first_offset.clone(),
            ..Default::default()
        };
        let error = case.read(behind).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
    }
}

/// A create answer names the configured backend, and the Store never reads it.
async fn a_create_answer_names_the_backend_and_the_store_never_reads_it() {
    for ext in super::support::named_runtimes("redis-a") {
        let unique = unique();
        let case = Case {
            ext,
            namespace: format!("ns-{unique}"),
            stream: format!("s-{unique}"),
        };
        let created = case.create(None).await.unwrap();
        let stream = created.r#ref.unwrap();
        assert_eq!(stream.id, case.stream);
        assert_eq!(stream.backend, "redis-a");
        // A ref with any backend, or none, reaches the same Stream.
        let named = |backend: &str| {
            Some(StreamRef {
                id: case.stream.clone(),
                backend: backend.to_string(),
            })
        };
        for (sequence, stream) in [(1, named("redis-a")), (2, named("")), (3, named("other"))] {
            let request = AppendRecordsRequest {
                namespace: case.namespace.clone(),
                r#ref: stream,
                writer: writer_for(client(1)),
                sequence,
                records: vec![record(&sequence.to_string())],
                ..Default::default()
            };
            let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", request).await.unwrap();
        }
        let read = ReadRecordsRequest {
            namespace: case.namespace.clone(),
            r#ref: named("other"),
            ..Default::default()
        };
        let read: ReadRecordsResponse = call(&case.ext, "ReadRecords", read).await.unwrap();
        assert_eq!(texts(&read), ["1", "2", "3"]);
    }
    // With no backend in the config, the ref has none, and lang reads it as the default.
    for case in cases_for(Needs::OWN_CONFIG).await {
        let created = case.create(None).await.unwrap();
        assert_eq!(created.r#ref.unwrap().backend, "");
    }
}

/// A read of a closed Stream gives `done` only with its last item. With no item left, a read
/// answers at once with `done`, even with a long wait.
async fn a_read_of_a_closed_stream_with_no_item_left_answers_at_once() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let records = vec![record("1"), record("2")];
        let written = case.append(client(1), 1, records).await.unwrap();
        case.close(client(1)).await.unwrap();
        let first = case
            .read(ReadRecordsRequest {
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&first), ["1"]);
        assert!(!first.done);
        let rest = case
            .read(ReadRecordsRequest {
                from_offset: first.next_offset,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&rest), ["2", "<close>"]);
        assert!(rest.done);
        for request in [
            ReadRecordsRequest {
                from_offset: written.last_offset.clone(),
                ..Default::default()
            },
            ReadRecordsRequest {
                start: StreamReadStart::End as i32,
                ..Default::default()
            },
        ] {
            let started = Instant::now();
            let read = case
                .read(ReadRecordsRequest {
                    wait: millis(5000),
                    ..request
                })
                .await
                .unwrap();
            assert!(started.elapsed() < Duration::from_secs(2));
            assert!(read.records.is_empty() && read.done);
            assert_eq!(read.next_offset, written.last_offset);
        }
    }
}

/// A long poll at the end of an open Stream wakes on its close and answers with `done`.
async fn a_waiting_read_wakes_with_done_on_a_close() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let written = case.append(client(1), 1, vec![record("1")]).await.unwrap();
        let ext = case.ext.clone();
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: case.stream_ref(),
            writer: writer_for(client(1)),
            ..Default::default()
        };
        let closer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            call::<_, CloseStreamResponse>(&ext, "CloseStream", close).await
        });
        let started = Instant::now();
        let woke = case
            .read(ReadRecordsRequest {
                from_offset: written.last_offset,
                wait: millis(5000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(4));
        assert!(woke.records.is_empty() && woke.done);
        closer.await.unwrap().unwrap();
    }
}

/// A last read gives the newest Records oldest first, and gives `done` for a closed Stream.
async fn a_last_read_gives_the_newest_records_oldest_first() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let last = |n: i32| ReadRecordsRequest {
            last: n,
            ..Default::default()
        };
        assert!(case.read(last(5)).await.unwrap().records.is_empty());
        let records = vec![record("c1"), record("c2"), record("c3")];
        case.append(client(1), 1, records).await.unwrap();
        case.append(named("w", 1), 1, vec![record("a2")])
            .await
            .unwrap();
        assert_eq!(
            texts(&case.read(last(3)).await.unwrap()),
            ["c2", "c3", "a2"]
        );
        let read = case.read(last(2)).await.unwrap();
        assert_eq!(texts(&read), ["c3", "a2"]);
        assert!(!read.done);
        let newest = case.describe().await.unwrap().last_offset;
        assert_eq!(read.next_offset, newest);
        let error = case
            .read(ReadRecordsRequest {
                from_offset: wire(1),
                ..last(1)
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        case.close(client(1)).await.unwrap();
        let read = case.read(last(1)).await.unwrap();
        assert_eq!(texts(&read), ["a2"]);
        assert!(read.done);
    }
}

/// A page stays under `max_bytes`, and a larger Record is still returned alone.
async fn a_page_keeps_under_max_bytes_and_a_larger_record_comes_alone() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let big = "x".repeat(1000);
        let records = vec![record("a"), record("b"), record(&big), record("c")];
        case.append(client(1), 1, records).await.unwrap();
        let capped = |from_offset| ReadRecordsRequest {
            from_offset,
            max_bytes: 200,
            ..Default::default()
        };
        let first = case.read(capped(vec![])).await.unwrap();
        assert_eq!(texts(&first), ["a", "b"]);
        let second = case.read(capped(first.next_offset)).await.unwrap();
        assert_eq!(texts(&second), [big]);
        let third = case.read(capped(second.next_offset)).await.unwrap();
        assert_eq!(texts(&third), ["c"]);
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(a_create_returns_the_stream_that_exists_whatever_its_retention),
    case!(a_create_of_an_open_stream_fails_unless_it_uses_it_or_retries),
    case!(a_create_of_a_closed_stream_fails),
    case!(a_close_writes_no_record_and_a_second_close_succeeds),
    case!(a_create_of_an_expired_stream_fails),
    case!(a_producer_must_match_its_kind_and_not_be_a_workflow),
    case!(an_append_with_no_sequence_takes_the_next_one_of_its_epoch),
    case!(a_read_is_exclusive_from_its_offset_and_get_offset_reads_only_new_records),
    case!(a_read_waits_for_the_create_only_as_long_as_create_wait),
    case!(a_quiet_read_ends_empty_after_its_wait_and_a_long_poll_wakes_on_append),
    case!(a_close_ends_every_read_with_done),
    case!(a_closed_read_ends_with_a_close_item),
    case!(an_unknown_method_is_unimplemented),
    case!(a_read_of_an_expired_closed_stream_fails_instead_of_waiting),
    case!(a_read_behind_an_expired_log_fails_before_and_after_the_next_write),
    case!(a_close_after_the_log_expired_keeps_the_gap),
    case!(a_create_answer_names_the_backend_and_the_store_never_reads_it),
    case!(a_read_of_a_closed_stream_with_no_item_left_answers_at_once),
    case!(a_waiting_read_wakes_with_done_on_a_close),
    case!(a_last_read_gives_the_newest_records_oldest_first),
    case!(a_page_keeps_under_max_bytes_and_a_larger_record_comes_alone),
];
