//! The Stream service operations through the runtime's byte call, as lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{case, support::Needs};

use crate::{
    StreamError, StreamRuntime,
    epoch::withdraws,
    ids::{activity_producer_id, client_producer_id, named_client_producer_id},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, Producer, ProducerEpoch, ReadRecordsRequest, ReadRecordsResponse,
        StreamClose, StreamCloseStatus, StreamErrorDetails, StreamProducerKind, StreamReadStart,
        StreamRecord, StreamRef, StreamStatus, Supersession, WorkflowTaskEpoch,
        stream_record::Item,
    },
};
use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload,
    failure::v1::{Failure, failure::FailureInfo},
};
use tonic::Code;

struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
    /// What the backend gives, for a step that needs more than the case asked for.
    gives: Needs,
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
                gives: runtime.gives,
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

fn client(attempt: i32) -> Producer {
    Producer {
        kind: StreamProducerKind::Client as i32,
        id: client_producer_id("me", "p"),
        epoch: Some(ProducerEpoch {
            attempt,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

/// A client Producer that a test names, at `attempt`. Every backend accepts it as sent.
fn named(name: &str, attempt: i32) -> Producer {
    Producer {
        kind: StreamProducerKind::Client as i32,
        id: named_client_producer_id(name),
        epoch: Some(ProducerEpoch {
            attempt,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

/// An Activity Producer of a made-up run, at `attempt`. A higher attempt withdraws the lower ones'
/// Records. The server requires the Activity's task token, so only Redis and memory accept it.
fn activity(attempt: i32, scheduled: i64) -> Producer {
    Producer {
        kind: StreamProducerKind::Activity as i32,
        id: activity_producer_id("r1", "a", "s"),
        epoch: Some(ProducerEpoch {
            attempt_scheduled_time: Some(Timestamp {
                seconds: scheduled,
                nanos: 0,
            }),
            attempt,
            ..Default::default()
        }),
        run_id: "r1".to_string(),
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
    fn stream(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        })
    }

    async fn create(
        &self,
        retention: Option<prost_types::Duration>,
    ) -> Result<CreateStreamResponse, StreamError> {
        call(
            &self.ext,
            "CreateStream",
            CreateStreamRequest {
                namespace: self.namespace.clone(),
                stream_id: self.stream.clone(),
                retention,
                ..Default::default()
            },
        )
        .await
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
                stream: self.stream(),
                producer: Some(producer),
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
                stream: self.stream(),
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
                stream: self.stream(),
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
                stream: self.stream(),
                producer: Some(producer),
                result: Some(Payload {
                    data: b"done".to_vec(),
                    ..Default::default()
                }),
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
            Some(Item::Superseded(_)) => "<superseded>".to_string(),
            Some(Item::Close(_)) => "<close>".to_string(),
            None => "?".to_string(),
        })
        .collect()
}

fn superseded_of(record: &StreamRecord) -> &Supersession {
    match &record.item {
        Some(Item::Superseded(supersession)) => supersession,
        other => panic!("not a Superseded item: {other:?}"),
    }
}

fn close_of(record: &StreamRecord) -> &StreamClose {
    match &record.item {
        Some(Item::Close(close)) => close,
        other => panic!("not the close: {other:?}"),
    }
}

/// A create of a Stream that exists returns it with `created` false, whatever its Retention. A
/// Retention outside a proto Duration's range fails.
async fn a_create_returns_the_stream_that_exists_whatever_its_retention() {
    for case in cases_for(Needs::CALLS).await {
        for seconds in [i64::MAX, -1] {
            let retention = Some(prost_types::Duration { seconds, nanos: 0 });
            let error = case.create(retention).await.unwrap_err();
            assert_eq!(reason(&error), "stream_invalid");
        }
        assert!(case.create(None).await.unwrap().created);
        assert!(!case.create(None).await.unwrap().created);
        assert!(!case.create(millis(1000)).await.unwrap().created);
    }
}

/// A create of a closed Stream fails as closed.
async fn a_create_of_a_closed_stream_fails() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            stream: case.stream(),
            producer: Some(client(1)),
            ..Default::default()
        };
        let _: CloseStreamResponse = call(&case.ext, "CloseStream", close).await.unwrap();
        let error = case.create(None).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
    }
}

/// A create of a Stream whose Retention passed fails as expired while its Tombstone is there.
async fn a_create_of_an_expired_stream_fails() {
    for case in cases_for(Needs::SHORT_TIMES).await {
        case.create(millis(300)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let error = case.create(None).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::NotFound, "stream_expired")
        );
    }
}

/// A create with an owner needs a Workflow Producer and a ref of its run chain, and the answer's
/// ref names that chain. A create that names another owner fails, and no run withdraws itself.
async fn a_create_checks_its_owner_and_its_ref() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        let workflow = Producer {
            kind: StreamProducerKind::Workflow as i32,
            id: crate::ids::workflow_producer_id(&run),
            run_id: run.clone(),
            ..Default::default()
        };
        let local_activity = Producer {
            kind: StreamProducerKind::LocalActivity as i32,
            id: crate::ids::local_activity_producer_id(&run, "call-1"),
            ..workflow.clone()
        };
        let chain = |workflow_id: &str| StreamRef {
            id: case.stream.clone(),
            owner: super::support::workflow_execution(workflow_id, "first-run"),
            ..Default::default()
        };
        let create =
            |owner: Option<&Producer>, stream: StreamRef, withdraw_run: &str| CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_id: case.stream.clone(),
                owner: owner.cloned(),
                r#ref: Some(stream),
                withdraw_run: withdraw_run.to_string(),
                ..Default::default()
            };
        for request in [
            create(Some(&local_activity), chain("agent"), ""),
            create(Some(&workflow), case.stream().unwrap(), ""),
            create(
                Some(&workflow),
                StreamRef {
                    id: format!("{}-other", case.stream),
                    ..chain("agent")
                },
                "",
            ),
            create(Some(&workflow), chain("agent"), &run),
        ] {
            let error = call::<_, CreateStreamResponse>(&case.ext, "CreateStream", request)
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_invalid");
        }
        let created: CreateStreamResponse = call(
            &case.ext,
            "CreateStream",
            create(Some(&workflow), chain("agent"), ""),
        )
        .await
        .unwrap();
        assert_eq!(created.stream.unwrap().owner, chain("agent").owner);
        for request in [
            create(Some(&workflow), chain("another-agent"), ""),
            create(None, case.stream().unwrap(), ""),
        ] {
            let error = call::<_, CreateStreamResponse>(&case.ext, "CreateStream", request)
                .await
                .unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::FailedPrecondition, "stream_not_owner")
            );
        }
    }
}

/// A Producer's id must match its kind, a Workflow appends only through a Local Activity, and a
/// Local Activity's epoch names the Workflow Task that dispatched it.
async fn a_producer_must_match_its_kind_and_not_be_a_workflow() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let wrong = Producer {
            kind: StreamProducerKind::Activity as i32,
            ..client(1)
        };
        let error = case.append(wrong, 1, vec![record("x")]).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::InvalidArgument, "stream_invalid")
        );
        let workflow = Producer {
            kind: StreamProducerKind::Workflow as i32,
            id: crate::ids::workflow_producer_id("r1"),
            run_id: "r1".to_string(),
            epoch: None,
        };
        let error = case.append(workflow, 1, vec![]).await.unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        let no_task = Producer {
            kind: StreamProducerKind::LocalActivity as i32,
            id: crate::ids::local_activity_producer_id("r1", "call-1"),
            run_id: "r1".to_string(),
            epoch: Some(ProducerEpoch {
                attempt: 1,
                ..Default::default()
            }),
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

/// A read is inclusive from its offset, a read from the end gives only new Records, an offset past
/// the newest + 1 fails with `stream_offset`, and one below zero is invalid.
async fn a_read_is_inclusive_from_its_offset_and_get_offset_reads_only_new_records() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let first = case
            .append(client(1), 1, vec![record("1"), record("2")])
            .await
            .unwrap();
        let all = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&all), ["1", "2"]);
        assert_eq!(all.records[0].offset, first.first_offset);
        assert_eq!(all.next_offset, first.last_offset + 1);
        let from = case
            .read(ReadRecordsRequest {
                from_offset: first.last_offset,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&from), ["2"]);
        let next = case.describe().await.unwrap().last_offset + 1;
        let none = case
            .read(ReadRecordsRequest {
                from_offset: next,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(none.records.is_empty());
        assert_eq!(none.next_offset, next);
        let end = case
            .read(ReadRecordsRequest {
                start: StreamReadStart::End as i32,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(end.next_offset, next);
        let error = case
            .read(ReadRecordsRequest {
                from_offset: next + 1,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::OutOfRange, "stream_offset")
        );
        let error = case
            .read(ReadRecordsRequest {
                from_offset: -1,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// The first batch of a higher Activity attempt puts its `Superseded` item just before its
/// Records, and a page keeps the two together under `max_records`.
async fn a_higher_attempt_puts_its_superseded_item_before_its_batch() {
    for case in cases_for(Needs::UNCHECKED).await {
        case.create(None).await.unwrap();
        case.append(activity(1, 10), 1, vec![record("old")])
            .await
            .unwrap();
        case.append(activity(2, 20), 1, vec![record("new1"), record("new2")])
            .await
            .unwrap();
        let all = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&all), ["old", "<superseded>", "new1", "new2"]);
        // The item is just before the batch's first Record. Redis stores both in one entry with one
        // offset, and the server gives the item its own offset just below.
        let superseded = &all.records[1];
        assert!(all.records[0].offset < superseded.offset);
        assert!(superseded.offset <= all.records[2].offset);
        let supersession = superseded_of(superseded);
        assert_eq!(supersession.producer_id, activity(2, 20).id);
        let mut from_offset = 0;
        for expected in [&["old"][..], &["<superseded>", "new1"], &["new2"]] {
            let page = case
                .read(ReadRecordsRequest {
                    from_offset,
                    max_records: 2,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(texts(&page), expected);
            from_offset = page.next_offset;
        }
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
                stream_id: stream,
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
        let from = first.last_offset + 1;
        let started = Instant::now();
        let quiet = case
            .read(ReadRecordsRequest {
                from_offset: from,
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
                stream: Some(StreamRef {
                    id: stream,
                    ..Default::default()
                }),
                producer: Some(client(1)),
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

/// A close ends every read with its close Record and refuses later appends.
async fn a_close_ends_every_read_with_its_record() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        case.append(client(1), 1, vec![record("1")]).await.unwrap();
        case.close(client(1)).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["1", "<close>"]);
        assert!(read.done);
        assert_eq!(
            close_of(&read.records[1]).result.as_ref().unwrap().data,
            b"done"
        );
        let again = case
            .read(ReadRecordsRequest {
                from_offset: read.next_offset,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&again), ["<close>"]);
        assert!(again.done);
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

/// An unknown method fails with UNIMPLEMENTED.
async fn an_unknown_method_is_unimplemented() {
    for case in cases_for(Needs::CALLS).await {
        let error = case.ext.call("Nope", &[]).await.unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);
    }
}

/// A read of an open Stream that expired fails at once instead of waiting.
async fn a_read_of_an_expired_open_stream_fails_instead_of_waiting() {
    for case in cases_for(Needs::SHORT_TIMES).await {
        case.create(millis(300)).await.unwrap();
        case.append(client(1), 1, vec![record("1")]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        for from_offset in [0, 1] {
            let started = Instant::now();
            let error = case
                .read(ReadRecordsRequest {
                    from_offset,
                    wait: millis(5000),
                    ..Default::default()
                })
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_expired");
            assert!(started.elapsed() < Duration::from_secs(2));
        }
        let error = case.append(client(1), -1, vec![]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
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
                from_offset: describe.last_offset + 1,
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

/// Creates the case's Stream, owned by `run`, with a 300 ms Retention. While it is open, its meta
/// stays and only its log expires.
async fn create_owned_short(case: &Case, run: &str) {
    let _: CreateStreamResponse = call(
        &case.ext,
        "CreateStream",
        CreateStreamRequest {
            namespace: case.namespace.clone(),
            stream_id: case.stream.clone(),
            retention: millis(300),
            owner: Some(workflow_owner(run)),
            r#ref: owned_ref(case),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

/// A read from an offset that an idle open Stream's expired log took fails as expired at once,
/// before the next write and after it. A read from the start skips the gap.
async fn a_read_behind_an_expired_log_fails_before_and_after_the_next_write() {
    for case in cases_for(Needs::UNCHECKED.and(Needs::SHORT_TIMES)).await {
        create_owned_short(&case, &format!("{}-run", case.stream)).await;
        let first = case
            .append(client(1), 1, vec![record("a"), record("b")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let behind = ReadRecordsRequest {
            from_offset: first.first_offset + 1,
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

/// A close as the first write after an idle open Stream's log expired keeps the gap, so a read from
/// an offset that the log took fails as expired.
async fn a_close_after_the_log_expired_keeps_the_gap() {
    for case in cases_for(Needs::UNCHECKED.and(Needs::SHORT_TIMES)).await {
        let run = format!("{}-run", case.stream);
        create_owned_short(&case, &run).await;
        let first = case
            .append(client(1), 1, vec![record("a"), record("b")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            stream: owned_ref(&case),
            producer: Some(workflow_owner(&run)),
            ..Default::default()
        };
        let _: CloseStreamResponse = call(&case.ext, "CloseStream", close).await.unwrap();
        let behind = ReadRecordsRequest {
            from_offset: first.first_offset + 1,
            ..Default::default()
        };
        let error = case.read(behind).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
    }
}

/// A ref with an owner and one without reach the same Stream.
async fn a_ref_with_an_owner_and_one_without_reach_the_same_stream() {
    for case in cases_for(Needs::ID_KEYS).await {
        case.create(None).await.unwrap();
        let owned = Some(StreamRef {
            id: case.stream.clone(),
            owner: super::support::workflow_execution("agent-1", "run-1"),
            ..Default::default()
        });
        // An owner that doesn't match the id changes nothing either.
        let other = Some(StreamRef {
            id: case.stream.clone(),
            owner: super::support::workflow_execution("someone-else", ""),
            ..Default::default()
        });
        for (sequence, stream) in [(1, owned.clone()), (2, case.stream()), (3, other)] {
            let request = AppendRecordsRequest {
                namespace: case.namespace.clone(),
                stream,
                producer: Some(client(1)),
                sequence,
                records: vec![record(&sequence.to_string())],
                ..Default::default()
            };
            let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", request).await.unwrap();
        }
        let read = |stream: Option<StreamRef>| ReadRecordsRequest {
            namespace: case.namespace.clone(),
            stream,
            ..Default::default()
        };
        let plain: ReadRecordsResponse = call(&case.ext, "ReadRecords", read(case.stream()))
            .await
            .unwrap();
        let with_owner: ReadRecordsResponse =
            call(&case.ext, "ReadRecords", read(owned)).await.unwrap();
        assert_eq!(texts(&plain), ["1", "2", "3"]);
        assert_eq!(plain, with_owner);
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
            gives: Needs::ALL,
        };
        let created = case.create(None).await.unwrap();
        let stream = created.stream.unwrap();
        assert_eq!(stream.id, case.stream);
        assert_eq!(stream.backend, "redis-a");
        // A ref with any backend, or none, reaches the same Stream.
        let named = |backend: &str| {
            Some(StreamRef {
                id: case.stream.clone(),
                backend: backend.to_string(),
                ..Default::default()
            })
        };
        for (sequence, stream) in [(1, named("redis-a")), (2, named("")), (3, named("other"))] {
            let request = AppendRecordsRequest {
                namespace: case.namespace.clone(),
                stream,
                producer: Some(client(1)),
                sequence,
                records: vec![record(&sequence.to_string())],
                ..Default::default()
            };
            let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", request).await.unwrap();
        }
        let read = ReadRecordsRequest {
            namespace: case.namespace.clone(),
            stream: named("other"),
            ..Default::default()
        };
        let read: ReadRecordsResponse = call(&case.ext, "ReadRecords", read).await.unwrap();
        assert_eq!(texts(&read), ["1", "2", "3"]);
    }
    // With no backend in the config, the ref has none, and lang reads it as the default.
    for case in cases_for(Needs::OWN_CONFIG).await {
        let created = case.create(None).await.unwrap();
        assert_eq!(created.stream.unwrap().backend, "");
    }
}

/// The Local Activity Producer of a Workflow call in run `run`.
fn local_activity(run: &str) -> Producer {
    Producer {
        kind: StreamProducerKind::LocalActivity as i32,
        id: crate::ids::local_activity_producer_id(run, "call-1"),
        epoch: Some(ProducerEpoch {
            task_started_event_id: 3,
            task_attempt: 1,
            task_started_time: Some(Timestamp {
                seconds: 100,
                nanos: 0,
            }),
            attempt: 1,
            ..Default::default()
        }),
        run_id: run.to_string(),
    }
}

fn workflow_owner(run: &str) -> Producer {
    Producer {
        kind: StreamProducerKind::Workflow as i32,
        id: crate::ids::workflow_producer_id(run),
        run_id: run.to_string(),
        ..Default::default()
    }
}

/// The case's ref with the owner of a made-up run chain, which a create with an owner needs.
fn owned_ref(case: &Case) -> Option<StreamRef> {
    Some(StreamRef {
        id: case.stream.clone(),
        owner: super::support::workflow_execution(&format!("{}-wf", case.stream), "first-run"),
        ..Default::default()
    })
}

/// A retry run's Local Activity calls withdraw the previous run.
async fn a_retry_run_s_local_activity_calls_withdraw_the_previous_run() {
    for case in cases_for(Needs::UNCHECKED).await {
        let (previous, retry) = (format!("{}-r1", case.stream), format!("{}-r2", case.stream));
        let create = |run: &str, withdraw_run: &str| CreateStreamRequest {
            namespace: case.namespace.clone(),
            stream_id: case.stream.clone(),
            withdraw_run: withdraw_run.to_string(),
            owner: Some(workflow_owner(run)),
            r#ref: owned_ref(&case),
            ..Default::default()
        };
        let append = |run: &str, withdraw_run: &str, text: &str| AppendRecordsRequest {
            namespace: case.namespace.clone(),
            stream: case.stream(),
            producer: Some(local_activity(run)),
            sequence: 1,
            records: vec![record(text)],
            withdraw_run: withdraw_run.to_string(),
            ..Default::default()
        };
        let _: CreateStreamResponse = call(&case.ext, "CreateStream", create(&previous, ""))
            .await
            .unwrap();
        let _: AppendRecordsResponse =
            call(&case.ext, "AppendRecords", append(&previous, "", "old"))
                .await
                .unwrap();
        // The retry run's first create withdraws the previous run.
        let _: CreateStreamResponse = call(&case.ext, "CreateStream", create(&retry, &previous))
            .await
            .unwrap();
        let first: AppendRecordsResponse =
            call(&case.ext, "AppendRecords", append(&retry, &previous, "new"))
                .await
                .unwrap();
        // A retried Local Activity sends the same batch and gets its first answer.
        let again: AppendRecordsResponse =
            call(&case.ext, "AppendRecords", append(&retry, &previous, "new"))
                .await
                .unwrap();
        assert_eq!(
            (again.first_offset, again.last_offset),
            (first.first_offset, first.last_offset)
        );
        assert!(first.last_offset > 0);
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["old", "<superseded>", "new"]);
        let superseded = superseded_of(&read.records[1]);
        assert_eq!(superseded.withdraw_run, previous);
        // The item names its Producer, which the WIT requires.
        assert_eq!(
            superseded.producer_id,
            crate::ids::workflow_producer_id(&retry)
        );
        assert_eq!(superseded.run_id, retry);
        let error = call::<_, CreateStreamResponse>(
            &case.ext,
            "CreateStream",
            CreateStreamRequest {
                owner: None,
                ..create(&retry, &previous)
            },
        )
        .await
        .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        assert!(withdraws(superseded, &read.records[0]));
        assert!(!withdraws(superseded, &read.records[2]));
    }
}

/// Only a Local Activity of a later run withdraws a run on append. A client, an Activity or the
/// run's own Local Activity that names a run fails with `stream_invalid` and withdraws nothing.
async fn only_a_later_run_s_local_activity_withdraws_a_run_on_append() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-1", 3, 2, "workflow output"),
        )
        .await
        .unwrap();
        for producer in [client(1), activity(1, 10), local_activity(&run)] {
            let error = call::<_, AppendRecordsResponse>(
                &case.ext,
                "AppendRecords",
                AppendRecordsRequest {
                    namespace: case.namespace.clone(),
                    stream: case.stream(),
                    producer: Some(producer),
                    sequence: 1,
                    records: vec![record("other")],
                    withdraw_run: run.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert_eq!(reason(&error), "stream_invalid");
        }
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["workflow output"]);
        let next = local_activity_write(&case, &run, "call-2", 4, 3, "next");
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", next).await.unwrap();
    }
}

/// The user Local Activity `call-1` of `run`, dispatched by the Workflow Task `[started]`. A rerun
/// in a later task keeps the Producer id and has a newer dispatching task.
fn user_local_activity(run: &str, started: i64, seconds: i64) -> Producer {
    Producer {
        kind: StreamProducerKind::LocalActivity as i32,
        id: crate::ids::local_activity_producer_id(run, "call-1"),
        epoch: Some(ProducerEpoch {
            task_started_event_id: started,
            task_attempt: 1,
            task_started_time: Some(Timestamp { seconds, nanos: 0 }),
            attempt: 1,
            ..Default::default()
        }),
        run_id: run.to_string(),
    }
}

/// A Local Activity rerun in a later task supersedes its first attempt.
async fn a_rerun_local_activity_supersedes_its_first_attempt() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        let _: CreateStreamResponse = call(
            &case.ext,
            "CreateStream",
            CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_id: case.stream.clone(),
                owner: Some(workflow_owner(&run)),
                r#ref: owned_ref(&case),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let write = |producer: Producer, text: &str| AppendRecordsRequest {
            namespace: case.namespace.clone(),
            stream: case.stream(),
            producer: Some(producer),
            sequence: 1,
            records: vec![record(text)],
            ..Default::default()
        };
        // Task [3] runs the Local Activity, which writes, and then the task fails.
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            write(user_local_activity(&run, 3, 100), "first"),
        )
        .await
        .unwrap();
        // The next task [7] runs the same call again, and it writes again.
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            write(user_local_activity(&run, 7, 200), "second"),
        )
        .await
        .unwrap();
        // A late write of the failed attempt is fenced.
        let late = call::<_, AppendRecordsResponse>(
            &case.ext,
            "AppendRecords",
            AppendRecordsRequest {
                sequence: 2,
                ..write(user_local_activity(&run, 3, 100), "late")
            },
        )
        .await
        .unwrap_err();
        assert_eq!(reason(&late), "stream_fenced");
        // A Reader sees the first attempt, then the Superseded item that withdraws it, then the
        // second attempt. After it applies the item, only the second attempt is visible.
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["first", "<superseded>", "second"]);
        let superseded = superseded_of(&read.records[1]);
        assert!(withdraws(superseded, &read.records[0]));
        assert!(!withdraws(superseded, &read.records[2]));
    }
}

/// A write of the Local Activity `call` of `run`, dispatched by task `[started]`, with the run's
/// last completed task `[completed]`, or 0 before the first.
fn local_activity_write(
    case: &Case,
    run: &str,
    call: &str,
    started: i64,
    completed: i64,
    text: &str,
) -> AppendRecordsRequest {
    let mut producer = user_local_activity(run, started, 100 + started);
    producer.id = crate::ids::local_activity_producer_id(run, call);
    AppendRecordsRequest {
        namespace: case.namespace.clone(),
        stream: case.stream(),
        producer: Some(producer),
        sequence: 1,
        records: vec![record(text)],
        previous_completed_task: Some(crate::proto::WorkflowTaskEpoch {
            started_event_id: completed,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The close Activity closes a Workflow's Stream as the run's Workflow Producer, which the service
/// refuses on an append.
async fn the_close_activity_closes_a_workflow_stream_as_its_workflow_producer() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        let chain = StreamRef {
            id: case.stream.clone(),
            owner: super::support::workflow_execution(&format!("{}-wf", case.stream), "first-run"),
            ..Default::default()
        };
        let _: CreateStreamResponse = call(
            &case.ext,
            "CreateStream",
            CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_id: case.stream.clone(),
                owner: Some(workflow_owner(&run)),
                r#ref: Some(chain.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            stream: Some(chain),
            producer: Some(workflow_owner(&run)),
            ..Default::default()
        };
        let _: CloseStreamResponse = call(&case.ext, "CloseStream", close).await.unwrap();
        assert_eq!(
            case.describe().await.unwrap().status,
            StreamStatus::Closed as i32
        );
    }
}

/// A Local Activity of a task that never completed is withdrawn by the run's next write.
async fn a_local_activity_of_a_task_that_never_completed_is_withdrawn_by_the_next_write() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        // Task [2] completed. Task [3] runs a Local Activity that writes, then task [3] fails.
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-1", 2, 0, "done"),
        )
        .await
        .unwrap();
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-2", 3, 2, "orphan"),
        )
        .await
        .unwrap();
        // Task [4] takes another path and writes once.
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-3", 4, 2, "other"),
        )
        .await
        .unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["done", "orphan", "<superseded>", "other"]);
        let superseded = superseded_of(&read.records[2]);
        let kept: Vec<_> = [0, 1, 3]
            .iter()
            .filter(|&&i| !withdraws(superseded, &read.records[i]))
            .map(|&i| texts(&read)[i].clone())
            .collect();
        assert_eq!(kept, ["done", "other"]);
        // A late write of the orphaned call is fenced.
        let late = call::<_, AppendRecordsResponse>(
            &case.ext,
            "AppendRecords",
            AppendRecordsRequest {
                sequence: 2,
                ..local_activity_write(&case, &run, "call-2", 3, 2, "late")
            },
        )
        .await
        .unwrap_err();
        assert_eq!(reason(&late), "stream_fenced");
        // A later write that knows task [4] completed withdraws nothing more.
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-4", 7, 4, "later"),
        )
        .await
        .unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(
            texts(&read),
            ["done", "orphan", "<superseded>", "other", "later"]
        );
    }
}

/// A failed task before the last completed one is withdrawn by its id.
async fn a_failed_task_before_the_last_completed_one_is_withdrawn_by_its_id() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-1", 3, 2, "orphan"),
        )
        .await
        .unwrap();
        // Task [3] failed and task [4] completed with no write here. Task [5] knows both.
        let mut next = local_activity_write(&case, &run, "call-2", 5, 4, "next");
        next.failed_tasks = vec![3];
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", next).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["orphan", "<superseded>", "next"]);
        let superseded = superseded_of(&read.records[1]);
        assert!(withdraws(superseded, &read.records[0]));
        assert!(!withdraws(superseded, &read.records[2]));
    }
}

/// The Workflow Task `[started_event_id]` in its attempt `attempt`, started at `seconds`.
fn task(started_event_id: i64, attempt: i32, seconds: i64) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id,
        attempt,
        started_time: Some(Timestamp { seconds, nanos: 0 }),
    }
}

/// A write of the Local Activity `call` of `run`, dispatched by `task`, with the run's last
/// completed task, if the write sends it.
fn task_write(
    case: &Case,
    run: &str,
    call: &str,
    task: WorkflowTaskEpoch,
    completed: Option<WorkflowTaskEpoch>,
    text: &str,
) -> AppendRecordsRequest {
    let mut producer = user_local_activity(run, task.started_event_id, 0);
    producer.id = crate::ids::local_activity_producer_id(run, call);
    if let Some(epoch) = producer.epoch.as_mut() {
        epoch.task_attempt = task.attempt;
        epoch.task_started_time = task.started_time;
    }
    AppendRecordsRequest {
        namespace: case.namespace.clone(),
        stream: case.stream(),
        producer: Some(producer),
        sequence: 1,
        records: vec![record(text)],
        previous_completed_task: completed,
        ..Default::default()
    }
}

/// A Local Activity of a transient attempt of a task that later completed is withdrawn by the
/// next write, which names the completed attempt. A Record of the completed attempt stays.
async fn a_local_activity_of_a_lower_attempt_of_the_completed_task_is_withdrawn() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        let write =
            |call: &str,
             task: WorkflowTaskEpoch,
             completed: Option<WorkflowTaskEpoch>,
             text: &str| task_write(&case, &run, call, task, completed, text);
        // Attempt 2 of task [9] writes, then fails with no event in History.
        let c = Some(task(3, 1, 100));
        let orphan = write("call-1", task(9, 2, 200), c, "orphan");
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", orphan).await.unwrap();
        // Attempt 3 completes. Its write sends no task facts, so it withdraws nothing itself.
        let kept = write("call-2", task(9, 3, 300), None, "kept");
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", kept).await.unwrap();
        let later = write("call-3", task(12, 1, 400), Some(task(9, 3, 300)), "later");
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", later).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["orphan", "kept", "<superseded>", "later"]);
        let superseded = superseded_of(&read.records[2]);
        let kept: Vec<_> = [0, 1, 3]
            .iter()
            .filter(|&&i| !withdraws(superseded, &read.records[i]))
            .map(|&i| texts(&read)[i].clone())
            .collect();
        assert_eq!(kept, ["kept", "later"]);
    }
}

/// A write sends the facts of its own task, attempt 3 of `[9]`. It withdraws the Records of
/// attempt 2 of `[9]`, which can't have completed, and keeps its own.
async fn a_write_withdraws_a_lower_attempt_of_its_own_task() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        let completed = Some(task(3, 1, 100));
        let orphan = task_write(&case, &run, "call-1", task(9, 2, 200), completed, "orphan");
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", orphan).await.unwrap();
        let own = task_write(&case, &run, "call-2", task(9, 3, 300), completed, "own");
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", own).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["orphan", "<superseded>", "own"]);
        let superseded = superseded_of(&read.records[1]);
        assert!(withdraws(superseded, &read.records[0]));
        assert!(!withdraws(superseded, &read.records[2]));
    }
}

/// A run that a retry run withdrew can't close the Stream, and the retry run can.
async fn a_withdrawn_run_cant_close_the_stream() {
    for case in cases_for(Needs::UNCHECKED).await {
        let (previous, retry) = (format!("{}-r1", case.stream), format!("{}-r2", case.stream));
        let create = |run: &str, withdraw_run: &str| CreateStreamRequest {
            namespace: case.namespace.clone(),
            stream_id: case.stream.clone(),
            withdraw_run: withdraw_run.to_string(),
            owner: Some(workflow_owner(run)),
            r#ref: owned_ref(&case),
            ..Default::default()
        };
        let close = |run: &str| CloseStreamRequest {
            namespace: case.namespace.clone(),
            stream: owned_ref(&case),
            producer: Some(workflow_owner(run)),
            ..Default::default()
        };
        let _: CreateStreamResponse = call(&case.ext, "CreateStream", create(&previous, ""))
            .await
            .unwrap();
        let _: CreateStreamResponse = call(&case.ext, "CreateStream", create(&retry, &previous))
            .await
            .unwrap();
        let error = call::<_, CloseStreamResponse>(&case.ext, "CloseStream", close(&previous))
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
        let _: CloseStreamResponse = call(&case.ext, "CloseStream", close(&retry)).await.unwrap();
    }
}

/// A retry run's create of a new Stream fences the previous run, whose later write fails.
async fn a_retry_run_s_create_of_a_new_stream_fences_the_previous_run() {
    for case in cases_for(Needs::UNCHECKED).await {
        let (previous, retry) = (format!("{}-r1", case.stream), format!("{}-r2", case.stream));
        let created: CreateStreamResponse = call(
            &case.ext,
            "CreateStream",
            CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_id: case.stream.clone(),
                withdraw_run: previous.clone(),
                owner: Some(workflow_owner(&retry)),
                r#ref: owned_ref(&case),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(created.created);
        let append = |run: &str| AppendRecordsRequest {
            namespace: case.namespace.clone(),
            stream: case.stream(),
            producer: Some(local_activity(run)),
            sequence: 1,
            records: vec![record(run)],
            ..Default::default()
        };
        let error = call::<_, AppendRecordsResponse>(&case.ext, "AppendRecords", append(&previous))
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", append(&retry))
            .await
            .unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), [retry]);
    }
}

/// A retry run's create after the log expired keeps the gap, so a read from an offset that the
/// log took fails as expired.
async fn a_retry_run_s_create_after_the_log_expired_keeps_the_gap() {
    for case in cases_for(Needs::UNCHECKED.and(Needs::SHORT_TIMES)).await {
        let (previous, retry) = (format!("{}-r1", case.stream), format!("{}-r2", case.stream));
        let create = |run: &str, withdraw_run: &str| CreateStreamRequest {
            namespace: case.namespace.clone(),
            stream_id: case.stream.clone(),
            retention: millis(300),
            withdraw_run: withdraw_run.to_string(),
            owner: Some(workflow_owner(run)),
            r#ref: owned_ref(&case),
            ..Default::default()
        };
        let _: CreateStreamResponse = call(&case.ext, "CreateStream", create(&previous, ""))
            .await
            .unwrap();
        let first: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            AppendRecordsRequest {
                namespace: case.namespace.clone(),
                stream: case.stream(),
                producer: Some(local_activity(&previous)),
                sequence: 1,
                records: vec![record("a"), record("b")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let _: CreateStreamResponse = call(&case.ext, "CreateStream", create(&retry, &previous))
            .await
            .unwrap();
        let behind = ReadRecordsRequest {
            from_offset: first.first_offset + 1,
            ..Default::default()
        };
        let error = case.read(behind).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
    }
}

/// A close Record takes its status from the failure. A failure is canceled when it is a cancel, or
/// an Activity, child Workflow or Nexus Operation failure whose cause is a cancel, and failed
/// otherwise. A close Record over the Record limit is refused.
async fn a_close_record_takes_its_status_from_the_failure() {
    let canceled_activity = Failure {
        message: "boom".to_string(),
        failure_info: Some(FailureInfo::ActivityFailureInfo(Default::default())),
        cause: Some(Box::new(Failure {
            failure_info: Some(FailureInfo::CanceledFailureInfo(Default::default())),
            ..Default::default()
        })),
        ..Default::default()
    };
    let other = Failure {
        message: "boom".to_string(),
        ..Default::default()
    };
    for (failure, status) in [
        (other, StreamCloseStatus::Failed),
        (canceled_activity, StreamCloseStatus::Canceled),
    ] {
        for case in cases_for(Needs::CALLS).await {
            case.create(None).await.unwrap();
            let close = CloseStreamRequest {
                namespace: case.namespace.clone(),
                stream: case.stream(),
                producer: Some(client(1)),
                failure: Some(failure.clone()),
                ..Default::default()
            };
            let huge = CloseStreamRequest {
                result: Some(Payload {
                    data: vec![0; crate::store::MAX_RECORD_BYTES],
                    ..Default::default()
                }),
                ..close.clone()
            };
            let error = call::<_, CloseStreamResponse>(&case.ext, "CloseStream", huge)
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_too_large");
            call::<_, CloseStreamResponse>(&case.ext, "CloseStream", close)
                .await
                .unwrap();
            let read = case.read(ReadRecordsRequest::default()).await.unwrap();
            let record = read.records.last().unwrap();
            assert_eq!(close_of(record).status, status as i32);
            assert_eq!(close_of(record).failure.as_ref().unwrap().message, "boom");
        }
    }
}

/// A last read gives the newest Records oldest first, skips withdrawn ones, and ends with the
/// close Record of a closed Stream.
async fn a_last_read_gives_the_newest_records_oldest_first_without_withdrawn_ones() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let last = |n: i32| ReadRecordsRequest {
            last: n,
            ..Default::default()
        };
        assert!(case.read(last(5)).await.unwrap().records.is_empty());
        let records = vec![record("c1"), record("c2"), record("c3")];
        case.append(client(1), 1, records).await.unwrap();
        if case.gives.gives(Needs::UNCHECKED) {
            case.append(activity(1, 10), 1, vec![record("a1")])
                .await
                .unwrap();
            // The retry's first batch withdraws a1, so a1 doesn't count.
            case.append(activity(2, 20), 1, vec![record("a2")])
                .await
                .unwrap();
        } else {
            case.append(named("w", 1), 1, vec![record("a2")])
                .await
                .unwrap();
        }
        assert_eq!(
            texts(&case.read(last(3)).await.unwrap()),
            ["c2", "c3", "a2"]
        );
        let read = case.read(last(2)).await.unwrap();
        assert_eq!(texts(&read), ["c3", "a2"]);
        assert!(!read.done);
        let newest = case.describe().await.unwrap().last_offset;
        assert_eq!(read.next_offset, newest + 1);
        let error = case
            .read(ReadRecordsRequest {
                from_offset: 1,
                ..last(1)
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        case.close(client(1)).await.unwrap();
        let read = case.read(last(1)).await.unwrap();
        assert_eq!(texts(&read), ["a2", "<close>"]);
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
        let first = case.read(capped(0)).await.unwrap();
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
    case!(a_create_of_a_closed_stream_fails),
    case!(a_create_of_an_expired_stream_fails),
    case!(a_create_checks_its_owner_and_its_ref),
    case!(a_producer_must_match_its_kind_and_not_be_a_workflow),
    case!(an_append_with_no_sequence_takes_the_next_one_of_its_epoch),
    case!(a_read_is_inclusive_from_its_offset_and_get_offset_reads_only_new_records),
    case!(a_higher_attempt_puts_its_superseded_item_before_its_batch),
    case!(a_read_waits_for_the_create_only_as_long_as_create_wait),
    case!(a_quiet_read_ends_empty_after_its_wait_and_a_long_poll_wakes_on_append),
    case!(a_close_ends_every_read_with_its_record),
    case!(an_unknown_method_is_unimplemented),
    case!(a_read_of_an_expired_open_stream_fails_instead_of_waiting),
    case!(a_read_of_an_expired_closed_stream_fails_instead_of_waiting),
    case!(a_read_behind_an_expired_log_fails_before_and_after_the_next_write),
    case!(a_close_after_the_log_expired_keeps_the_gap),
    case!(a_ref_with_an_owner_and_one_without_reach_the_same_stream),
    case!(a_create_answer_names_the_backend_and_the_store_never_reads_it),
    case!(a_retry_run_s_local_activity_calls_withdraw_the_previous_run),
    case!(only_a_later_run_s_local_activity_withdraws_a_run_on_append),
    case!(a_rerun_local_activity_supersedes_its_first_attempt),
    case!(the_close_activity_closes_a_workflow_stream_as_its_workflow_producer),
    case!(a_local_activity_of_a_task_that_never_completed_is_withdrawn_by_the_next_write),
    case!(a_failed_task_before_the_last_completed_one_is_withdrawn_by_its_id),
    case!(a_local_activity_of_a_lower_attempt_of_the_completed_task_is_withdrawn),
    case!(a_write_withdraws_a_lower_attempt_of_its_own_task),
    case!(a_withdrawn_run_cant_close_the_stream),
    case!(a_retry_run_s_create_of_a_new_stream_fences_the_previous_run),
    case!(a_retry_run_s_create_after_the_log_expired_keeps_the_gap),
    case!(a_close_record_takes_its_status_from_the_failure),
    case!(a_last_read_gives_the_newest_records_oldest_first_without_withdrawn_ones),
    case!(a_page_keeps_under_max_bytes_and_a_larger_record_comes_alone),
];
