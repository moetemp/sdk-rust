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
        StreamRecord, StreamRef, StreamStatus, Supersession, stream_record::Item,
    },
};
use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::temporal::api::{common::v1::Payload, failure::v1::Failure};
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

/// A create of a Stream that exists returns it with `created` false, whatever its Retention.
async fn a_create_returns_the_stream_that_exists_whatever_its_retention() {
    for case in cases_for(Needs::CALLS).await {
        let created = case.create(None).await.unwrap();
        assert!(created.created);
        assert_eq!(created.status, StreamStatus::Open as i32);
        assert!(!case.create(None).await.unwrap().created);
        let other = case.create(millis(1000)).await.unwrap();
        assert!(!other.created);
        assert_eq!(other.status, StreamStatus::Open as i32);
    }
}

/// A Producer's id must match its kind, and a Workflow appends only through a Local Activity.
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
    }
}

/// A read is inclusive from its offset, a read from the end gives only new Records, and an
/// offset past the newest + 1 fails with `stream_offset`.
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
            (Code::InvalidArgument, "stream_offset")
        );
    }
}

/// The first batch of a higher Activity attempt puts its `Superseded` item just before its
/// Records.
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

/// A close ends every read with its close Record and refuses later appends, and a create finds
/// the Stream closed.
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
        let created = case.create(None).await.unwrap();
        assert_eq!(created.status, StreamStatus::Closed as i32);
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
        let error = case.append(client(1), 0, vec![]).await.unwrap_err();
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
        assert_eq!(describe.status, StreamStatus::Closed as i32);
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

fn workflow_owner(run: &str) -> Producer {
    Producer {
        kind: StreamProducerKind::Workflow as i32,
        id: crate::ids::workflow_producer_id(run),
        run_id: run.to_string(),
        ..Default::default()
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

/// A close Record gives its status, and a caller can't close as terminated or timed out.
async fn a_close_record_gives_its_status_and_a_caller_cant_terminate() {
    for case in cases_for(Needs::CALLS).await {
        case.create(None).await.unwrap();
        let close = |status: StreamCloseStatus, failure: Option<Failure>| CloseStreamRequest {
            namespace: case.namespace.clone(),
            stream: case.stream(),
            producer: Some(client(1)),
            failure,
            status: status as i32,
            ..Default::default()
        };
        for status in [StreamCloseStatus::Terminated, StreamCloseStatus::TimedOut] {
            let error =
                call::<_, CloseStreamResponse>(&case.ext, "CloseStream", close(status, None))
                    .await
                    .unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::InvalidArgument, "stream_invalid")
            );
        }
        let failure = Failure {
            message: "boom".to_string(),
            ..Default::default()
        };
        let failed = close(StreamCloseStatus::Unspecified, Some(failure));
        call::<_, CloseStreamResponse>(&case.ext, "CloseStream", failed)
            .await
            .unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        let record = read.records.last().unwrap();
        assert_eq!(close_of(record).status, StreamCloseStatus::Failed as i32);
        assert_eq!(close_of(record).failure.as_ref().unwrap().message, "boom");
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
    case!(a_producer_must_match_its_kind_and_not_be_a_workflow),
    case!(a_read_is_inclusive_from_its_offset_and_get_offset_reads_only_new_records),
    case!(a_higher_attempt_puts_its_superseded_item_before_its_batch),
    case!(a_read_waits_for_the_create_only_as_long_as_create_wait),
    case!(a_quiet_read_ends_empty_after_its_wait_and_a_long_poll_wakes_on_append),
    case!(a_close_ends_every_read_with_its_record),
    case!(an_unknown_method_is_unimplemented),
    case!(a_read_of_an_expired_open_stream_fails_instead_of_waiting),
    case!(a_read_of_an_expired_closed_stream_fails_instead_of_waiting),
    case!(a_ref_with_an_owner_and_one_without_reach_the_same_stream),
    case!(a_create_answer_names_the_backend_and_the_store_never_reads_it),
    case!(a_rerun_local_activity_supersedes_its_first_attempt),
    case!(a_close_record_gives_its_status_and_a_caller_cant_terminate),
    case!(a_last_read_gives_the_newest_records_oldest_first_without_withdrawn_ones),
    case!(a_page_keeps_under_max_bytes_and_a_larger_record_comes_alone),
];
