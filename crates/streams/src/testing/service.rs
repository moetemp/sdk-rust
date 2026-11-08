//! The Stream service operations through the runtime's byte call, as lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{case, support::Needs};

use crate::{
    StreamError, StreamRuntime,
    epoch::{withdraws, writer_withdraw_run},
    ids::{activity_producer_id, client_producer_id, named_client_producer_id},
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecord, AppendRecordsRequest, AppendRecordsResponse,
        ClientEpoch, ClientProducer, CloseStreamRequest, CloseStreamResponse, CreateStreamRequest,
        CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse, LocalActivityEpoch,
        LocalActivityProducer, Producer, ReadRecordsRequest, ReadRecordsResponse, RunWithdrawal,
        StreamCloseStatus, StreamErrorDetails, StreamIdConflictPolicy, StreamReadStart,
        StreamRecord, StreamRef, StreamStatus, StreamSuperseded, StreamWriter, WorkflowProducer,
        WorkflowTaskEpoch, producer::Kind, stream_record::Item,
    },
};
use prost::Message;
use prost_types::Timestamp;
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

/// The writer of a write by `producer`, with no withdrawal facts.
fn writer_for(producer: Producer) -> Option<StreamWriter> {
    Some(StreamWriter {
        producer: Some(producer),
        ..Default::default()
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

/// An Activity Producer of a made-up run, at `attempt`. A higher attempt withdraws the lower ones'
/// Records. The server requires the Activity's task token, so only Redis and memory accept it.
fn activity(attempt: i32, scheduled: i64) -> Producer {
    Producer {
        id: activity_producer_id("r1", "a", "s"),
        kind: Some(Kind::Activity(ActivityProducer {
            run_id: "r1".to_string(),
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: Some(Timestamp {
                    seconds: scheduled,
                    nanos: 0,
                }),
                attempt,
            }),
        })),
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
            Some(Item::Superseded(_)) => "<superseded>".to_string(),
            Some(Item::Close(_)) => "<close>".to_string(),
            None => "?".to_string(),
        })
        .collect()
}

fn superseded_of(record: &StreamRecord) -> &StreamSuperseded {
    match &record.item {
        Some(Item::Superseded(superseded)) => superseded,
        other => panic!("not a Superseded item: {other:?}"),
    }
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

/// A Stream with no owner takes a close from a Workflow or a client. The close of an Activity
/// fails as `not_owner`.
async fn a_workflow_or_a_client_closes_a_stream_with_no_owner() {
    for case in cases_for(Needs::UNCHECKED).await {
        case.create(None).await.unwrap();
        let run = format!("{}-run", case.stream);
        let activity = Producer {
            id: activity_producer_id(&run, "a", "s"),
            kind: Some(Kind::Activity(ActivityProducer {
                run_id: run.clone(),
                epoch: Some(ActivityEpoch {
                    attempt: 1,
                    ..Default::default()
                }),
            })),
        };
        let error = case.close(activity).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_not_owner")
        );
        case.close(workflow(&run)).await.unwrap();
    }
}

/// A create of a Stream whose Retention passed fails as expired while its Tombstone is there, not
/// as `already_exists`.
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
/// ref has that chain. A create with another owner fails as `not_owner`, not as
/// `already_exists`, and no run withdraws itself.
async fn a_create_checks_its_owner_and_its_ref() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        let workflow = workflow(&run);
        let task = WorkflowTaskEpoch {
            started_event_id: 3,
            ..Default::default()
        };
        let local_activity = Producer {
            id: crate::ids::local_activity_producer_id(&run, "call-1"),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: run.clone(),
                epoch: Some(LocalActivityEpoch {
                    task: Some(task),
                    attempt: 1,
                }),
            })),
        };
        let chain = |workflow_id: &str| StreamRef {
            id: case.stream.clone(),
            owner: super::support::workflow_execution(workflow_id, "first-run"),
            ..Default::default()
        };
        let create = |owner: Option<&Producer>, stream_ref: StreamRef, withdraw_run: &str| {
            CreateStreamRequest {
                namespace: case.namespace.clone(),
                owner: owner.cloned(),
                stream_ref: Some(stream_ref),
                withdraw_run: withdraw_run.to_string(),
                ..Default::default()
            }
        };
        for request in [
            create(Some(&local_activity), chain("agent"), ""),
            create(Some(&workflow), case.stream_ref().unwrap(), ""),
            create(
                Some(&workflow),
                StreamRef {
                    id: String::new(),
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
        assert_eq!(created.r#ref.unwrap().owner, chain("agent").owner);
        for request in [
            create(Some(&workflow), chain("another-agent"), ""),
            create(None, case.stream_ref().unwrap(), ""),
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

/// A read is inclusive from its offset, a read from the end gives only new Records, an offset past
/// the newest + 1 fails with `stream_offset_too_high`, and one below zero is invalid.
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
            (Code::OutOfRange, "stream_offset_too_high")
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
        let writer = superseded_of(superseded).writer.as_ref().unwrap();
        assert_eq!(writer.producer.as_ref().unwrap().id, activity(2, 20).id);
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
                from_offset: read.next_offset,
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
        assert_eq!(last.offset, written.last_offset + 1);
        let Some(Item::Close(close)) = &last.item else {
            panic!("not a close item: {:?}", last.item);
        };
        assert_eq!(close.status, StreamCloseStatus::Closed as i32);
        assert_eq!(close.producer, Some(client(1)));
        assert!(close.result.is_none() && close.failure.is_none());
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
            retention: millis(300),
            owner: Some(workflow(run)),
            stream_ref: owned_ref(case),
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

/// A close after an idle open Stream's log expired keeps the gap, so a read from an offset that the
/// log took fails as expired.
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
            r#ref: owned_ref(&case),
            writer: writer_for(workflow(&run)),
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
        for (sequence, stream) in [(1, owned.clone()), (2, case.stream_ref()), (3, other)] {
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
        let read = |stream_ref: Option<StreamRef>| ReadRecordsRequest {
            namespace: case.namespace.clone(),
            r#ref: stream_ref,
            ..Default::default()
        };
        let plain: ReadRecordsResponse = call(&case.ext, "ReadRecords", read(case.stream_ref()))
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
        let stream = created.r#ref.unwrap();
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

/// The Local Activity Producer of a Workflow call in run `run`.
fn local_activity(run: &str) -> Producer {
    user_local_activity(run, 3, 100)
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
            withdraw_run: withdraw_run.to_string(),
            owner: Some(workflow(run)),
            stream_ref: owned_ref(&case),
            ..use_existing()
        };
        let append = |run: &str, withdraw_run: &str, text: &str| AppendRecordsRequest {
            namespace: case.namespace.clone(),
            r#ref: case.stream_ref(),
            writer: Some(StreamWriter {
                producer: Some(local_activity(run)),
                withdrawal: Some(RunWithdrawal {
                    withdraw_run: withdraw_run.to_string(),
                    ..Default::default()
                }),
            }),
            sequence: 1,
            records: vec![record(text)],
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
        let writer = superseded.writer.as_ref().unwrap();
        assert_eq!(writer_withdraw_run(writer), previous);
        // The item's writer has its Producer, which the WIT requires. A create's item has no kind,
        // so a Reader can't read it as an epoch, and the run stays in the id.
        let producer = writer.producer.as_ref().unwrap();
        assert_eq!(producer.id, crate::ids::workflow_producer_id(&retry));
        assert!(producer.kind.is_none());
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
/// run's own Local Activity that sends a run to withdraw fails with `stream_invalid` and withdraws
/// nothing.
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
                    r#ref: case.stream_ref(),
                    writer: Some(StreamWriter {
                        producer: Some(producer),
                        withdrawal: Some(RunWithdrawal {
                            withdraw_run: run.clone(),
                            ..Default::default()
                        }),
                    }),
                    sequence: 1,
                    records: vec![record("other")],
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
    let task = WorkflowTaskEpoch {
        started_event_id: started,
        attempt: 1,
        started_time: Some(Timestamp { seconds, nanos: 0 }),
    };
    Producer {
        id: crate::ids::local_activity_producer_id(run, "call-1"),
        kind: Some(Kind::LocalActivity(LocalActivityProducer {
            run_id: run.to_string(),
            epoch: Some(LocalActivityEpoch {
                task: Some(task),
                attempt: 1,
            }),
        })),
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
                owner: Some(workflow(&run)),
                stream_ref: owned_ref(&case),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let write = |producer: Producer, text: &str| AppendRecordsRequest {
            namespace: case.namespace.clone(),
            r#ref: case.stream_ref(),
            writer: writer_for(producer),
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
        r#ref: case.stream_ref(),
        writer: Some(StreamWriter {
            producer: Some(producer),
            withdrawal: Some(RunWithdrawal {
                previous_completed_task: Some(WorkflowTaskEpoch {
                    started_event_id: completed,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }),
        sequence: 1,
        records: vec![record(text)],
        ..Default::default()
    }
}

/// The writer of a write, to change its Producer.
fn writer_mut(request: &mut AppendRecordsRequest) -> &mut StreamWriter {
    request.writer.get_or_insert_with(Default::default)
}

/// The withdrawal facts of a write, to change its task facts.
fn withdrawal_mut(request: &mut AppendRecordsRequest) -> &mut RunWithdrawal {
    writer_mut(request)
        .withdrawal
        .get_or_insert_with(Default::default)
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
                owner: Some(workflow(&run)),
                stream_ref: Some(chain.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: Some(chain),
            writer: writer_for(workflow(&run)),
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
        withdrawal_mut(&mut next).failed_tasks = vec![3];
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", next).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["orphan", "<superseded>", "next"]);
        let superseded = superseded_of(&read.records[1]);
        assert!(withdraws(superseded, &read.records[0]));
        assert!(!withdraws(superseded, &read.records[2]));
    }
}

/// A call of a task that failed before the call wrote is fenced on its first write, by the task
/// facts of the run's highest task. A write of a lower task keeps those facts.
async fn a_first_write_of_a_task_that_never_completed_is_fenced() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        let _: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-1", 2, 0, "done"),
        )
        .await
        .unwrap();
        // Task [3] runs call-2 and fails before call-2 writes. Task [4] knows that [3] failed.
        let mut retry = local_activity_write(&case, &run, "call-3", 4, 2, "retry");
        withdrawal_mut(&mut retry).failed_tasks = vec![3];
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", retry).await.unwrap();
        // Call-1 of the completed task [2] still runs and writes with its older facts.
        let more = AppendRecordsRequest {
            sequence: 2,
            ..local_activity_write(&case, &run, "call-1", 2, 0, "more")
        };
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", more).await.unwrap();
        let late = call::<_, AppendRecordsResponse>(
            &case.ext,
            "AppendRecords",
            local_activity_write(&case, &run, "call-2", 3, 2, "late"),
        )
        .await
        .unwrap_err();
        assert_eq!(reason(&late), "stream_fenced");
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["done", "retry", "more"]);
    }
}

/// Task facts that a Store would drop or misread fail before any write. Only a Local Activity sends
/// them, failed tasks need the last completed task, and a started event id is positive.
async fn a_write_with_bad_task_facts_fails() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        let facts = |failed: Vec<i64>| {
            let mut request = local_activity_write(&case, &run, "call-1", 4, 2, "bad");
            withdrawal_mut(&mut request).failed_tasks = failed;
            request
        };
        let mut no_completed = facts(vec![3]);
        withdrawal_mut(&mut no_completed).previous_completed_task = None;
        let mut by_client = facts(vec![]);
        writer_mut(&mut by_client).producer = Some(client(1));
        let mut by_activity = facts(vec![3]);
        writer_mut(&mut by_activity).producer = Some(activity(1, 100));
        for bad in [
            facts(vec![0]),
            facts(vec![-5]),
            no_completed,
            by_client,
            by_activity,
        ] {
            let error = call::<_, AppendRecordsResponse>(&case.ext, "AppendRecords", bad)
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_invalid");
        }
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert!(texts(&read).is_empty());
    }
}

/// A Local Activity write with no task facts, as from an older Core, is withdrawn by a later write
/// whose facts show that its task never completed.
async fn a_write_with_no_task_facts_is_withdrawn_by_a_later_write() {
    for case in cases_for(Needs::UNCHECKED).await {
        let run = format!("{}-run", case.stream);
        case.create(None).await.unwrap();
        let mut orphan = local_activity_write(&case, &run, "call-1", 3, 2, "orphan");
        withdrawal_mut(&mut orphan).previous_completed_task = None;
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", orphan).await.unwrap();
        let mut next = local_activity_write(&case, &run, "call-2", 5, 4, "next");
        withdrawal_mut(&mut next).failed_tasks = vec![3];
        let _: AppendRecordsResponse = call(&case.ext, "AppendRecords", next).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["orphan", "<superseded>", "next"]);
        let superseded = superseded_of(&read.records[1]);
        assert!(withdraws(superseded, &read.records[0]));
        assert!(!withdraws(superseded, &read.records[2]));
    }
}

/// A rerun of a call in a later task withdraws the failed task's Records with two items. The
/// orphan item has an entry of its own, and the newer epoch's item comes with the first Record.
async fn a_batch_writes_an_orphan_item_and_an_epoch_item() {
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
        let mut rerun = local_activity_write(&case, &run, "call-1", 5, 4, "rerun");
        withdrawal_mut(&mut rerun).failed_tasks = vec![3];
        let answer: AppendRecordsResponse = call(&case.ext, "AppendRecords", rerun).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(
            texts(&read),
            ["orphan", "<superseded>", "<superseded>", "rerun"]
        );
        let (orphans, epoch) = (
            superseded_of(&read.records[1]),
            superseded_of(&read.records[2]),
        );
        let kind = |item: &StreamSuperseded| -> Option<Kind> {
            Some(
                item.writer
                    .as_ref()?
                    .producer
                    .as_ref()?
                    .kind
                    .as_ref()?
                    .clone(),
            )
        };
        assert!(matches!(kind(orphans), Some(Kind::Workflow(_))));
        assert!(matches!(kind(epoch), Some(Kind::LocalActivity(_))));
        assert!(read.records[1].offset < answer.first_offset);
        assert_eq!(read.records[2].offset, answer.first_offset);
        assert_eq!(read.records[3].offset, answer.last_offset);
        for item in [orphans, epoch] {
            assert!(withdraws(item, &read.records[0]));
            assert!(!withdraws(item, &read.records[3]));
        }
    }
}

/// A resend of an empty batch that withdrew orphans gets the offsets of its orphan item again, and
/// adds no second item.
async fn a_resend_of_an_orphan_batch_withdraws_nothing_again() {
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
        // The writer of call-2 in task [4] opens with an empty batch.
        let open = AppendRecordsRequest {
            records: vec![],
            ..local_activity_write(&case, &run, "call-2", 4, 2, "")
        };
        let first: AppendRecordsResponse = call(&case.ext, "AppendRecords", open.clone())
            .await
            .unwrap();
        let again: AppendRecordsResponse = call(&case.ext, "AppendRecords", open).await.unwrap();
        assert!(first.first_offset > 0);
        assert_eq!(
            (again.first_offset, again.last_offset),
            (first.first_offset, first.last_offset)
        );
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["orphan", "<superseded>"]);
        assert_eq!(read.records[1].offset, first.last_offset);
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
    let producer = Producer {
        id: crate::ids::local_activity_producer_id(run, call),
        kind: Some(Kind::LocalActivity(LocalActivityProducer {
            run_id: run.to_string(),
            epoch: Some(LocalActivityEpoch {
                task: Some(task),
                attempt: 1,
            }),
        })),
    };
    AppendRecordsRequest {
        namespace: case.namespace.clone(),
        r#ref: case.stream_ref(),
        writer: Some(StreamWriter {
            producer: Some(producer),
            withdrawal: Some(RunWithdrawal {
                previous_completed_task: completed,
                ..Default::default()
            }),
        }),
        sequence: 1,
        records: vec![record(text)],
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
            withdraw_run: withdraw_run.to_string(),
            owner: Some(workflow(run)),
            stream_ref: owned_ref(&case),
            ..use_existing()
        };
        let close = |run: &str| CloseStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: owned_ref(&case),
            writer: writer_for(workflow(run)),
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
                withdraw_run: previous.clone(),
                owner: Some(workflow(&retry)),
                stream_ref: owned_ref(&case),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(created.created);
        let append = |run: &str| AppendRecordsRequest {
            namespace: case.namespace.clone(),
            r#ref: case.stream_ref(),
            writer: writer_for(local_activity(run)),
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
            retention: millis(300),
            withdraw_run: withdraw_run.to_string(),
            owner: Some(workflow(run)),
            stream_ref: owned_ref(&case),
            ..use_existing()
        };
        let _: CreateStreamResponse = call(&case.ext, "CreateStream", create(&previous, ""))
            .await
            .unwrap();
        let first: AppendRecordsResponse = call(
            &case.ext,
            "AppendRecords",
            AppendRecordsRequest {
                namespace: case.namespace.clone(),
                r#ref: case.stream_ref(),
                writer: writer_for(local_activity(&previous)),
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
                from_offset: written.last_offset + 1,
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
            assert_eq!(read.next_offset, written.last_offset + 1);
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
                from_offset: written.last_offset + 1,
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

/// A last read gives the newest Records oldest first, skips withdrawn ones, and gives `done` for a
/// closed Stream.
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
    case!(a_create_of_an_open_stream_fails_unless_it_uses_it_or_retries),
    case!(a_create_of_a_closed_stream_fails),
    case!(a_close_writes_no_record_and_a_second_close_succeeds),
    case!(a_workflow_or_a_client_closes_a_stream_with_no_owner),
    case!(a_create_of_an_expired_stream_fails),
    case!(a_create_checks_its_owner_and_its_ref),
    case!(a_producer_must_match_its_kind_and_not_be_a_workflow),
    case!(an_append_with_no_sequence_takes_the_next_one_of_its_epoch),
    case!(a_read_is_inclusive_from_its_offset_and_get_offset_reads_only_new_records),
    case!(a_higher_attempt_puts_its_superseded_item_before_its_batch),
    case!(a_read_waits_for_the_create_only_as_long_as_create_wait),
    case!(a_quiet_read_ends_empty_after_its_wait_and_a_long_poll_wakes_on_append),
    case!(a_close_ends_every_read_with_done),
    case!(a_closed_read_ends_with_a_close_item),
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
    case!(a_first_write_of_a_task_that_never_completed_is_fenced),
    case!(a_write_with_bad_task_facts_fails),
    case!(a_write_with_no_task_facts_is_withdrawn_by_a_later_write),
    case!(a_batch_writes_an_orphan_item_and_an_epoch_item),
    case!(a_resend_of_an_orphan_batch_withdraws_nothing_again),
    case!(a_local_activity_of_a_lower_attempt_of_the_completed_task_is_withdrawn),
    case!(a_write_withdraws_a_lower_attempt_of_its_own_task),
    case!(a_withdrawn_run_cant_close_the_stream),
    case!(a_retry_run_s_create_of_a_new_stream_fences_the_previous_run),
    case!(a_retry_run_s_create_after_the_log_expired_keeps_the_gap),
    case!(a_read_of_a_closed_stream_with_no_item_left_answers_at_once),
    case!(a_waiting_read_wakes_with_done_on_a_close),
    case!(a_last_read_gives_the_newest_records_oldest_first_without_withdrawn_ones),
    case!(a_page_keeps_under_max_bytes_and_a_larger_record_comes_alone),
];
