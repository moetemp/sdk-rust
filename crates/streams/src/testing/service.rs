//! The Stream service operations through the runtime's byte call, as lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{case, support::Needs};

use crate::{
    StreamError, StreamRuntime,
    ids::{activity_producer_id, client_producer_id},
    proto::{
        ActivityEpoch, AppendRecord, AppendRecordsRequest, AppendRecordsResponse, ClientEpoch,
        CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
        DescribeStreamRequest, DescribeStreamResponse, LocalActivityEpoch, Producer, ProducerEpoch,
        StreamErrorDetails, StreamIdConflictPolicy, StreamRef, StreamStatus, StreamWriter,
        WorkflowTaskEpoch, producer_epoch::Kind,
    },
};
use prost::Message;
use std::{sync::Arc, time::Duration};
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
        epoch: Some(ProducerEpoch {
            kind: Some(Kind::Client(ClientEpoch { attempt })),
        }),
        run_id: String::new(),
    }
}

/// The Producer of run `run` in its first Workflow Task, as its close Activity sends it.
fn workflow(run: &str) -> Producer {
    Producer {
        id: crate::ids::workflow_producer_id(run),
        epoch: Some(ProducerEpoch {
            kind: Some(Kind::Workflow(WorkflowTaskEpoch::default())),
        }),
        run_id: run.to_string(),
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

    async fn close(&self, producer: Producer) -> Result<CloseStreamResponse, StreamError> {
        let close = CloseStreamRequest {
            namespace: self.namespace.clone(),
            r#ref: self.stream_ref(),
            writer: writer_for(producer),
            ..Default::default()
        };
        call(&self.ext, "CloseStream", close).await
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
            epoch: Some(ProducerEpoch {
                kind: Some(Kind::Activity(ActivityEpoch {
                    attempt: 1,
                    ..Default::default()
                })),
            }),
            run_id: run.clone(),
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
            epoch: Some(ProducerEpoch {
                kind: Some(Kind::LocalActivity(LocalActivityEpoch {
                    task: Some(task),
                    attempt: 1,
                })),
            }),
            ..workflow.clone()
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
        let activity = ProducerEpoch {
            kind: Some(Kind::Activity(ActivityEpoch::default())),
        };
        for epoch in [Some(activity), None, Some(ProducerEpoch::default())] {
            let wrong = Producer { epoch, ..client(1) };
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
            run_id: "r1".to_string(),
            epoch: Some(ProducerEpoch {
                kind: Some(Kind::LocalActivity(LocalActivityEpoch {
                    task: None,
                    attempt: 1,
                })),
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

/// An unknown method fails with UNIMPLEMENTED.
async fn an_unknown_method_is_unimplemented() {
    for case in cases_for(Needs::CALLS).await {
        let error = case.ext.call("Nope", &[]).await.unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);
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
    case!(an_unknown_method_is_unimplemented),
];
