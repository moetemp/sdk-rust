//! The Stream service operations through the runtime's byte call, as lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{case, support::Needs};

use crate::{
    StreamError, StreamRuntime,
    ids::client_producer_id,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, Producer, ProducerEpoch,
        StreamErrorDetails, StreamProducerKind, StreamRef,
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
    case!(a_create_of_a_closed_stream_fails),
    case!(a_create_of_an_expired_stream_fails),
    case!(a_create_checks_its_owner_and_its_ref),
    case!(a_producer_must_match_its_kind_and_not_be_a_workflow),
    case!(an_append_with_no_sequence_takes_the_next_one_of_its_epoch),
    case!(an_unknown_method_is_unimplemented),
];
