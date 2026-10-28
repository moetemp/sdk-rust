//! Producer scopes of Activity attempts and clients, through the runtime's byte call, as lang sends
//! them.
//!
//! Every Store runs the suite through its own runtime.

use super::case;
use crate::{
    StreamError, StreamRuntime,
    epoch::withdraws,
    proto::{
        ActivityContext, AppendRecord, CloseScopeRequest, CloseScopeResponse, CloseStreamAsRequest,
        CloseStreamRequest, CloseStreamResponse, CloseWritersRequest, CloseWritersResponse,
        CreateStreamRequest, CreateStreamResponse, DeriveProducerRequest, DeriveProducerResponse,
        DescribeStreamRequest, DescribeStreamResponse, FreeResponse, FreeScopeRequest,
        FreeWriterRequest, FreeWriterResponse, OpenActivityScopeRequest, OpenClientScopeRequest,
        OpenScopeResponse, OpenWriterRequest, OpenWriterResponse, Producer, ProducerPurpose,
        PublishRequest, PublishResponse, ReadRecordsRequest, ReadRecordsResponse, ShutdownRequest,
        ShutdownResponse, StreamErrorReason, StreamRecord, StreamRef, StreamStatus,
        WorkflowTaskAttempt, stream_record::Item,
    },
};
use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
}

async fn cases() -> Vec<Case> {
    let mut cases = vec![];
    for ext in super::support::stream_runtimes() {
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
        case.create(&case.stream).await;
        cases.push(case);
    }
    cases
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

fn reason(error: &StreamError) -> &'static str {
    error.reason.token()
}

/// The context of attempt `attempt` of Activity `a` in run `r1`.
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
        call_key: "1".into(),
        ..Default::default()
    }
}

/// The context of a Local Activity of run `r1` with call key `call`, dispatched by the task that
/// started at `started`.
fn local_activity(call: &str, started: i64, completed: i64, failed: Vec<i64>) -> ActivityContext {
    ActivityContext {
        is_local: true,
        in_workflow: true,
        workflow_id: "wf".into(),
        workflow_run_id: "r1".into(),
        activity_id: call.into(),
        attempt: 1,
        call_key: call.into(),
        workflow_task: Some(WorkflowTaskAttempt {
            started_event_id: started,
            attempt: 1,
            started_time: Some(Timestamp {
                seconds: started,
                nanos: 0,
            }),
            previous_started_event_id: completed,
            failed_tasks: failed,
            previous_attempt: 1,
            previous_started_time: Some(Timestamp {
                seconds: completed,
                nanos: 0,
            }),
        }),
        ..Default::default()
    }
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

    async fn create(&self, stream: &str) {
        let created: CreateStreamResponse = self
            .call(
                "CreateStream",
                CreateStreamRequest {
                    namespace: self.namespace.clone(),
                    stream_id: stream.to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(created.created);
    }

    fn client_scope(&self, name: &str, epoch: i32) -> Result<OpenScopeResponse, StreamError> {
        self.call_sync(
            "OpenClientScope",
            OpenClientScopeRequest {
                namespace: self.namespace.clone(),
                identity: "me@host".into(),
                name: name.into(),
                epoch,
            },
        )
    }

    async fn activity_scope(&self, context: ActivityContext, streams: &[&str]) -> u64 {
        let opened: OpenScopeResponse = self
            .call(
                "OpenActivityScope",
                OpenActivityScopeRequest {
                    namespace: self.namespace.clone(),
                    context: Some(context),
                    streams: streams
                        .iter()
                        .map(|id| StreamRef {
                            id: id.to_string(),
                            ..Default::default()
                        })
                        .collect(),
                },
            )
            .await
            .unwrap();
        opened.scope_id
    }

    async fn publish(&self, scope_id: u64, texts: &[&str]) -> Result<(i64, i64), StreamError> {
        let published: PublishResponse = self
            .call(
                "Publish",
                PublishRequest {
                    scope_id,
                    stream: Some(self.stream_ref()),
                    records: records(texts),
                },
            )
            .await?;
        Ok((published.first_offset, published.last_offset))
    }

    async fn close_as(&self, scope_id: u64) -> Result<CloseStreamResponse, StreamError> {
        self.call(
            "CloseStreamAs",
            CloseStreamAsRequest {
                scope_id,
                close: Some(CloseStreamRequest {
                    stream: Some(self.stream_ref()),
                    ..Default::default()
                }),
            },
        )
        .await
    }

    async fn close_scope(&self, scope_id: u64) -> Result<CloseScopeResponse, StreamError> {
        self.call("CloseScope", CloseScopeRequest { scope_id })
            .await
    }

    async fn items(&self) -> Vec<StreamRecord> {
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
    }

    async fn visible(&self) -> Vec<String> {
        let mut kept: Vec<StreamRecord> = vec![];
        for record in self.items().await {
            match &record.item {
                Some(Item::Superseded(s)) => kept.retain(|old| !withdraws(s, old)),
                Some(Item::Data(_)) => kept.push(record),
                _ => {}
            }
        }
        kept.into_iter()
            .filter_map(|record| match record.item {
                Some(Item::Data(data)) => Some(String::from_utf8(data.body.unwrap().data).unwrap()),
                _ => None,
            })
            .collect()
    }

    async fn producers(&self) -> Vec<Producer> {
        self.items()
            .await
            .into_iter()
            .filter_map(|record| match record.item {
                Some(Item::Data(data)) => data.producer,
                Some(Item::Close(close)) => close.producer,
                _ => None,
            })
            .collect()
    }
}

/// A client scope writes as `client:<identity>:<key>`, and its close comes after its Records.
async fn a_client_scope_publishes_and_closes_as_one_producer() {
    for case in cases().await {
        let opened = case.client_scope("", 0).unwrap();
        let producer = opened.producer.unwrap();
        assert!(
            producer.id.starts_with("client:me%40host:"),
            "{}",
            producer.id
        );
        assert_eq!(producer.epoch.unwrap().attempt, 1);
        let (first, last) = case.publish(opened.scope_id, &["a", "b"]).await.unwrap();
        assert!(0 < first && first < last);
        case.close_as(opened.scope_id).await.unwrap();
        let items = case.items().await;
        assert!(matches!(items.last().unwrap().item, Some(Item::Close(_))));
        assert!(case.producers().await.iter().all(|p| *p == producer));
        assert_eq!(case.visible().await, ["a", "b"]);
        // The scope's writer closed with the Stream, so a later publish opens a new one.
        let error = case.publish(opened.scope_id, &["late"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_closed");
    }
}

/// Each client scope with no name has its own Producer, so two clients never share a sequence.
async fn two_client_scopes_are_two_producers() {
    for case in cases().await {
        let a = case.client_scope("", 0).unwrap();
        let b = case.client_scope("", 0).unwrap();
        assert_ne!(a.scope_id, b.scope_id);
        assert_ne!(a.producer.unwrap().id, b.producer.unwrap().id);
        case.publish(a.scope_id, &["a"]).await.unwrap();
        case.publish(b.scope_id, &["b"]).await.unwrap();
        case.publish(a.scope_id, &["c"]).await.unwrap();
        assert_eq!(case.visible().await, ["a", "b", "c"]);
    }
}

/// A named scope at a higher epoch fences the earlier one, and withdraws none of its Records.
async fn a_named_client_scope_fences_its_earlier_epoch() {
    for case in cases().await {
        let old = case.client_scope("ingest", 1).unwrap();
        assert_eq!(old.producer.as_ref().unwrap().id, "client:ingest");
        case.publish(old.scope_id, &["old"]).await.unwrap();
        let new = case.client_scope("ingest", 2).unwrap();
        case.publish(new.scope_id, &["new"]).await.unwrap();
        let error = case.publish(old.scope_id, &["late"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_fenced");
        assert_eq!(case.visible().await, ["old", "new"]);
        let error = case.client_scope("ingest", 0).unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// The Producer of an Activity scope is the one that `DeriveProducer` gives.
async fn an_activity_scope_writes_as_its_derived_producer() {
    for case in cases().await {
        let scope = case.activity_scope(activity(1), &[]).await;
        case.publish(scope, &["a1"]).await.unwrap();
        let derived: DeriveProducerResponse = case
            .call_sync(
                "DeriveProducer",
                DeriveProducerRequest {
                    context: Some(activity(1)),
                    purpose: ProducerPurpose::ActivityWriter as i32,
                },
            )
            .unwrap();
        assert_eq!(case.producers().await, vec![derived.producer.unwrap()]);
        let closed = case.close_scope(scope).await.unwrap();
        assert_eq!(closed, CloseScopeResponse::default());
    }
}

/// A retry's scope withdraws the earlier attempt's Records on each Stream of its arguments, even
/// if the retry writes nothing. A missing Stream there doesn't fail the open.
async fn an_activity_retry_scope_withdraws_the_streams_of_its_arguments() {
    for case in cases().await {
        let first = case.activity_scope(activity(1), &[]).await;
        case.publish(first, &["a1"]).await.unwrap();
        case.close_scope(first).await.unwrap();
        let missing = format!("{}-missing", case.stream);
        let retry = case
            .activity_scope(activity(2), &[&case.stream, &case.stream, &missing])
            .await;
        assert!(case.visible().await.is_empty());
        case.publish(retry, &["a2"]).await.unwrap();
        assert_eq!(case.visible().await, ["a2"]);
    }
}

/// An Activity doesn't close a Stream, and its refusal calls no Store.
async fn an_activity_scope_doesnt_close_a_stream() {
    for case in cases().await {
        let scope = case.activity_scope(activity(1), &[]).await;
        let error = case.close_as(scope).await.unwrap_err();
        assert_eq!(reason(&error), "stream_not_owner");
        let described: DescribeStreamResponse = case
            .call(
                "DescribeStream",
                DescribeStreamRequest {
                    namespace: case.namespace.clone(),
                    stream: Some(case.stream_ref()),
                },
            )
            .await
            .unwrap();
        assert_eq!(described.status(), StreamStatus::Open);
    }
}

/// A Local Activity scope sends its run's task facts, so its first write withdraws the Records of
/// a Local Activity whose task failed.
async fn a_local_activity_scope_withdraws_the_records_of_a_failed_task() {
    for case in cases().await {
        let orphan = case
            .activity_scope(local_activity("1", 3, 2, vec![]), &[])
            .await;
        case.publish(orphan, &["orphan"]).await.unwrap();
        case.close_scope(orphan).await.unwrap();
        let next = case
            .activity_scope(local_activity("2", 5, 4, vec![3]), &[])
            .await;
        case.publish(next, &["next"]).await.unwrap();
        assert_eq!(case.visible().await, ["next"]);
    }
}

/// The two first publishes of a scope to a Stream share one writer and one sequence.
async fn concurrent_first_publishes_share_one_writer() {
    for case in cases().await {
        let scope = case.activity_scope(activity(2), &[]).await;
        let (a, b) = tokio::join!(case.publish(scope, &["a"]), case.publish(scope, &["b"]));
        a.unwrap();
        b.unwrap();
        let mut visible = case.visible().await;
        visible.sort();
        assert_eq!(visible, ["a", "b"]);
        assert_eq!(case.close_scope(scope).await.unwrap().failed_writers, 0);
    }
}

/// The close of a scope reports a failed writer instead of failing, and frees the scope.
async fn a_scope_close_reports_a_failed_writer_and_frees_the_scope() {
    for case in cases().await {
        let scope = case.client_scope("", 0).unwrap().scope_id;
        case.publish(scope, &["a"]).await.unwrap();
        let other = case.client_scope("", 0).unwrap().scope_id;
        case.close_as(other).await.unwrap();
        assert_eq!(
            reason(&case.publish(scope, &["late"]).await.unwrap_err()),
            "stream_closed"
        );
        let closed = case.close_scope(scope).await.unwrap();
        assert_eq!(closed.failed_writers, 1);
        assert_eq!(closed.failure.unwrap().reason(), StreamErrorReason::Closed);
        let error = case.publish(scope, &["x"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_unknown_writer");
        assert!(case.close_scope(scope).await.is_err());
    }
}

/// A freed scope releases its writers with no wait and is gone.
async fn a_freed_scope_is_gone() {
    for case in cases().await {
        let scope = case.client_scope("", 0).unwrap().scope_id;
        case.publish(scope, &["a"]).await.unwrap();
        let _: FreeResponse = case
            .call_sync("FreeScope", FreeScopeRequest { scope_id: scope })
            .unwrap();
        let error = case.publish(scope, &["x"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_unknown_writer");
        let error = case
            .call_sync::<_, FreeResponse>("FreeScope", FreeScopeRequest { scope_id: scope })
            .unwrap_err();
        assert_eq!(reason(&error), "stream_unknown_writer");
        assert_eq!(case.visible().await, ["a"]);
    }
}

/// The shutdown closes every scope. Then every call fails, except the `Free` methods, which do
/// nothing, and the static ones.
async fn the_shutdown_closes_every_scope_and_refuses_later_calls() {
    for case in cases().await {
        let client = case.client_scope("", 0).unwrap().scope_id;
        let attempt = case.activity_scope(activity(1), &[]).await;
        case.publish(client, &["c"]).await.unwrap();
        case.publish(attempt, &["a"]).await.unwrap();
        let shut: ShutdownResponse = case.call("Shutdown", ShutdownRequest {}).await.unwrap();
        assert_eq!(shut.failed_writers, 0);
        let error = case.publish(client, &["x"]).await.unwrap_err();
        assert_eq!(reason(&error), "stream_unsupported");
        assert_eq!(
            reason(&case.client_scope("", 0).unwrap_err()),
            "stream_unsupported"
        );
        let _: FreeResponse = case
            .call_sync("FreeScope", FreeScopeRequest { scope_id: client })
            .unwrap();
        case.ext.free_writer(u64::MAX).unwrap();
        let _: DeriveProducerResponse = case
            .call_sync(
                "DeriveProducer",
                DeriveProducerRequest {
                    context: Some(activity(1)),
                    purpose: ProducerPurpose::ActivityWriter as i32,
                },
            )
            .unwrap();
    }
}

/// Lang's `CloseWriters` closes only the writers of `OpenWriter`. A scope keeps its own, since
/// another Worker of the client may still write through it.
async fn close_writers_leaves_the_writers_of_scopes() {
    for case in cases().await {
        let scope = case.client_scope("", 0).unwrap().scope_id;
        case.publish(scope, &["a"]).await.unwrap();
        let closed: CloseWritersResponse = case
            .call("CloseWriters", CloseWritersRequest {})
            .await
            .unwrap();
        assert_eq!(closed.failed, 0);
        case.publish(scope, &["b"]).await.unwrap();
        assert_eq!(case.close_scope(scope).await.unwrap().failed_writers, 0);
        assert_eq!(case.visible().await, ["a", "b"]);
    }
}

/// A late `FreeWriter` from lang, after `CloseWriters`, takes no handle of a scope that shares the
/// writer.
async fn a_late_lang_free_leaves_the_writer_of_a_scope() {
    for case in cases().await {
        let opened = case.client_scope("shared", 1).unwrap();
        case.publish(opened.scope_id, &["a"]).await.unwrap();
        let writer: OpenWriterResponse = case
            .call(
                "OpenWriter",
                OpenWriterRequest {
                    namespace: case.namespace.clone(),
                    stream: Some(case.stream_ref()),
                    producer: opened.producer.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let _: CloseWritersResponse = case
            .call("CloseWriters", CloseWritersRequest {})
            .await
            .unwrap();
        let free = FreeWriterRequest {
            writer_id: writer.writer_id,
        };
        let error = case
            .call_sync::<_, FreeWriterResponse>("FreeWriter", free)
            .unwrap_err();
        assert_eq!(reason(&error), "stream_unknown_writer");
        case.publish(opened.scope_id, &["b"]).await.unwrap();
        assert_eq!(case.visible().await, ["a", "b"]);
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(a_client_scope_publishes_and_closes_as_one_producer),
    case!(two_client_scopes_are_two_producers),
    case!(a_named_client_scope_fences_its_earlier_epoch),
    case!(an_activity_scope_writes_as_its_derived_producer),
    case!(an_activity_retry_scope_withdraws_the_streams_of_its_arguments),
    case!(an_activity_scope_doesnt_close_a_stream),
    case!(a_local_activity_scope_withdraws_the_records_of_a_failed_task),
    case!(concurrent_first_publishes_share_one_writer),
    case!(a_scope_close_reports_a_failed_writer_and_frees_the_scope),
    case!(a_freed_scope_is_gone),
    case!(the_shutdown_closes_every_scope_and_refuses_later_calls),
    case!(close_writers_leaves_the_writers_of_scopes),
    case!(a_late_lang_free_leaves_the_writer_of_a_scope),
];
