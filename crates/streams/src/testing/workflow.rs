//! The Workflow write calls of the Local Activities and the close Activity, through the runtime's
//! byte call, as lang sends them.
//!
//! Every Store runs the suite through its own runtime.

use super::{
    case,
    support::{Flaky, Needs, stores_for},
};
use crate::{
    MemoryStreamRuntime, StreamError, StreamRuntime,
    epoch::withdraws,
    proto::{
        ActivityContext, AppendRecord, AppendRecordsRequest, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, Producer, ProducerEpoch,
        PublishOutcome, ReadRecordsRequest, ReadRecordsResponse, StreamErrorReason,
        StreamProducerKind, StreamRecord, StreamRef, WorkflowAppendPendingResponse,
        WorkflowAppendRequest, WorkflowAppendResponse, WorkflowCloseRequest, WorkflowTaskAttempt,
        publish_outcome::Outcome, stream_record::Item,
    },
};
use prost::Message;
use prost_types::Timestamp;
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::{SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// One Workflow-owned Stream of run `run` of Workflow `workflow`.
struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
    workflow: String,
    run: String,
}

fn unique() -> String {
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{now}-{n}")
}

async fn case_on(ext: Arc<StreamRuntime>) -> Case {
    let unique = unique();
    let case = Case {
        ext,
        namespace: format!("ns-{unique}"),
        stream: format!("s-{unique}"),
        workflow: format!("wf-{unique}"),
        run: "r1".into(),
    };
    case.create(&case.run, "").await.unwrap();
    case
}

async fn cases() -> Vec<Case> {
    let mut cases = vec![];
    for ext in super::support::stream_runtimes() {
        cases.push(case_on(ext).await);
    }
    cases
}

fn text_records(texts: &[&str]) -> Vec<AppendRecord> {
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

fn at(seconds: i64) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos: 0 })
}

fn texts_of(records: &[StreamRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match &record.item {
            Some(Item::Data(data)) => {
                Some(String::from_utf8(data.body.clone().unwrap().data).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn last_offset(outcome: &PublishOutcome) -> i64 {
    match outcome.outcome {
        Some(Outcome::LastOffset(offset)) => offset,
        ref other => panic!("no offset: {other:?}"),
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

    /// The ref with the owner run chain, which a create with an owner and the close need.
    fn stream_ref(&self) -> StreamRef {
        StreamRef {
            id: self.stream.clone(),
            owner: super::support::workflow_execution(&self.workflow, "first-run"),
            ..Default::default()
        }
    }

    async fn create(&self, run: &str, withdraw_run: &str) -> Result<(), StreamError> {
        let _: CreateStreamResponse = self
            .call(
                "CreateStream",
                CreateStreamRequest {
                    namespace: self.namespace.clone(),
                    stream_id: self.stream.clone(),
                    withdraw_run: withdraw_run.into(),
                    owner: Some(Producer {
                        kind: StreamProducerKind::Workflow as i32,
                        id: crate::ids::workflow_producer_id(run),
                        run_id: run.into(),
                        ..Default::default()
                    }),
                    r#ref: Some(self.stream_ref()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    /// The context of the append Local Activity with key `call`, in the task that started at
    /// `started`, after the completed task `completed` and the failed tasks `failed`.
    fn local(&self, call: &str, started: i64, completed: i64, failed: Vec<i64>) -> ActivityContext {
        ActivityContext {
            is_local: true,
            in_workflow: true,
            workflow_id: self.workflow.clone(),
            workflow_run_id: self.run.clone(),
            activity_id: call.into(),
            attempt: 1,
            call_key: call.into(),
            workflow_task: Some(WorkflowTaskAttempt {
                started_event_id: started,
                attempt: 1,
                started_time: at(started),
                previous_started_event_id: completed,
                failed_tasks: failed,
                previous_attempt: 1,
                previous_started_time: at(completed),
            }),
            ..Default::default()
        }
    }

    fn append_request(
        &self,
        context: ActivityContext,
        records: Vec<AppendRecord>,
        counts: &[i32],
        withdraw_run: &str,
    ) -> WorkflowAppendRequest {
        WorkflowAppendRequest {
            namespace: self.namespace.clone(),
            context: Some(context),
            append: Some(AppendRecordsRequest {
                stream: Some(self.stream_ref()),
                records,
                withdraw_run: withdraw_run.into(),
                ..Default::default()
            }),
            counts: counts.to_vec(),
        }
    }

    async fn append(
        &self,
        context: ActivityContext,
        texts: &[&str],
        counts: &[i32],
    ) -> Result<Vec<PublishOutcome>, StreamError> {
        let request = self.append_request(context, text_records(texts), counts, "");
        let appended: WorkflowAppendResponse = self.call("WorkflowAppend", request).await?;
        Ok(appended.outcomes)
    }

    fn pending(&self, context: ActivityContext) -> bool {
        let request = self.append_request(context, vec![], &[], "");
        let bytes = self
            .ext
            .call_sync("WorkflowAppendPending", &request.encode_to_vec())
            .unwrap();
        WorkflowAppendPendingResponse::decode(bytes.as_slice())
            .unwrap()
            .pending
    }

    async fn close(&self, context: ActivityContext) -> Result<CloseStreamResponse, StreamError> {
        self.call(
            "WorkflowClose",
            WorkflowCloseRequest {
                namespace: self.namespace.clone(),
                context: Some(context),
                close: Some(CloseStreamRequest {
                    stream: Some(self.stream_ref()),
                    ..Default::default()
                }),
            },
        )
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
        texts_of(&kept)
    }
}

/// Each merged publish gets the offset of its own last Record, and the Records carry the Local
/// Activity's Producer at attempt 1.
async fn an_append_gives_each_publish_its_last_offset() {
    for case in cases().await {
        let mut context = case.local("1", 3, 2, vec![]);
        context.attempt = 3;
        let outcomes = case
            .append(context, &["a", "b", "c"], &[2, 1])
            .await
            .unwrap();
        let items = case.items().await;
        let offsets: Vec<i64> = items.iter().map(|record| record.offset).collect();
        assert_eq!(texts_of(&items), ["a", "b", "c"]);
        assert_eq!(
            outcomes.iter().map(last_offset).collect::<Vec<_>>(),
            [offsets[1], offsets[2]]
        );
        let Some(Item::Data(data)) = &items[0].item else {
            panic!("not data");
        };
        let producer = data.producer.clone().unwrap();
        assert_eq!(
            producer.id,
            crate::ids::local_activity_producer_id("r1", "1")
        );
        let epoch = producer.epoch.unwrap();
        assert_eq!((epoch.task_started_event_id, epoch.attempt), (3, 1));
    }
}

/// A retry whose first attempt lost the Store's answer resends the kept batch, so the Store keeps
/// the Records once, and the retry needs no Records.
async fn a_retry_resends_the_kept_batch_once() {
    for target in stores_for(Needs::CALLS).await {
        let store = Arc::new(Flaky {
            lose_next: AtomicBool::new(true),
            ..Flaky::new(target.store)
        });
        let case = case_on(MemoryStreamRuntime::with_store(store, None)).await;
        let context = case.local("1", 3, 2, vec![]);
        let error = case
            .append(context.clone(), &["a", "b"], &[2])
            .await
            .unwrap_err();
        assert_eq!(error.reason.token(), "stream_storage");
        assert!(case.pending(context.clone()));
        let outcomes = case.append(context.clone(), &[], &[]).await.unwrap();
        assert_eq!(case.visible().await, ["a", "b"]);
        assert_eq!(last_offset(&outcomes[0]), case.items().await[1].offset);
        assert!(!case.pending(context.clone()));
        let error = case.append(context, &[], &[]).await.unwrap_err();
        assert_eq!(error.reason.token(), "stream_storage");
    }
}

/// A publish that the codec grew past the Record limit fails alone, and the others keep their
/// offsets. A merge past the batch limit goes in one batch for each publish.
async fn an_oversize_publish_fails_alone() {
    for case in cases().await {
        let big = "x".repeat(1_400_000);
        let huge = "y".repeat(1_700_000);
        let outcomes = case
            .append(
                case.local("1", 3, 2, vec![]),
                &[&big, &huge, "z"],
                &[1, 1, 1],
            )
            .await
            .unwrap();
        assert!(last_offset(&outcomes[0]) > 0);
        assert!(matches!(
            &outcomes[1].outcome,
            Some(Outcome::Failure(failure)) if failure.reason() == StreamErrorReason::TooLarge
        ));
        assert!(last_offset(&outcomes[2]) > last_offset(&outcomes[0]));
        let outcomes = case
            .append(
                case.local("2", 3, 2, vec![]),
                &[&big, &big, &big],
                &[1, 1, 1],
            )
            .await
            .unwrap();
        let offsets: Vec<i64> = outcomes.iter().map(last_offset).collect();
        assert!(offsets[0] < offsets[1] && offsets[1] < offsets[2]);
        assert_eq!(case.visible().await.len(), 5);
    }
}

/// A retry run's split append withdraws the previous run's Records, also when the Store refuses
/// its first batch as too large.
async fn a_split_append_withdraws_the_previous_run() {
    for mut case in cases().await {
        case.append(case.local("1", 3, 2, vec![]), &["old"], &[1])
            .await
            .unwrap();
        case.run = "r2".into();
        case.create("r2", "").await.unwrap();
        let (huge, big) = ("y".repeat(1_700_000), "x".repeat(1_400_000));
        let request = case.append_request(
            case.local("1", 3, 2, vec![]),
            text_records(&[&huge, &big, "new"]),
            &[1, 1, 1],
            "r1",
        );
        let appended: WorkflowAppendResponse = case.call("WorkflowAppend", request).await.unwrap();
        assert!(matches!(
            appended.outcomes[0].outcome,
            Some(Outcome::Failure(_))
        ));
        let visible = case.visible().await;
        assert_eq!(visible.len(), 2);
        assert_eq!(visible.last().unwrap(), "new");
    }
}

/// An append in a later task withdraws the Records of a Local Activity whose task failed.
async fn a_later_append_withdraws_a_failed_task_s_records() {
    for case in cases().await {
        case.append(case.local("1", 3, 2, vec![]), &["orphan"], &[1])
            .await
            .unwrap();
        case.append(case.local("2", 5, 4, vec![3]), &["next"], &[1])
            .await
            .unwrap();
        assert_eq!(case.visible().await, ["next"]);
    }
}

/// A refusal that a retry can't fix drops the kept batch, so the next attempt encodes again.
async fn a_lasting_refusal_drops_the_kept_batch() {
    for case in cases().await {
        let context = case.local("1", 3, 2, vec![]);
        let mut close = context.clone();
        close.is_local = false;
        close.workflow_task = None;
        case.close(close).await.unwrap();
        let error = case
            .append(context.clone(), &["late"], &[1])
            .await
            .unwrap_err();
        assert_eq!(error.reason.token(), "stream_closed");
        assert!(!case.pending(context));
    }
}

/// Bad counts are refused before any Store call.
async fn the_counts_must_cover_the_records() {
    for case in cases().await {
        for counts in [vec![1], vec![2, 0], vec![3]] {
            let error = case
                .append(case.local("1", 3, 2, vec![]), &["a", "b"], &counts)
                .await
                .unwrap_err();
            assert_eq!(error.reason.token(), "stream_invalid");
        }
        assert!(case.visible().await.is_empty());
    }
}

/// The close Activity closes as the run that scheduled it, and only its own Workflow's Stream.
async fn the_close_closes_as_its_run_for_its_own_workflow_only() {
    for case in cases().await {
        let mut context = case.local("1", 3, 2, vec![]);
        context.is_local = false;
        context.workflow_task = None;
        let mut stranger = context.clone();
        stranger.workflow_id = "someone-else".into();
        let error = case.close(stranger).await.unwrap_err();
        assert_eq!(error.reason.token(), "stream_not_owner");
        let mut local = context.clone();
        local.is_local = true;
        let error = case.close(local).await.unwrap_err();
        assert_eq!(error.reason.token(), "stream_invalid");
        case.close(context).await.unwrap();
        let items = case.items().await;
        let Some(Item::Close(close)) = &items.last().unwrap().item else {
            panic!("not closed");
        };
        assert_eq!(
            close.producer,
            Some(Producer {
                kind: StreamProducerKind::Workflow as i32,
                id: crate::ids::workflow_producer_id("r1"),
                epoch: None::<ProducerEpoch>,
                run_id: "r1".into(),
            })
        );
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(an_append_gives_each_publish_its_last_offset),
    case!(a_retry_resends_the_kept_batch_once),
    case!(an_oversize_publish_fails_alone),
    case!(a_split_append_withdraws_the_previous_run),
    case!(a_later_append_withdraws_a_failed_task_s_records),
    case!(a_lasting_refusal_drops_the_kept_batch),
    case!(the_counts_must_cover_the_records),
    case!(the_close_closes_as_its_run_for_its_own_workflow_only),
];
