//! Attachments: the server puts them on a Workflow Task's scheduled event, and Core hands them to
//! lang as one job per task, live and on replay alike.

use crate::{
    replay::TestHistoryBuilder,
    test_help::{MockPollCfg, ResponseType, build_mock_pollers, mock_worker, start_timer_cmd},
    worker::client::mocks::mock_worker_client,
};
use std::time::Duration;
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_activation::{
                WorkflowActivation, WorkflowTaskAttachments, workflow_activation_job,
            },
            workflow_commands::CompleteWorkflowExecution,
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
        },
    },
    worker::WorkerTaskTypes,
};

fn attachment(value: &str) -> prost_types::Any {
    prost_types::Any {
        type_url: "type.googleapis.com/test.Attachment".to_string(),
        value: value.as_bytes().to_vec(),
    }
}

fn attachment_jobs(activation: &WorkflowActivation) -> Vec<WorkflowTaskAttachments> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::WorkflowTaskAttachments(a)) => Some(a.clone()),
            _ => None,
        })
        .collect()
}

fn job_of(values: &[&str]) -> WorkflowTaskAttachments {
    WorkflowTaskAttachments {
        attachments: values.iter().map(|v| attachment(v)).collect(),
    }
}

fn job_names(activation: &WorkflowActivation) -> Vec<String> {
    activation
        .jobs
        .iter()
        .map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::SignalWorkflow(s)) => {
                format!("Signal({})", s.signal_name)
            }
            Some(other) => other.to_string(),
            None => "none".to_string(),
        })
        .collect()
}

fn worker(history: TestHistoryBuilder, batches: Vec<ResponseType>) -> crate::Worker {
    let mut mock = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fake_wf_id",
        history,
        batches,
        mock_worker_client(),
    ));
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

async fn complete_empty(worker: &crate::Worker, run_id: &str) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.to_string()))
        .await
        .unwrap();
}

/// A first task with nothing for lang to do, then two tasks scheduled only because the server had
/// something to attach. The first and the second complete with no commands, which is the shape of
/// a heartbeat chain, so replay must still give each attachment task an activation of its own.
fn attachments_after_a_quiet_task() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_attachments(vec![attachment("first")]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_workflow_task_scheduled_with_attachments(vec![attachment("second")]);
    t.add_workflow_task_started();
    t
}

async fn run_attachments_after_a_quiet_task(batches: Vec<ResponseType>, replaying: bool) {
    let worker = worker(attachments_after_a_quiet_task(), batches);

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    assert!(attachment_jobs(&init).is_empty(), "{:?}", job_names(&init));
    complete_empty(&worker, &run_id).await;

    let first = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        attachment_jobs(&first),
        vec![job_of(&["first"])],
        "the attachment task gets an activation of its own: {:?}",
        job_names(&first)
    );
    assert_eq!(first.is_replaying, replaying);
    complete_empty(&worker, &run_id).await;

    let second = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(attachment_jobs(&second), vec![job_of(&["second"])]);
    assert!(!second.is_replaying);
}

#[tokio::test]
async fn each_attachment_task_gets_its_own_activation_live() {
    run_attachments_after_a_quiet_task(
        vec![
            ResponseType::ToTaskNum(1),
            ResponseType::ToTaskNum(2),
            ResponseType::ToTaskNum(3),
        ],
        false,
    )
    .await;
}

#[tokio::test]
async fn each_attachment_task_gets_its_own_activation_on_replay() {
    run_attachments_after_a_quiet_task(vec![ResponseType::AllHistory], true).await;
}

#[tokio::test]
async fn a_failed_task_and_its_retry_give_one_job_in_event_order() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_attachments(vec![attachment("a"), attachment("b")]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure::default(),
    );
    t.add_workflow_task_scheduled_with_attachments(vec![attachment("a")]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![ResponseType::AllHistory]);

    let init = worker.poll_workflow_activation().await.unwrap();
    complete_empty(&worker, &init.run_id).await;
    let attached = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        attachment_jobs(&attached),
        vec![job_of(&["a", "b", "a"])],
        "{:?}",
        job_names(&attached)
    );
}

#[tokio::test]
async fn attachments_share_the_activation_and_come_after_resolutions() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_timer_started("1".to_string());
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_with_attachments(vec![attachment("x")]);
    t.add_workflow_task_started();
    let worker = worker(
        t,
        vec![ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)],
    );

    let init = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            init.run_id.clone(),
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    let attached = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        job_names(&attached),
        ["Signal(go)", "FireTimer(1)", "WorkflowTaskAttachments(1)"]
    );
}

/// A heartbeat task that a sticky Worker did not finish, then its retry scheduled with attachments.
/// The server writes the retry at attempt 1, so it sits inside the heartbeat chain and not right
/// after a completion.
#[tokio::test]
async fn a_sticky_retry_with_attachments_ends_a_heartbeat_chain_on_replay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_with_attachments(vec![attachment("w")]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let worker = worker(t, vec![ResponseType::AllHistory]);

    let init = worker.poll_workflow_activation().await.unwrap();
    assert!(init.is_replaying);
    assert!(attachment_jobs(&init).is_empty(), "{:?}", job_names(&init));
    complete_empty(&worker, &init.run_id).await;

    let attached = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        attachment_jobs(&attached),
        vec![job_of(&["w"])],
        "{:?}",
        job_names(&attached)
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            attached.run_id.clone(),
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();
}
