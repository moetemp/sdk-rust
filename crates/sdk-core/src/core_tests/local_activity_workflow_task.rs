//! The workflow task that each local activity start names.

use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers, mock_worker,
        schedule_local_activity_cmd, start_timer_cmd,
    },
};
use std::time::{Duration, SystemTime};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        activity_task::{ActivityTask, Start, WorkflowTaskAttempt, activity_task},
        workflow_activation::WorkflowActivation,
        workflow_commands::{ActivityCancellationType, ScheduleLocalActivity, workflow_command},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{common::v1::RetryPolicy, enums::v1::EventType, failure::v1::Failure},
};

async fn complete(
    worker: &crate::Worker,
    activation: &WorkflowActivation,
    commands: Vec<workflow_command::Variant>,
) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id.clone(),
            commands,
        ))
        .await
        .unwrap();
}

fn la_cmd() -> workflow_command::Variant {
    schedule_local_activity_cmd(
        1,
        "1",
        ActivityCancellationType::TryCancel,
        Duration::from_secs(60),
    )
}

fn start_of(task: &ActivityTask) -> &Start {
    match &task.variant {
        Some(activity_task::Variant::Start(start)) => start,
        other => panic!("Expected an activity start, got {other:?}"),
    }
}

fn dispatching(start: &Start) -> (i64, u32) {
    let task = start
        .workflow_task
        .as_ref()
        .expect("a local activity names its task");
    (task.started_event_id, task.attempt)
}

async fn finish(worker: crate::Worker, task: ActivityTask) {
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&resolved.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_local_activity_names_the_live_task_that_dispatched_it() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let started_time: SystemTime = t
        .get_full_history_info()
        .unwrap()
        .as_poll_wft_response()
        .started_time
        .unwrap()
        .try_into()
        .unwrap();
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    complete(&worker, &fired, vec![la_cmd()]).await;

    let la = worker.poll_activity_task().await.unwrap();
    let start = start_of(&la);
    assert!(start.is_local);
    assert_eq!(
        start.workflow_task,
        Some(WorkflowTaskAttempt {
            started_event_id: 8,
            attempt: 1,
            started_time: Some(started_time.into()),
        })
    );
    finish(worker, la).await;
}

#[tokio::test]
async fn a_rerun_after_a_timed_out_task_names_the_later_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(60));
    t.add_full_wf_task();
    // The task after it ran the local activity, then the worker died and the task timed out.
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_and_started();
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![la_cmd()]).await;

    // History has no marker for it, so it runs again, dispatched by the live task.
    let la = worker.poll_activity_task().await.unwrap();
    assert_eq!(dispatching(start_of(&la)), (9, 1));
    finish(worker, la).await;
}

#[tokio::test]
async fn a_retry_that_core_runs_keeps_the_dispatching_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            ScheduleLocalActivity {
                seq: 1,
                activity_id: "1".to_string(),
                activity_type: "act".to_string(),
                start_to_close_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
                retry_policy: Some(RetryPolicy {
                    initial_interval: Some(Duration::from_millis(10).try_into().unwrap()),
                    maximum_attempts: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }
            .into(),
        ],
    )
    .await;

    let first_attempt = worker.poll_activity_task().await.unwrap();
    assert_eq!(start_of(&first_attempt).attempt, 1);
    assert_eq!(dispatching(start_of(&first_attempt)), (3, 1));
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: first_attempt.task_token,
            result: Some(ActivityExecutionResult::fail(Failure::application_failure(
                "retry me".to_string(),
                false,
            ))),
        })
        .await
        .unwrap();
    let second_attempt = worker.poll_activity_task().await.unwrap();
    assert_eq!(start_of(&second_attempt).attempt, 2);
    assert_eq!(dispatching(start_of(&second_attempt)), (3, 1));
    finish(worker, second_attempt).await;
}
