//! The Workflow Task each activation names, as History defines it.

use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, WorkerExt, WorkerTestHelpers,
        build_mock_pollers, hist_to_poll_resp, mock_worker, schedule_local_activity_cmd,
        start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use std::time::Duration;
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        activity_task::activity_task,
        workflow_activation::{
            WorkflowActivation, WorkflowActivationJob, WorkflowTaskHistoryInfo,
            workflow_activation_job,
        },
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, UpdateResponse,
            update_response::Response, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::{EventType, TimeoutType, WorkflowTaskFailedCause},
        history::v1::{WorkflowTaskTimedOutEventAttributes, history_event},
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};

fn task(started: i64, failed: u32, completed: &[i64]) -> WorkflowTaskHistoryInfo {
    WorkflowTaskHistoryInfo {
        started_event_id: started,
        failed_attempts: failed,
        completed_started_event_ids: completed.to_vec(),
    }
}

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

/// Start, a task that starts a timer, the timer firing, whatever `between` adds, then a task
/// scheduled at `attempt`.
fn history_with_reruns(
    between: impl FnOnce(&mut TestHistoryBuilder),
    attempt: i32,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    between(&mut t);
    t.add_workflow_task_scheduled();
    let scheduled = t.current_event_id();
    t.modify_event(scheduled, |e| {
        if let Some(history_event::Attributes::WorkflowTaskScheduledEventAttributes(a)) =
            e.attributes.as_mut()
        {
            a.attempt = attempt;
        }
    });
    t.add_workflow_task_started();
    t
}

/// Replays the first task of `history`, then answers the task the second activation names.
async fn second_task(history: TestHistoryBuilder) -> WorkflowTaskHistoryInfo {
    let worker = mock_worker(build_mock_pollers(MockPollCfg::from_resp_batches(
        "fakeid",
        history,
        [2],
        mock_worker_client(),
    )));
    let first = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(first.workflow_task, Some(task(3, 0, &[])));
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let second = worker.poll_workflow_activation().await.unwrap();
    assert!(!second.is_replaying);
    worker.complete_execution(&second.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    second.workflow_task.unwrap()
}

#[tokio::test]
async fn each_live_task_names_its_started_event_and_the_completed_tasks() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let mut mock = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [1, 2],
        mock_worker_client(),
    ));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(first.workflow_task, Some(task(3, 0, &[])));
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let second = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(second.workflow_task, Some(task(8, 0, &[3])));
    worker.complete_execution(&second.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn failed_attempts_count_written_failures_and_the_ones_the_server_leaves_out() {
    // An unhandled command, a crash, then two attempts the server writes no event for.
    let history = history_with_reruns(
        |t| {
            t.add_workflow_task_scheduled_and_started();
            t.add_workflow_task_failed_with_failure(
                WorkflowTaskFailedCause::UnhandledCommand,
                Default::default(),
            );
            t.add_workflow_task_scheduled_and_started();
            t.add_workflow_task_timed_out();
        },
        4,
    );
    assert_eq!(second_task(history).await, task(14, 4, &[3]));
}

#[tokio::test]
async fn a_start_to_close_timeout_counts_as_a_failed_attempt() {
    let history = history_with_reruns(
        |t| {
            t.add_workflow_task_scheduled_and_started();
            let scheduled = t.current_event_id() - 1;
            t.add(WorkflowTaskTimedOutEventAttributes {
                scheduled_event_id: scheduled,
                timeout_type: TimeoutType::StartToClose as i32,
                ..Default::default()
            });
        },
        1,
    );
    assert_eq!(second_task(history).await, task(11, 1, &[3]));
}

#[tokio::test]
async fn a_schedule_to_start_timeout_is_not_a_failed_attempt() {
    // No Worker ran the task that timed out.
    let history = history_with_reruns(
        |t| {
            t.add_workflow_task_scheduled();
            let scheduled = t.current_event_id();
            t.add(WorkflowTaskTimedOutEventAttributes {
                scheduled_event_id: scheduled,
                timeout_type: TimeoutType::ScheduleToStart as i32,
                ..Default::default()
            });
        },
        1,
    );
    assert_eq!(second_task(history).await, task(10, 0, &[3]));
}

#[tokio::test]
async fn the_completed_tasks_stop_at_eight_newest_first() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    for seq in 1..=10 {
        t.add_full_wf_task();
        let timer_started = t.add_timer_started(seq.to_string());
        t.add_timer_fired(timer_started, seq.to_string());
    }
    t.add_workflow_task_scheduled_and_started();
    let worker = mock_worker(build_mock_pollers(MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [ResponseType::AllHistory],
        mock_worker_client(),
    )));

    // Task k starts at event 3 + 5k.
    let started = |k: i64| 3 + 5 * k;
    for k in 0..=10 {
        let activation = worker.poll_workflow_activation().await.unwrap();
        let completed: Vec<i64> = (0..k).rev().take(8).map(started).collect();
        assert_eq!(
            activation.workflow_task,
            Some(task(started(k), 0, &completed)),
            "task {k}"
        );
        if k == 10 {
            worker.complete_execution(&activation.run_id).await;
        } else {
            let seq = u32::try_from(k + 1).unwrap();
            complete(
                &worker,
                &activation,
                vec![start_timer_cmd(seq, Duration::from_secs(1))],
            )
            .await;
        }
    }
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_replayed_heartbeat_chain_names_its_last_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    // Two tasks a Local Activity heartbeat created, with no commands.
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", "done".into());
    t.add_workflow_execution_completed();
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    assert_eq!(first.workflow_task, Some(task(9, 0, &[6, 3])));
    complete(
        &worker,
        &first,
        vec![schedule_local_activity_cmd(
            1,
            "1",
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        )],
    )
    .await;
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved.workflow_task, Some(task(9, 0, &[6, 3])));
    complete(
        &worker,
        &resolved,
        vec![CompleteWorkflowExecution { result: None }.into()],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_rejected_speculative_task_is_never_a_completed_task() {
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);

    let mut spec_task_hist = t.clone();
    spec_task_hist.add_workflow_task_scheduled_and_started();

    let mut real_hist = t.clone();
    real_hist.add_timer_fired(timer_started, "1".to_string());
    real_hist.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let mut speculative_task = hist_to_poll_resp(&spec_task_hist, wfid, ResponseType::OneTask(2));
    speculative_task.add_update_request(update_id, 1);

    let mut mh = MockPollCfg::from_resp_batches(
        wfid,
        real_hist,
        [
            ResponseType::ToTaskNum(1),
            speculative_task.into(),
            ResponseType::OneTask(2),
        ],
        mock_worker_client(),
    );
    let mut completes = 0;
    mh.completion_mock_fn = Some(Box::new(move |_| {
        completes += 1;
        let mut r = RespondWorkflowTaskCompletedResponse::default();
        if completes == 2 {
            // The rejection drops the speculative task from History.
            r.reset_history_event_id = 3;
        }
        Ok(r)
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;

    let update = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(update.workflow_task, Some(task(7, 0, &[3])));
    complete(
        &worker,
        &update,
        vec![
            UpdateResponse {
                protocol_instance_id: update_id.to_string(),
                response: Some(Response::Rejected(Default::default())),
            }
            .into(),
        ],
    )
    .await;

    let fired = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        fired.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    assert_eq!(fired.workflow_task, Some(task(8, 0, &[3])));
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_heartbeat_chain_that_reaches_the_live_task_replays_its_completed_part() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(60));
    t.add_full_wf_task();
    // A Local Activity heartbeat, then a Worker restart that times its task out.
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled();
    let scheduled = t.current_event_id();
    t.modify_event(scheduled, |e| {
        if let Some(history_event::Attributes::WorkflowTaskScheduledEventAttributes(a)) =
            e.attributes.as_mut()
        {
            a.attempt = 2;
        }
    });
    t.add_workflow_task_started();
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        first.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    assert!(first.is_replaying);
    assert_eq!(first.workflow_task, Some(task(3, 0, &[])));
    complete(
        &worker,
        &first,
        vec![schedule_local_activity_cmd(
            1,
            "1",
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        )],
    )
    .await;

    let la = worker.poll_activity_task().await.unwrap();
    assert_matches!(
        &la.variant,
        Some(activity_task::Variant::Start(start)) if start.is_local
    );
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: la.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();

    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        resolved.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
        }]
    );
    assert!(!resolved.is_replaying);
    assert_eq!(resolved.workflow_task, Some(task(9, 1, &[3])));
    worker.complete_execution(&resolved.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}
