//! Nexus operation progress: the server folds it onto a Workflow Task's scheduled event, and Core
//! hands it to lang as one job per started operation per task, live and on replay alike.

use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, build_mock_pollers, mock_worker,
        schedule_local_activity_cmd, start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use std::time::Duration;
use temporalio_common::{
    protos::{
        coresdk::{
            nexus::NexusOperationCancellationType,
            workflow_activation::{
                ResolveNexusOperationProgress, WorkflowActivation, workflow_activation_job,
            },
            workflow_commands::{
                ActivityCancellationType, CompleteWorkflowExecution, RequestCancelNexusOperation,
                ScheduleNexusOperation,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::Payload,
            enums::v1::EventType,
            history::v1::{
                NexusOperationCancelRequestedEventAttributes,
                NexusOperationCompletedEventAttributes, NexusOperationScheduledEventAttributes,
                NexusOperationStartedEventAttributes, WorkflowTaskScheduledEventAttributes,
            },
            nexus::v1::{NexusOperationProgress, nexus_operation_progress},
            stream::v1::StreamWake,
        },
    },
    worker::WorkerTaskTypes,
};

fn progress(scheduled_event_id: i64, counter: i64) -> NexusOperationProgress {
    NexusOperationProgress {
        operation: Some(nexus_operation_progress::Operation::ScheduledEventId(
            scheduled_event_id,
        )),
        position: format!("position-{counter}"),
        counter,
        metadata: [("records".to_string(), counter.to_string())].into(),
    }
}

/// The job lang should get for `progress` of the operation lang scheduled as `seq`.
fn job_for(seq: u32, progress: &NexusOperationProgress) -> workflow_activation_job::Variant {
    workflow_activation_job::Variant::ResolveNexusOperationProgress(ResolveNexusOperationProgress {
        seq,
        position: progress.position.clone(),
        counter: progress.counter,
        metadata: progress.metadata.clone(),
    })
}

fn schedule_operation(seq: u32) -> ScheduleNexusOperation {
    ScheduleNexusOperation {
        seq,
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
        ..Default::default()
    }
}

fn add_operation_scheduled(t: &mut TestHistoryBuilder) -> i64 {
    t.add(NexusOperationScheduledEventAttributes {
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
        ..Default::default()
    })
}

fn add_operation_started(t: &mut TestHistoryBuilder, scheduled_event_id: i64) {
    t.add(NexusOperationStartedEventAttributes {
        scheduled_event_id,
        operation_token: format!("token-{scheduled_event_id}"),
        ..Default::default()
    });
}

fn job_names(activation: &WorkflowActivation) -> Vec<String> {
    activation
        .jobs
        .iter()
        .map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::ResolveNexusOperationProgress(p)) => {
                format!("Progress({}, {})", p.seq, p.counter)
            }
            Some(workflow_activation_job::Variant::ResolveNexusOperationStart(s)) => {
                format!("Start({})", s.seq)
            }
            Some(workflow_activation_job::Variant::ResolveNexusOperation(r)) => {
                format!("Resolve({})", r.seq)
            }
            Some(workflow_activation_job::Variant::ResolveActivity(r)) => {
                format!("ResolveActivity({})", r.seq)
            }
            Some(other) => other.to_string(),
            None => "none".to_string(),
        })
        .collect()
}

fn progress_jobs(activation: &WorkflowActivation) -> Vec<workflow_activation_job::Variant> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            v @ Some(workflow_activation_job::Variant::ResolveNexusOperationProgress(_)) => {
                v.clone()
            }
            _ => None,
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

async fn complete(
    worker: &crate::Worker,
    run_id: &str,
    commands: Vec<
        impl Into<temporalio_common::protos::coresdk::workflow_commands::workflow_command::Variant>,
    >,
) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            commands.into_iter().map(Into::into).collect(),
        ))
        .await
        .unwrap();
}

/// A started operation, then a task with nothing for lang to do, then two tasks scheduled only
/// because progress arrived. The second-to-last one completes with no commands, which is the
/// shape of a heartbeat chain, so replay must still give it an activation of its own.
fn progress_after_a_quiet_task() -> (TestHistoryBuilder, [NexusOperationProgress; 2]) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_full_wf_task();
    let first = progress(scheduled, 1);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![first.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    let second = progress(scheduled, 2);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![second.clone()]);
    t.add_workflow_task_started();
    (t, [first, second])
}

/// Drives `progress_after_a_quiet_task` the way a Workflow would, checking each activation.
async fn run_progress_after_a_quiet_task(batches: Vec<ResponseType>, replaying: bool) {
    let (history, [first, second]) = progress_after_a_quiet_task();
    let worker = worker(history, batches);

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    complete(&worker, &run_id, vec![schedule_operation(1)]).await;

    let started = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&started), vec!["Start(1)"]);
    assert_eq!(started.is_replaying, replaying);
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let first_progress = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        progress_jobs(&first_progress),
        vec![job_for(1, &first)],
        "the progress-only task gets an activation of its own: {:?}",
        job_names(&first_progress)
    );
    assert_eq!(first_progress.is_replaying, replaying);
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let second_progress = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(progress_jobs(&second_progress), vec![job_for(1, &second)]);
    assert!(!second_progress.is_replaying);
    complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn progress_reaches_lang_as_a_job_per_task_live() {
    run_progress_after_a_quiet_task(vec![1.into(), 2.into(), 3.into(), 4.into()], false).await;
}

#[tokio::test]
async fn progress_reaches_lang_in_the_same_activations_on_replay() {
    run_progress_after_a_quiet_task(vec![4.into()], true).await;
}

/// Drives a task scheduled with both progress and a stream wake, after a quiet task. It completes
/// with no commands, the shape of a heartbeat chain, so replay must give it the activation it had
/// live, with both jobs.
async fn run_progress_and_a_wake_on_one_task(batches: Vec<ResponseType>, replaying: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_full_wf_task();
    let both = progress(scheduled, 1);
    let wake = StreamWake {
        subscription_id: "reader-1".to_string(),
        position: "position-9".to_string(),
        counter: 9,
        ..Default::default()
    };
    t.add_workflow_task_scheduled_with(WorkflowTaskScheduledEventAttributes {
        nexus_operation_progress: vec![both.clone()],
        stream_wakes: vec![wake],
        ..Default::default()
    });
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    let last = progress(scheduled, 2);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![last.clone()]);
    t.add_workflow_task_started();
    let worker = worker(t, batches);

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    complete(&worker, &run_id, vec![schedule_operation(1)]).await;
    let started = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&started), vec!["Start(1)"]);
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let woken = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        job_names(&woken),
        vec!["Progress(1, 1)", "ResolveStreamWake(reader-1, 9)"]
    );
    assert_eq!(progress_jobs(&woken), vec![job_for(1, &both)]);
    assert_eq!(woken.is_replaying, replaying);
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let next = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&next), vec!["Progress(1, 2)"]);
    assert!(!next.is_replaying);
    complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn progress_and_a_stream_wake_on_one_task_reach_lang_together_live() {
    run_progress_and_a_wake_on_one_task(vec![1.into(), 2.into(), 3.into(), 4.into()], false).await;
}

#[tokio::test]
async fn progress_and_a_stream_wake_on_one_task_replay_in_the_activation_they_had_live() {
    run_progress_and_a_wake_on_one_task(vec![4.into()], true).await;
}

#[tokio::test]
async fn progress_follows_the_start_it_arrived_with() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    let only = progress(scheduled, 7);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![only.clone()]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![1.into(), 2.into()]);

    let init = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &init.run_id, vec![schedule_operation(1)]).await;
    let next = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&next), vec!["Start(1)", "Progress(1, 7)"]);
    assert_eq!(progress_jobs(&next), vec![job_for(1, &only)]);
    complete(
        &worker,
        &init.run_id,
        vec![CompleteWorkflowExecution::default()],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_burst_across_a_failed_task_and_its_retry_is_one_job() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    // The failed task's scheduled event and its retry's are applied together. The first holds two
    // entries for the same operation, out of order, to show the fold does not depend on the
    // server having folded already.
    t.add_workflow_task_scheduled_with_nexus_progress(vec![
        progress(scheduled, 4),
        progress(scheduled, 2),
    ]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        temporalio_common::protos::temporal::api::enums::v1::WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    let latest = progress(scheduled, 6);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 5), latest.clone()]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![1.into(), ResponseType::AllHistory]);

    let init = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &init.run_id, vec![schedule_operation(1)]).await;
    let burst = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&burst), vec!["Start(1)", "Progress(1, 6)"]);
    assert_eq!(progress_jobs(&burst), vec![job_for(1, &latest)]);
    complete(
        &worker,
        &init.run_id,
        vec![CompleteWorkflowExecution::default()],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn progress_on_a_failed_task_reaches_lang_when_the_retry_carries_none() {
    // The server keeps progress on the scheduled event it folded into and does not repeat it on
    // the retry, so the failed task's scheduled event is the only record of it.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    let only = progress(scheduled, 4);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![only.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        temporalio_common::protos::temporal::api::enums::v1::WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();
    for batches in [
        vec![1.into(), ResponseType::AllHistory],
        vec![ResponseType::AllHistory],
    ] {
        let worker = worker(t.clone(), batches);
        let init = worker.poll_workflow_activation().await.unwrap();
        complete(&worker, &init.run_id, vec![schedule_operation(1)]).await;
        let retried = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(job_names(&retried), vec!["Start(1)", "Progress(1, 4)"]);
        assert_eq!(progress_jobs(&retried), vec![job_for(1, &only)]);
        complete(
            &worker,
            &init.run_id,
            vec![CompleteWorkflowExecution::default()],
        )
        .await;
        worker.drain_pollers_and_shutdown().await;
    }
}

#[tokio::test]
async fn progress_at_or_below_a_delivered_counter_is_dropped_in_later_tasks() {
    // After a failover the new active cluster can deliver a counter the Workflow already saw.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    let first_timer = t.add_timer_started("1".to_string());
    add_operation_started(&mut t, scheduled);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 5)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    let second_timer = t.add_timer_started("2".to_string());
    t.add_timer_fired(first_timer, "1".to_string());
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 5)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_timer_fired(second_timer, "2".to_string());
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 4)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 6)]);
    t.add_workflow_task_started();

    let mut runs = vec![];
    for batches in [
        vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()],
        vec![5.into()],
    ] {
        let worker = worker(t.clone(), batches);
        let mut seen = vec![];
        let init = worker.poll_workflow_activation().await.unwrap();
        let run_id = init.run_id.clone();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                run_id.clone(),
                vec![
                    schedule_operation(1).into(),
                    start_timer_cmd(1, Duration::from_secs(60)),
                ],
            ))
            .await
            .unwrap();
        let started = worker.poll_workflow_activation().await.unwrap();
        seen.push(job_names(&started));
        complete(
            &worker,
            &run_id,
            vec![start_timer_cmd(2, Duration::from_secs(60))],
        )
        .await;
        for _ in 0..3 {
            let next = worker.poll_workflow_activation().await.unwrap();
            seen.push(job_names(&next));
            if seen.len() == 4 {
                complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
            } else {
                complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;
            }
        }
        worker.drain_pollers_and_shutdown().await;
        runs.push(seen);
    }
    assert_eq!(
        runs[0],
        vec![
            vec!["Start(1)".to_string(), "Progress(1, 5)".to_string()],
            vec!["FireTimer(1)".to_string()],
            vec!["FireTimer(2)".to_string()],
            vec!["Progress(1, 6)".to_string()],
        ]
    );
    assert_eq!(runs[0], runs[1]);
}

#[tokio::test]
async fn progress_for_an_unknown_closed_or_stale_operation_is_dropped() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let open = add_operation_scheduled(&mut t);
    let closing = add_operation_scheduled(&mut t);
    let timer = t.add_by_type(EventType::TimerStarted);
    add_operation_started(&mut t, open);
    add_operation_started(&mut t, closing);
    let delivered = progress(open, 5);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![delivered.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add(NexusOperationCompletedEventAttributes {
        scheduled_event_id: closing,
        ..Default::default()
    });
    t.add_workflow_task_scheduled_with_nexus_progress(vec![
        // Resolved in this same task, so lang gets the result and no progress.
        progress(closing, 1),
        // Lower than what lang already has.
        progress(open, 3),
        // No event with this id.
        progress(999, 1),
        // An event that is not a Nexus operation.
        progress(timer, 1),
    ]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![1.into(), 2.into(), 3.into()]);

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                schedule_operation(1).into(),
                schedule_operation(2).into(),
                start_timer_cmd(1, Duration::from_secs(60)),
            ],
        ))
        .await
        .unwrap();
    let started = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        job_names(&started),
        vec!["Start(1)", "Start(2)", "Progress(1, 5)"]
    );
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let last = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&last), vec!["Resolve(2)"]);
    complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
    worker.drain_pollers_and_shutdown().await;
}

/// What a live run leaves in History when a local activity outlives the Workflow Task timeout:
/// Core completes the task with no commands to heartbeat it, the activity resolves in the
/// heartbeat task, lang starts a timer, and the timer's task carries progress. With
/// `progress_on_heartbeat`, the server also put progress on the heartbeat task's scheduled event,
/// which the server must never do.
fn local_activity_heartbeat_then_progress(progress_on_heartbeat: bool) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed();
    if progress_on_heartbeat {
        t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 1)]);
    } else {
        t.add_workflow_task_scheduled();
    }
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_local_activity_result_marker(1, "1", Payload::default());
    let timer = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer, "1".to_string());
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 2)]);
    t.add_workflow_task_started();
    t
}

/// Replays `local_activity_heartbeat_then_progress` and returns each activation's jobs.
async fn replay_local_activity_heartbeat(progress_on_heartbeat: bool) -> Vec<Vec<String>> {
    let mut mock = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fake_wf_id",
        local_activity_heartbeat_then_progress(progress_on_heartbeat),
        [ResponseType::AllHistory],
        mock_worker_client(),
    ));
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes {
            enable_local_activities: true,
            ..WorkerTaskTypes::workflow_only()
        };
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);
    let mut seen = vec![];

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    seen.push(job_names(&init));
    complete(&worker, &run_id, vec![schedule_operation(1)]).await;

    let started = worker.poll_workflow_activation().await.unwrap();
    seen.push(job_names(&started));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            schedule_local_activity_cmd(
                1,
                "1",
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
            ),
        ))
        .await
        .unwrap();

    // Lang waits on the activity, so any activation without its result gets no new commands.
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let jobs = job_names(&next);
        seen.push(jobs.clone());
        if jobs.iter().any(|j| j.starts_with("ResolveActivity")) {
            complete(
                &worker,
                &run_id,
                vec![start_timer_cmd(1, Duration::from_secs(1))],
            )
            .await;
            break;
        }
        complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;
    }

    let fired = worker.poll_workflow_activation().await.unwrap();
    seen.push(job_names(&fired));
    complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
    worker.drain_pollers_and_shutdown().await;
    seen
}

/// Pins the server rule this replay depends on (DD-52): the server never puts progress on a
/// Workflow Task created by a local activity heartbeat. A history that keeps that rule replays in
/// the activations the run had live. A history that breaks it hands lang the progress together
/// with the activity result, which the live run never did. Core's own split at a task with
/// progress is covered by `progress_reaches_lang_in_the_same_activations_on_replay` and
/// `a_task_scheduled_with_nexus_progress_ends_a_heartbeat_chain`; this test cannot catch a Core
/// change, only a change to the rule.
#[tokio::test]
async fn a_history_with_no_progress_on_heartbeat_tasks_replays_as_it_ran_live() {
    let live = vec![
        vec!["InitializeWorkflow".to_string()],
        vec!["Start(1)".to_string()],
        vec!["ResolveActivity(1)".to_string()],
        vec!["FireTimer(1)".to_string(), "Progress(1, 2)".to_string()],
    ];
    let replayed = replay_local_activity_heartbeat(false).await;
    assert_eq!(replayed, live);

    // Progress on the heartbeat task's scheduled event splits the chain there, so the progress
    // reaches lang in the activation that carries the activity result.
    let violating = replay_local_activity_heartbeat(true).await;
    assert_eq!(
        violating,
        vec![
            vec!["InitializeWorkflow".to_string()],
            vec!["Start(1)".to_string()],
            vec![
                "Progress(1, 1)".to_string(),
                "ResolveActivity(1)".to_string()
            ],
            vec!["FireTimer(1)".to_string(), "Progress(1, 2)".to_string()],
        ]
    );
}

/// A task scheduled only to carry progress lang already has (DD-55's follow-up can repeat it),
/// then a timer's task. The repeated progress gives the first task no jobs.
fn stale_progress_only_task() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    let timer = t.add_by_type(EventType::TimerStarted);
    add_operation_started(&mut t, scheduled);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 5)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 5)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_timer_fired(timer, "1".to_string());
    t.add_full_wf_task();
    t
}

async fn run_stale_progress_only_task(batches: Vec<ResponseType>) -> Vec<Vec<String>> {
    let worker = worker(stale_progress_only_task(), batches);
    let mut seen = vec![];
    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    seen.push(job_names(&init));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                schedule_operation(1).into(),
                start_timer_cmd(1, Duration::from_secs(60)),
            ],
        ))
        .await
        .unwrap();
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let jobs = job_names(&next);
        seen.push(jobs.clone());
        if jobs.iter().any(|j| j.starts_with("FireTimer")) {
            complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
            break;
        }
        complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;
    }
    worker.drain_pollers_and_shutdown().await;
    seen
}

/// Live, the task with only stale progress has no jobs, so Core completes it without lang. On
/// replay the same task yields no activation either, so lang sees the same activations.
#[tokio::test]
async fn a_stale_progress_only_task_autocompletes_live_and_replays_without_an_activation() {
    let expected = vec![
        vec!["InitializeWorkflow".to_string()],
        vec!["Start(1)".to_string(), "Progress(1, 5)".to_string()],
        vec!["FireTimer(1)".to_string()],
    ];
    let live = run_stale_progress_only_task(vec![1.into(), 2.into(), 3.into(), 4.into()]).await;
    assert_eq!(live, expected);
    let replayed = run_stale_progress_only_task(vec![ResponseType::AllHistory]).await;
    assert_eq!(replayed, expected);
}

/// Lang cancels a started operation, then a later task carries progress for it, beside a timer
/// so the task has an activation to look at.
fn progress_after_a_cancel(cancel_type: NexusOperationCancellationType) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_full_wf_task();
    // Commands land in the order lang sent them: the cancel request, then the timer.
    if cancel_type == NexusOperationCancellationType::TryCancel {
        t.add(NexusOperationCancelRequestedEventAttributes {
            scheduled_event_id: scheduled,
            ..Default::default()
        });
    }
    let timer = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer, "1".to_string());
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 1)]);
    t.add_workflow_task_started();
    t
}

async fn run_progress_after_a_cancel(
    cancel_type: NexusOperationCancellationType,
    batches: Vec<ResponseType>,
) -> Vec<Vec<String>> {
    let worker = worker(progress_after_a_cancel(cancel_type), batches);
    let mut seen = vec![];
    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    seen.push(job_names(&init));
    complete(
        &worker,
        &run_id,
        vec![ScheduleNexusOperation {
            cancellation_type: cancel_type as i32,
            ..schedule_operation(1)
        }],
    )
    .await;
    let started = worker.poll_workflow_activation().await.unwrap();
    seen.push(job_names(&started));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                RequestCancelNexusOperation { seq: 1 }.into(),
                start_timer_cmd(1, Duration::from_secs(60)),
            ],
        ))
        .await
        .unwrap();
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let jobs = job_names(&next);
        seen.push(jobs.clone());
        if jobs.iter().any(|j| j.starts_with("FireTimer")) {
            complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
            break;
        }
        complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;
    }
    worker.drain_pollers_and_shutdown().await;
    seen
}

/// Once lang cancels with `TryCancel` or `Abandon`, the operation is resolved for lang, so later
/// progress for it is dropped, live and on replay.
#[tokio::test]
async fn progress_after_a_cancelled_operation_is_dropped() {
    for cancel_type in [
        NexusOperationCancellationType::TryCancel,
        NexusOperationCancellationType::Abandon,
    ] {
        let live =
            run_progress_after_a_cancel(cancel_type, vec![1.into(), 2.into(), 3.into()]).await;
        let replayed =
            run_progress_after_a_cancel(cancel_type, vec![ResponseType::AllHistory]).await;
        assert_eq!(live, replayed, "{cancel_type:?}");
        assert!(
            live.iter()
                .flatten()
                .all(|job| !job.starts_with("Progress")),
            "{cancel_type:?}: {live:?}"
        );
        assert!(
            live.iter().flatten().any(|job| job == "Resolve(1)"),
            "{cancel_type:?}: {live:?}"
        );
    }
}
