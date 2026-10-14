//! Lang's early workflow task heartbeat: `Success.heartbeat_task` completes the task while local
//! activities run, and replay must not depend on it.

use crate::{
    Worker,
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerTestHelpers, build_mock_pollers,
        drain_pollers_and_shutdown, mock_worker, schedule_local_activity_cmd, start_timer_cmd,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        activity_task::activity_task,
        workflow_activation::{WorkflowActivation, WorkflowActivationJob, workflow_activation_job},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, workflow_command,
        },
        workflow_completion::{WorkflowActivationCompletion, workflow_activation_completion},
    },
    temporal::api::{
        command::v1::command::Attributes,
        enums::v1::{CommandType, EventType},
        history::v1::{MarkerRecordedEventAttributes, history_event},
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};

// Long enough that Core's own heartbeat, at 80% of it, never fires during a test.
const WFT_TIMEOUT: Duration = Duration::from_secs(60);

fn first_commands() -> Vec<workflow_command::Variant> {
    vec![
        start_timer_cmd(1, Duration::from_secs(3600)),
        schedule_local_activity_cmd(
            1,
            "1",
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        ),
    ]
}

fn completion(
    run_id: &str,
    commands: Vec<workflow_command::Variant>,
    heartbeat_task: bool,
) -> WorkflowActivationCompletion {
    let mut completion = WorkflowActivationCompletion::from_cmds(run_id, commands);
    let Some(workflow_activation_completion::Status::Successful(success)) =
        completion.status.as_mut()
    else {
        unreachable!("from_cmds builds a successful completion");
    };
    success.heartbeat_task = heartbeat_task;
    completion
}

fn is_init(activation: &WorkflowActivation) -> bool {
    matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    )
}

fn resolves_la_1(activation: &WorkflowActivation) -> bool {
    matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(resolve)),
        }] if resolve.seq == 1 && resolve.is_local
    )
}

fn command_types(completion: &WorkflowTaskCompletion) -> Vec<CommandType> {
    completion
        .commands
        .iter()
        .map(|command| command.command_type())
        .collect()
}

/// A worker over `history` that records every workflow task completion it reports.
fn recording_worker(
    history: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    expected_completions: usize,
) -> (Worker, Arc<Mutex<Vec<WorkflowTaskCompletion>>>) {
    let reported = Arc::new(Mutex::new(vec![]));
    let mut cfg =
        MockPollCfg::from_resp_batches("fake_wf_id", history, responses, mock_worker_client());
    cfg.num_expected_fails = 0;
    cfg.num_expected_completions = Some(expected_completions.into());
    let recorder = reported.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        recorder.lock().unwrap().push(completion.clone());
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    (mock_worker(mock), reported)
}

/// What a cut produces on the server: the first task completes with the timer and asks for a new
/// task while the local activity runs, and the next task records the marker.
fn cut_history_up_to_second_task() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(WFT_TIMEOUT);
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The history the server writes for `completions`, one workflow task each.
fn history_of(completions: &[WorkflowTaskCompletion]) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(WFT_TIMEOUT);
    for completion in completions {
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_completed();
        let completed_id = t.current_event_id();
        if let Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(attrs)) =
            t.last_event().and_then(|event| event.attributes.as_mut())
        {
            attrs.sdk_metadata = Some(completion.sdk_metadata.clone());
        }
        for command in &completion.commands {
            match command.attributes.clone() {
                Some(Attributes::StartTimerCommandAttributes(timer)) => {
                    t.add_timer_started(timer.timer_id);
                }
                Some(Attributes::RecordMarkerCommandAttributes(marker)) => {
                    t.add(MarkerRecordedEventAttributes {
                        marker_name: marker.marker_name,
                        details: marker.details,
                        workflow_task_completed_event_id: completed_id,
                        header: marker.header,
                        failure: marker.failure,
                    });
                }
                Some(Attributes::CompleteWorkflowExecutionCommandAttributes(_)) => {
                    t.add_workflow_execution_completed();
                }
                other => panic!("no history event for {other:?}"),
            }
        }
    }
    t
}

/// Runs the cut live and answers the two completions it reported.
async fn run_cut_live() -> Vec<WorkflowTaskCompletion> {
    let (core, reported) = recording_worker(
        cut_history_up_to_second_task(),
        [ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)],
        2,
    );
    let activation = core.poll_workflow_activation().await.unwrap();
    assert!(is_init(&activation), "{activation:?}");
    core.complete_workflow_activation(completion(&activation.run_id, first_commands(), true))
        .await
        .unwrap();
    let task = core.poll_activity_task().await.unwrap();
    let Some(activity_task::Variant::Start(start)) = &task.variant else {
        panic!("expected the local activity, got {task:?}");
    };
    assert!(start.is_local);
    {
        let reported = reported.lock().unwrap();
        let [cut] = reported.as_slice() else {
            panic!("the cut reports one completion while the local activity runs: {reported:?}");
        };
        assert!(cut.force_create_new_workflow_task);
        assert_eq!(command_types(cut), [CommandType::StartTimer]);
    }
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::ok(b"done".to_vec().into())),
    })
    .await
    .unwrap();
    let activation = core.poll_workflow_activation().await.unwrap();
    assert!(resolves_la_1(&activation), "{activation:?}");
    core.complete_execution(&activation.run_id).await;
    drain_pollers_and_shutdown(&core).await;
    let reported = reported.lock().unwrap().clone();
    let [_, last] = reported.as_slice() else {
        panic!("expected two completions, got {reported:?}");
    };
    assert!(!last.force_create_new_workflow_task);
    assert_eq!(
        command_types(last),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    reported
}

#[tokio::test]
async fn a_heartbeat_task_completes_the_task_while_the_local_activity_runs() {
    run_cut_live().await;
}

#[tokio::test]
async fn a_heartbeat_task_without_local_activities_completes_the_task_as_usual() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(WFT_TIMEOUT);
    t.add_workflow_task_scheduled_and_started();
    let (core, reported) = recording_worker(t, [ResponseType::ToTaskNum(1)], 1);
    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(completion(
        &activation.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(3600))],
        true,
    ))
    .await
    .unwrap();
    drain_pollers_and_shutdown(&core).await;
    let reported = reported.lock().unwrap();
    let [only] = reported.as_slice() else {
        panic!("expected one completion, got {reported:?}");
    };
    assert!(
        !only.force_create_new_workflow_task,
        "with nothing running, there is nothing to heartbeat for"
    );
    assert_eq!(command_types(only), [CommandType::StartTimer]);
}

/// Replays `history` to its end with lang deciding the cut by `heartbeat_task`, and checks that
/// lang sees the activations of the live run and that nothing goes to the server.
async fn replay(history: TestHistoryBuilder, heartbeat_task: bool) {
    let (core, reported) = recording_worker(history, [ResponseType::AllHistory], 0);
    let activation = core.poll_workflow_activation().await.unwrap();
    assert!(is_init(&activation), "{activation:?}");
    core.complete_workflow_activation(completion(
        &activation.run_id,
        first_commands(),
        heartbeat_task,
    ))
    .await
    .unwrap();
    let activation = tokio::time::timeout(Duration::from_secs(10), core.poll_workflow_activation())
        .await
        .expect("replay resolves the local activity from its marker")
        .unwrap();
    assert!(resolves_la_1(&activation), "{activation:?}");
    core.complete_execution(&activation.run_id).await;
    drain_pollers_and_shutdown(&core).await;
    assert!(reported.lock().unwrap().is_empty());
}

#[rstest::rstest]
#[case::with_the_cut(true)]
#[case::without_the_cut(false)]
#[tokio::test]
async fn a_cut_history_replays_whatever_lang_decides(#[case] heartbeat_task: bool) {
    let live = run_cut_live().await;
    let history = history_of(&live);
    let tasks = history
        .get_full_history_info()
        .unwrap()
        .events()
        .iter()
        .filter(|event| event.event_type() == EventType::WorkflowTaskCompleted)
        .count();
    assert_eq!(tasks, 2, "the cut adds a workflow task");
    replay(history, heartbeat_task).await;
}

#[tokio::test]
async fn an_uncut_history_replays_when_lang_asks_for_a_cut() {
    // The local activity resolved within the first task, so its marker sits in that task. A
    // replay whose clock says to cut must not ask for the task that the live run never made.
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(WFT_TIMEOUT);
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_local_activity_result_marker(1, "1", b"done".to_vec().into());
    t.add_workflow_execution_completed();
    replay(t, true).await;
}

#[tokio::test]
async fn a_cut_task_runs_its_local_activity_after_a_cache_miss() {
    // A new Worker gets the second task with the whole history. It replays the first task, where
    // the cut is ignored, and then runs the local activity that the cut left without a marker.
    let (core, reported) = recording_worker(
        cut_history_up_to_second_task(),
        [ResponseType::AllHistory],
        1,
    );
    let activation = core.poll_workflow_activation().await.unwrap();
    assert!(is_init(&activation), "{activation:?}");
    core.complete_workflow_activation(completion(&activation.run_id, first_commands(), true))
        .await
        .unwrap();
    let task = core.poll_activity_task().await.unwrap();
    assert!(matches!(
        &task.variant,
        Some(activity_task::Variant::Start(start)) if start.is_local
    ));
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::ok(b"done".to_vec().into())),
    })
    .await
    .unwrap();
    let activation = core.poll_workflow_activation().await.unwrap();
    assert!(resolves_la_1(&activation), "{activation:?}");
    core.complete_workflow_activation(completion(
        &activation.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
        true,
    ))
    .await
    .unwrap();
    drain_pollers_and_shutdown(&core).await;
    let reported = reported.lock().unwrap();
    let [only] = reported.as_slice() else {
        panic!("expected one completion, got {reported:?}");
    };
    assert!(!only.force_create_new_workflow_task);
    assert_eq!(
        command_types(only),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
}

#[tokio::test]
async fn a_cut_history_has_the_events_the_server_writes() {
    let live = run_cut_live().await;
    let events: Vec<EventType> = history_of(&live)
        .get_full_history_info()
        .unwrap()
        .events()
        .iter()
        .map(|event| event.event_type())
        .collect();
    assert_eq!(
        events,
        [
            EventType::WorkflowExecutionStarted,
            EventType::WorkflowTaskScheduled,
            EventType::WorkflowTaskStarted,
            EventType::WorkflowTaskCompleted,
            EventType::TimerStarted,
            EventType::WorkflowTaskScheduled,
            EventType::WorkflowTaskStarted,
            EventType::WorkflowTaskCompleted,
            EventType::MarkerRecorded,
            EventType::WorkflowExecutionCompleted,
        ]
    );
}
