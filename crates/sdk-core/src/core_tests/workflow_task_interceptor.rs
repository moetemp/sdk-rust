//! The hooks a [WorkflowTaskInterceptor] gets around each workflow task completion.

use crate::{
    WorkerExtension, WorkflowTaskCompletionInfo, WorkflowTaskEpoch, WorkflowTaskFailedInfo,
    WorkflowTaskInterceptError, WorkflowTaskInterceptor, init_replay_worker,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, WorkerExt, WorkerTestHelpers,
        build_mock_pollers, hist_to_poll_resp, mock_worker, schedule_local_activity_cmd,
        start_timer_cmd, test_worker_cfg,
    },
    worker::client::mocks::mock_worker_client,
};
use futures_util::stream;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        activity_task::activity_task,
        workflow_activation::{WorkflowActivation, workflow_activation_job},
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

/// What the hooks and the mock server saw, in order.
#[derive(Default)]
struct Recorder {
    log: Mutex<Vec<String>>,
    infos: Mutex<Vec<WorkflowTaskCompletionInfo>>,
    /// `before_complete` fails for the tasks with these started event ids.
    fail_before: Vec<i64>,
    /// `before_complete` waits this long first.
    before_delay: Option<Duration>,
    /// `after_complete` never returns for the tasks with these started event ids.
    hang_after: Vec<i64>,
}

impl Recorder {
    fn push(&self, entry: impl Into<String>) {
        self.log.lock().unwrap().push(entry.into());
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    fn infos(&self) -> Vec<WorkflowTaskCompletionInfo> {
        self.infos.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl WorkflowTaskInterceptor for Recorder {
    async fn before_complete(
        &self,
        task: &WorkflowTaskCompletionInfo,
    ) -> Result<(), WorkflowTaskInterceptError> {
        self.push(format!("before {}", task.started_event_id));
        self.infos.lock().unwrap().push(task.clone());
        if let Some(delay) = self.before_delay {
            tokio::time::sleep(delay).await;
        }
        if self.fail_before.contains(&task.started_event_id) {
            return Err(WorkflowTaskInterceptError::new("store down"));
        }
        Ok(())
    }

    async fn after_complete(&self, task: &WorkflowTaskCompletionInfo, accepted: bool) {
        self.push(format!("after {} {accepted}", task.started_event_id));
        if self.hang_after.contains(&task.started_event_id) {
            futures_util::future::pending::<()>().await;
        }
    }

    fn on_task_failed(&self, task: &WorkflowTaskFailedInfo) {
        self.push(format!("failed {} {}", task.attempt, task.reported));
    }

    fn on_evicted(&self, _run_id: &str) {
        self.push("evicted");
    }
}

struct Extension(Arc<Recorder>);

impl WorkerExtension for Extension {
    fn name(&self) -> &str {
        "recorder"
    }

    fn workflow_task_interceptor(&self) -> Option<Arc<dyn WorkflowTaskInterceptor>> {
        Some(self.0.clone())
    }
}

/// Builds a worker with the recorder as its interceptor. The mock server logs each completion it
/// gets, and answers with `answer`.
fn worker_with(
    mut cfg: MockPollCfg,
    recorder: &Arc<Recorder>,
    mut answer: impl FnMut(usize) -> Result<RespondWorkflowTaskCompletedResponse, tonic::Status>
    + Send
    + 'static,
) -> crate::Worker {
    // An empty poll stream would start shutdown before the run is evicted.
    cfg.make_poll_stream_interminable = true;
    let log = recorder.clone();
    let mut completions = 0;
    cfg.completion_mock_fn = Some(Box::new(move |c| {
        completions += 1;
        log.push(format!(
            "sent commands={} force_new={}",
            c.commands.len(),
            c.force_create_new_workflow_task
        ));
        answer(completions)
    }));
    let mut mock = build_mock_pollers(cfg);
    let extension: Arc<dyn WorkerExtension> = Arc::new(Extension(recorder.clone()));
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.extension = Some(extension);
    });
    mock_worker(mock)
}

fn started_ids(epoch: &Option<WorkflowTaskEpoch>) -> Option<i64> {
    epoch.as_ref().map(|e| e.started_event_id)
}

/// Sets the attempt of the last WorkflowTaskScheduled event.
fn set_scheduled_attempt(t: &mut TestHistoryBuilder, attempt: i32) {
    let scheduled = t.current_event_id() - 1;
    t.modify_event(scheduled, |e| {
        if let Some(history_event::Attributes::WorkflowTaskScheduledEventAttributes(a)) =
            e.attributes.as_mut()
        {
            a.attempt = attempt;
        }
    });
}

/// Replays the task that starts the timer, then completes the run in the live task. Returns the
/// info of the live task.
async fn live_task_after_timer(
    history: TestHistoryBuilder,
    responses: Vec<ResponseType>,
) -> WorkflowTaskCompletionInfo {
    let recorder = Arc::new(Recorder::default());
    let worker = worker_with(
        MockPollCfg::from_resp_batches("fakeid", history, responses, mock_worker_client()),
        &recorder,
        accept,
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    complete_and_evict(&worker, &fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    let mut infos = recorder.infos();
    assert_eq!(infos.len(), 1);
    infos.remove(0)
}

fn accept(_: usize) -> Result<RespondWorkflowTaskCompletedResponse, tonic::Status> {
    Ok(Default::default())
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

/// Completes the run, then answers the eviction that follows.
async fn complete_and_evict(worker: &crate::Worker, run_id: &str) {
    worker.complete_execution(run_id).await;
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        eviction.jobs[0].variant,
        Some(workflow_activation_job::Variant::RemoveFromCache(_))
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
}

fn timer_then_complete() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(10));
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

#[tokio::test]
async fn hooks_run_before_and_after_each_completion() {
    let recorder = Arc::new(Recorder::default());
    let worker = worker_with(
        MockPollCfg::from_resp_batches(
            "fakeid",
            timer_then_complete(),
            [1, 2],
            mock_worker_client(),
        ),
        &recorder,
        accept,
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    let received = Instant::now();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let second = worker.poll_workflow_activation().await.unwrap();
    complete_and_evict(&worker, &second.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        recorder.log(),
        [
            "before 3",
            "sent commands=1 force_new=false",
            "after 3 true",
            "before 8",
            "sent commands=1 force_new=false",
            "after 8 true",
            "evicted",
        ]
    );
    let infos = recorder.infos();
    assert_eq!(infos[0].run_id, first.run_id);
    assert_eq!(infos[0].workflow_id, "fakeid");
    assert_eq!(
        infos[0].namespace,
        test_worker_cfg().build().unwrap().namespace
    );
    assert_eq!(infos[0].attempt, 1);
    assert!(infos[0].started_time.is_some());
    assert_eq!(infos[0].closes_run, None);
    assert_eq!(infos[1].closes_run, Some(crate::RunCloseKind::Completed));
    // The hook gets nine of the ten seconds of the task timeout, from the task's receipt.
    let left = infos[0].deadline - received;
    assert!(left <= Duration::from_secs(9), "{left:?}");
    assert!(left > Duration::from_secs(8), "{left:?}");
    assert!(infos[1].deadline > infos[0].deadline);
}

#[tokio::test]
async fn an_error_fails_the_task_and_evicts_the_run() {
    let recorder = Arc::new(Recorder {
        fail_before: vec![3],
        ..Default::default()
    });
    let mut cfg =
        MockPollCfg::from_resp_batches("fakeid", timer_then_complete(), [1], mock_worker_client());
    cfg.num_expected_fails = 1;
    cfg.num_expected_completions = Some(0.into());
    let worker = worker_with(cfg, &recorder, accept);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        eviction.jobs[0].variant,
        Some(workflow_activation_job::Variant::RemoveFromCache(_))
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(recorder.log(), ["before 3", "failed 1 true", "evicted"]);
}

#[tokio::test]
async fn a_hook_that_misses_the_deadline_fails_the_task() {
    let recorder = Arc::new(Recorder {
        before_delay: Some(Duration::from_secs(2)),
        ..Default::default()
    });
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(1));
    t.add_workflow_task_scheduled_and_started();
    let mut cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    cfg.num_expected_fails = 1;
    cfg.num_expected_completions = Some(0.into());
    let worker = worker_with(cfg, &recorder, accept);

    let first = worker.poll_workflow_activation().await.unwrap();
    let started = Instant::now();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    assert!(started.elapsed() < Duration::from_secs(2));
    let eviction = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(recorder.log(), ["before 3", "failed 1 true", "evicted"]);
}

#[tokio::test]
async fn a_refused_completion_is_not_accepted() {
    let recorder = Arc::new(Recorder::default());
    let mut cfg =
        MockPollCfg::from_resp_batches("fakeid", timer_then_complete(), [1], mock_worker_client());
    cfg.num_expected_completions = Some(1.into());
    let worker = worker_with(cfg, &recorder, |_| {
        Err(tonic::Status::invalid_argument("UnhandledCommand"))
    });

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let eviction = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        recorder.log(),
        [
            "before 3",
            "sent commands=1 force_new=false",
            "after 3 false",
            "evicted"
        ]
    );
}

#[tokio::test]
async fn a_dropped_speculative_task_is_not_accepted() {
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
    let cfg = MockPollCfg::from_resp_batches(
        wfid,
        real_hist,
        [
            ResponseType::ToTaskNum(1),
            speculative_task.into(),
            ResponseType::OneTask(2),
        ],
        mock_worker_client(),
    );
    let recorder = Arc::new(Recorder::default());
    let worker = worker_with(cfg, &recorder, |n| {
        let mut r = RespondWorkflowTaskCompletedResponse::default();
        if n == 2 {
            // The rejection drops the speculative task from History.
            r.reset_history_event_id = 3;
        }
        Ok(r)
    });

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let update = worker.poll_workflow_activation().await.unwrap();
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
    complete_and_evict(&worker, &fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        recorder.log(),
        [
            "before 3",
            "sent commands=1 force_new=false",
            "after 3 true",
            "before 7",
            "sent commands=0 force_new=false",
            "after 7 false",
            "before 8",
            "sent commands=1 force_new=false",
            "after 8 true",
            "evicted",
        ]
    );
    // History never keeps the dropped task, so the next task follows the first one.
    let infos = recorder.infos();
    assert_eq!(started_ids(&infos[2].previous_completed), Some(3));
    assert!(infos[2].failed_tasks.is_empty());
}

#[tokio::test]
async fn the_local_activity_heartbeat_completion_runs_the_hooks() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(1));
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();
    let cfg = MockPollCfg::from_resp_batches("fakeid", t, [1, 2], mock_worker_client());
    let recorder = Arc::new(Recorder::default());
    let worker = worker_with(cfg, &recorder, accept);

    let first = worker.poll_workflow_activation().await.unwrap();
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
    assert_matches!(&la.variant, Some(activity_task::Variant::Start(s)) if s.is_local);
    // Core heartbeats the task at 80% of its one-second timeout, with no lang activation.
    let resolved = {
        let wf_poll = worker.poll_workflow_activation();
        tokio::pin!(wf_poll);
        tokio::select! {
            act = &mut wf_poll => panic!("Unexpected activation {act:?}"),
            _ = tokio::time::sleep(Duration::from_millis(1500)) => {}
        }
        assert_eq!(
            recorder.log(),
            ["before 3", "sent commands=0 force_new=true", "after 3 true"]
        );
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: la.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        wf_poll.await.unwrap()
    };
    complete_and_evict(&worker, &resolved.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        recorder.log(),
        [
            "before 3",
            "sent commands=0 force_new=true",
            "after 3 true",
            "before 6",
            "sent commands=2 force_new=false",
            "after 6 true",
            "evicted",
        ]
    );
    // The heartbeat completed the first task, so the next one follows it.
    let infos = recorder.infos();
    assert_eq!(infos[0].previous_completed, None);
    assert_eq!(started_ids(&infos[1].previous_completed), Some(3));
}

#[tokio::test]
async fn an_eviction_with_output_runs_the_hooks_before_on_evicted() {
    let mut t = TestHistoryBuilder::default();
    // Keep the LA heartbeat from firing mid-test.
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(60));
    t.add_workflow_task_scheduled_and_started();
    let cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    let recorder = Arc::new(Recorder::default());
    let worker = worker_with(cfg, &recorder, accept);

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    let la_cmd = |seq: u32| {
        schedule_local_activity_cmd(
            seq,
            &seq.to_string(),
            ActivityCancellationType::WaitCancellationCompleted,
            Duration::from_secs(60),
        )
    };
    complete(&worker, &first, vec![la_cmd(1), la_cmd(2)]).await;
    let mut tokens = vec![];
    for _ in 0..2 {
        let task = worker.poll_activity_task().await.unwrap();
        let Some(activity_task::Variant::Start(start)) = &task.variant else {
            panic!("Expected LA start, got {task:?}");
        };
        tokens.push((start.activity_id.clone(), task.task_token));
    }
    tokens.sort();
    let (_, first_token) = tokens.remove(0);
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: first_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    // The marker of the first LA is output that waits on the second one.
    let resolved = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &resolved, vec![]).await;
    assert!(recorder.log().is_empty());

    worker.request_workflow_eviction(&run_id);
    let cancel = worker.poll_activity_task().await.unwrap();
    assert_matches!(cancel.variant, Some(activity_task::Variant::Cancel(_)));
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: cancel.task_token,
            result: Some(ActivityExecutionResult::cancel_from_details(None)),
        })
        .await
        .unwrap();
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        eviction.jobs[0].variant,
        Some(workflow_activation_job::Variant::RemoveFromCache(_))
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        recorder.log(),
        [
            "before 3",
            "sent commands=1 force_new=true",
            "after 3 true",
            "evicted"
        ]
    );
}

#[tokio::test]
async fn the_replay_worker_never_calls_the_hooks() {
    let recorder = Arc::new(Recorder::default());
    let extension: Arc<dyn WorkerExtension> = Arc::new(Extension(recorder.clone()));
    let mut cfg = test_worker_cfg().build().unwrap();
    cfg.extension = Some(extension);
    let worker = init_replay_worker(ReplayWorkerInput::new(
        cfg,
        stream::iter([HistoryForReplay::from(canned_histories::single_timer("1"))]),
    ))
    .unwrap();

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &fired,
        vec![CompleteWorkflowExecution { result: None }.into()],
    )
    .await;
    while let Ok(activation) = worker.poll_workflow_activation().await {
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await
            .unwrap();
    }
    worker.shutdown().await;

    assert!(recorder.log().is_empty());
}

#[tokio::test]
async fn a_failed_attempt_is_a_failed_task() {
    let mut t = timer_then_complete();
    // Task 3 completed on its third attempt, which the server wrote when it completed.
    t.modify_event(2, |e| {
        if let Some(history_event::Attributes::WorkflowTaskScheduledEventAttributes(a)) =
            e.attributes.as_mut()
        {
            a.attempt = 3;
        }
    });
    // Task 8 fails, and the server writes its retry as attempt 2.
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();
    set_scheduled_attempt(&mut t, 2);
    let info = live_task_after_timer(t, vec![ResponseType::AllHistory]).await;

    assert_eq!(info.started_event_id, 11);
    let previous = info.previous_completed.unwrap();
    assert_eq!(previous.started_event_id, 3);
    assert_eq!(previous.attempt, 3);
    assert!(previous.started_time.is_some());
    assert_eq!(info.failed_tasks, [8]);
}

#[tokio::test]
async fn timed_out_attempts_are_failed_tasks_unless_no_worker_started_them() {
    let mut t = timer_then_complete();
    t.add_workflow_task_timed_out();
    // A sticky retry that no worker picks up times out before it starts.
    t.add_workflow_task_scheduled();
    let scheduled = t.current_event_id();
    t.add(WorkflowTaskTimedOutEventAttributes {
        scheduled_event_id: scheduled,
        timeout_type: TimeoutType::ScheduleToStart as i32,
        ..Default::default()
    });
    t.add_workflow_task_scheduled_and_started();
    let info = live_task_after_timer(t, vec![ResponseType::AllHistory]).await;

    assert_eq!(info.started_event_id, 13);
    assert_eq!(info.failed_tasks, [8]);
}

#[tokio::test]
async fn a_refused_completion_shows_up_as_a_failed_task_of_the_retry() {
    let mut t = timer_then_complete();
    let refused = hist_to_poll_resp(&t, "fakeid", ResponseType::AllHistory).resp;
    // The server refuses task 8 with UNHANDLED_COMMAND and writes the failure.
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();
    let recorder = Arc::new(Recorder::default());
    let worker = worker_with(
        MockPollCfg::from_resp_batches(
            "fakeid",
            t.clone(),
            [ResponseType::Raw(refused), ResponseType::AllHistory],
            mock_worker_client(),
        ),
        &recorder,
        |n| {
            if n == 1 {
                Err(tonic::Status::invalid_argument("UnhandledCommand"))
            } else {
                Ok(Default::default())
            }
        },
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&fired.run_id).await;
    let eviction = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    // The retry replays the run from History.
    let replay = worker.poll_workflow_activation().await.unwrap();
    assert!(replay.is_replaying);
    complete(
        &worker,
        &replay,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    complete_and_evict(&worker, &fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        recorder.log(),
        [
            "before 8",
            "sent commands=1 force_new=false",
            "after 8 false",
            "evicted",
            "before 11",
            "sent commands=1 force_new=false",
            "after 11 true",
            "evicted",
        ]
    );
    let infos = recorder.infos();
    assert!(infos[0].failed_tasks.is_empty());
    assert_eq!(started_ids(&infos[1].previous_completed), Some(3));
    assert_eq!(infos[1].failed_tasks, [8]);
}

#[tokio::test]
async fn a_reset_shows_the_reset_task_as_failed() {
    let mut t = timer_then_complete();
    // A reset to task 8 fails it in the new run and schedules a new task.
    t.add_workflow_task_failed_new_id(WorkflowTaskFailedCause::ResetWorkflow, "new-run");
    t.add_workflow_task_scheduled_and_started();
    let info = live_task_after_timer(t, vec![ResponseType::AllHistory]).await;

    assert_eq!(info.started_event_id, 11);
    assert_eq!(started_ids(&info.previous_completed), Some(3));
    assert_eq!(info.failed_tasks, [8]);
}

#[tokio::test]
async fn a_hung_after_complete_does_not_stall_the_run() {
    let recorder = Arc::new(Recorder {
        hang_after: vec![3],
        ..Default::default()
    });
    let worker = worker_with(
        MockPollCfg::from_resp_batches(
            "fakeid",
            timer_then_complete(),
            [1, 2],
            mock_worker_client(),
        ),
        &recorder,
        accept,
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let second = worker.poll_workflow_activation().await.unwrap();
    complete_and_evict(&worker, &second.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        recorder.log(),
        [
            "before 3",
            "sent commands=1 force_new=false",
            "after 3 true",
            "before 8",
            "sent commands=1 force_new=false",
            "after 8 true",
            "evicted",
        ]
    );
}

#[tokio::test]
async fn a_failed_task_before_an_earlier_completion_stays_listed() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(10));
    // Task 3 fails, and task 6 completes without touching what task 3 wrote.
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let info = live_task_after_timer(t, vec![ResponseType::AllHistory]).await;

    assert_eq!(info.started_event_id, 11);
    assert_eq!(started_ids(&info.previous_completed), Some(6));
    assert_eq!(info.failed_tasks, [3]);
}

#[tokio::test]
async fn a_sticky_task_after_a_failure_lists_it() {
    let mut t = timer_then_complete();
    // Task 8 times out while the run stays cached, so the next task is a partial history.
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_and_started();
    let recorder = Arc::new(Recorder::default());
    let worker = worker_with(
        MockPollCfg::from_resp_batches(
            "fakeid",
            t,
            [ResponseType::ToTaskNum(1), ResponseType::OneTask(2)],
            mock_worker_client(),
        ),
        &recorder,
        accept,
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    complete_and_evict(&worker, &fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let infos = recorder.infos();
    assert_eq!(infos.len(), 2);
    let last = &infos[1];
    assert_eq!(last.started_event_id, 11);
    assert_eq!(started_ids(&last.previous_completed), Some(3));
    assert_eq!(last.failed_tasks, [8]);
}
