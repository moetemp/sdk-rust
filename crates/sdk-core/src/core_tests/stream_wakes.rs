//! Stream wakes: the server folds them onto a Workflow Task's scheduled event, and Core hands them
//! to lang as one job per subscription per task, live and on replay alike.

use crate::{
    replay::TestHistoryBuilder,
    test_help::{MockPollCfg, ResponseType, build_mock_pollers, mock_worker},
    worker::client::mocks::mock_worker_client,
};
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_activation::{ResolveStreamWake, WorkflowActivation, workflow_activation_job},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::Payload,
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
            stream::v1::StreamWake,
        },
    },
    worker::WorkerTaskTypes,
};

fn wake(subscription_id: &str, counter: i64) -> StreamWake {
    StreamWake {
        subscription_id: subscription_id.to_string(),
        position: format!("position-{counter}"),
        counter,
        metadata: [("records".to_string(), counter.to_string())].into(),
        ..Default::default()
    }
}

/// The job lang should get for `wake`.
fn job_for(wake: &StreamWake) -> workflow_activation_job::Variant {
    workflow_activation_job::Variant::ResolveStreamWake(ResolveStreamWake {
        subscription_id: wake.subscription_id.clone(),
        position: wake.position.clone(),
        counter: wake.counter,
        metadata: wake.metadata.clone(),
        recheck: wake.recheck,
        closed: wake.closed,
        close_result: wake.close_result.clone(),
        close_failure: wake.close_failure.clone(),
        resubscribe: wake.resubscribe,
    })
}

fn wake_jobs(activation: &WorkflowActivation) -> Vec<workflow_activation_job::Variant> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            v @ Some(workflow_activation_job::Variant::ResolveStreamWake(_)) => v.clone(),
            _ => None,
        })
        .collect()
}

fn job_names(activation: &WorkflowActivation) -> Vec<String> {
    activation
        .jobs
        .iter()
        .map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::ResolveStreamWake(w)) => {
                format!("Wake({}, {})", w.subscription_id, w.counter)
            }
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

/// A first task with nothing for lang to do, then two tasks scheduled only because wakes arrived.
/// The first and the second complete with no commands, which is the shape of a heartbeat chain, so
/// replay must still give each wake task an activation of its own.
fn wakes_after_a_quiet_task() -> (TestHistoryBuilder, [StreamWake; 2]) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let first = wake("1", 41);
    t.add_workflow_task_scheduled_with_stream_wakes(vec![first.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    let second = wake("1", 42);
    t.add_workflow_task_scheduled_with_stream_wakes(vec![second.clone()]);
    t.add_workflow_task_started();
    (t, [first, second])
}

async fn run_wakes_after_a_quiet_task(batches: Vec<ResponseType>, replaying: bool) {
    let (history, [first, second]) = wakes_after_a_quiet_task();
    let worker = worker(history, batches);

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    assert!(wake_jobs(&init).is_empty(), "{:?}", job_names(&init));
    complete_empty(&worker, &run_id).await;

    let first_wake = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        wake_jobs(&first_wake),
        vec![job_for(&first)],
        "the wake-only task gets an activation of its own: {:?}",
        job_names(&first_wake)
    );
    assert_eq!(first_wake.is_replaying, replaying);
    complete_empty(&worker, &run_id).await;

    let second_wake = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(wake_jobs(&second_wake), vec![job_for(&second)]);
    assert!(!second_wake.is_replaying);
}

#[tokio::test]
async fn each_wake_task_gets_its_own_activation_live() {
    run_wakes_after_a_quiet_task(
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
async fn each_wake_task_gets_its_own_activation_on_replay() {
    run_wakes_after_a_quiet_task(vec![ResponseType::AllHistory], true).await;
}

#[tokio::test]
async fn a_failed_task_and_its_retry_fold_their_wakes_per_subscription() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_stream_wakes(vec![wake("a", 5), wake("b", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure::default(),
    );
    let closed = StreamWake {
        closed: true,
        close_result: Some(Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        }),
        ..wake("a", 7)
    };
    t.add_workflow_task_scheduled_with_stream_wakes(vec![closed.clone()]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![ResponseType::AllHistory]);

    let init = worker.poll_workflow_activation().await.unwrap();
    complete_empty(&worker, &init.run_id).await;
    let woken = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        wake_jobs(&woken),
        vec![job_for(&closed), job_for(&wake("b", 1))],
        "{:?}",
        job_names(&woken)
    );
}

#[tokio::test]
async fn a_wake_and_a_signal_of_one_task_share_its_activation() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_with_stream_wakes(vec![StreamWake {
        recheck: true,
        ..wake("1", 0)
    }]);
    t.add_workflow_task_started();
    let worker = worker(
        t,
        vec![ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)],
    );

    let init = worker.poll_workflow_activation().await.unwrap();
    complete_empty(&worker, &init.run_id).await;
    let woken = worker.poll_workflow_activation().await.unwrap();
    let mut names = job_names(&woken);
    names.sort();
    assert_eq!(names, ["Signal(go)", "Wake(1, 0)"]);
}
