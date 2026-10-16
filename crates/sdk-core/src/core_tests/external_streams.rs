//! A Workflow's stream output: the records an activation commits, which the Worker appends to its
//! stream store before it reports the completion, under an attempt that History alone gives.

use crate::{
    TaskToken,
    replay::{TestHistoryBuilder, canned_histories, default_act_sched, default_wes_attribs},
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers,
        hist_to_poll_resp, mock_worker, schedule_local_activity_cmd, start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use mockall::TimesRange;
use parking_lot::Mutex;
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::{
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            workflow_activation::{WorkflowActivation, workflow_activation_job},
            workflow_commands::{
                ActivityCancellationType, CompleteWorkflowExecution, OutputRecord,
                RequestCancelActivity, ScheduleActivity, WorkflowOutputStreamCommit,
                workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::{
            api::{
                common::v1::Payload,
                enums::v1::{EventType, TimeoutType, WorkflowTaskFailedCause},
                failure::v1::Failure,
                history::v1::{
                    WorkflowExecutionStartedEventAttributes, WorkflowTaskTimedOutEventAttributes,
                    history_event,
                },
                workflowservice::v1::RespondWorkflowTaskCompletedResponse,
            },
            sdk::streams::v1::{StreamRecord, StreamRecordKind},
        },
    },
    streams::{CONTENT_HASH_KEY, content_hash_text},
    worker::WorkerTaskTypes,
};
use temporalio_streams::{
    BEGINNING, MemoryStore, ReadTarget, StreamError, StreamResult, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, ReadRequest, StoreAppendRequest,
        StoreAppendResponse, StoreLatestRequest, StoreLatestResponse, StoreReadRequest,
        StoreReadResponse, StoredClose, StreamOwnerKind, read_record,
    },
    read_page, stream_hash, workflow_batch_digest,
};
use tokio::sync::Notify;

/// A memory stream store that logs every call Core makes, and can be told to fail appends.
#[derive(Default)]
struct RecordingStore {
    inner: MemoryStore,
    appends: Mutex<Vec<StoreAppendRequest>>,
    calls: Mutex<Vec<&'static str>>,
    /// Each retired session as `(topic, producer, attempt)`.
    retired: Mutex<Vec<(String, String, i64)>>,
    /// Failures the next appends answer with, one each, before the store takes them.
    failures: Mutex<std::collections::VecDeque<StreamError>>,
    /// The failure every append answers with.
    refuse: Option<StreamError>,
    /// How long each append takes.
    append_delay: Option<Duration>,
    /// Each stream close as `(chain, topic, close)`.
    closes: Mutex<Vec<(ChainId, String, StoredClose)>>,
    /// Refuses every topic close.
    refuse_closes: bool,

    /// A retire waits for a permit here, when set.
    retire_gate: Option<Arc<tokio::sync::Semaphore>>,
}

impl RecordingStore {
    fn refusing(error: StreamError) -> Self {
        Self {
            refuse: Some(error),
            ..Default::default()
        }
    }

    fn failing_first(errors: impl IntoIterator<Item = StreamError>) -> Self {
        Self {
            failures: Mutex::new(errors.into_iter().collect()),
            ..Default::default()
        }
    }

    fn appends(&self) -> Vec<StoreAppendRequest> {
        self.appends.lock().clone()
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().clone()
    }

    fn retired(&self) -> Vec<(String, String, i64)> {
        self.retired.lock().clone()
    }

    /// Each append as `(topic, producer, attempt, sequence, record count)`.
    fn append_log(&self) -> Vec<(String, String, i64, i64, usize)> {
        self.appends()
            .into_iter()
            .map(|a| {
                (
                    a.topic,
                    a.producer_id,
                    a.attempt,
                    a.sequence,
                    a.records.len(),
                )
            })
            .collect()
    }

    /// What a reader of `topic` gets from the start: each body's text, or `SUPERSEDED`.
    async fn read(&self, topic: &str) -> Vec<String> {
        let chain = self.appends()[0].chain.clone().unwrap();
        let target = ReadTarget {
            stream_hash: stream_hash(&chain.namespace, "workflow", &chain.workflow_id, topic),
            chain,
            topic: topic.to_string(),
        };
        let request = ReadRequest {
            after: BEGINNING.to_string(),
            ..Default::default()
        };
        read_page(&self.inner, &target, &request)
            .await
            .unwrap()
            .records
            .into_iter()
            .map(|record| match record.record.unwrap() {
                read_record::Record::Stored(stored) => {
                    String::from_utf8(stored.body.unwrap().data).unwrap()
                }
                read_record::Record::Superseded(_) => "SUPERSEDED".to_string(),
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl StreamStore for RecordingStore {
    fn name(&self) -> &str {
        "recording"
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        self.calls.lock().push("append");
        if let Some(error) = &self.refuse {
            return Err(error.clone());
        }
        if let Some(error) = self.failures.lock().pop_front() {
            return Err(error);
        }
        if let Some(delay) = self.append_delay {
            tokio::time::sleep(delay).await;
        }
        let response = self.inner.append(request.clone()).await?;
        self.appends.lock().push(request);
        Ok(response)
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        self.calls.lock().push("read");
        self.inner.read(request).await
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        self.calls.lock().push("latest");
        self.inner.latest(request).await
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        self.inner.record_at(chain, topic, position).await
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.calls.lock().push("close_chain");
        self.inner.close_chain(chain).await
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.calls.lock().push("close_topic");
        self.inner.close_topic(chain, topic).await
    }

    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<StoredClose> {
        self.calls.lock().push("close_topic_with");
        if self.refuse_closes {
            return Err(StreamError::storage("the store is down"));
        }
        self.closes
            .lock()
            .push((chain.clone(), topic.to_string(), close.clone()));
        self.inner.close_topic_with(chain, topic, close).await
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        self.calls.lock().push("retire_producer");
        if let Some(gate) = &self.retire_gate {
            let _permit = gate.acquire().await.unwrap();
        }
        self.retired
            .lock()
            .push((topic.to_string(), producer_id.to_string(), attempt));
        self.inner
            .retire_producer(chain, topic, producer_id, attempt)
            .await
    }

    async fn written_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        self.calls.lock().push("written_topics");
        self.inner.written_topics(chain).await
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        self.calls.lock().push("delete_owner");
        self.inner.delete_owner(request).await
    }
}

const TOPIC: &str = "results";

/// The attempt of a task whose last completed task started at event `prefix`, after `retry - 1`
/// reruns.
fn attempt(prefix: i64, retry: i64) -> i64 {
    (prefix << 16) | retry
}

/// What a Workflow publishing `values` on one topic commits, bodies already through the codec.
fn records(values: &[&str]) -> Vec<OutputRecord> {
    records_on(TOPIC, values)
}

fn records_on(topic: &str, values: &[&str]) -> Vec<OutputRecord> {
    values
        .iter()
        .map(|value| OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: format!("encrypted {value}").into_bytes(),
                ..Default::default()
            }),
            content_hash: vec![value.as_bytes()[0]; 32],
        })
        .collect()
}

fn output_commit_command(records: Vec<OutputRecord>) -> workflow_command::Variant {
    workflow_command::Variant::WorkflowOutputStreamCommit(WorkflowOutputStreamCommit { records })
}

/// Start, a task that starts a timer, the timer firing, and the next task.
fn timer_history() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The predicate a mock applies to every Workflow Task failure it is asked to report.
type FailMatcher =
    Box<dyn Fn(&TaskToken, &WorkflowTaskFailedCause, &Option<Failure>) -> bool + Send>;

/// Accepts only a failure whose message holds each of `parts`, and signals `failed` when one comes.
fn failure_matcher(parts: &'static [&'static str], failed: Arc<Notify>) -> FailMatcher {
    Box::new(move |_, _, failure| {
        let matches = failure
            .as_ref()
            .is_some_and(|f| parts.iter().all(|part| f.message.contains(part)));
        if matches {
            failed.notify_one();
        }
        matches
    })
}

/// Waits a bounded time for the expected Workflow Task failure. Without it, a test whose failure
/// never comes waits forever in shutdown instead of failing.
async fn failed_within_deadline(failed: &Notify) {
    tokio::time::timeout(Duration::from_secs(10), failed.notified())
        .await
        .expect("the expected Workflow Task failure never came");
}

fn with_store(mock: &mut crate::test_help::MocksHolder, store: Option<Arc<RecordingStore>>) {
    mock.worker_cfg(|w| {
        w.stream_store = store.map(|s| s as Arc<dyn StreamStore>);
        w.task_types = WorkerTaskTypes {
            enable_local_activities: true,
            ..WorkerTaskTypes::workflow_only()
        };
        w.max_cached_workflows = 1;
    });
}

/// A worker whose one completion must fail its Workflow Task with a message holding `parts`.
fn worker_failing_with(
    store: Option<Arc<RecordingStore>>,
    parts: &'static [&'static str],
) -> (crate::Worker, Arc<Notify>) {
    let failed = Arc::new(Notify::new());
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(TimesRange::from(0));
    mock_cfg.expect_fail_wft_matcher = failure_matcher(parts, failed.clone());
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, store);
    (mock_worker(mock), failed)
}

/// A worker on `store` that records how many appends `store` held when each completion was sent.
fn worker_appending_to(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    store: Arc<RecordingStore>,
    appended_at_completion: Arc<Mutex<Vec<usize>>>,
) -> crate::Worker {
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    let seen_store = store.clone();
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        appended_at_completion
            .lock()
            .push(seen_store.appends().len());
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, Some(store));
    mock_worker(mock)
}

/// Completes the activation for `run_id` with `commands`.
async fn complete(worker: &crate::Worker, run_id: &str, commands: Vec<workflow_command::Variant>) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            commands,
        ))
        .await
        .unwrap();
}

/// Runs a task that commits `first` and starts a timer, then a task that commits `second` and
/// completes the Workflow when the timer fires. Returns the run id.
async fn commit_in_two_tasks(worker: crate::Worker, first: &[&str], second: &[&str]) -> String {
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    complete(
        &worker,
        &run_id,
        vec![
            output_commit_command(records(first)),
            start_timer_cmd(1, Duration::from_secs(10)),
        ],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    let mut commands = vec![];
    if !second.is_empty() {
        commands.push(output_commit_command(records(second)));
    }
    commands.push(CompleteWorkflowExecution::default().into());
    complete(&worker, &fired.run_id, commands).await;
    worker.drain_pollers_and_shutdown().await;
    run_id
}

#[tokio::test]
async fn a_commit_is_in_the_store_before_its_completion_is_sent() {
    let store = Arc::new(RecordingStore::default());
    let appended_at_completion: Arc<Mutex<Vec<usize>>> = Default::default();
    let worker = worker_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        store.clone(),
        appended_at_completion.clone(),
    );
    let run_id = commit_in_two_tasks(worker, &["ab", "c"], &[]).await;

    assert_eq!(
        appended_at_completion.lock()[0],
        1,
        "the commit is in the store before the completion goes out"
    );
    let [append] = store.appends().try_into().unwrap();
    assert_eq!(append.topic, TOPIC);
    assert_eq!(append.producer_id, format!("@{run_id}"));
    assert_eq!((append.attempt, append.sequence), (1, 1));
    let committed = records(&["ab", "c"]);
    assert_eq!(
        append.digest,
        workflow_batch_digest(
            TOPIC,
            committed
                .iter()
                .map(|r| (r.kind, r.content_hash.as_slice()))
        ),
        "the digest never depends on the codec, so a repeat of the batch deduplicates"
    );
    let chain = append.chain.unwrap();
    assert_eq!(
        (chain.workflow_id.as_str(), chain.first_run_id.as_str()),
        ("fakeid", run_id.as_str())
    );
    for ((stored, committed), sequence) in append.records.iter().zip(&committed).zip(1..) {
        let stored = StreamRecord::decode(stored.as_slice()).unwrap();
        assert_eq!(
            stored.body, committed.body,
            "the body stays as the codec left it"
        );
        assert_eq!(
            (stored.producer_id.as_str(), stored.attempt, stored.sequence),
            (format!("@{run_id}").as_str(), 1, sequence)
        );
        assert_eq!(
            stored.metadata[CONTENT_HASH_KEY].data,
            content_hash_text(&committed.content_hash).into_bytes()
        );
    }
    assert_eq!(store.read(TOPIC).await, ["encrypted ab", "encrypted c"]);
}

#[tokio::test]
async fn an_append_that_may_not_have_landed_is_sent_again() {
    let store = Arc::new(RecordingStore::failing_first([
        StreamError::outcome_unknown("the answer was lost"),
        StreamError::storage("redis is restarting"),
    ]));
    let worker = worker_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        store.clone(),
        Default::default(),
    );
    commit_in_two_tasks(worker, &["a"], &[]).await;
    assert_eq!(
        store.calls(),
        ["append", "append", "append", "retire_producer"]
    );
    assert_eq!(store.read(TOPIC).await, ["encrypted a"]);
}

#[tokio::test]
async fn a_store_that_refuses_the_append_fails_the_task_and_sends_no_completion() {
    let store = Arc::new(RecordingStore::refusing(StreamError::refused(
        "the ACL denies the key",
    )));
    let (worker, failed) =
        worker_failing_with(Some(store.clone()), &["Could not append", "ACL denies"]);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(records(&["a"])),
            start_timer_cmd(1, Duration::from_secs(10)),
        ],
    )
    .await;
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
    assert_eq!(store.calls(), ["append"], "a refusal is not sent again");
}

#[tokio::test]
async fn a_live_commit_on_a_worker_without_a_store_fails_the_task() {
    let (worker, failed) = worker_failing_with(None, &["has no stream store"]);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![output_commit_command(records(&["a"]))],
    )
    .await;
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn two_commits_in_one_completion_fail_the_task() {
    let store = Arc::new(RecordingStore::default());
    let (worker, failed) = worker_failing_with(
        Some(store.clone()),
        &["more than one WorkflowOutputStreamCommit"],
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(records(&["a"])),
            output_commit_command(records(&["b"])),
        ],
    )
    .await;
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
    assert!(store.calls().is_empty());
}

#[tokio::test]
async fn a_record_no_writer_stores_fails_the_task_with_its_reason() {
    let store = Arc::new(RecordingStore::default());
    let (worker, failed) =
        worker_failing_with(Some(store.clone()), &["record 0 is DATA without a body"]);
    let first = worker.poll_workflow_activation().await.unwrap();
    let mut unencoded = records(&["a"]);
    unencoded[0].body = None;
    complete(
        &worker,
        &first.run_id,
        vec![output_commit_command(unencoded)],
    )
    .await;
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
    assert!(store.calls().is_empty());
}

fn schedule_local_activity(seq: u32) -> workflow_command::Variant {
    schedule_local_activity_cmd(
        seq,
        &seq.to_string(),
        ActivityCancellationType::TryCancel,
        Duration::from_secs(10),
    )
}

/// Runs the local activity lang just scheduled and returns the activation that resolves it.
async fn run_local_activity(worker: &crate::Worker) -> WorkflowActivation {
    let task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(Payload {
                data: b"done".to_vec(),
                ..Default::default()
            })),
        })
        .await
        .unwrap();
    worker.poll_workflow_activation().await.unwrap()
}

fn only_resolves_local_activity(activation: &WorkflowActivation, seq: u32) -> bool {
    matches!(
        activation.jobs.as_slice(),
        [job] if matches!(
            &job.variant,
            Some(workflow_activation_job::Variant::ResolveActivity(r)) if r.seq == seq
        )
    )
}

#[tokio::test]
async fn each_commit_around_a_local_activity_is_visible_before_the_task_completes() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let appended_at_completion: Arc<Mutex<Vec<usize>>> = Default::default();
    let worker = worker_appending_to(
        history,
        vec![1.into()],
        store.clone(),
        appended_at_completion.clone(),
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    complete(
        &worker,
        &run_id,
        vec![
            output_commit_command(records(&["a"])),
            schedule_local_activity(1),
        ],
    )
    .await;
    // The task is still open while the local activity runs, and a reader already sees the commit.
    assert_eq!(store.read(TOPIC).await, ["encrypted a"]);
    let resolved = run_local_activity(&worker).await;
    assert!(only_resolves_local_activity(&resolved, 1));
    complete(
        &worker,
        &run_id,
        vec![
            output_commit_command(records(&["b"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(*appended_at_completion.lock(), [2]);
    let producer = format!("@{run_id}");
    assert_eq!(
        store.append_log(),
        [
            (TOPIC.to_string(), producer.clone(), 1, 1, 1),
            (TOPIC.to_string(), producer, 1, 2, 1),
        ],
        "one task's activations share its attempt, and the sequence goes on"
    );
}

#[tokio::test]
async fn replay_makes_no_store_call() {
    let store = Arc::new(RecordingStore::default());
    let worker = worker_appending_to(
        timer_history(),
        vec![2.into()],
        store.clone(),
        Default::default(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    // Lang sends no commit while it replays. Core drops one that comes anyway.
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(records(&["a"])),
            start_timer_cmd(1, Duration::from_secs(10)),
        ],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(store.calls().is_empty(), "{:?}", store.calls());
}

#[tokio::test]
async fn the_next_task_moves_the_prefix_and_starts_the_sequence_again() {
    let store = Arc::new(RecordingStore::default());
    let worker = worker_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        store.clone(),
        Default::default(),
    );
    let run_id = commit_in_two_tasks(worker, &["a", "b"], &["c"]).await;
    let producer = format!("@{run_id}");
    assert_eq!(
        store.append_log(),
        [
            (TOPIC.to_string(), producer.clone(), 1, 1, 2),
            // The first task started at event 3.
            (TOPIC.to_string(), producer.clone(), attempt(3, 1), 1, 1),
        ]
    );
    assert_eq!(
        store.retired(),
        [(TOPIC.to_string(), producer, 1)],
        "the second task's completion retires the first task's session, and keeps its own"
    );
    assert_eq!(
        store.read(TOPIC).await,
        ["encrypted a", "encrypted b", "encrypted c"],
        "a later task goes on with no supersession, and a heartbeat task is a task like any"
    );
}

#[tokio::test]
async fn a_completion_does_not_wait_for_the_retire_and_shutdown_does() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let store = Arc::new(RecordingStore {
        retire_gate: Some(gate.clone()),
        ..Default::default()
    });
    let worker = worker_appending_to(
        timer_history(),
        vec![1.into(), 2.into()],
        store.clone(),
        Default::default(),
    );
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    complete(
        &worker,
        &run_id,
        vec![
            output_commit_command(records(&["a"])),
            start_timer_cmd(1, Duration::from_secs(10)),
        ],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    let completed = complete(
        &worker,
        &fired.run_id,
        vec![
            output_commit_command(records(&["b"])),
            CompleteWorkflowExecution::default().into(),
        ],
    );
    tokio::time::timeout(Duration::from_secs(5), completed)
        .await
        .expect("the completion returns while the retire waits");
    assert!(store.retired().is_empty());
    let shutdown = worker.drain_pollers_and_shutdown();
    tokio::pin!(shutdown);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut shutdown)
            .await
            .is_err(),
        "shutdown waits for the retire"
    );
    gate.add_permits(10);
    shutdown.await;
    assert_eq!(store.retired().len(), 1);
}

/// Replays the first task of `history`, then commits `["c"]` live in the task after it, and
/// answers each append's attempt and sequence.
async fn commit_after_replay(history: TestHistoryBuilder) -> Vec<(i64, i64)> {
    let store = Arc::new(RecordingStore::default());
    let worker = worker_appending_to(history, vec![2.into()], store.clone(), Default::default());
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    complete(
        &worker,
        &fired.run_id,
        vec![
            output_commit_command(records(&["c"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;
    store
        .appends()
        .into_iter()
        .map(|a| (a.attempt, a.sequence))
        .collect()
}

/// A first task that starts a timer, the timer firing, whatever `between` adds, then the task
/// that commits, scheduled at `attempt`.
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

#[tokio::test]
async fn a_task_after_failed_attempts_publishes_under_a_higher_retry() {
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
    assert_eq!(
        commit_after_replay(history).await,
        [(attempt(3, 5), 1)],
        "1 + an unhandled command + a timeout + the 2 unwritten failures before attempt 4"
    );
}

#[tokio::test]
async fn a_schedule_to_start_timeout_keeps_the_retry() {
    // No Worker ran the task that timed out, so it published nothing.
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
    assert_eq!(commit_after_replay(history).await, [(attempt(3, 1), 1)]);
}

#[tokio::test]
async fn an_unhandled_command_rerun_supersedes_what_the_refused_task_appended() {
    // The first completion is refused because a signal came in. Core evicts, and the rerun
    // replays History, which shows the failure, and publishes under the next retry.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let first_task =
        TestHistoryBuilder::from_history(t.get_full_history_info().unwrap().events()[..3].to_vec());
    let first_task = hist_to_poll_resp(&first_task, "fakeid", 1.into()).resp;
    let store = Arc::new(RecordingStore::default());
    let mut answers = 0;
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [ResponseType::Raw(first_task), ResponseType::AllHistory],
        mock_worker_client(),
    );
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        answers += 1;
        if answers == 1 {
            Err(tonic::Status::invalid_argument("UnhandledCommand"))
        } else {
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, Some(store.clone()));
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(records(&["a", "b"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert!(eviction.eviction_reason().is_some());
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    let rerun = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &rerun.run_id,
        vec![
            output_commit_command(records(&["a", "b"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;

    let attempts: Vec<_> = store
        .appends()
        .iter()
        .map(|a| (a.attempt, a.sequence))
        .collect();
    assert_eq!(
        attempts,
        [(1, 1), (2, 1)],
        "the rerun starts its sequence at 1"
    );
    assert_eq!(
        store.read(TOPIC).await,
        [
            "encrypted a",
            "encrypted b",
            "SUPERSEDED",
            "encrypted a",
            "encrypted b"
        ]
    );
}

#[tokio::test]
async fn a_rerun_that_skips_the_topic_still_withdraws_what_the_failed_task_appended() {
    // The first completion is refused. The rerun takes another branch, writes nothing to the
    // topic and completes. The task after it writes again under a new prefix.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );
    t.add_we_signaled("go", vec![]);
    t.add_full_wf_task();
    let timer = t.add_timer_started("1".to_string());
    t.add_timer_fired(timer, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let events = t.get_full_history_info().unwrap().events().to_vec();
    let first_task = TestHistoryBuilder::from_history(events[..3].to_vec());
    let first_task = hist_to_poll_resp(&first_task, "fakeid", 1.into()).resp;
    let rerun_task = TestHistoryBuilder::from_history(events[..7].to_vec());
    let rerun_task = hist_to_poll_resp(&rerun_task, "fakeid", ResponseType::AllHistory).resp;
    let store = Arc::new(RecordingStore::default());
    let mut answers = 0;
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [
            ResponseType::Raw(first_task),
            ResponseType::Raw(rerun_task),
            ResponseType::AllHistory,
        ],
        mock_worker_client(),
    );
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        answers += 1;
        if answers == 1 {
            Err(tonic::Status::invalid_argument("UnhandledCommand"))
        } else {
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, Some(store.clone()));
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(records(&["draft"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert!(eviction.eviction_reason().is_some());
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    let rerun = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &rerun.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(10))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &fired.run_id,
        vec![
            output_commit_command(records(&["final"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;

    let appends = store.appends();
    assert_eq!(
        appends.iter().map(|a| a.attempt).collect::<Vec<_>>(),
        [attempt(0, 1), attempt(7, 1)]
    );
    assert_eq!(
        store.read(TOPIC).await,
        ["encrypted draft", "SUPERSEDED", "encrypted final"]
    );
}

#[tokio::test]
async fn a_rerun_after_a_crash_supersedes_what_the_stopped_worker_appended() {
    let store = Arc::new(RecordingStore::default());
    // The first Worker appends, then stops before its completion reaches the server.
    let mut first_history = TestHistoryBuilder::default();
    first_history.add_by_type(EventType::WorkflowExecutionStarted);
    first_history.add_workflow_task_scheduled_and_started();
    let run_id = first_history.get_orig_run_id().to_string();
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", first_history, [1], mock_worker_client());
    mock_cfg.num_expected_completions = Some(TimesRange::from(0..=1));
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, Some(store.clone()));
    let stopped = mock_worker(mock);
    let first = stopped.poll_workflow_activation().await.unwrap();
    complete(
        &stopped,
        &first.run_id,
        vec![
            output_commit_command(records(&["a", "b"])),
            schedule_local_activity(1),
        ],
    )
    .await;
    assert_eq!(store.read(TOPIC).await, ["encrypted a", "encrypted b"]);
    drop(stopped);

    // The task times out, and another Worker runs it again on the same store.
    let mut t = TestHistoryBuilder::default();
    t.add(WorkflowExecutionStartedEventAttributes {
        original_execution_run_id: run_id.clone(),
        first_execution_run_id: run_id.clone(),
        ..default_wes_attribs()
    });
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_and_started();
    let rerun = worker_appending_to(
        t,
        vec![ResponseType::AllHistory],
        store.clone(),
        Default::default(),
    );
    let activation = rerun.poll_workflow_activation().await.unwrap();
    complete(
        &rerun,
        &activation.run_id,
        vec![
            output_commit_command(records(&["a", "b"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    rerun.drain_pollers_and_shutdown().await;

    let producer = format!("@{run_id}");
    assert_eq!(
        store.append_log(),
        [
            (TOPIC.to_string(), producer.clone(), 1, 1, 2),
            (TOPIC.to_string(), producer.clone(), 2, 1, 2),
        ],
        "no append was refused as stale"
    );
    assert_eq!(
        store.read(TOPIC).await,
        [
            "encrypted a",
            "encrypted b",
            "SUPERSEDED",
            "encrypted a",
            "encrypted b"
        ]
    );
    assert_eq!(
        store.retired(),
        [],
        "the rerun's Worker never appended under the first attempt, so it has nothing to retire"
    );
}

#[tokio::test]
async fn a_failed_task_leaves_what_it_appended() {
    // The first activation commits and waits on a local activity. The second fails the task, so
    // nothing reaches History, but readers already saw the commit.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let failed = Arc::new(Notify::new());
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [1], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(TimesRange::from(0));
    mock_cfg.expect_fail_wft_matcher = failure_matcher(&["the workflow broke"], failed.clone());
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, Some(store.clone()));
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(records(&["a"])),
            schedule_local_activity(1),
        ],
    )
    .await;
    let resolved = run_local_activity(&worker).await;
    assert!(only_resolves_local_activity(&resolved, 1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            first.run_id,
            Failure {
                message: "the workflow broke".to_string(),
                ..Default::default()
            },
            None,
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
    assert_eq!(store.read(TOPIC).await, ["encrypted a"]);
}

#[tokio::test]
async fn a_lost_task_repeats_its_attempt_and_the_rerun_supersedes() {
    // The server loses the first task after the append, so History doesn't move and the next
    // task publishes under the same attempt. Other records diverge, the task fails, and the
    // failure the server writes moves the rerun to the next retry.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let lost_task = hist_to_poll_resp(&t, "fakeid", 1.into()).resp;
    // The server schedules a new task, which has a new token.
    let mut next_task = lost_task.clone();
    next_task.task_token = b"next-task".to_vec();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let failed = Arc::new(Notify::new());
    let mut answers = 0;
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [
            ResponseType::Raw(lost_task),
            ResponseType::Raw(next_task),
            ResponseType::AllHistory,
        ],
        mock_worker_client(),
    );
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        answers += 1;
        if answers == 1 {
            Err(tonic::Status::not_found("Workflow task not found"))
        } else {
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }
    }));
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher =
        failure_matcher(&["Could not append", "different content"], failed.clone());
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, Some(store.clone()));
    let worker = mock_worker(mock);

    let mut evictions = vec![];
    for values in [["a"], ["b"]] {
        let activation = worker.poll_workflow_activation().await.unwrap();
        complete(
            &worker,
            &activation.run_id,
            vec![
                output_commit_command(records(&values)),
                CompleteWorkflowExecution::default().into(),
            ],
        )
        .await;
        let eviction = worker.poll_workflow_activation().await.unwrap();
        evictions.push(eviction.jobs.iter().find_map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::RemoveFromCache(remove)) => {
                Some(remove.message.clone())
            }
            _ => None,
        }));
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
            .await
            .unwrap();
    }
    assert_eq!(
        evictions,
        [
            Some("Error reporting WFT to server".to_string()),
            Some("The Workflow Task's stream output could not be appended".to_string()),
        ]
    );
    failed_within_deadline(&failed).await;
    let rerun = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &rerun.run_id,
        vec![
            output_commit_command(records(&["b"])),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(store.calls()[..3], ["append", "append", "append"]);
    let attempts: Vec<_> = store
        .appends()
        .iter()
        .map(|a| (a.attempt, a.sequence))
        .collect();
    assert_eq!(
        attempts,
        [(1, 1), (2, 1)],
        "the store took no divergent batch"
    );
    assert_eq!(
        store.read(TOPIC).await,
        ["encrypted a", "SUPERSEDED", "encrypted b"]
    );
}

#[tokio::test]
async fn a_local_activity_starts_only_once_its_activations_output_is_in_the_store() {
    // The append is slow, so a Local Activity that started before it would find nothing.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore {
        append_delay: Some(Duration::from_millis(300)),
        ..Default::default()
    });
    let worker = Arc::new(worker_appending_to(
        history,
        vec![1.into()],
        store.clone(),
        Default::default(),
    ));
    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    let completing = {
        let worker = worker.clone();
        let run_id = run_id.clone();
        tokio::spawn(async move {
            complete(
                &worker,
                &run_id,
                vec![
                    output_commit_command(records(&["a"])),
                    schedule_local_activity(1),
                ],
            )
            .await
        })
    };
    let task = worker.poll_activity_task().await.unwrap();
    assert_eq!(
        store.appends().len(),
        1,
        "the activity starts after its activation's records landed"
    );
    completing.await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(Payload::default())),
        })
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(only_resolves_local_activity(&resolved, 1));
    complete(
        &worker,
        &run_id,
        vec![CompleteWorkflowExecution::default().into()],
    )
    .await;
    let Ok(worker) = Arc::try_unwrap(worker) else {
        panic!("the completing task let go of the worker");
    };
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_topic_closed_before_the_publish_drops_its_records_and_the_task_goes_on() {
    let store = Arc::new(RecordingStore::default());
    let chain = ChainId {
        namespace: crate::test_help::NAMESPACE.to_string(),
        workflow_id: "fakeid".to_string(),
        first_run_id: String::new(),
        ..Default::default()
    };
    let appended_at_completion: Arc<Mutex<Vec<usize>>> = Default::default();
    let history = timer_history();
    let first_run_id = history.get_orig_run_id().to_string();
    // Another party closed the topic before the Workflow published.
    store
        .close_topic(
            &ChainId {
                first_run_id: first_run_id.clone(),
                ..chain
            },
            TOPIC,
        )
        .await
        .unwrap();
    let worker = worker_appending_to(
        history,
        vec![1.into(), 2.into()],
        store.clone(),
        appended_at_completion.clone(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(
                records(&["a"])
                    .into_iter()
                    .chain(records_on("open", &["b"]))
                    .collect(),
            ),
            start_timer_cmd(1, Duration::from_secs(10)),
        ],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        appended_at_completion.lock().len(),
        2,
        "both tasks completed"
    );
    let landed: Vec<_> = store.appends().iter().map(|a| a.topic.clone()).collect();
    assert_eq!(landed, ["open"], "the closed topic's records were dropped");
}

#[tokio::test]
async fn a_rerun_after_the_run_closed_its_own_topic_drops_its_records_and_goes_on() {
    // The task publishes and closes its topic, then the server refuses the completion. The rerun
    // publishes the same records under the next retry, which the closed topic refuses.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let first_task =
        TestHistoryBuilder::from_history(t.get_full_history_info().unwrap().events()[..3].to_vec());
    let first_task = hist_to_poll_resp(&first_task, "fakeid", 1.into()).resp;
    let store = Arc::new(RecordingStore::default());
    let completions = Arc::new(Mutex::new(0));
    let mut answers = 0;
    let counted = completions.clone();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [ResponseType::Raw(first_task), ResponseType::AllHistory],
        mock_worker_client(),
    );
    mock_cfg.completion_mock_fn = Some(Box::new(move |_| {
        answers += 1;
        *counted.lock() += 1;
        if answers == 1 {
            Err(tonic::Status::invalid_argument("UnhandledCommand"))
        } else {
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    with_store(&mut mock, Some(store.clone()));
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    let mut finished = records(&["a"]);
    finished.push(OutputRecord {
        topic: TOPIC.to_string(),
        kind: StreamRecordKind::Finish as i32,
        ..Default::default()
    });
    complete(
        &worker,
        &first.run_id,
        vec![
            output_commit_command(finished.clone()),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    // The run's close, as lang's close does after the FINISH landed.
    let chain = store.appends()[0].chain.clone().unwrap();
    store.close_topic(&chain, TOPIC).await.unwrap();
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert!(eviction.eviction_reason().is_some());
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    let rerun = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &rerun.run_id,
        vec![
            output_commit_command(finished),
            CompleteWorkflowExecution::default().into(),
        ],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(*completions.lock(), 2, "the rerun's completion went out");
    assert_eq!(
        store.append_log(),
        [(TOPIC.to_string(), format!("@{}", first.run_id), 1, 1, 2)],
        "the rerun's records were dropped"
    );
}

/// What a Workflow Activity's stream holds once the agent `fakeid` scheduled it in `run_id`.
fn activity_chain(run_id: &str, activity_id: &str) -> ChainId {
    ChainId {
        namespace: "default".to_string(),
        workflow_id: "fakeid".to_string(),
        first_run_id: run_id.to_string(),
        owner_kind: StreamOwnerKind::WorkflowActivity as i32,
        activity_id: activity_id.to_string(),
    }
}

/// A `ScheduleActivity` that marks Activity `activity_id` as the owner of its streams, or not.
fn schedule_activity(
    activity_id: &str,
    marked: bool,
    cancellation_type: ActivityCancellationType,
) -> workflow_command::Variant {
    let mut schedule = ScheduleActivity {
        seq: 1,
        activity_id: activity_id.to_string(),
        cancellation_type: cancellation_type as i32,
        ..default_act_sched()
    };
    if marked {
        schedule
            .headers
            .insert("__temporal_stream_owner".to_string(), Payload::default());
    }
    schedule.into()
}

/// Appends one record to each of `topics` of the Activity's stream, as its producer would.
async fn activity_wrote(store: &RecordingStore, run_id: &str, activity_id: &str, topics: &[&str]) {
    for topic in topics {
        store
            .inner
            .append(StoreAppendRequest {
                chain: Some(activity_chain(run_id, activity_id)),
                topic: topic.to_string(),
                producer_id: format!("{activity_id}@{run_id}"),
                attempt: 1,
                sequence: 1,
                digest: vec![1; 32],
                records: vec![b"record".to_vec()],
            })
            .await
            .unwrap();
    }
}

/// Waits a bounded time for `count` stream closes, which run in the background.
async fn closes_within_deadline(
    store: &RecordingStore,
    count: usize,
) -> Vec<(String, StoredClose)> {
    for _ in 0..200 {
        let closes = store.closes.lock().clone();
        if closes.len() >= count {
            return closes
                .into_iter()
                .map(|(chain, topic, close)| {
                    assert_eq!(chain.owner_kind, StreamOwnerKind::WorkflowActivity as i32);
                    (topic, close)
                })
                .collect();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "expected {count} stream closes, got {:?}",
        store.closes.lock()
    );
}

/// Runs a Workflow that schedules Activity `tool`, which writes `topics`, and completes once the
/// Activity resolves. Returns the run id.
async fn run_activity_owner(
    history: TestHistoryBuilder,
    store: Arc<RecordingStore>,
    marked: bool,
    topics: &[&str],
) -> String {
    let worker = worker_appending_to(
        history,
        vec![1.into(), 2.into()],
        store.clone(),
        Default::default(),
    );
    let start = worker.poll_workflow_activation().await.unwrap();
    let run_id = start.run_id.clone();
    activity_wrote(&store, &run_id, "tool", topics).await;
    complete(
        &worker,
        &run_id,
        vec![schedule_activity(
            "tool",
            marked,
            ActivityCancellationType::TryCancel,
        )],
    )
    .await;
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(!resolved.is_replaying);
    assert!(resolved.jobs.iter().any(|job| matches!(
        job.variant,
        Some(workflow_activation_job::Variant::ResolveActivity(_))
    )));
    worker.complete_execution(&resolved.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    run_id
}

#[tokio::test]
async fn a_marked_activity_that_completes_closes_each_topic_it_wrote_with_null() {
    let store = Arc::new(RecordingStore::default());
    let run_id = run_activity_owner(
        canned_histories::single_activity("tool"),
        store.clone(),
        true,
        &["status", "tokens"],
    )
    .await;
    let closes = closes_within_deadline(&store, 2).await;
    assert_eq!(
        closes
            .iter()
            .map(|(topic, _)| topic.as_str())
            .collect::<Vec<_>>(),
        ["status", "tokens"]
    );
    for (_, close) in &closes {
        assert_eq!(close.result, Some(temporalio_streams::empty_close_result()));
        assert!(close.failure.is_none());
        assert_eq!(close.closed_by, format!("tool@{run_id}"));
    }
    let chain = store.closes.lock()[0].0.clone();
    assert_eq!(chain, activity_chain(&run_id, "tool"));
    // A reader of the stream finds the close.
    let read = store
        .inner
        .read(StoreReadRequest {
            chain: Some(chain),
            topic: "tokens".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(read.closed);
}

#[tokio::test]
async fn a_marked_activity_that_fails_closes_with_the_failure_the_workflow_saw() {
    let store = Arc::new(RecordingStore::default());
    run_activity_owner(
        canned_histories::single_failed_activity("tool"),
        store.clone(),
        true,
        &["tokens"],
    )
    .await;
    let closes = closes_within_deadline(&store, 1).await;
    let failure = closes[0].1.failure.clone().unwrap();
    assert!(closes[0].1.result.is_none());
    assert!(
        matches!(
            failure.failure_info,
            Some(temporalio_common::protos::temporal::api::failure::v1::failure::FailureInfo::ActivityFailureInfo(_))
        ),
        "{failure:?}"
    );
}

#[tokio::test]
async fn a_marked_activity_that_times_out_closes_with_its_timeout() {
    let store = Arc::new(RecordingStore::default());
    run_activity_owner(
        canned_histories::started_activity_timeout("tool"),
        store.clone(),
        true,
        &["tokens"],
    )
    .await;
    let closes = closes_within_deadline(&store, 1).await;
    let failure = closes[0].1.failure.clone().unwrap();
    assert_eq!(failure.message, "Activity task timed out", "{failure:?}");
}

#[tokio::test]
async fn an_unmarked_activity_costs_no_store_call() {
    let store = Arc::new(RecordingStore::default());
    run_activity_owner(
        canned_histories::single_activity("tool"),
        store.clone(),
        false,
        &[],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(store.calls().is_empty(), "{:?}", store.calls());
}

#[tokio::test]
async fn a_store_that_refuses_the_close_leaves_the_task_completed() {
    let store = Arc::new(RecordingStore {
        refuse_closes: true,
        ..Default::default()
    });
    let completions: Arc<Mutex<Vec<usize>>> = Default::default();
    let worker = worker_appending_to(
        canned_histories::single_activity("tool"),
        vec![1.into(), 2.into()],
        store.clone(),
        completions.clone(),
    );
    let start = worker.poll_workflow_activation().await.unwrap();
    activity_wrote(&store, &start.run_id, "tool", &["tokens"]).await;
    complete(
        &worker,
        &start.run_id,
        vec![schedule_activity(
            "tool",
            true,
            ActivityCancellationType::TryCancel,
        )],
    )
    .await;
    let resolved = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&resolved.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    for _ in 0..200 {
        if store.calls().contains(&"close_topic_with") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        store.calls().contains(&"close_topic_with"),
        "{:?}",
        store.calls()
    );
    assert_eq!(completions.lock().len(), 2, "both tasks completed");
}

#[tokio::test]
async fn replay_of_a_resolution_makes_no_close() {
    let mut history = canned_histories::single_activity("tool");
    // Another task after the resolution, so the resolution replays.
    history.add_workflow_task_completed();
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let worker = worker_appending_to(history, vec![3.into()], store.clone(), Default::default());
    let start = worker.poll_workflow_activation().await.unwrap();
    assert!(start.is_replaying);
    complete(
        &worker,
        &start.run_id,
        vec![schedule_activity(
            "tool",
            true,
            ActivityCancellationType::TryCancel,
        )],
    )
    .await;
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(resolved.is_replaying);
    complete(
        &worker,
        &resolved.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(store.calls().is_empty(), "{:?}", store.calls());
}

#[tokio::test]
async fn a_try_cancel_closes_the_streams_at_once_with_the_canceled_failure() {
    let store = Arc::new(RecordingStore::default());
    let worker = worker_appending_to(
        canned_histories::cancel_scheduled_activity("tool", "signal"),
        vec![1.into(), 2.into()],
        store.clone(),
        Default::default(),
    );
    let start = worker.poll_workflow_activation().await.unwrap();
    activity_wrote(&store, &start.run_id, "tool", &["tokens"]).await;
    complete(
        &worker,
        &start.run_id,
        vec![schedule_activity(
            "tool",
            true,
            ActivityCancellationType::TryCancel,
        )],
    )
    .await;
    let signaled = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &signaled.run_id,
        vec![RequestCancelActivity { seq: 1 }.into()],
    )
    .await;
    let closes = closes_within_deadline(&store, 1).await;
    let failure = closes[0].1.failure.clone().unwrap();
    assert!(
        matches!(
            failure.cause.as_deref().and_then(|cause| cause.failure_info.as_ref()),
            Some(temporalio_common::protos::temporal::api::failure::v1::failure::FailureInfo::CanceledFailureInfo(_))
        ),
        "{failure:?}"
    );
    let resolved = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&resolved.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}
