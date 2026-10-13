//! External stream output commit: the records a completion commits, the marker Core builds from
//! them and writes ahead of the completion's other commands, and how replay checks the records lang
//! commits again against the recorded marker.

use crate::{
    TaskToken,
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers, mock_worker,
        schedule_local_activity_cmd, start_timer_cmd,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use mockall::TimesRange;
use parking_lot::Mutex;
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::{
    protos::{
        constants::{EXTERNAL_STREAM_MARKER_NAME, LOCAL_ACTIVITY_MARKER_NAME},
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            common::extract_local_activity_marker_data,
            external_data::{
                ExternalOutputSegmentManifest, ExternalOutputStreamManifest,
                ExternalOutputTopicManifest, ExternalStreamBoundary, ExternalStreamMarkerData,
                extract_external_stream_marker_data,
            },
            workflow_activation::{WorkflowActivation, workflow_activation_job},
            workflow_commands::{
                ActivityCancellationType, CompleteWorkflowExecution, OutputClose, OutputRecord,
                WorkflowOutputStreamCommit, workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::{
            api::{
                command::v1::{Command, command},
                common::v1::Payload,
                enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
                failure::v1::Failure,
                workflowservice::v1::RespondWorkflowTaskCompletedResponse,
            },
            sdk::streams::v1::{StreamRecord, StreamRecordKind},
        },
    },
    streams::{CONTENT_HASH_KEY, FingerprintRecord, RUN_ID_KEY, content_hash_text, fingerprint},
    worker::WorkerTaskTypes,
};
use temporalio_streams::{
    StreamError, StreamResult, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, PendingStage, PromoteResult, StageRef,
        StagedBatch, StoreAppendRequest, StoreAppendResponse, StoreLatestRequest,
        StoreLatestResponse, StoreReadRequest, StoreReadResponse,
    },
};
use tokio::sync::Notify;

/// A stream store that keeps what Core stages, and can be told to refuse it.
#[derive(Default)]
struct RecordingStore {
    staged: Mutex<Vec<StagedBatch>>,
    refuse: Option<StreamError>,
}

impl RecordingStore {
    fn refusing(error: StreamError) -> Self {
        Self {
            refuse: Some(error),
            ..Default::default()
        }
    }

    fn staged(&self) -> Vec<StagedBatch> {
        self.staged.lock().clone()
    }
}

#[async_trait::async_trait]
impl StreamStore for RecordingStore {
    fn name(&self) -> &str {
        STORE
    }

    async fn append(&self, _: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        Err(StreamError::unsupported("append"))
    }

    async fn read(&self, _: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        Err(StreamError::unsupported("read"))
    }

    async fn latest(&self, _: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        Err(StreamError::unsupported("latest"))
    }

    async fn record_at(&self, _: &ChainId, _: &str, _: &str) -> StreamResult<Option<Vec<u8>>> {
        Err(StreamError::unsupported("record_at"))
    }

    async fn stage(&self, batch: StagedBatch) -> StreamResult<()> {
        if let Some(error) = &self.refuse {
            return Err(error.clone());
        }
        self.staged.lock().push(batch);
        Ok(())
    }

    async fn promote(&self, _: &StageRef) -> StreamResult<PromoteResult> {
        Err(StreamError::unsupported("promote"))
    }

    async fn abort(&self, _: &StageRef) -> StreamResult<()> {
        Err(StreamError::unsupported("abort"))
    }

    async fn close_chain(&self, _: &ChainId) -> StreamResult<()> {
        Err(StreamError::unsupported("close_chain"))
    }

    async fn close_topic(&self, _: &ChainId, _: &str) -> StreamResult<()> {
        Err(StreamError::unsupported("close_topic"))
    }

    async fn pending_stages(&self, _: &ChainId) -> StreamResult<Vec<PendingStage>> {
        Err(StreamError::unsupported("pending_stages"))
    }

    async fn delete_owner(&self, _: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        Err(StreamError::unsupported("delete_owner"))
    }
}

/// Every completion's external stream markers, in the order the completions were reported.
type RecordedMarkers = Arc<Mutex<Vec<Vec<ExternalStreamMarkerData>>>>;

const TOPIC: &str = "results";
/// The name the test store records under, which Core writes into each manifest.
const STORE: &str = "recording";

fn stream_marker_data(wft: &WorkflowTaskCompletion) -> Vec<ExternalStreamMarkerData> {
    wft.commands
        .iter()
        .filter_map(|c| match &c.attributes {
            Some(command::Attributes::RecordMarkerCommandAttributes(m))
                if m.marker_name == EXTERNAL_STREAM_MARKER_NAME =>
            {
                extract_external_stream_marker_data(&m.details)
            }
            _ => None,
        })
        .collect()
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
            logical_size: value.len() as u64,
        })
        .collect()
}

/// What lang commits again while replaying `records`: the same records, with no body.
fn replayed(mut records: Vec<OutputRecord>) -> Vec<OutputRecord> {
    for record in &mut records {
        record.body = None;
    }
    records
}

/// The manifest Core must build for `records`, all on one topic, computed here from the wire
/// contract.
fn manifest_for(
    records: &[OutputRecord],
    run_id: &str,
    history_floor_event_id: i64,
) -> ExternalOutputStreamManifest {
    let topic = records[0].topic.as_str();
    ExternalOutputStreamManifest {
        schema_version: 1,
        fingerprint_version: 2,
        stage_token: "recorded-stage-token".to_string(),
        history_floor_event_id,
        run_id: run_id.to_string(),
        topics: vec![ExternalOutputTopicManifest {
            topic: topic.to_string(),
            record_count: records.len() as u32,
            logical_byte_count: records.iter().map(|r| r.logical_size).sum(),
            logical_fingerprint: fingerprint(records.iter().map(|r| FingerprintRecord {
                topic,
                kind: r.kind,
                content_hash: &r.content_hash,
            }))
            .to_vec(),
            finished: false,
        }],
        segments: vec![ExternalOutputSegmentManifest {
            record_counts_by_topic: vec![records.len() as u32],
        }],
        provider_id: STORE.to_string(),
        provider_format_version: 1,
    }
}

fn output_commit_command(records: Vec<OutputRecord>) -> workflow_command::Variant {
    workflow_command::Variant::WorkflowOutputStreamCommit(WorkflowOutputStreamCommit {
        records,
        closes: vec![],
    })
}

fn output_marker(
    terminal: ExternalStreamBoundary,
    manifest: ExternalOutputStreamManifest,
) -> ExternalStreamMarkerData {
    ExternalStreamMarkerData {
        schema_version: 1,
        terminal_boundary: terminal as i32,
        output: Some(manifest),
    }
}

/// The written markers with their stage tokens checked and cleared, since Core mints them.
fn without_tokens(written: &[Vec<ExternalStreamMarkerData>]) -> Vec<Vec<ExternalStreamMarkerData>> {
    written
        .iter()
        .map(|markers| {
            markers
                .iter()
                .cloned()
                .map(|mut marker| {
                    let output = marker.output.as_mut().expect("an output marker");
                    assert_eq!(output.stage_token.len(), 32, "{}", output.stage_token);
                    assert!(output.stage_token.chars().all(|c| c.is_ascii_hexdigit()));
                    output.stage_token.clear();
                    marker
                })
                .collect()
        })
        .collect()
}

fn without_token(mut marker: ExternalStreamMarkerData) -> ExternalStreamMarkerData {
    if let Some(output) = marker.output.as_mut() {
        output.stage_token.clear();
    }
    marker
}

/// Start, a task that commits `values` and starts a timer, the timer firing, and the next task.
fn output_then_timer_history(
    values: &[&str],
) -> (TestHistoryBuilder, ExternalOutputStreamManifest) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = manifest_for(&records(values), t.get_orig_run_id(), 1);
    t.add_full_wf_task();
    t.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest.clone(),
    ));
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    (t, manifest)
}

fn has_fire_timer(activation: &WorkflowActivation) -> bool {
    activation.jobs.iter().any(|job| {
        matches!(
            job.variant,
            Some(workflow_activation_job::Variant::FireTimer(_))
        )
    })
}

/// A worker that records the external stream markers and command types of every completion.
fn worker_recording(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    markers: RecordedMarkers,
    command_types: Arc<Mutex<Vec<Vec<CommandType>>>>,
) -> crate::Worker {
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
        markers.lock().push(stream_marker_data(wft));
        command_types
            .lock()
            .push(wft.commands.iter().map(|c| c.command_type()).collect());
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

/// A worker whose one completion must fail its Workflow Task with a message holding `expected`.
fn worker_failing_with(history: TestHistoryBuilder, expected: &'static str) -> crate::Worker {
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [1], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(move |_, _, failure| {
        failure
            .as_ref()
            .is_some_and(|f| f.message.contains(expected))
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

#[tokio::test]
async fn the_marker_names_the_exact_history_floor_before_its_scheduled_event() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    let floor = history.current_event_id();
    let manifest = manifest_for(&records(&["a"]), history.get_orig_run_id(), floor);
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::WorkflowCompleted,
        manifest.clone(),
    ));
    history.add_workflow_execution_completed();

    let markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into(), 2.into()],
        markers.clone(),
        Default::default(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            fired.run_id,
            vec![
                output_commit_command(records(&["a"])),
                CompleteWorkflowExecution::default().into(),
            ],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    let written = without_tokens(&markers.lock());
    assert_eq!(
        floor, 6,
        "TimerFired precedes the second WorkflowTaskScheduled"
    );
    assert_eq!(
        written,
        vec![
            vec![],
            vec![without_token(output_marker(
                ExternalStreamBoundary::WorkflowCompleted,
                manifest
            ))]
        ]
    );
}

#[tokio::test]
async fn two_commits_in_one_completion_fail_the_task() {
    let worker = worker_failing_with(
        canned_histories::single_timer("1"),
        "more than one WorkflowOutputStreamCommit",
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![
                output_commit_command(records(&["a"])),
                output_commit_command(records(&["b"])),
            ],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_record_no_writer_stores_fails_the_task_with_its_reason() {
    let worker = worker_failing_with(
        canned_histories::single_timer("1"),
        "record 0 is DATA without a body",
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    let mut unencoded = records(&["a"]);
    unencoded[0].body = None;
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![output_commit_command(unencoded)],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_commit_on_an_accepted_task_writes_one_marker() {
    let (history, manifest) = output_then_timer_history(&["ab", "cde"]);
    let markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into(), 2.into()],
        markers.clone(),
        Default::default(),
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(records(&["ab", "cde"])),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        without_tokens(&markers.lock()),
        vec![
            vec![without_token(output_marker(
                ExternalStreamBoundary::CommandsProduced,
                manifest
            ))],
            vec![]
        ]
    );
}

#[tokio::test]
async fn each_commit_gets_a_stage_token_of_its_own() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let run_id = history.get_orig_run_id().to_string();
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest_for(&records(&["a"]), &run_id, 1),
    ));
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    let floor = history.current_event_id();
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::WorkflowCompleted,
        manifest_for(&records(&["a"]), &run_id, floor),
    ));
    history.add_workflow_execution_completed();

    let markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into(), 2.into()],
        markers.clone(),
        Default::default(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(records(&["a"])),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            fired.run_id,
            vec![
                output_commit_command(records(&["a"])),
                CompleteWorkflowExecution::default().into(),
            ],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    let written = markers.lock();
    let tokens: Vec<_> = written
        .iter()
        .flatten()
        .map(|m| m.output.as_ref().unwrap().stage_token.clone())
        .collect();
    assert_eq!(tokens.len(), 2);
    assert_ne!(tokens[0], tokens[1]);
    assert!(tokens.iter().all(|t| t.len() == 32));
}

#[tokio::test]
async fn a_commit_with_nothing_else_completes_the_task_with_its_marker() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = manifest_for(&records(&["a"]), history.get_orig_run_id(), 1);
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::TaskCompleted,
        manifest.clone(),
    ));
    history.add_workflow_execution_completed();

    let markers: RecordedMarkers = Default::default();
    let command_types: Arc<Mutex<Vec<Vec<CommandType>>>> = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into()],
        markers.clone(),
        command_types.clone(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            first.run_id,
            output_commit_command(records(&["a"])),
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        without_tokens(&markers.lock()),
        vec![vec![without_token(output_marker(
            ExternalStreamBoundary::TaskCompleted,
            manifest
        ))]]
    );
    assert_eq!(*command_types.lock(), vec![vec![CommandType::RecordMarker]]);
}

#[tokio::test]
async fn a_terminal_command_writes_the_output_marker_ordered_before_it() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = manifest_for(&records(&["a"]), history.get_orig_run_id(), 1);
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::WorkflowCompleted,
        manifest.clone(),
    ));
    history.add_workflow_execution_completed();

    let markers: RecordedMarkers = Default::default();
    let command_types: Arc<Mutex<Vec<Vec<CommandType>>>> = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into()],
        markers.clone(),
        command_types.clone(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    // Lang puts the commit last; Core still orders the marker first.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![
                CompleteWorkflowExecution::default().into(),
                output_commit_command(records(&["a"])),
            ],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        without_tokens(&markers.lock()),
        vec![vec![without_token(output_marker(
            ExternalStreamBoundary::WorkflowCompleted,
            manifest
        ))]]
    );
    assert_eq!(
        *command_types.lock(),
        vec![vec![
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]]
    );
}

#[tokio::test]
async fn a_marker_in_history_with_a_different_manifest_is_nondeterministic() {
    let (history, _) = output_then_timer_history(&["a"]);

    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, [1, 2], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, cause, failure| {
        *cause == WorkflowTaskFailedCause::NonDeterministicError
            && failure
                .as_ref()
                .is_some_and(|f| f.message.contains("different external output manifest"))
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![
                output_commit_command(records(&["b"])),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    // Applying the next task reconciles the written marker against History and fails it.
    worker.handle_eviction().await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn replay_checks_the_committed_records_and_writes_nothing() {
    let (history, _) = output_then_timer_history(&["a", "bc"]);
    let replay_markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![2.into()],
        replay_markers.clone(),
        Default::default(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    assert_eq!(
        first.jobs.len(),
        1,
        "replayed output stays inside Core: {:?}",
        first.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(replayed(records(&["a", "bc"]))),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        *replay_markers.lock(),
        vec![Vec::<ExternalStreamMarkerData>::new()],
        "the marker found by replay lookahead must not be written again"
    );
}

#[tokio::test]
async fn the_output_marker_survives_a_cache_eviction() {
    let (history, manifest) = output_then_timer_history(&["a"]);
    let markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into(), ResponseType::AllHistory],
        markers.clone(),
        Default::default(),
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(records(&["a"])),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    worker.request_workflow_eviction(&run_id);
    worker.handle_eviction().await;

    let rebuilt = worker.poll_workflow_activation().await.unwrap();
    assert!(rebuilt.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(records(&["a"]))),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        without_tokens(&markers.lock()),
        vec![
            vec![without_token(output_marker(
                ExternalStreamBoundary::CommandsProduced,
                manifest
            ))],
            vec![],
        ],
        "only the live task writes the marker; the replayed one does not"
    );
}

/// Replays `history` and answers its first activation with `commit`.
async fn replay_with_commit(
    history: TestHistoryBuilder,
    commit: Vec<OutputRecord>,
    num_expected_fails: usize,
) {
    let markers: RecordedMarkers = Default::default();
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [2], mock_worker_client());
    mock_cfg.num_expected_fails = num_expected_fails;
    mock_cfg.expect_fail_wft_matcher =
        Box::new(|_, cause, _| *cause == WorkflowTaskFailedCause::NonDeterministicError);
    if num_expected_fails == 0 {
        let recorded = markers.clone();
        mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
            recorded.lock().push(stream_marker_data(wft));
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }));
    }
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(commit),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    if num_expected_fails == 0 {
        let fired = worker.poll_workflow_activation().await.unwrap();
        assert!(has_fire_timer(&fired));
        worker.complete_execution(&fired.run_id).await;
        worker.drain_pollers_and_shutdown().await;
    } else {
        worker.shutdown().await;
        worker.finalize_shutdown().await;
    }
    assert!(
        markers.lock().iter().all(Vec::is_empty),
        "replay never writes an output marker"
    );
}

#[tokio::test]
async fn replayed_records_without_bodies_match_the_recorded_manifest() {
    let (history, _) = output_then_timer_history(&["a"]);
    replay_with_commit(history, replayed(records(&["a"])), 0).await;
}

#[tokio::test]
async fn a_replayed_commit_matches_a_marker_a_reset_copied_from_the_base_run() {
    // A reset forks the base run's History, so its markers keep naming the base run while Core
    // builds the replayed manifest with the new run's id.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest_for(&records(&["a"]), "reset-base-run", 1),
    ));
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();
    replay_with_commit(history, replayed(records(&["a"])), 0).await;
}

#[tokio::test]
async fn a_replayed_commit_matches_a_marker_recorded_under_another_store_name() {
    // A Replayer has no store, and a store can be renamed, without the output changing.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let mut manifest = manifest_for(&records(&["a"]), history.get_orig_run_id(), 1);
    manifest.provider_id = "redis".to_string();
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest,
    ));
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();
    replay_with_commit(history, replayed(records(&["a"])), 0).await;
}

#[tokio::test]
async fn a_replayed_commit_that_differs_from_history_is_nondeterministic() {
    let (history, _) = output_then_timer_history(&["a"]);
    replay_with_commit(history, replayed(records(&["a", "b"])), 1).await;
}

#[tokio::test]
async fn a_replayed_commit_where_history_recorded_none_is_nondeterministic() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [2], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, cause, failure| {
        *cause == WorkflowTaskFailedCause::NonDeterministicError
            && failure
                .as_ref()
                .is_some_and(|f| f.message.contains("recorded none"))
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(replayed(records(&["a"]))),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

/// The predicate a mock applies to every Workflow Task failure it is asked to report.
type FailMatcher =
    Box<dyn Fn(&TaskToken, &WorkflowTaskFailedCause, &Option<Failure>) -> bool + Send>;

/// Accepts only a nondeterminism failure naming `message`, and signals `failed` when one comes.
fn nondeterminism_matcher(message: &'static str, failed: Arc<Notify>) -> FailMatcher {
    Box::new(move |_, cause, failure| {
        let matches = *cause == WorkflowTaskFailedCause::NonDeterministicError
            && failure
                .as_ref()
                .is_some_and(|f| f.message.contains(message));
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

/// A worker that expects exactly one Workflow Task failure: nondeterminism naming `message`.
fn worker_expecting_nondeterminism(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    message: &'static str,
) -> (crate::Worker, Arc<Notify>) {
    let failed = Arc::new(Notify::new());
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = nondeterminism_matcher(message, failed.clone());
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    (mock_worker(mock), failed)
}

#[tokio::test]
async fn a_replay_that_commits_less_than_history_fails_when_going_live() {
    let (history, _) = output_then_timer_history(&["a"]);
    let (worker, failed) =
        worker_expecting_nondeterminism(history, vec![2.into()], "did not commit");

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    // The Workflow no longer publishes, so lang sends the timer alone.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_replay_that_commits_less_than_history_fails_before_the_next_replayed_task() {
    let (mut history, _) = output_then_timer_history(&["a"]);
    history.add_workflow_task_completed();
    let second_timer = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(second_timer, "2".to_string());
    history.add_workflow_task_scheduled_and_started();
    let (worker, failed) =
        worker_expecting_nondeterminism(history, vec![3.into()], "did not commit");

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_replay_that_commits_less_in_the_final_task_of_a_closed_history_fails() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = manifest_for(&records(&["a"]), history.get_orig_run_id(), 1);
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::WorkflowCompleted,
        manifest,
    ));
    history.add_workflow_execution_completed();
    let (worker, failed) =
        worker_expecting_nondeterminism(history, vec![ResponseType::AllHistory], "did not commit");

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

/// Every completion's commands, in the order the completions were reported.
type RecordedCommands = Arc<Mutex<Vec<Vec<Command>>>>;

/// A worker that runs local activities and records the commands of every completion.
fn local_activity_worker(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    completions: RecordedCommands,
    num_expected_fails: usize,
) -> (crate::Worker, Arc<Notify>) {
    let failed = Arc::new(Notify::new());
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    mock_cfg.num_expected_fails = num_expected_fails;
    if num_expected_fails == 0 {
        mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
            completions.lock().push(wft.commands.clone());
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }));
    } else {
        mock_cfg.expect_fail_wft_matcher = nondeterminism_matcher("did not commit", failed.clone());
    }
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes {
            enable_local_activities: true,
            ..WorkerTaskTypes::workflow_only()
        };
        w.max_cached_workflows = 1;
    });
    (mock_worker(mock), failed)
}

fn schedule_local_activity(seq: u32) -> workflow_command::Variant {
    schedule_local_activity_cmd(
        seq,
        &seq.to_string(),
        ActivityCancellationType::TryCancel,
        Duration::from_secs(10),
    )
}

fn local_activity_result() -> Payload {
    Payload {
        data: b"done".to_vec(),
        ..Default::default()
    }
}

/// Runs the local activity lang just scheduled and returns the activation that resolves it.
async fn run_local_activity(worker: &crate::Worker) -> WorkflowActivation {
    let task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(local_activity_result())),
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

fn stream_markers_in(commands: &[Command]) -> Vec<ExternalStreamMarkerData> {
    commands
        .iter()
        .filter_map(|c| match &c.attributes {
            Some(command::Attributes::RecordMarkerCommandAttributes(m))
                if m.marker_name == EXTERNAL_STREAM_MARKER_NAME =>
            {
                extract_external_stream_marker_data(&m.details)
            }
            _ => None,
        })
        .collect()
}

/// The activation index each local activity marker in `commands` recorded.
fn local_activity_activation_indexes(commands: &[Command]) -> Vec<Option<u64>> {
    commands
        .iter()
        .filter_map(|c| match &c.attributes {
            Some(command::Attributes::RecordMarkerCommandAttributes(m))
                if m.marker_name == LOCAL_ACTIVITY_MARKER_NAME =>
            {
                extract_local_activity_marker_data(&m.details).map(|d| d.activation_index)
            }
            _ => None,
        })
        .collect()
}

/// The two commits a task makes around a local activity, told apart by their topic.
fn two_commits() -> [Vec<OutputRecord>; 2] {
    [records(&["a"]), records_on("progress", &["b"])]
}

/// What a first task that commits, runs a local activity, then commits again and starts a timer
/// leaves in History, followed by the timer firing and the next task. Also returns that next
/// task's history floor.
fn two_commits_around_a_local_activity_history() -> (TestHistoryBuilder, i64) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let run_id = t.get_orig_run_id().to_string();
    let [first, second] = two_commits();
    t.add_full_wf_task();
    t.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest_for(&first, &run_id, 1),
    ));
    t.add_local_activity_marker(1, "1", Some(local_activity_result()), None, |d| {
        d.activation_index = Some(1)
    });
    t.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest_for(&second, &run_id, 1),
    ));
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    let next_floor = t.current_event_id();
    t.add_workflow_task_scheduled_and_started();
    (t, next_floor)
}

#[tokio::test]
async fn commits_around_a_local_activity_write_one_marker_each_in_order() {
    let (history, _) = two_commits_around_a_local_activity_history();
    let completions: RecordedCommands = Default::default();
    let (worker, _) =
        local_activity_worker(history, vec![1.into(), 2.into()], completions.clone(), 0);
    let [first_commit, second_commit] = two_commits();

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(first_commit.clone()),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    let resolved = run_local_activity(&worker).await;
    assert!(only_resolves_local_activity(&resolved, 1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(second_commit.clone()),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    // The next task's History holds the markers written above, and Core matches them in order.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let completions = completions.lock();
    let first_task = &completions[0];
    assert_eq!(
        first_task
            .iter()
            .map(|c| c.command_type())
            .collect::<Vec<_>>(),
        vec![
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::StartTimer,
        ]
    );
    // Both activations belong to the same Workflow Task, so both commits name its floor.
    assert_eq!(
        without_tokens(&[stream_markers_in(first_task)]),
        vec![
            [first_commit, second_commit]
                .iter()
                .map(|records| without_token(output_marker(
                    ExternalStreamBoundary::CommandsProduced,
                    manifest_for(records, &run_id, 1)
                )))
                .collect::<Vec<_>>()
        ]
    );
    assert_eq!(
        local_activity_activation_indexes(first_task),
        vec![Some(1)],
        "the local activity resolved in the task's second activation"
    );
}

#[tokio::test]
async fn replay_splits_commits_around_a_local_activity_into_the_live_activations() {
    let (history, _) = two_commits_around_a_local_activity_history();
    let completions: RecordedCommands = Default::default();
    let (worker, _) = local_activity_worker(history, vec![2.into()], completions.clone(), 0);
    let [first_commit, second_commit] = two_commits();

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    assert!(first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(first_commit)),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    // The recorded result resolves the activity in its own activation, as it did live.
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(resolved.is_replaying);
    assert!(only_resolves_local_activity(&resolved, 1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(second_commit)),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert!(
        completions
            .lock()
            .iter()
            .all(|commands| stream_markers_in(commands).is_empty()),
        "replay never writes an output marker"
    );
}

#[tokio::test]
async fn a_replay_that_drops_the_commit_after_a_local_activity_is_nondeterministic() {
    let (history, _) = two_commits_around_a_local_activity_history();
    let (worker, failed) = local_activity_worker(history, vec![2.into()], Default::default(), 1);
    let [first_commit, _] = two_commits();

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(first_commit)),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(only_resolves_local_activity(&resolved, 1));
    // The Workflow no longer publishes after the activity.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn commits_around_a_local_activity_replay_then_go_live_in_the_next_task() {
    let (history, live_floor) = two_commits_around_a_local_activity_history();
    let completions: RecordedCommands = Default::default();
    let (worker, _) = local_activity_worker(history, vec![2.into()], completions.clone(), 0);
    let [first_commit, second_commit] = two_commits();

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(first_commit.clone())),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(only_resolves_local_activity(&resolved, 1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(second_commit.clone())),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    // The first live task publishes around a local activity of its own.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    assert!(has_fire_timer(&fired));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(first_commit.clone()),
                schedule_local_activity(2),
            ],
        ))
        .await
        .unwrap();
    let resolved = run_local_activity(&worker).await;
    assert!(!resolved.is_replaying);
    assert!(only_resolves_local_activity(&resolved, 2));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                CompleteWorkflowExecution::default().into(),
                output_commit_command(second_commit.clone()),
            ],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    let completions = completions.lock();
    assert_eq!(completions.len(), 1, "only the live task completes");
    assert_eq!(
        without_tokens(&[stream_markers_in(&completions[0])]),
        vec![vec![
            without_token(output_marker(
                ExternalStreamBoundary::CommandsProduced,
                manifest_for(&first_commit, &run_id, live_floor)
            )),
            without_token(output_marker(
                ExternalStreamBoundary::WorkflowCompleted,
                manifest_for(&second_commit, &run_id, live_floor)
            )),
        ]]
    );
    assert_eq!(
        local_activity_activation_indexes(&completions[0]),
        vec![Some(1)]
    );
}

#[tokio::test]
async fn replay_keeps_each_local_activity_result_and_its_commit_in_the_live_activation() {
    // Live, lang committed, scheduled two local activities, and committed again after each one
    // resolved in an activation of its own. Both activity markers sit where they were scheduled.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let run_id = history.get_orig_run_id().to_string();
    let [first, second] = two_commits();
    let commits = [first, second, records_on("summary", &["c"])];
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest_for(&commits[0], &run_id, 1),
    ));
    for seq in 1..=2 {
        history.add_local_activity_marker(
            seq,
            &seq.to_string(),
            Some(local_activity_result()),
            None,
            |d| d.activation_index = Some(u64::from(seq)),
        );
    }
    for (commit, boundary) in commits[1..].iter().zip([
        ExternalStreamBoundary::TaskCompleted,
        ExternalStreamBoundary::CommandsProduced,
    ]) {
        history.add_external_stream_marker_data(output_marker(
            boundary,
            manifest_for(commit, &run_id, 1),
        ));
    }
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();

    let (worker, _) = local_activity_worker(history, vec![2.into()], Default::default(), 0);
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(commits[0].clone())),
                schedule_local_activity(1),
                schedule_local_activity(2),
            ],
        ))
        .await
        .unwrap();
    let resolved_first = worker.poll_workflow_activation().await.unwrap();
    assert!(
        only_resolves_local_activity(&resolved_first, 1),
        "the second result was recorded for a later activation: {:?}",
        resolved_first.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            output_commit_command(replayed(commits[1].clone())),
        ))
        .await
        .unwrap();
    let resolved_second = worker.poll_workflow_activation().await.unwrap();
    assert!(only_resolves_local_activity(&resolved_second, 2));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(replayed(commits[2].clone())),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// A marker with our name whose details do not decode is a broken History, not someone else's
/// marker, so replay must say so instead of failing later on an unmatched marker.
#[tokio::test]
async fn an_external_stream_marker_that_does_not_decode_fails_the_task() {
    use temporalio_common::protos::temporal::api::{
        common::v1::Payloads, history::v1::MarkerRecordedEventAttributes,
    };
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add(MarkerRecordedEventAttributes {
        marker_name: EXTERNAL_STREAM_MARKER_NAME.to_string(),
        details: [(
            "external_stream".to_string(),
            Payloads {
                payloads: vec![Payload {
                    data: vec![0xff, 0xff, 0xff],
                    ..Default::default()
                }],
            },
        )]
        .into(),
        ..Default::default()
    });
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let failed = Arc::new(Notify::new());
    let notify = failed.clone();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [ResponseType::AllHistory],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(move |_, _, failure| {
        let matches = failure
            .as_ref()
            .is_some_and(|f| f.message.contains("External stream marker was unparsable"));
        if matches {
            notify.notify_one();
        }
        matches
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(Arc::new(RecordingStore::default()));
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);
    let _ = worker.poll_workflow_activation().await;
    failed_within_deadline(&failed).await;
    worker.drain_pollers_and_shutdown().await;
}

/// A worker on `store` that records the markers each completion carried, and what `store` held
/// when each completion was sent.
fn worker_staging_in(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    store: Arc<RecordingStore>,
    markers: RecordedMarkers,
    staged_at_completion: Arc<Mutex<Vec<Vec<String>>>>,
) -> crate::Worker {
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    let seen_store = store.clone();
    mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
        markers.lock().push(stream_marker_data(wft));
        staged_at_completion
            .lock()
            .push(seen_store.staged().into_iter().map(|b| b.token).collect());
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(store);
        w.task_types = WorkerTaskTypes {
            enable_local_activities: true,
            ..WorkerTaskTypes::workflow_only()
        };
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

fn marker_tokens(markers: &[Vec<ExternalStreamMarkerData>]) -> Vec<Vec<String>> {
    markers
        .iter()
        .map(|task| {
            task.iter()
                .map(|m| m.output.as_ref().unwrap().stage_token.clone())
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn a_commit_is_staged_before_its_completion_is_sent() {
    let (history, manifest) = output_then_timer_history(&["ab", "c"]);
    let run_id = manifest.run_id.clone();
    let store = Arc::new(RecordingStore::default());
    let markers: RecordedMarkers = Default::default();
    let staged_at_completion: Arc<Mutex<Vec<Vec<String>>>> = Default::default();
    let worker = worker_staging_in(
        history,
        vec![1.into(), 2.into()],
        store.clone(),
        markers.clone(),
        staged_at_completion.clone(),
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(records(&["ab", "c"])),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let tokens = marker_tokens(&markers.lock());
    assert_eq!(
        staged_at_completion.lock()[0],
        tokens[0],
        "the completion's output is in the store before the completion goes out"
    );
    let [staged] = store.staged().try_into().unwrap();
    assert_eq!(staged.token, tokens[0][0]);
    assert_eq!(staged.run_id, run_id);
    assert_eq!(staged.history_floor_event_id, 1);
    let chain = staged.chain.unwrap();
    assert_eq!(
        (chain.workflow_id.as_str(), chain.first_run_id.as_str()),
        ("fakeid", run_id.as_str())
    );
    let committed = records(&["ab", "c"]);
    for (staged, committed) in staged.records.iter().zip(&committed) {
        assert_eq!(staged.topic, TOPIC);
        let stored = StreamRecord::decode(staged.record.as_slice()).unwrap();
        assert_eq!(
            stored.body, committed.body,
            "the body stays as lang's codec left it"
        );
        assert_eq!(stored.kind, StreamRecordKind::Data as i32);
        assert_eq!(stored.metadata[RUN_ID_KEY].data, run_id.as_bytes());
        assert_eq!(
            stored.metadata[CONTENT_HASH_KEY].data,
            content_hash_text(&committed.content_hash).into_bytes()
        );
    }
}

#[tokio::test]
async fn a_store_that_refuses_the_stage_fails_the_task_and_sends_no_completion() {
    let store = Arc::new(RecordingStore::refusing(StreamError::storage(
        "redis is down",
    )));
    let failed = Arc::new(Notify::new());
    let notify = failed.clone();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(TimesRange::from(0));
    mock_cfg.expect_fail_wft_matcher = Box::new(move |_, cause, failure| {
        let matches = *cause == WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
            && failure.as_ref().is_some_and(|f| {
                f.message.contains("Could not stage") && f.message.contains("redis is down")
            });
        if matches {
            notify.notify_one();
        }
        matches
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.stream_store = Some(store);
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![
                output_commit_command(records(&["a"])),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_live_commit_on_a_worker_without_a_store_fails_the_task() {
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, _, failure| {
        failure
            .as_ref()
            .is_some_and(|f| f.message.contains("has no stream store"))
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![output_commit_command(records(&["a"]))],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn commits_around_a_local_activity_are_staged_in_commit_order() {
    let (history, _) = two_commits_around_a_local_activity_history();
    let store = Arc::new(RecordingStore::default());
    let markers: RecordedMarkers = Default::default();
    let staged_at_completion: Arc<Mutex<Vec<Vec<String>>>> = Default::default();
    let worker = worker_staging_in(
        history,
        vec![1.into(), 2.into()],
        store.clone(),
        markers.clone(),
        staged_at_completion.clone(),
    );
    let [first_commit, second_commit] = two_commits();

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(first_commit),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    let resolved = run_local_activity(&worker).await;
    assert!(only_resolves_local_activity(&resolved, 1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(second_commit),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let tokens = marker_tokens(&markers.lock());
    assert_eq!(tokens[0].len(), 2);
    assert_eq!(
        store
            .staged()
            .into_iter()
            .map(|b| (b.token, b.records[0].topic.clone()))
            .collect::<Vec<_>>(),
        vec![
            (tokens[0][0].clone(), TOPIC.to_string()),
            (tokens[0][1].clone(), "progress".to_string()),
        ]
    );
}

#[tokio::test]
async fn replay_stages_nothing() {
    let (history, _) = output_then_timer_history(&["a"]);
    let store = Arc::new(RecordingStore::default());
    let worker = worker_staging_in(
        history,
        vec![2.into()],
        store.clone(),
        Default::default(),
        Default::default(),
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(replayed(records(&["a"]))),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(store.staged().is_empty());
}

#[tokio::test]
async fn a_failed_task_publishes_nothing() {
    // The first activation commits and waits on a local activity. The second fails, so the task
    // fails before any completion reports the commit, and nothing reaches the store.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();
    let store = Arc::new(RecordingStore::default());
    let failed = Arc::new(Notify::new());
    let notify = failed.clone();
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [1], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(TimesRange::from(0));
    mock_cfg.expect_fail_wft_matcher = Box::new(move |_, _, failure| {
        let matches = failure
            .as_ref()
            .is_some_and(|f| f.message.contains("the workflow broke"));
        if matches {
            notify.notify_one();
        }
        matches
    });
    let mut mock = build_mock_pollers(mock_cfg);
    let worker_store = store.clone();
    mock.worker_cfg(|w| {
        w.stream_store = Some(worker_store);
        w.task_types = WorkerTaskTypes {
            enable_local_activities: true,
            ..WorkerTaskTypes::workflow_only()
        };
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(records(&["a"])),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
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
    assert!(store.staged().is_empty());
}

#[tokio::test]
async fn a_close_without_its_finish_fails_the_task() {
    let worker = worker_failing_with(
        canned_histories::single_timer("1"),
        "has no FINISH record in the same commit",
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    let commit = WorkflowOutputStreamCommit {
        records: records(&["a"]),
        closes: vec![OutputClose {
            topic: TOPIC.to_string(),
            result: Some(Payload::default()),
        }],
    };
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![workflow_command::Variant::WorkflowOutputStreamCommit(
                commit,
            )],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}
