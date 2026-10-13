//! External stream output commit: the records a completion commits, and the marker Core builds
//! from them and writes ahead of the completion's other commands.

use crate::{
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers, mock_worker,
        start_timer_cmd,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use temporalio_common::{
    protos::{
        constants::EXTERNAL_STREAM_MARKER_NAME,
        coresdk::{
            external_data::{
                ExternalOutputSegmentManifest, ExternalOutputStreamManifest,
                ExternalOutputTopicManifest, ExternalStreamBoundary, ExternalStreamMarkerData,
                extract_external_stream_marker_data,
            },
            workflow_activation::{WorkflowActivation, workflow_activation_job},
            workflow_commands::{
                CompleteWorkflowExecution, OutputClose, OutputRecord, WorkflowOutputStreamCommit,
                workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::{
            api::{
                command::v1::command,
                common::v1::Payload,
                enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
                workflowservice::v1::RespondWorkflowTaskCompletedResponse,
            },
            sdk::streams::v1::StreamRecordKind,
        },
    },
    streams::{FingerprintRecord, fingerprint},
    worker::WorkerTaskTypes,
};

/// Every completion's external stream markers, in the order the completions were reported.
type RecordedMarkers = Arc<Mutex<Vec<Vec<ExternalStreamMarkerData>>>>;

const TOPIC: &str = "results";

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
    values
        .iter()
        .map(|value| OutputRecord {
            topic: TOPIC.to_string(),
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

/// The manifest Core must build for `records`, computed here from the wire contract.
fn manifest_for(
    records: &[OutputRecord],
    run_id: &str,
    history_floor_event_id: i64,
) -> ExternalOutputStreamManifest {
    ExternalOutputStreamManifest {
        schema_version: 1,
        fingerprint_version: 2,
        stage_token: "recorded-stage-token".to_string(),
        history_floor_event_id,
        run_id: run_id.to_string(),
        topics: vec![ExternalOutputTopicManifest {
            topic: TOPIC.to_string(),
            record_count: records.len() as u32,
            logical_byte_count: records.iter().map(|r| r.logical_size).sum(),
            logical_fingerprint: fingerprint(records.iter().map(|r| FingerprintRecord {
                topic: TOPIC,
                kind: r.kind,
                content_hash: &r.content_hash,
            }))
            .to_vec(),
            finished: false,
        }],
        segments: vec![ExternalOutputSegmentManifest {
            record_counts_by_topic: vec![records.len() as u32],
        }],
        provider_id: String::new(),
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
