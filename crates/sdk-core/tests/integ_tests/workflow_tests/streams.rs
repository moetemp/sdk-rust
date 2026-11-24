use crate::common::{CoreWfStarter, integ_worker_config};
use futures_util::stream;
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_common::protos::{
    coresdk::{
        external_data::{StreamLedger, stream_read_result},
        workflow_activation::{WorkflowActivation, workflow_activation_job::Variant as Job},
        workflow_commands::{
            AppendToStream, CloseStream, CompleteWorkflowExecution, ReadStream,
            RequestCancelStreamRead, StartTimer, workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload, failure::v1::Failure, history::v1::history_event::Attributes,
    },
};
use temporalio_sdk_core::{
    StreamRuntimes, init_replay_worker, prost_dur,
    replay::{HistoryForReplay, ReplayWorkerInput},
    test_help::{
        WorkerTestHelpers,
        stream_store::{MemoryStore, Request},
    },
};
use temporalio_streams::{
    StreamRuntime, StreamStore,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CreateStreamRequest, Producer,
        StreamRef, producer::Kind,
    },
    record::StoreLimits,
};

const STORE: &str = "mem";

fn stream(id: &str) -> StreamRef {
    StreamRef {
        id: id.to_string(),
        store: STORE.to_string(),
    }
}

/// The Workflow of the test. It appends to `out`. Then it reads `in` twice and `quiet` once, and
/// starts a timer, so the task completes while the read of `quiet` waits for a Record. At the page
/// of the second read of `in`, it cancels the read of `quiet` and closes `out`. It completes at the
/// result of the close.
fn workflow(activation: &WorkflowActivation) -> Vec<Command> {
    let mut commands: Vec<Command> = vec![];
    for job in &activation.jobs {
        match &job.variant {
            Some(Job::InitializeWorkflow(_)) => {
                let body = Payload::from(b"hello".to_vec());
                let append = AppendToStream {
                    seq: 1,
                    stream: Some(stream("out")),
                    bodies: vec![body],
                };
                commands.push(append.into());
            }
            Some(Job::ResolveStreamAppend(_)) => {
                let read = |seq, id: &str| ReadStream {
                    seq,
                    stream: Some(stream(id)),
                    max_records: 2,
                    ..Default::default()
                };
                commands.push(read(1, "in").into());
                commands.push(read(2, "in").into());
                let waits = ReadStream {
                    wait: Some(prost_dur!(from_secs(60))),
                    ..read(3, "quiet")
                };
                commands.push(waits.into());
                // The Workflow completes long before the timer fires.
                let timer = StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(60))),
                };
                commands.push(timer.into());
            }
            Some(Job::ResolveStreamRead(read)) if read.seq == 2 => {
                commands.push(RequestCancelStreamRead { seq: 3 }.into());
                let close = CloseStream {
                    seq: 1,
                    stream: Some(stream("out")),
                };
                commands.push(close.into());
            }
            Some(Job::ResolveStreamClose(_)) => {
                commands.push(CompleteWorkflowExecution { result: None }.into());
            }
            _ => {}
        }
    }
    commands
}

/// Returns the results of Stream calls in `activation`, as text.
fn stream_jobs(activation: &WorkflowActivation) -> Vec<String> {
    let jobs = activation
        .jobs
        .iter()
        .filter_map(|job| job.variant.as_ref());
    jobs.filter(|job| {
        matches!(
            job,
            Job::ResolveStreamAppend(_) | Job::ResolveStreamRead(_) | Job::ResolveStreamClose(_)
        )
    })
    .map(|job| format!("{job:?}"))
    .collect()
}

/// Returns the entries of a ledger, as text.
fn entries(ledger: &StreamLedger) -> Vec<String> {
    let appends = ledger.appends.iter().map(|a| format!("append {}", a.seq));
    let intents = (ledger.close_intents.iter()).map(|i| format!("close intent {}", i.seq));
    let closes = ledger.closes.iter().map(|c| format!("close {}", c.seq));
    let reads = ledger.reads.iter().map(|read| match &read.result {
        Some(stream_read_result::Result::Page(page)) => {
            format!("read {} with a digest of {}", read.seq, page.record_count)
        }
        Some(stream_read_result::Result::Cancelled(())) => format!("read {} cancelled", read.seq),
        other => format!("read {}: {other:?}", read.seq),
    });
    appends.chain(intents).chain(closes).chain(reads).collect()
}

/// Returns a Store with the Streams `out`, `in` and `quiet`, and its runtime for a Worker. The
/// Stream `in` has three Records.
async fn store() -> (Arc<MemoryStore>, StreamRuntimes) {
    let store = Arc::new(MemoryStore::default());
    for id in ["out", "in", "quiet"] {
        let create = CreateStreamRequest {
            stream_ref: Some(stream(id)),
            ..Default::default()
        };
        store.create_stream(create, id.to_string()).await.unwrap();
    }
    let client = Kind::Client(ClientProducer {
        namespace: "default".to_string(),
        id: "test".to_string(),
    });
    let bodies: Vec<Payload> = (0..3u8).map(|at| Payload::from(vec![at])).collect();
    let fill = AppendRecordsRequest {
        r#ref: Some(stream("in")),
        producer: Some(Producer { kind: Some(client) }),
        records: (bodies.iter().cloned())
            .map(|body| AppendRecord { body: Some(body) })
            .collect(),
    };
    store.append_records(fill, String::new()).await.unwrap();
    let runtime = Arc::new(StreamRuntime::new(store.clone(), StoreLimits::default()));
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime)]);
    (store, runtimes)
}

// The attempt of a Stream call and the ledger of a Workflow Task depend on what the server does
// with a failed task and with a task that Core forces, which a mock server can't show.
#[tokio::test]
async fn stream_calls_write_as_the_servers_task_and_replay_from_its_ledgers() {
    let (store, runtimes) = store().await;
    let mut starter = CoreWfStarter::new("stream_calls_write_as_the_servers_task");
    // A task must not reach its heartbeat, which would split its results over two ledgers.
    starter.workflow_options.task_timeout = Some(Duration::from_secs(60));
    let of_worker = runtimes.clone();
    starter.set_core_cfg_mutator(move |config| config.stream_runtimes = of_worker.clone());
    let core = starter.get_core_worker().await;
    starter.start_wf().await;

    // The first attempt of task 1 fails once its append has a result, and the server sends the
    // task again.
    let mut failed = false;
    let mut live = vec![];
    loop {
        let activation = core.poll_workflow_activation().await.unwrap();
        let run_id = activation.run_id.clone();
        if activation.is_only_eviction() {
            let evicted = WorkflowActivationCompletion::empty(run_id);
            core.complete_workflow_activation(evicted).await.unwrap();
            continue;
        }
        let jobs = stream_jobs(&activation);
        if !failed && !jobs.is_empty() {
            failed = true;
            let failure = Failure::application_failure("The test fails this task".into(), false);
            let fails = WorkflowActivationCompletion::fail(run_id, failure, None);
            core.complete_workflow_activation(fails).await.unwrap();
            continue;
        }
        live.extend(jobs);
        let commands = workflow(&activation);
        let ends = matches!(commands[..], [Command::CompleteWorkflowExecution(_)]);
        let completion = WorkflowActivationCompletion::from_cmds(run_id, commands);
        core.complete_workflow_activation(completion).await.unwrap();
        if ends {
            break;
        }
    }
    core.handle_eviction().await;
    assert_eq!(live.len(), 5, "{live:?}");

    // History has a ledger on the completion of each Workflow Task with a result or an intent, so
    // the server kept the field. The task of the append completed for its timer while the reads
    // ran, so their results are in the ledger of the next task.
    let history = starter.get_history().await;
    let mut ledgers = vec![];
    // The attempt of each Workflow Task, by the id of its started event.
    let mut attempts = HashMap::new();
    let mut scheduled_attempt = 0;
    for event in &history.events {
        match event.attributes.as_ref() {
            Some(Attributes::WorkflowTaskScheduledEventAttributes(scheduled)) => {
                scheduled_attempt = scheduled.attempt;
            }
            Some(Attributes::WorkflowTaskStartedEventAttributes(_)) => {
                attempts.insert(event.event_id, scheduled_attempt);
            }
            Some(Attributes::WorkflowTaskCompletedEventAttributes(completed)) => {
                let metadata = completed.sdk_metadata.clone().unwrap_or_default();
                if !metadata.core_stream_calls.is_empty() {
                    let ledger = StreamLedger::decode(&metadata.core_stream_calls[..]).unwrap();
                    ledgers.push((event.event_id, entries(&ledger)));
                }
            }
            // The Workflow has no Local Activity and no patch, so a marker can only be Core's.
            Some(Attributes::MarkerRecordedEventAttributes(marker)) => {
                panic!("History has a marker: {marker:?}");
            }
            _ => {}
        }
    }
    let tasks: Vec<i64> = ledgers.iter().map(|(completed, _)| *completed).collect();
    assert!(tasks.windows(2).all(|pair| pair[0] < pair[1]), "{tasks:?}");
    let recorded: Vec<Vec<String>> = ledgers.into_iter().map(|(_, entries)| entries).collect();
    let expected = [
        vec!["append 1"],
        vec![
            "close intent 1",
            "read 1 with a digest of 2",
            "read 2 with a digest of 2",
            "read 3 cancelled",
        ],
        vec!["close 1"],
    ];
    assert_eq!(recorded, expected);

    // Each write has the attempt that the server gave its Workflow Task. The append ran in both
    // attempts of task 1, and the close ran as the task that recorded its intent.
    let writes: Vec<(i64, i32)> = (store.requests().iter())
        .filter_map(|request| match request {
            Request::Append(append, _) if append.r#ref == Some(stream("out")) => {
                append.producer.as_ref()
            }
            Request::Close(close) => close.producer.as_ref(),
            _ => None,
        })
        .map(|producer| match &producer.kind {
            Some(Kind::Workflow(workflow)) => {
                let attempt = workflow.attempt.expect("A Workflow call has its attempt");
                (attempt.started_event_id, attempt.attempt)
            }
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(writes.len(), 3, "{writes:?}");
    for (started, attempt) in &writes {
        assert_eq!(
            attempts.get(started),
            Some(attempt),
            "{writes:?} {attempts:?}"
        );
    }
    assert_eq!((writes[0].1, writes[1].1, writes[2].1), (1, 2, 1));
    assert!(writes[0].0 < writes[1].0 && writes[1].0 < writes[2].0);

    // A second Worker, which doesn't have the run, replays the History of the server. It reads
    // the Records of the digest from the Store, and gives Workflow code the jobs of the first run.
    let mut config = integ_worker_config(starter.get_task_queue());
    config.stream_runtimes = runtimes;
    let history = HistoryForReplay::new(history, starter.get_wf_id());
    let replayer = init_replay_worker(ReplayWorkerInput::new(config, stream::iter([history])));
    let replayer = replayer.expect("The replay Worker starts");
    let mut replayed = vec![];
    loop {
        let activation = replayer.poll_workflow_activation().await.unwrap();
        let run_id = activation.run_id.clone();
        if activation.is_only_eviction() {
            let evicted = WorkflowActivationCompletion::empty(run_id);
            replayer
                .complete_workflow_activation(evicted)
                .await
                .unwrap();
            break;
        }
        replayed.extend(stream_jobs(&activation));
        let completion = WorkflowActivationCompletion::from_cmds(run_id, workflow(&activation));
        (replayer.complete_workflow_activation(completion).await).unwrap();
    }
    assert_eq!(replayed, live);
}
