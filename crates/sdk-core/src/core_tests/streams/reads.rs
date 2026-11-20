//! The Stream reads of Workflow code, through a Worker on a mock server. These tests need the real
//! machinery of a Worker, such as the completion of a task while a read runs, or a replay.

use super::*;
use crate::worker::WorkflowErrorType;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::collections::HashSet;
use temporalio_common::protos::{
    constants::{LOCAL_ACTIVITY_MARKER_NAME, PATCH_MARKER_NAME},
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        workflow_activation::{FireTimer, ResolveStreamRead},
        workflow_commands::{SetPatchMarker, StartStreamRead, UpsertWorkflowSearchAttributes},
        workflow_completion::workflow_activation_completion::Status,
    },
    temporal::api::{
        common::v1::SearchAttributes, enums::v1::EventType, failure::v1::failure::FailureInfo,
    },
};
use temporalio_streams::{
    proto::{ClientProducer, CloseStreamRequest, ReadRecordsRequest, StreamRecord},
    workflow::peeked_marker,
};

fn client() -> Option<Producer> {
    Some(Producer {
        kind: Some(ProducerKind::Client(ClientProducer {
            namespace: NAMESPACE.to_string(),
            id: "test".to_string(),
        })),
    })
}

/// Appends `count` Records to the Stream `id` as a client. The data of each is its place in the
/// Stream.
async fn fill(store: &MemoryStore, id: &str, count: usize) {
    let had = store.records(id).len();
    let records = (had..had + count)
        .map(|at| AppendRecord {
            body: Some(Payload {
                data: vec![at as u8],
                ..Default::default()
            }),
        })
        .collect();
    let append = AppendRecordsRequest {
        stream_id: id.to_string(),
        producer: client(),
        records,
        ..Default::default()
    };
    store.append_records(append).await.unwrap();
}

/// Appends a Record whose body has `size` bytes to the Stream `id` as a client.
async fn append_sized(store: &MemoryStore, id: &str, size: usize) {
    let body = Payload {
        data: vec![0; size],
        ..Default::default()
    };
    let append = AppendRecordsRequest {
        stream_id: id.to_string(),
        producer: client(),
        records: vec![AppendRecord { body: Some(body) }],
        ..Default::default()
    };
    store.append_records(append).await.unwrap();
}

async fn close_as_client(store: &MemoryStore, id: &str) {
    let close = CloseStreamRequest {
        stream_id: id.to_string(),
        producer: client(),
    };
    store.close_stream(close).await.unwrap();
}

fn read(seq: u32, id: &str, idle: Duration, records_in_history: bool) -> Command {
    StartStreamRead {
        seq,
        stream: Some(stream(id)),
        from_offset: vec![],
        idle_timeout: Some(idle.try_into().unwrap()),
        records_in_history,
    }
    .into()
}

fn jobs_of(activation: &WorkflowActivation) -> Vec<Job> {
    (activation.jobs.iter())
        .map(|job| job.variant.clone().unwrap())
        .collect()
}

/// Returns the pages of reads in `activation`.
fn pages(activation: &WorkflowActivation) -> impl Iterator<Item = &ResolveStreamRead> {
    (activation.jobs.iter()).filter_map(|job| match &job.variant {
        Some(Job::ResolveStreamRead(read)) => Some(read),
        _ => None,
    })
}

fn ends(activation: &WorkflowActivation) -> bool {
    pages(activation).any(|read| read.end.is_some())
}

/// Returns the data of the last Record that `activation` gives, if it gives one.
fn last_data(activation: &WorkflowActivation) -> Option<u8> {
    let last = pages(activation).flat_map(|read| &read.records).last()?;
    last.body.as_ref()?.data.first().copied()
}

/// Returns the Records that each page of `jobs` gave, in order.
fn records_of(jobs: &[Vec<Job>]) -> Vec<StreamRecord> {
    (jobs.iter().flatten())
        .filter_map(|job| match job {
            Job::ResolveStreamRead(read) => Some(read.records.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// What makes the task complete while a read runs.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Completes {
    /// The heartbeat, while the read has a backlog that Workflow code takes slowly.
    AtTheHeartbeat,
    /// A timer that Workflow code starts at the first page, with the Records in History.
    ForATimer,
    /// The rejection of an Update while the read waits.
    ForAnUpdateRejection,
    /// The intent of a close.
    ForACloseIntent,
}

fn pages_of_five(wc: &mut WorkerConfig) {
    wc.stream_runtimes.settings.read_page_records = 5;
}

// A task that only reads hold completes at its heartbeat, and immediately for a command, a
// message or a close intent, which must not wait for a read.
#[rstest::rstest]
#[case(Completes::AtTheHeartbeat)]
#[case(Completes::ForATimer)]
#[case(Completes::ForAnUpdateRejection)]
#[case(Completes::ForACloseIntent)]
#[tokio::test]
async fn a_task_completes_while_a_read_runs(#[case] completes: Completes) {
    let store = store(&["s", "c"]).await;
    let backlog = if completes == Completes::AtTheHeartbeat {
        1000
    } else {
        12
    };
    fill(&store, "s", backlog).await;
    let mut opts = StartOpts::new(&store);
    opts.configure = Box::new(pages_of_five);
    if completes == Completes::ForAnUpdateRejection {
        // The task that the timer forces has the Update.
        opts.served = Some(Box::new(move |number, t| {
            let mut task = next_task(t);
            if number == 1 {
                task.add_update_request("u", t.current_event_id() - 1);
            }
            Some(task)
        }));
    }
    let task_timeout = match completes {
        Completes::AtTheHeartbeat => Duration::from_millis(200),
        _ => Duration::from_secs(10),
    };
    let run = start(history(task_timeout), opts);
    let workflow: Workflow = match completes {
        Completes::AtTheHeartbeat => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::ZERO, false)],
            _ if ends(activation) => vec![complete()],
            _ => vec![],
        },
        Completes::ForATimer => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::ZERO, true)],
            _ if ends(activation) => vec![complete()],
            _ if last_data(activation) == Some(4) => vec![timer(1)],
            _ => vec![],
        },
        Completes::ForAnUpdateRejection => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![read(1, "s", Duration::from_secs(60), false), timer(1)]
            }
            Some(Job::DoUpdate(_)) => {
                let rejected = update_response::Response::Rejected(Default::default());
                vec![update_response(activation, rejected)]
            }
            _ if ends(activation) => vec![complete()],
            _ => vec![],
        },
        Completes::ForACloseIntent => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![read(1, "s", Duration::ZERO, false), close(1, "c")]
            }
            _ if ends(activation) => vec![complete()],
            _ => vec![],
        },
    };
    // Workflow code takes its activations slowly, so that the backlog outlasts the heartbeat.
    let pause = match completes {
        Completes::AtTheHeartbeat => Duration::from_millis(2),
        _ => Duration::ZERO,
    };
    let drive = async {
        let mut jobs = vec![];
        while let Ok(activation) = run.worker.poll_workflow_activation().await {
            if activation.is_only_eviction() {
                let empty = WorkflowActivationCompletion::empty(activation.run_id);
                let _ = run.worker.complete_workflow_activation(empty).await;
                break;
            }
            jobs.push(
                activation
                    .jobs
                    .iter()
                    .map(|job| job.variant.clone().unwrap())
                    .collect(),
            );
            tokio::time::sleep(pause).await;
            respond(&run.worker, &activation, workflow).await;
            // Once the rejection went out, the read gets one more Record and the close.
            if let Some(Job::DoUpdate(_)) = &activation.jobs[0].variant {
                fill(&store, "s", 1).await;
                close_as_client(&store, "s").await;
            }
        }
        jobs
    };
    // Each early completion goes out with no wait for a heartbeat at 8 s.
    let live: Vec<Vec<Job>> = tokio::time::timeout(Duration::from_secs(5), drive)
        .await
        .expect("A task waited for its heartbeat");
    let sent = run.sent();
    let early = match completes {
        Completes::ForAnUpdateRejection => &sent[1],
        _ => &sent[0],
    };
    assert!(early.force_create_new_workflow_task, "{early:?}");
    let read_entry = marker(early).and_then(|marker| marker.reads.first().cloned());
    assert!(read_entry.as_ref().is_none_or(|read| read.end.is_none()));
    match completes {
        Completes::AtTheHeartbeat => {
            assert!(read_entry.is_some_and(|read| read.digest.len() == 16))
        }
        Completes::ForATimer => {
            assert_eq!(
                command_types(early)[..2],
                [CommandType::StartTimer, CommandType::RecordMarker]
            );
            assert!(read_entry.is_some_and(|read| read.records.len() == 5));
        }
        Completes::ForAnUpdateRejection => assert_eq!(early.messages.len(), 1),
        Completes::ForACloseIntent => {
            assert_eq!(marker(early).unwrap().close_intents.len(), 1);
        }
    }
    // Workflow code got each Record of the Stream once, in order, over several tasks.
    assert!(sent.len() > 1);
    assert_eq!(records_of(&live), store.records("s"));
    // A cached run that gets its whole History reads nothing for a replay, which would ask for
    // more than a live read does.
    let page_bytes = StreamSettings::default().read_ahead_bytes as i64;
    let reads = store.reads();
    assert!(reads.iter().all(|read| read.max_bytes <= page_bytes));
    run.worker.drain_pollers_and_shutdown_ref().await;
    // The mock server writes no Update, so that History doesn't replay.
    if completes == Completes::ForAnUpdateRejection {
        return;
    }
    // A replay gives the same jobs. With `records_in_history` it reads no Store.
    assert_eq!(run.replay(workflow).await, live);
    let replay_reads = store.reads().len() - reads.len();
    let in_history = completes == Completes::ForATimer;
    assert_eq!(replay_reads == 0, in_history, "{replay_reads}");
}

/// How History reaches a Worker that doesn't have the run.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// The poll has the whole History.
    FullHistory,
    /// The poll has the last task, and Core fetches the History from the start.
    CacheMiss,
    /// The poll has the whole History, and the bound of the Records ends the first update before
    /// the marker of the second read.
    AfterACut,
}

/// What a replay finds of the Records of its read.
#[derive(Clone, Copy, Debug)]
enum Replay {
    /// The Stream is gone, and the Worker fails the Workflow for it.
    Gone,
    /// The Stream is gone, and the Worker fails the Workflow only for nondeterminism.
    GoneWithTheNondeterminismOptIn,
    /// The Store gives no answer.
    StoreDown,
    /// The Worker has no runtime for the Store.
    NoRuntime,
}

fn fails_workflow(wc: &mut WorkerConfig) {
    wc.workflow_failure_errors = HashSet::from([WorkflowErrorType::StreamReplay]);
}

fn fails_workflow_for_nondeterminism(wc: &mut WorkerConfig) {
    wc.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
}

fn fails_workflow_with_its_store_down(wc: &mut WorkerConfig) {
    fails_workflow(wc);
    fast_tries(wc);
    wc.stream_runtimes.settings.replay_retry_bound = Duration::from_millis(100);
}

fn fails_workflow_with_no_runtime(wc: &mut WorkerConfig) {
    fails_workflow(wc);
    wc.stream_runtimes = StreamRuntimes::default();
}

fn fails_workflow_after_a_cut(wc: &mut WorkerConfig) {
    fails_workflow(wc);
    wc.stream_runtimes.settings.max_task_record_bytes = 1;
}

/// Reads Stream `r` in task 2 and Stream `s` in task 4, with a timer in each task, and completes
/// after timer 4.
fn read_in_the_second_and_fourth_tasks(activation: &WorkflowActivation) -> Vec<Command> {
    let read_ends = |seq| pages(activation).any(|read| read.seq == seq && read.end.is_some());
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::FireTimer(fired)) if fired.seq == 1 => vec![read(1, "r", Duration::ZERO, false)],
        Some(Job::FireTimer(fired)) if fired.seq == 2 => vec![timer(3)],
        Some(Job::FireTimer(fired)) if fired.seq == 3 => vec![read(2, "s", Duration::ZERO, false)],
        Some(Job::FireTimer(_)) => vec![complete()],
        _ if read_ends(1) => vec![timer(2)],
        _ if read_ends(2) => vec![timer(4)],
        _ => vec![],
    }
}

// A replay whose Records are gone fails the Workflow with the opt-in. A Store that gives no answer,
// or a Worker with no runtime for it, fails only the task, since another Worker may still read.
#[rstest::rstest]
#[case(Path::FullHistory, Replay::Gone)]
#[case(Path::CacheMiss, Replay::Gone)]
#[case(Path::AfterACut, Replay::Gone)]
#[case(Path::FullHistory, Replay::GoneWithTheNondeterminismOptIn)]
#[case(Path::CacheMiss, Replay::StoreDown)]
#[case(Path::FullHistory, Replay::NoRuntime)]
#[tokio::test]
async fn the_replay_rule_holds_on_each_history_path(#[case] path: Path, #[case] replay: Replay) {
    let store = store(&["r", "s"]).await;
    fill(&store, "r", 2).await;
    fill(&store, "s", 3).await;
    // The first run reads in two Workers. Each one replays the timers that History fired, and
    // reads in the last task.
    let mut t = history(Duration::from_secs(10));
    t.add_workflow_task_completed();
    let started = t.add_timer_started("1".to_string());
    t.add_timer_fired(started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    for seq in [1, 2] {
        let first = start(t, StartOpts::new(&store));
        loop {
            let activation = first.worker.poll_workflow_activation().await.unwrap();
            respond(
                &first.worker,
                &activation,
                read_in_the_second_and_fourth_tasks,
            )
            .await;
            if pages(&activation).any(|read| read.seq == seq && read.end.is_some()) {
                break;
            }
        }
        first.worker.drain_pollers_and_shutdown_ref().await;
        t = first.history.lock().clone();
        let events = t.get_full_history_info().unwrap().into_events();
        let started = (events.iter().rev())
            .find(|event| event.event_type() == EventType::TimerStarted)
            .unwrap();
        t.add_timer_fired(started.event_id, (2 * seq).to_string());
        if seq == 1 {
            t.add_full_wf_task();
            let started = t.add_timer_started("3".to_string());
            t.add_timer_fired(started, "3".to_string());
            t.add_workflow_task_scheduled_and_started();
        }
    }
    let whole = next_task(&mut t);
    match replay {
        Replay::Gone | Replay::GoneWithTheNondeterminismOptIn => store.forget("s"),
        Replay::StoreDown => store.fail(
            Op::Read,
            StreamError::store_unavailable("The Store is down"),
            usize::MAX,
        ),
        Replay::NoRuntime => {}
    }
    let mut opts = StartOpts::new(&store);
    let events = whole.history.clone().unwrap().events;
    opts.responses = vec![ResponseType::Raw(match path {
        Path::CacheMiss => {
            opts.pages = vec![page(&events)];
            let tasks = t.get_full_history_info().unwrap().wf_task_count();
            hist_to_poll_resp(&t, "fake_wf_id", ResponseType::OneTask(tasks)).resp
        }
        Path::FullHistory | Path::AfterACut => whole,
    })];
    opts.served = None;
    let configure: fn(&mut WorkerConfig) = match (path, replay) {
        (Path::AfterACut, _) => fails_workflow_after_a_cut,
        (_, Replay::Gone) => fails_workflow,
        (_, Replay::GoneWithTheNondeterminismOptIn) => fails_workflow_for_nondeterminism,
        (_, Replay::StoreDown) => fails_workflow_with_its_store_down,
        (_, Replay::NoRuntime) => fails_workflow_with_no_runtime,
    };
    opts.configure = Box::new(configure);
    let fails_the_workflow = matches!(replay, Replay::Gone);
    opts.fails = usize::from(!fails_the_workflow);
    let second = start(t, opts);
    second.drive(read_in_the_second_and_fourth_tasks).await;
    second.worker.drain_pollers_and_shutdown_ref().await;
    let failed = second.failed.lock().clone();
    if fails_the_workflow {
        assert_eq!(failed, []);
        let sent = second.sent();
        let commands = &sent.last().unwrap().commands;
        let attributes = commands
            .last()
            .and_then(|command| command.attributes.as_ref());
        let Some(Attributes::FailWorkflowExecutionCommandAttributes(fail)) = attributes else {
            panic!("{commands:?}");
        };
        let info = fail.failure.as_ref().and_then(|f| f.failure_info.as_ref());
        assert!(
            matches!(info, Some(FailureInfo::ApplicationFailureInfo(info))
                if info.r#type == "StreamReplayError"),
            "{fail:?}"
        );
    } else {
        assert!(second.sent().is_empty());
        let [(cause, message)] = &failed[..] else {
            panic!("{failed:?}");
        };
        assert_eq!(
            *cause,
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        );
        let reason = match replay {
            Replay::StoreDown => "The Store is unavailable",
            Replay::NoRuntime => "This Worker has no Stream runtime",
            _ => "The Store doesn't have the 3 Records",
        };
        assert!(message.contains(reason), "{message}");
    }
}

/// Who takes the task that the shutdown forces.
#[derive(Clone, Copy, Debug, PartialEq)]
enum After {
    /// Another Worker, which goes on with the read from History.
    AnotherWorker,
    /// The same Worker, through a poll that was still open.
    TheSameWorker,
}

/// Reads Stream `s`, and completes once it got the Record with the data 4.
fn read_until_the_fifth_record(activation: &WorkflowActivation) -> Vec<Command> {
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::from_secs(60), false)],
        _ if last_data(activation) == Some(4) || ends(activation) => vec![complete()],
        _ => vec![],
    }
}

// The shutdown forces a new task on the normal queue, so another Worker goes on with the read from
// History. A task that comes back to this Worker starts no read.
#[rstest::rstest]
#[case(After::AnotherWorker)]
#[case(After::TheSameWorker)]
#[tokio::test]
async fn a_shutdown_ends_a_task_that_only_reads_hold(#[case] after: After) {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let t = history(Duration::from_secs(10));
    let mut opts = StartOpts::new(&store);
    opts.served = None;
    if after == After::TheSameWorker {
        let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
        tasks.send(task_of(&t)).unwrap();
        opts.tasks = Some(receiver);
        let back: NextTask = Box::new(|number, t| (number == 1).then(|| next_task(t)));
        opts.during_report = Some((tasks, back));
    }
    let run = start(t, opts);
    let workflow = read_until_the_fifth_record;
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, workflow).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(last_data(&page), Some(2));
    respond(&run.worker, &page, workflow).await;
    wait_until(|| store.reads().len() >= 2).await;
    let reads = store.reads().len();
    // The shutdown would wait for the heartbeat at 8 s otherwise.
    tokio::time::timeout(
        Duration::from_secs(5),
        run.worker.drain_pollers_and_shutdown_ref(),
    )
    .await
    .expect("The shutdown waited for the read");
    let sent = run.sent();
    for completion in &sent {
        assert!(completion.force_create_new_workflow_task);
        assert!(!completion.return_new_workflow_task && completion.sticky_attributes.is_none());
    }
    let first_marker = marker(&sent[0]).unwrap();
    assert!(first_marker.reads[0].end.is_none(), "{first_marker:?}");
    if after == After::TheSameWorker {
        // The task came back, and its completion forced one more task with no new read.
        assert_eq!(sent.len(), 2);
        assert_eq!(store.reads().len(), reads);
        return;
    }
    assert_eq!(sent.len(), 1);
    fill(&store, "s", 2).await;
    let mut t = run.history.lock().clone();
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::Raw(next_task(&mut t))];
    let next = start(t, opts);
    let jobs = next.drive(workflow).await;
    // The next Worker replays the first page, and reads only the Records after it.
    assert_eq!(records_of(&jobs), store.records("s"));
    next.worker.drain_pollers_and_shutdown_ref().await;
}

// The cached run has no Stream call, and another Worker read Stream `s` in task 3.
#[tokio::test]
async fn a_cached_run_with_no_call_replays_the_read_of_another_worker() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::FireTimer(fired)) if fired.seq == 1 => vec![timer(2)],
        Some(Job::FireTimer(_)) => vec![read(1, "s", Duration::ZERO, false)],
        Some(Job::SignalWorkflow(_)) => vec![complete()],
        _ => vec![],
    };
    let mut t = history(Duration::from_secs(10));
    for seq in ["1", "2"] {
        t.add_workflow_task_completed();
        let started = t.add_timer_started(seq.to_string());
        t.add_timer_fired(started, seq.to_string());
        t.add_workflow_task_scheduled_and_started();
    }
    let other = start(t, StartOpts::new(&store));
    loop {
        let activation = other.worker.poll_workflow_activation().await.unwrap();
        respond(&other.worker, &activation, workflow).await;
        if ends(&activation) {
            break;
        }
    }
    other.worker.drain_pollers_and_shutdown_ref().await;
    let mut t = other.history.lock().clone();
    t.add_we_signaled("done", vec![]);
    let fourth = next_task(&mut t);
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::ToTaskNum(1), ResponseType::Raw(fourth)];
    opts.served = None;
    let run = start(t, opts);
    assert_eq!(run.drive(workflow).await.len(), 1);
    let replayed = run.drive(workflow).await;
    assert_eq!(records_of(&replayed), store.records("s"));
    assert!(matches!(
        replayed.last().unwrap()[..],
        [Job::SignalWorkflow(_)]
    ));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// Reads Stream `s`, and starts a timer at each page, which completes the task while the read goes
/// on, so each task gives one page. Once the read ends it starts timer 1, and it completes once
/// timer 1 fires.
fn read_with_a_timer_at_each_page(activation: &WorkflowActivation) -> Vec<Command> {
    // The offset of a page's last Record gives its timer a seq of its own.
    let seq = |record: &StreamRecord| u64::from_be_bytes(record.offset[..].try_into().unwrap());
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::ZERO, false)],
        Some(Job::FireTimer(_)) => vec![complete()],
        _ if ends(activation) => vec![timer(1)],
        _ => (pages(activation).filter_map(|page| page.records.last()))
            .map(|last| timer(seq(last) as u32))
            .collect(),
    }
}

/// Runs `read_with_a_timer_at_each_page` on a Worker that `configure` sets, until its read ends.
/// Returns the jobs of each activation, and the History that the run wrote, to which it adds the
/// firing of timer 1 and the next task. The next task comes last, with the whole History.
async fn first_run_with_a_timer_at_each_page(
    store: &Arc<MemoryStore>,
    configure: fn(&mut WorkerConfig),
) -> (
    Vec<Vec<Job>>,
    TestHistoryBuilder,
    PollWorkflowTaskQueueResponse,
) {
    let mut opts = StartOpts::new(store);
    opts.configure = Box::new(configure);
    let first = start(history(Duration::from_secs(10)), opts);
    let mut live = vec![];
    loop {
        let activation = first.worker.poll_workflow_activation().await.unwrap();
        live.push(jobs_of(&activation));
        respond(&first.worker, &activation, read_with_a_timer_at_each_page).await;
        if ends(&activation) {
            break;
        }
    }
    first.worker.drain_pollers_and_shutdown_ref().await;
    let mut t = first.history.lock().clone();
    let events = t.get_full_history_info().unwrap().into_events();
    let started = (events.iter().rev())
        .find(|event| event.event_type() == EventType::TimerStarted)
        .unwrap();
    t.add_timer_fired(started.event_id, "1".to_string());
    let whole = next_task(&mut t);
    (live, t, whole)
}

/// Returns the offset that the read entry of each marker of `t` with Records starts after, and the
/// bytes of its Records in Stream `s`.
fn markers_of(t: &TestHistoryBuilder, store: &MemoryStore) -> Vec<(Vec<u8>, usize)> {
    let records = store.records("s");
    let events = t.get_full_history_info().unwrap().into_events();
    (events.iter())
        .filter_map(|event| peeked_marker(event)?.ok())
        .map(|marker| {
            let entry = &marker.reads[0];
            let count: u32 = entry.page_records.iter().sum();
            let bytes = (records.iter())
                .filter(|record| record.offset > entry.after_offset)
                .take(count as usize)
                .map(Message::encoded_len)
                .sum();
            (entry.after_offset.clone(), bytes)
        })
        .filter(|(_, bytes)| *bytes > 0)
        .collect()
}

/// Checks the reads of a replay of `markers`. The reads before an activation are those of an
/// update, which reads the next markers in order and holds at most the default
/// `max_task_record_bytes` of Records. The replay reads each marker once. Returns the number of
/// markers that each update reads.
fn check_updates(
    seed: u64,
    markers: &[(Vec<u8>, usize)],
    reads: &[Vec<ReadRecordsRequest>],
) -> Vec<usize> {
    let starts = |reads: &[ReadRecordsRequest], after: &[u8]| {
        (reads.iter())
            .filter(|read| read.from_offset == after)
            .count()
    };
    for (after, _) in markers {
        assert_eq!(starts(&reads.concat(), after), 1, "seed {seed}: {after:?}");
    }
    let bound = crate::StreamSettings::default().max_task_record_bytes;
    let (mut next, mut updates) = (0, vec![]);
    for update in reads.iter().filter(|reads| !reads.is_empty()) {
        let read = (markers[next..].iter())
            .take_while(|(after, _)| starts(update, after) > 0)
            .count();
        assert!(read > 0, "seed {seed}: marker {next}");
        let held: usize = markers[next..next + read].iter().map(|(_, b)| b).sum();
        assert!(held <= bound, "seed {seed}: {held}");
        next += read;
        updates.push(read);
    }
    assert_eq!(next, markers.len(), "seed {seed}");
    updates
}

/// Replays `t` from the poll `task` on a Worker that `configure` sets, with the rest of its
/// History on `pages`. Returns the jobs of each activation, and the reads of the replay before
/// each activation.
async fn replay_with_reads(
    store: &Arc<MemoryStore>,
    t: &TestHistoryBuilder,
    task: PollWorkflowTaskQueueResponse,
    pages: Vec<GetWorkflowExecutionHistoryResponse>,
    configure: fn(&mut WorkerConfig),
) -> (Vec<Vec<Job>>, Vec<Vec<ReadRecordsRequest>>) {
    let mut opts = StartOpts::new(store);
    opts.responses = vec![ResponseType::Raw(task)];
    opts.pages = pages;
    opts.configure = Box::new(configure);
    let run = start(t.clone(), opts);
    let (mut jobs, mut reads, mut seen) = (vec![], vec![], store.reads().len());
    while let Ok(activation) = run.worker.poll_workflow_activation().await {
        let all = store.reads();
        reads.push(all[seen..].to_vec());
        seen = all.len();
        if activation.is_only_eviction() {
            let empty = WorkflowActivationCompletion::empty(activation.run_id);
            let _ = run.worker.complete_workflow_activation(empty).await;
            break;
        }
        jobs.push(jobs_of(&activation));
        respond(&run.worker, &activation, read_with_a_timer_at_each_page).await;
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    (jobs, reads)
}

// An update ends before the task of the marker whose Records would pass `max_task_record_bytes`,
// and the replay reads the Records of each entry once.
#[tokio::test]
async fn a_replay_holds_at_most_16_mib_of_records_for_an_update() {
    let mut most_cuts = 0;
    for seed in 0..3 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s"]).await;
        // Records of up to 1.5 MiB, with more than two updates of them.
        let total = rng.random_range(34 << 20..=48 << 20);
        let mut filled = 0;
        while filled < total {
            let size = match rng.random_bool(0.9) {
                true => rng.random_range(256 << 10..=1536 << 10),
                false => rng.random_range(1..1000),
            };
            append_sized(&store, "s", size).await;
            filled += size;
        }
        let (live, t, whole) = first_run_with_a_timer_at_each_page(&store, |_| {}).await;
        let markers = markers_of(&t, &store);
        let events = whole.history.clone().unwrap().events;
        // A sticky poll of a Worker that doesn't have the run, which fetches the History.
        let tasks = t.get_full_history_info().unwrap().wf_task_count();
        let sticky = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::OneTask(tasks)).resp;
        for (task, pages) in [(whole, vec![]), (sticky, vec![page(&events)])] {
            let (jobs, reads) = replay_with_reads(&store, &t, task, pages, |_| {}).await;
            assert_eq!(jobs[..live.len()], live[..], "seed {seed}");
            let fired = vec![Job::FireTimer(FireTimer { seq: 1 })];
            assert_eq!(jobs[live.len()..], [fired], "seed {seed}");
            // Each update after the first comes from a cut.
            let updates = check_updates(seed, &markers, &reads);
            most_cuts = most_cuts.max(updates.len() - 1);
        }
    }
    // A replay ends more than one update early.
    assert!(most_cuts > 1, "{most_cuts}");
}

/// What Workflow code does at a page of its read, besides the timer that completes the task.
#[derive(Clone, Copy, Debug)]
enum AtAPage {
    Nothing,
    Patch,
    Append,
    LocalActivity,
}

impl AtAPage {
    /// Returns what the Workflow code of `seed` does at the page whose last Record has the offset
    /// `seq`. The first run and each replay decide the same way.
    fn of(seed: u64, seq: u32) -> Self {
        let mut rng = StdRng::seed_from_u64(seed << 32 | u64::from(seq));
        [
            Self::Nothing,
            Self::Patch,
            Self::Append,
            Self::LocalActivity,
        ][rng.random_range(0..4)]
    }
}

/// What the first run of a seed does. The seed decides all of it, and the first run lets one
/// result in at a time, so two runs of a seed write the same History.
#[derive(Clone, Debug)]
struct Plan {
    /// The Workflow waits for a Signal in its second task before it reads, so its first task
    /// completes with no command.
    waits_for_a_signal: bool,
    /// What Workflow code does at each page, by the offset of the page's last Record.
    at_pages: HashMap<u32, AtAPage>,
    /// The pages, counted from 1, whose Workflow Task starts with an Update. Each page has a task
    /// of its own, and the first one comes with the poll, so it has no Update.
    updates: HashSet<usize>,
}

/// Returns the Workflow code of `plan`. It reads Stream `s`, and at each page it starts a timer,
/// which completes the task while the read goes on, and does what the plan says. It accepts and
/// completes each Update, starts timer 1 once the read ends, and completes once timer 1 fires.
fn reads_with_peeked_results(plan: Plan) -> impl FnMut(&WorkflowActivation) -> Vec<Command> {
    let mut notified = HashSet::new();
    let mut updates = 0;
    move |activation| {
        for job in &activation.jobs {
            if let Some(Job::NotifyHasPatch(patch)) = &job.variant {
                notified.insert(patch.patch_id.clone());
            }
        }
        let mut commands = vec![];
        for job in &activation.jobs {
            match &job.variant {
                Some(Job::InitializeWorkflow(_)) if !plan.waits_for_a_signal => {
                    commands.push(read(1, "s", Duration::ZERO, false));
                }
                Some(Job::SignalWorkflow(_)) => commands.push(read(1, "s", Duration::ZERO, false)),
                Some(Job::DoUpdate(update)) => {
                    let respond = |response| {
                        let id = update.protocol_instance_id.clone();
                        Command::from(UpdateResponse {
                            protocol_instance_id: id,
                            response: Some(response),
                        })
                    };
                    commands.push(respond(update_response::Response::Accepted(())));
                    let done = update_response::Response::Completed(Payload::default());
                    commands.push(respond(done));
                    // The Local Activity keeps the task open, so the task's page comes in it too.
                    updates += 1;
                    commands.push(local_activity(updates));
                }
                Some(Job::FireTimer(_)) => commands.push(complete()),
                Some(Job::ResolveStreamRead(page)) if page.end.is_some() => {
                    commands.push(timer(1));
                }
                Some(Job::ResolveStreamRead(page)) => {
                    let last = page
                        .records
                        .last()
                        .expect("A page that goes on has a Record");
                    let seq = u64::from_be_bytes(last.offset[..].try_into().unwrap()) as u32;
                    commands.push(timer(seq));
                    match plan.at_pages[&seq] {
                        AtAPage::Nothing => {}
                        // A replay patches only once History told it of the patch, as lang does.
                        AtAPage::Patch => {
                            let patch_id = format!("p{seq}");
                            if !activation.is_replaying || notified.contains(&patch_id) {
                                let deprecated = false;
                                commands.push(
                                    SetPatchMarker {
                                        patch_id,
                                        deprecated,
                                    }
                                    .into(),
                                );
                            }
                        }
                        AtAPage::Append => commands.push(append(seq, "a")),
                        AtAPage::LocalActivity => commands.push(local_activity(seq)),
                    }
                }
                _ => {}
            }
        }
        commands
    }
}

/// Returns the name of each job of each activation, sorted, without the patches that only a
/// replay gets.
fn job_names(jobs: &[Vec<Job>]) -> Vec<Vec<String>> {
    let name = |job: &Job| match job {
        Job::NotifyHasPatch(_) => None,
        Job::DoUpdate(update) => Some(format!("update {}", update.id)),
        Job::ResolveActivity(result) => Some(format!("local activity {}", result.seq)),
        other => Some(format!("{other:?}")),
    };
    (jobs.iter())
        .map(|jobs| {
            let mut names: Vec<String> = jobs.iter().filter_map(name).collect();
            names.sort();
            names
        })
        .collect()
}

/// Lets the Store answer the next read, once the reader asked for it.
async fn release_read(store: &MemoryStore, released: &mut usize) {
    wait_until(|| store.reads().len() > *released).await;
    *released += 1;
    store.release();
    store.hold(Op::Read);
}

/// Runs the Workflow of `plan` until its read ends, and returns the jobs of each activation, and
/// the History with the firing of timer 1. The Signal that a Workflow may wait for is in the
/// History of its first poll.
///
/// A page, the result of an append and the result of a Local Activity would race each other. So
/// the run waits for the one result that can come next, and lets the Store answer a read, or
/// completes a Local Activity, only when nothing else can come.
async fn first_run_with_peeked_results(
    plan: &Plan,
    store: &Arc<MemoryStore>,
    page_records: usize,
) -> (Vec<Vec<Job>>, TestHistoryBuilder) {
    let mut opts = StartOpts::new(store);
    opts.configure =
        Box::new(move |wc| wc.stream_runtimes.settings.read_page_records = page_records);
    let updates = plan.updates.clone();
    // The task after completion `number` is the one of page `number + 1`.
    opts.served = Some(Box::new(move |number, t| {
        let mut task = next_task(t);
        if updates.contains(&(number + 1)) {
            task.add_update_request(format!("u{number}"), task.started_event_id - 1);
        }
        Some(task)
    }));
    let mut t = history(Duration::from_secs(10));
    if plan.waits_for_a_signal {
        t.add_workflow_task_completed();
        t.add_we_signaled("go", vec![]);
        t.add_workflow_task_scheduled_and_started();
    }
    store.hold(Op::Read);
    let run = start(t, opts);
    let mut workflow = reads_with_peeked_results(plan.clone());
    let mut live = vec![];
    let (mut flags, mut released, mut page) = (0, 0, 1);
    // Whether the read runs and the task in hand had its page, and the appends and Local
    // Activities with no result yet.
    let (mut reading, mut paged, mut appends, mut local_activities) = (false, false, 0, 0);
    loop {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        live.push(jobs_of(&activation));
        let commands = workflow(&activation);
        paged |= pages(&activation).next().is_some();
        let ended = pages(&activation).any(|page| page.end.is_some());
        for job in &live[live.len() - 1] {
            match job {
                Job::ResolveStreamAppend(_) => appends -= 1,
                Job::ResolveActivity(_) => local_activities -= 1,
                _ => {}
            }
        }
        for command in &commands {
            match command {
                Command::StartStreamRead(_) => reading = true,
                Command::AppendToStream(_) => appends += 1,
                Command::ScheduleLocalActivity(_) => local_activities += 1,
                _ => {}
            }
        }
        let mut completion =
            WorkflowActivationCompletion::from_cmds(activation.run_id.clone(), commands);
        // Each page uses a new lang flag, so each completion has a flag that the ones before it
        // don't have.
        if pages(&activation).next().is_some()
            && let Some(Status::Successful(success)) = completion.status.as_mut()
        {
            flags += 1;
            success.used_internal_flags.push(flags);
        }
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
        if ended {
            break;
        }
        // The Store answers an append immediately, and before the read starts the next
        // activation is the Signal.
        if appends > 0 || !reading {
            continue;
        }
        if !paged {
            release_read(store, &mut released).await;
        } else if local_activities > 0 {
            let task = run.worker.poll_activity_task().await.unwrap();
            let completion = ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(Payload::default())),
            };
            run.worker.complete_activity_task(completion).await.unwrap();
        } else {
            // The task completes, and the next one starts with its Update or waits for its page.
            (page, paged) = (page + 1, false);
            if !plan.updates.contains(&page) {
                release_read(store, &mut released).await;
            }
        }
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    store.release();
    let mut t = run.history.lock().clone();
    let events = t.get_full_history_info().unwrap().into_events();
    let started = (events.iter().rev())
        .find(|event| event.event_type() == EventType::TimerStarted)
        .unwrap();
    t.add_timer_fired(started.event_id, "1".to_string());
    (live, t)
}

/// Returns what the machines peek in the completion of the event `completed` of `events`.
fn peeked_in(events: &[HistoryEvent], completed: i64) -> Vec<&'static str> {
    let completion = (events.iter())
        .skip_while(|event| event.event_id <= completed)
        .take_while(|event| event.event_type() != EventType::WorkflowTaskScheduled);
    completion
        .filter_map(|event| match &event.attributes {
            Some(history_event::Attributes::MarkerRecordedEventAttributes(marker)) => {
                match marker.marker_name.as_str() {
                    LOCAL_ACTIVITY_MARKER_NAME => Some("local activity"),
                    PATCH_MARKER_NAME => Some("patch"),
                    _ => Some("stream"),
                }
            }
            Some(history_event::Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(_)) => {
                Some("update")
            }
            _ => None,
        })
        .collect()
}

fn disable_chunking(t: &mut TestHistoryBuilder) {
    let flag = CoreInternalFlags::DisableWorkflowTaskChunking as u32;
    let first = (t.get_full_history_info().unwrap().into_events().iter())
        .find_map(|event| match &event.attributes {
            Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(a)) => {
                a.sdk_metadata.clone()
            }
            _ => None,
        })
        .unwrap_or_default();
    let core: Vec<CoreInternalFlags> = CoreInternalFlags::all_except_too_high()
        .filter(|known| {
            let known = *known as u32;
            known == flag || first.core_used_flags.contains(&known)
        })
        .collect();
    t.set_flags_first_wft(&core, &first.lang_used_flags);
}

fn recorded_markers(events: &[HistoryEvent]) -> Vec<(u64, i64)> {
    (events.iter())
        .filter_map(|event| {
            let marker = peeked_marker(event)?.ok()?;
            let recorded = marker.reads.iter().map(|read| read.record_bytes).sum();
            let Some(history_event::Attributes::MarkerRecordedEventAttributes(attrs)) =
                &event.attributes
            else {
                return None;
            };
            Some((recorded, attrs.workflow_task_completed_event_id))
        })
        .filter(|(recorded, _)| *recorded > 0)
        .collect()
}

type Poll = (
    PollWorkflowTaskQueueResponse,
    Vec<GetWorkflowExecutionHistoryResponse>,
);

fn poll_for_last_task(
    t: &TestHistoryBuilder,
    whole: PollWorkflowTaskQueueResponse,
    path: Path,
) -> Poll {
    match path {
        Path::CacheMiss => {
            let events = whole.history.unwrap().events;
            let tasks = t.get_full_history_info().unwrap().wf_task_count();
            let sticky = hist_to_poll_resp(t, "fake_wf_id", ResponseType::OneTask(tasks)).resp;
            (sticky, vec![page(&events)])
        }
        Path::FullHistory | Path::AfterACut => (whole, vec![]),
    }
}

#[derive(Debug, PartialEq)]
struct Replayed {
    jobs: Vec<Vec<String>>,
    flags: Vec<Vec<u32>>,
}

async fn replay_with_bound(
    store: &Arc<MemoryStore>,
    t: &TestHistoryBuilder,
    (task, pages): &Poll,
    bound: usize,
    label: String,
    mut workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
) -> Replayed {
    let mut opts = StartOpts::new(store);
    opts.responses = vec![ResponseType::Raw(task.clone())];
    opts.pages = pages.clone();
    opts.configure = Box::new(move |wc| wc.stream_runtimes.settings.max_task_record_bytes = bound);
    opts.label = label.clone();
    let replay = start(t.clone(), opts);
    let mut flags = vec![];
    let flagged = |activation: &WorkflowActivation| {
        flags.push(activation.available_internal_flags.clone());
        workflow(activation)
    };
    let jobs = tokio::time::timeout(Duration::from_secs(10), replay.drive(flagged))
        .await
        .unwrap_or_else(|_| panic!("{label}: the replay hung"));
    replay.worker.drain_pollers_and_shutdown_ref().await;
    Replayed {
        jobs: job_names(&jobs),
        flags,
    }
}

// The completion right after a cut has what the machines peek while they apply the task before
// it, so the cut replay must give the same jobs and lang flags as the replay with no cut.
#[tokio::test]
async fn a_replay_gives_the_jobs_of_the_first_run_wherever_a_cut_ends_an_update() {
    let (mut positions, mut peeked) = (HashMap::new(), HashMap::new());
    let mut chunked_after_no_command = 0;
    for seed in 0..40 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s", "a"]).await;
        let page_records = rng.random_range(1..=3);
        // At least four pages, so that a cut can fall after the first, a middle or the
        // next-to-last marker.
        fill(&store, "s", rng.random_range(3 * page_records + 1..=20)).await;
        let records = store.records("s");
        let pages: Vec<u32> = (records.chunks(page_records))
            .map(|page| u64::from_be_bytes(page[page.len() - 1].offset[..].try_into().unwrap()))
            .map(|seq| seq as u32)
            .collect();
        // The seed picks the place of the cut, and what the completion of the marker before it
        // has. That marker is the one of page `cut`, and the task of the first page has no Update.
        let kind = seed as usize % 4;
        let cut = match (seed / 4) % 3 {
            0 if kind != 3 => 1,
            0 | 1 => rng.random_range(2..pages.len() - 1),
            _ => pages.len() - 1,
        };
        let mut at_pages: HashMap<u32, AtAPage> = (pages.iter())
            .map(|seq| (*seq, AtAPage::of(seed, *seq)))
            .collect();
        let mut updates: HashSet<usize> =
            (2..=pages.len()).filter(|_| rng.random_bool(0.3)).collect();
        if kind == 3 {
            updates.insert(cut);
        } else {
            let at = [AtAPage::Patch, AtAPage::LocalActivity, AtAPage::Append][kind];
            at_pages.insert(pages[cut - 1], at);
        }
        let plan = Plan {
            waits_for_a_signal: seed % 2 == 1,
            at_pages,
            updates,
        };
        let (live, mut t) = first_run_with_peeked_results(&plan, &store, page_records).await;
        // A chunked replay of a Workflow that waits for a Signal ends its first update after a
        // sequence with no command when the cut is after the first marker, as in seed 1.
        let chunking = (seed / 2) % 2 == 0;
        if !chunking {
            disable_chunking(&mut t);
        }
        let whole = next_task(&mut t);
        let events = whole.history.clone().unwrap().events;
        let markers = recorded_markers(&events);
        assert_eq!(markers.len(), pages.len(), "seed {seed}: {markers:?}");
        // The first update takes the markers before `cut`, and ends before the task of that one.
        let bound = markers[..cut]
            .iter()
            .map(|(recorded, _)| recorded)
            .sum::<u64>() as usize;
        let position = match cut {
            1 => "after the first marker",
            _ if cut == markers.len() - 1 => "before the last marker",
            _ => "after a middle marker",
        };
        *positions.entry(position).or_insert(0) += 1;
        for kind in peeked_in(&events, markers[cut - 1].1) {
            *peeked.entry(kind).or_insert(0) += 1;
        }
        // The update ends with the sequence before the lookahead, which has the completion two
        // before the one of the unread marker.
        let completions: Vec<i64> = (events.iter())
            .filter(|event| event.event_type() == EventType::WorkflowTaskCompleted)
            .map(|event| event.event_id)
            .collect();
        let unread = completions
            .iter()
            .position(|id| *id == markers[cut].1)
            .unwrap();
        let no_command = unread >= 2
            && (events.iter())
                .skip_while(|event| event.event_id <= completions[unread - 2])
                .take_while(|event| event.event_type() != EventType::WorkflowTaskScheduled)
                .all(|event| !event.is_command_event());
        if chunking && no_command {
            chunked_after_no_command += 1;
        }
        let path = match rng.random_bool(0.5) {
            true => Path::FullHistory,
            false => Path::CacheMiss,
        };
        let poll = poll_for_last_task(&t, whole, path);
        let mut replays = vec![];
        for bound in [usize::MAX, bound] {
            let label = format!("seed {seed}, bound {bound}");
            let workflow = reads_with_peeked_results(plan.clone());
            replays.push(replay_with_bound(&store, &t, &poll, bound, label, workflow).await);
        }
        // With chunking, a replay gives the results of a task in fewer activations than the first
        // run did, so only the replay with no cut tells what a cut must give.
        let first_run = match chunking {
            true => replays[0].jobs.clone(),
            false => {
                let fired = vec![Job::FireTimer(FireTimer { seq: 1 })];
                job_names(&[live, vec![fired]].concat())
            }
        };
        let [uncut, cut] = [&replays[0], &replays[1]];
        assert_eq!([&uncut.jobs, &cut.jobs], [&first_run; 2], "seed {seed}");
        assert_eq!(cut.flags, uncut.flags, "seed {seed}");
    }
    // The seeds that the plans pick cut at each position, with each kind of peeked result after a
    // cut, and some chunked replays end an update with a sequence that has no command.
    assert_eq!(positions.len(), 3, "{positions:?}");
    assert_eq!(peeked.len(), 4, "{peeked:?}");
    assert!(chunked_after_no_command > 0);
}

#[derive(Clone, Copy, Debug)]
enum NoJob {
    Upsert,
    EmptyUnchunked,
}

fn reads_and_upserts(activation: &WorkflowActivation) -> Vec<Command> {
    let mut commands = vec![];
    for job in &activation.jobs {
        match &job.variant {
            Some(Job::InitializeWorkflow(_)) => {
                commands.push(read(1, "s", Duration::from_secs(60), false));
            }
            Some(Job::SignalWorkflow(_)) => {
                let fields = HashMap::from([("k".to_string(), Payload::default())]);
                let search_attributes = Some(SearchAttributes {
                    indexed_fields: fields,
                });
                commands.push(UpsertWorkflowSearchAttributes { search_attributes }.into());
            }
            Some(Job::ResolveStreamRead(page)) if page.end.is_some() => commands.push(timer(1)),
            Some(Job::ResolveStreamRead(page)) => {
                let last = page
                    .records
                    .last()
                    .expect("A page that goes on has a Record");
                commands.push(timer(
                    u64::from_be_bytes(last.offset[..].try_into().unwrap()) as u32,
                ));
            }
            Some(Job::FireTimer(_)) => commands.push(complete()),
            _ => {}
        }
    }
    commands
}

// A replay goes on after a sequence that gives Workflow code no job, when a cut leaves that
// sequence last in the update.
#[rstest::rstest]
#[case::upsert_in_the_poll(NoJob::Upsert, Path::FullHistory)]
#[case::upsert_on_a_cache_miss(NoJob::Upsert, Path::CacheMiss)]
#[case::empty_in_the_poll(NoJob::EmptyUnchunked, Path::FullHistory)]
#[case::empty_on_a_cache_miss(NoJob::EmptyUnchunked, Path::CacheMiss)]
#[tokio::test]
async fn a_replay_goes_on_after_a_task_with_no_job_before_a_cut(
    #[case] no_job: NoJob,
    #[case] path: Path,
) {
    let store = store(&["s"]).await;
    fill(&store, "s", 1).await;
    // The second Record and the close wait until the mock server serves the fourth task.
    store.hold(Op::Append);
    store.hold(Op::Close);
    let later = store.clone();
    tokio::spawn(async move {
        fill(&later, "s", 1).await;
        close_as_client(&later, "s").await;
    });
    let mut opts = StartOpts::new(&store);
    let held = store.clone();
    opts.served = Some(Box::new(move |number, t| {
        if number == 1 && matches!(no_job, NoJob::Upsert) {
            t.add_we_signaled("go", vec![]);
        }
        if number == 3 {
            held.release();
        }
        Some(next_task(t))
    }));
    // A task with no page waits for the read until its heartbeat.
    let run = start(history(Duration::from_secs(1)), opts);
    loop {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        let ended = pages(&activation).any(|page| page.end.is_some());
        let commands = reads_and_upserts(&activation);
        let completion = WorkflowActivationCompletion::from_cmds(activation.run_id, commands);
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
        if ended {
            break;
        }
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    let sent = run.sent.lock().clone();
    let no_job_commands = match no_job {
        NoJob::Upsert => vec![CommandType::UpsertWorkflowSearchAttributes],
        NoJob::EmptyUnchunked => vec![],
    };
    assert_eq!(command_types(&sent[1]), no_job_commands);
    assert_eq!(command_types(&sent[2]), []);
    let mut t = run.history.lock().clone();
    if let NoJob::EmptyUnchunked = no_job {
        disable_chunking(&mut t);
    }
    let events = t.get_full_history_info().unwrap().into_events();
    let started = (events.iter())
        .find(|event| {
            matches!(&event.attributes,
                Some(history_event::Attributes::TimerStartedEventAttributes(timer))
                    if timer.timer_id == "1")
        })
        .unwrap();
    t.add_timer_fired(started.event_id, "1".to_string());
    let whole = next_task(&mut t);
    // The first marker fits the bound, and the next one, after the two tasks with no page, ends
    // the update.
    let markers = recorded_markers(&whole.history.as_ref().unwrap().events);
    let poll = poll_for_last_task(&t, whole, path);
    let mut replays = vec![];
    for bound in [usize::MAX, markers[0].0 as usize] {
        let label = format!("{no_job:?} {path:?} bound {bound}");
        replays.push(replay_with_bound(&store, &t, &poll, bound, label, reads_and_upserts).await);
    }
    assert_eq!(replays[0], replays[1]);
}

#[derive(Clone, Copy, Debug)]
enum Nondeterminism {
    FoundByCore,
    ReportedByLang,
}

#[rstest::rstest]
#[case::found_by_core(Nondeterminism::FoundByCore, Path::FullHistory, 2)]
#[case::found_by_core_on_a_cache_miss(Nondeterminism::FoundByCore, Path::CacheMiss, 2)]
#[case::reported_by_lang(Nondeterminism::ReportedByLang, Path::FullHistory, 2)]
#[case::reported_by_lang_on_a_cache_miss(Nondeterminism::ReportedByLang, Path::CacheMiss, 2)]
#[case::found_by_core_after_a_fetch(Nondeterminism::FoundByCore, Path::FullHistory, 1)]
#[case::reported_by_lang_after_a_fetch(Nondeterminism::ReportedByLang, Path::FullHistory, 1)]
#[tokio::test]
async fn a_replay_fails_the_workflow_at_the_end_of_a_cut_update(
    #[case] nondeterminism: Nondeterminism,
    #[case] path: Path,
    #[case] markers_in_an_update: usize,
) {
    let store = store(&["s"]).await;
    fill(&store, "s", 4).await;
    let one_record = |wc: &mut WorkerConfig| wc.stream_runtimes.settings.read_page_records = 1;
    let (_, t, whole) = first_run_with_a_timer_at_each_page(&store, one_record).await;
    let markers = recorded_markers(&whole.history.as_ref().unwrap().events);
    let bound = (markers[..markers_in_an_update].iter())
        .map(|(recorded, _)| recorded)
        .sum::<u64>() as usize;
    let (task, history_pages) = poll_for_last_task(&t, whole, path);
    let mut runs = vec![];
    for bound in [usize::MAX, bound] {
        let mut opts = StartOpts::new(&store);
        opts.responses = vec![ResponseType::Raw(task.clone())];
        opts.pages = history_pages.clone();
        let label = format!("{nondeterminism:?} {path:?} bound {bound}");
        opts.label = label.clone();
        opts.configure = Box::new(move |wc| {
            fails_workflow_for_nondeterminism(wc);
            wc.stream_runtimes.settings.max_task_record_bytes = bound;
        });
        let replay = start(t.clone(), opts);
        let mut jobs = vec![];
        let drive = async {
            while let Ok(activation) = replay.worker.poll_workflow_activation().await {
                let run_id = activation.run_id.clone();
                if activation.is_only_eviction() {
                    let completion = WorkflowActivationCompletion::empty(run_id);
                    let _ = replay.worker.complete_workflow_activation(completion).await;
                    break;
                }
                jobs.push(jobs_of(&activation));
                let first = matches!(activation.jobs[0].variant, Some(Job::InitializeWorkflow(_)));
                let commands = read_with_a_timer_at_each_page(&activation);
                let completion = match nondeterminism {
                    Nondeterminism::FoundByCore if first => {
                        WorkflowActivationCompletion::empty(run_id)
                    }
                    Nondeterminism::ReportedByLang if first => {
                        let cause = Some(WorkflowTaskFailedCause::NonDeterministicError);
                        WorkflowActivationCompletion::fail(run_id, Default::default(), cause)
                    }
                    _ => WorkflowActivationCompletion::from_cmds(run_id, commands),
                };
                replay
                    .worker
                    .complete_workflow_activation(completion)
                    .await
                    .unwrap();
            }
        };
        tokio::time::timeout(Duration::from_secs(10), drive)
            .await
            .unwrap_or_else(|_| panic!("{label}: the replay hung"));
        replay.worker.drain_pollers_and_shutdown_ref().await;
        let sent: Vec<Vec<CommandType>> = replay.sent().iter().map(command_types).collect();
        runs.push((job_names(&jobs), sent));
    }
    let fails_the_workflow = vec![vec![CommandType::FailWorkflowExecution]];
    assert_eq!(runs[0].1, fails_the_workflow);
    assert_eq!(runs[1], runs[0]);
}

// The concurrent replay gives the same jobs in the same activations, and ends its updates at the
// same markers, as a replay that reads one entry at a time.
#[tokio::test]
async fn a_replay_reads_the_entries_of_many_markers_at_the_same_time() {
    for seed in 0..3 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s"]).await;
        // Each task of the first run gives one page of 25 small Records, so each marker has 25.
        fill(&store, "s", 25 * rng.random_range(15..=40)).await;
        let pages_of_25 =
            |wc: &mut WorkerConfig| wc.stream_runtimes.settings.read_page_records = 25;
        let (live, t, whole) = first_run_with_a_timer_at_each_page(&store, pages_of_25).await;
        let markers = markers_of(&t, &store);
        let one_at_a_time =
            |wc: &mut WorkerConfig| wc.stream_runtimes.settings.replay_concurrency = 1;
        let (alone, reads) =
            replay_with_reads(&store, &t, whole.clone(), vec![], one_at_a_time).await;
        assert_eq!(alone[..live.len()], live[..], "seed {seed}");
        let updates = check_updates(seed, &markers, &reads);
        assert!(updates[0] > 1, "seed {seed}: {updates:?}");
        // A replay asks for the bytes of an entry's Records, so this Store gives them in one read.
        assert_eq!(reads.concat().len(), markers.len(), "seed {seed}");
        // The Store holds the reads of the other replay until the reads of two entries wait
        // together. A retry of an entry asks from the same offset.
        store.hold(Op::Read);
        let before = store.reads().len();
        let two_wait = async {
            wait_until(|| {
                let reads = store.reads();
                let offsets: HashSet<_> = (reads[before..].iter())
                    .map(|read| &read.from_offset)
                    .collect();
                offsets.len() > 1
            })
            .await;
            store.release();
        };
        let replay = replay_with_reads(&store, &t, whole, vec![], |_| {});
        let ((jobs, reads), ()) = tokio::join!(replay, two_wait);
        assert_eq!(jobs, alone, "seed {seed}");
        assert_eq!(
            check_updates(seed, &markers, &reads),
            updates,
            "seed {seed}"
        );
    }
}
