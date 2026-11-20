//! The Stream reads of Workflow code, through a Worker on a mock server. These tests need the real
//! machinery of a Worker, such as the completion of a task while a read runs, or a replay.

use super::*;
use crate::worker::{WorkflowErrorType, streams::peeked_marker};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::collections::{HashSet, VecDeque};
use temporalio_common::protos::{
    constants::{LOCAL_ACTIVITY_MARKER_NAME, PATCH_MARKER_NAME},
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        workflow_activation::{FireTimer, ResolveStreamRead},
        workflow_commands::{SetPatchMarker, StartStreamRead, UpsertWorkflowSearchAttributes},
        workflow_completion::workflow_activation_completion::Status,
    },
    temporal::api::{common::v1::SearchAttributes, failure::v1::failure::FailureInfo},
};
use temporalio_streams::proto::{
    ClientProducer, CloseStreamRequest, ReadRecordsRequest, StreamRecord,
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
    /// The poll has the whole History, and the replay reads the Records of each marker in a part
    /// of its own, so those of the second read come while a completion waits.
    InALaterPart,
}

/// What a replay finds of the Records of its read.
#[derive(Clone, Copy, Debug)]
enum Replay {
    /// The Stream is gone, and the Worker fails the Workflow for it.
    Gone,
    /// The Stream is gone, and the Worker fails the Workflow only for nondeterminism.
    GoneWithTheNondeterminismOptIn,
    /// The Stream is gone, and the Worker fails the Workflow for it. The task is on its second
    /// attempt.
    GoneOnALaterAttempt,
    /// The Stream is gone, and the Worker fails the Workflow for it. The task is a query.
    GoneForAQuery,
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

fn fails_workflow_with_parts_of_one_marker(wc: &mut WorkerConfig) {
    fails_workflow(wc);
    wc.stream_runtimes.settings.bounds.max_task_record_bytes = 1;
}

/// Reads Stream `r` in task 2 and Stream `s` in task 4, with a timer in each task, and completes
/// after timer 4.
fn read_in_the_second_and_fourth_tasks(activation: &WorkflowActivation) -> Vec<Command> {
    let read_ends = |seq| pages(activation).any(|read| read.seq == seq && read.end.is_some());
    match &activation.jobs[0].variant {
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
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

// A replay whose Records are gone fails the Workflow with the opt-in, on the first attempt of a
// Workflow Task. A later attempt reports nothing, and a query only gets a failed answer. A Store
// that gives no answer, or a Worker with no runtime for it, fails only the task, since another
// Worker may still read.
#[rstest::rstest]
#[case(Path::FullHistory, Replay::Gone)]
#[case(Path::CacheMiss, Replay::Gone)]
#[case(Path::InALaterPart, Replay::Gone)]
#[case(Path::FullHistory, Replay::GoneWithTheNondeterminismOptIn)]
#[case(Path::FullHistory, Replay::GoneOnALaterAttempt)]
#[case(Path::FullHistory, Replay::GoneForAQuery)]
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
    let mut whole = next_task(&mut t);
    let mut opts = StartOpts::new(&store);
    match replay {
        Replay::Gone | Replay::GoneWithTheNondeterminismOptIn => store.forget("s"),
        // The mock poller numbers the attempts of its responses, so this task comes on a channel.
        Replay::GoneOnALaterAttempt => {
            store.forget("s");
            whole.attempt = 2;
            let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
            tasks.send(whole.clone()).unwrap();
            opts.tasks = Some(receiver);
        }
        // The server gave out the query before timer 4 fired.
        Replay::GoneForAQuery => {
            store.forget("s");
            whole = query_before(&whole, t.current_event_id() - 3);
            opts.queries = 1;
        }
        Replay::StoreDown => store.fail(
            Op::Read,
            StreamError::store_unavailable("The Store is down"),
            usize::MAX,
        ),
        Replay::NoRuntime => {}
    }
    let events = whole.history.clone().unwrap().events;
    opts.responses = vec![ResponseType::Raw(match path {
        Path::CacheMiss => {
            opts.pages = vec![page(&events)];
            let tasks = t.get_full_history_info().unwrap().wf_task_count();
            hist_to_poll_resp(&t, "fake_wf_id", ResponseType::OneTask(tasks)).resp
        }
        Path::FullHistory | Path::InALaterPart => whole,
    })];
    opts.served = None;
    let configure: fn(&mut WorkerConfig) = match (path, replay) {
        (Path::InALaterPart, _) => fails_workflow_with_parts_of_one_marker,
        (_, Replay::GoneWithTheNondeterminismOptIn) => fails_workflow_for_nondeterminism,
        (_, Replay::StoreDown) => fails_workflow_with_its_store_down,
        (_, Replay::NoRuntime) => fails_workflow_with_no_runtime,
        _ => fails_workflow,
    };
    opts.configure = Box::new(configure);
    let fails_the_workflow = matches!(replay, Replay::Gone);
    let reports_nothing = matches!(replay, Replay::GoneOnALaterAttempt | Replay::GoneForAQuery);
    opts.fails = usize::from(!fails_the_workflow && !reports_nothing);
    let second = start(t, opts);
    second.drive(read_in_the_second_and_fourth_tasks).await;
    second.worker.drain_pollers_and_shutdown_ref().await;
    let failed = second.failed.lock().clone();
    let answers = second.answers.lock().clone();
    if reports_nothing {
        assert!(failed.is_empty() && second.sent().is_empty());
        let gone = |(_, failure): &Answer| {
            (failure.as_ref()).is_some_and(|m| m.contains("The Store doesn't have the 3 Records"))
        };
        assert!(answers.iter().all(gone), "{answers:?}");
        let queries = usize::from(matches!(replay, Replay::GoneForAQuery));
        assert_eq!(answers.len(), queries);
    } else if fails_the_workflow {
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
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
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
    configure: impl FnOnce(&mut WorkerConfig) + 'static,
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

/// Checks the reads of a replay of `markers`. The reads before an activation are those of a part,
/// which reads the next markers in order, and whose Records fit `bound` unless it reads one
/// marker. The replay reads each marker once. Returns the number of markers that each part reads.
fn check_parts(
    seed: u64,
    markers: &[(Vec<u8>, usize)],
    reads: &[Vec<ReadRecordsRequest>],
    bound: usize,
) -> Vec<usize> {
    let starts = |reads: &[ReadRecordsRequest], after: &[u8]| {
        (reads.iter())
            .filter(|read| read.from_offset == after)
            .count()
    };
    for (after, _) in markers {
        assert_eq!(starts(&reads.concat(), after), 1, "seed {seed}: {after:?}");
    }
    let (mut next, mut parts) = (0, vec![]);
    for part in reads.iter().filter(|reads| !reads.is_empty()) {
        let read = (markers[next..].iter())
            .take_while(|(after, _)| starts(part, after) > 0)
            .count();
        assert!(read > 0, "seed {seed}: marker {next}");
        let held: usize = markers[next..next + read].iter().map(|(_, b)| b).sum();
        assert!(read == 1 || held <= bound, "seed {seed}: {held}");
        next += read;
        parts.push(read);
    }
    assert_eq!(next, markers.len(), "seed {seed}");
    parts
}

/// Replays `t` from the poll `task` on a Worker that `configure` sets, with the rest of its
/// History on `pages`. Returns the jobs of each activation, and the reads of the replay before
/// each activation.
async fn replay_with_reads(
    store: &Arc<MemoryStore>,
    t: &TestHistoryBuilder,
    task: PollWorkflowTaskQueueResponse,
    pages: Vec<GetWorkflowExecutionHistoryResponse>,
    configure: impl FnOnce(&mut WorkerConfig) + 'static,
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

// A replay reads the next part only once Workflow code got the Records of the part before it, but
// for those of one marker, which the machines peeked. So it never holds more than two parts.
#[tokio::test]
async fn a_replay_holds_at_most_two_parts_of_records() {
    const PART: usize = 64 << 10;
    let mut most_parts = 0;
    for seed in 0..8 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s"]).await;
        // Each task of the first run gives one page, so each marker has the Records of one.
        let page_records = rng.random_range(1..=8);
        for _ in 0..page_records * rng.random_range(5..=40) {
            let size = match rng.random_bool(0.7) {
                true => rng.random_range(1..=2 << 10),
                false => rng.random_range(2 << 10..=24 << 10),
            };
            append_sized(&store, "s", size).await;
        }
        let pages = move |wc: &mut WorkerConfig| {
            wc.stream_runtimes.settings.read_page_records = page_records;
        };
        let (live, t, whole) = first_run_with_a_timer_at_each_page(&store, pages).await;
        let markers = markers_of(&t, &store);
        let parts_of_64_kib =
            |wc: &mut WorkerConfig| wc.stream_runtimes.settings.bounds.max_task_record_bytes = PART;
        let (jobs, reads) = replay_with_reads(&store, &t, whole, vec![], parts_of_64_kib).await;
        assert_eq!(jobs[..live.len()], live[..], "seed {seed}");
        let fired = vec![Job::FireTimer(FireTimer { seq: 1 })];
        assert_eq!(jobs[live.len()..], [fired], "seed {seed}");
        let parts = check_parts(seed, &markers, &reads, PART);
        most_parts = most_parts.max(parts.len());
        // The bytes that the replay read, and those that Workflow code got, before each activation.
        let size = |record: &StreamRecord| record.encoded_len();
        let (mut read, mut given, mut next) = (0, 0, 0);
        let mut last_two = [0, 0];
        for (part, jobs) in reads.iter().zip(&jobs) {
            if !part.is_empty() {
                let markers = &markers[next..next + part.len()];
                next += part.len();
                let bytes: usize = markers.iter().map(|(_, bytes)| bytes).sum();
                last_two = [last_two[1], bytes];
                read += bytes;
            }
            let held = read - given;
            assert!(held <= last_two[0] + last_two[1], "seed {seed}: {held}");
            given += records_of(std::slice::from_ref(jobs))
                .iter()
                .map(size)
                .sum::<usize>();
        }
    }
    // Some replay reads more than three parts.
    assert!(most_parts > 3, "{most_parts}");
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
        Path::FullHistory | Path::InALaterPart => (whole, vec![]),
    }
}

/// Returns each job of a read in `jobs`, in the order that Workflow code got them.
fn read_jobs(jobs: &[Vec<Job>]) -> Vec<String> {
    let read = |job: &&Job| matches!(job, Job::ResolveStreamRead(_));
    let jobs = jobs.iter().flatten().filter(read);
    jobs.map(|job| format!("{job:?}")).collect()
}

/// What a replay gave Workflow code.
#[derive(Debug, PartialEq)]
struct Replayed {
    /// The jobs of each activation, by name.
    jobs: Vec<Vec<String>>,
    reads: Vec<String>,
    /// The lang flags of each activation.
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
    opts.configure =
        Box::new(move |wc| wc.stream_runtimes.settings.bounds.max_task_record_bytes = bound);
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
        reads: read_jobs(&jobs),
        flags,
    }
}

// The machines peek the results of a task one sequence early, also where the replay waits for the
// next part of Records. So a replay gives the same activations whatever the size of its parts.
// Without chunking they're the activations of the first run. With chunking a replay gives the
// results of a task in fewer activations, as for a Local Activity, and each job still comes once,
// with the jobs of a read in the order of the Stream.
#[tokio::test]
async fn a_replay_gives_the_jobs_of_the_first_run_in_both_chunking_modes() {
    let (mut positions, mut peeked) = (HashMap::new(), HashMap::new());
    let mut chunked_after_no_command = 0;
    for seed in 0..40 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s", "a"]).await;
        let page_records = rng.random_range(1..=3);
        // At least four pages, so that the first part can end after the first, a middle or the
        // next-to-last marker.
        fill(&store, "s", rng.random_range(3 * page_records + 1..=20)).await;
        let records = store.records("s");
        let pages: Vec<u32> = (records.chunks(page_records))
            .map(|page| u64::from_be_bytes(page[page.len() - 1].offset[..].try_into().unwrap()))
            .map(|seq| seq as u32)
            .collect();
        // The seed picks the last marker of the first part, and what the completion of that
        // marker has. It's the marker of page `last`, and the task of the first page has no Update.
        let kind = seed as usize % 4;
        let last = match (seed / 4) % 3 {
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
            updates.insert(last);
        } else {
            let at = [AtAPage::Patch, AtAPage::LocalActivity, AtAPage::Append][kind];
            at_pages.insert(pages[last - 1], at);
        }
        let plan = Plan {
            waits_for_a_signal: seed % 2 == 1,
            at_pages,
            updates,
        };
        let (live, mut t) = first_run_with_peeked_results(&plan, &store, page_records).await;
        let chunking = (seed / 2) % 2 == 0;
        if !chunking {
            disable_chunking(&mut t);
        }
        let whole = next_task(&mut t);
        let events = whole.history.clone().unwrap().events;
        let markers = recorded_markers(&events);
        assert_eq!(markers.len(), pages.len(), "seed {seed}: {markers:?}");
        // The first part takes the markers up to `last`, and the replay waits for the next one.
        let bound = markers[..last]
            .iter()
            .map(|(recorded, _)| recorded)
            .sum::<u64>() as usize;
        let position = match last {
            1 => "after the first marker",
            _ if last == markers.len() - 1 => "before the last marker",
            _ => "after a middle marker",
        };
        *positions.entry(position).or_insert(0) += 1;
        for kind in peeked_in(&events, markers[last - 1].1) {
            *peeked.entry(kind).or_insert(0) += 1;
        }
        // The replay waits once it applied the task whose completion is two before the one of
        // the first unread marker. With chunking, a completion with no command joins two tasks.
        let completions: Vec<i64> = (events.iter())
            .filter(|event| event.event_type() == EventType::WorkflowTaskCompleted)
            .map(|event| event.event_id)
            .collect();
        let unread = completions
            .iter()
            .position(|id| *id == markers[last].1)
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
        let [in_one_part, in_parts] = [&replays[0], &replays[1]];
        assert_eq!(in_parts, in_one_part, "seed {seed}");
        let fired = vec![Job::FireTimer(FireTimer { seq: 1 })];
        let first_run = [live, vec![fired]].concat();
        if chunking {
            let sorted = |jobs: &[Vec<String>]| {
                let mut jobs = jobs.concat();
                jobs.sort();
                jobs
            };
            let of_first_run = sorted(&job_names(&first_run));
            assert_eq!(sorted(&in_parts.jobs), of_first_run, "seed {seed}");
            assert_eq!(in_parts.reads, read_jobs(&first_run), "seed {seed}");
        } else {
            assert_eq!(in_parts.jobs, job_names(&first_run), "seed {seed}");
        }
    }
    // The seeds end the first part at each position, with each kind of peeked result in the
    // completion before the wait, and some chunked replays wait after a task with no command.
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

// A sequence that gives Workflow code no job leaves no completion to wait for the next part of
// Records, so the completion before it waits in its place.
#[rstest::rstest]
#[case::upsert(NoJob::Upsert)]
#[case::empty(NoJob::EmptyUnchunked)]
#[tokio::test]
async fn a_replay_goes_on_after_a_task_with_no_job_before_the_next_part(#[case] no_job: NoJob) {
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
    // The first marker is a part, and the next one comes after the two tasks with no page.
    let markers = recorded_markers(&whole.history.as_ref().unwrap().events);
    let poll = (whole, vec![]);
    let mut replays = vec![];
    for bound in [usize::MAX, markers[0].0 as usize] {
        let label = format!("{no_job:?} bound {bound}");
        replays.push(replay_with_bound(&store, &t, &poll, bound, label, reads_and_upserts).await);
    }
    assert_eq!(replays[0], replays[1]);
}

#[derive(Clone, Copy, Debug)]
enum Nondeterminism {
    FoundByCore,
    ReportedByLang,
}

// The completion that fails the Workflow is the last one of the run, so it applies no next task,
// and waits for no Records.
#[rstest::rstest]
#[case::found_by_core(Nondeterminism::FoundByCore)]
#[case::reported_by_lang(Nondeterminism::ReportedByLang)]
#[tokio::test]
async fn a_replay_fails_the_workflow_with_the_next_part_unread(
    #[case] nondeterminism: Nondeterminism,
) {
    let store = store(&["s"]).await;
    fill(&store, "s", 4).await;
    let one_record = |wc: &mut WorkerConfig| wc.stream_runtimes.settings.read_page_records = 1;
    let (_, t, whole) = first_run_with_a_timer_at_each_page(&store, one_record).await;
    let mut runs = vec![];
    // With parts of one marker, the first completion of the replay waits for the second part.
    for bound in [usize::MAX, 1] {
        let mut opts = StartOpts::new(&store);
        opts.responses = vec![ResponseType::Raw(whole.clone())];
        let label = format!("{nondeterminism:?} bound {bound}");
        opts.label = label.clone();
        opts.configure = Box::new(move |wc| {
            fails_workflow_for_nondeterminism(wc);
            wc.stream_runtimes.settings.bounds.max_task_record_bytes = bound;
        });
        let reads = store.reads().len();
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
        runs.push((job_names(&jobs), sent, store.reads().len() - reads));
    }
    let fails_the_workflow = vec![vec![CommandType::FailWorkflowExecution]];
    assert_eq!(runs[0].1, fails_the_workflow);
    assert_eq!(runs[1].0, runs[0].0);
    assert_eq!(runs[1].1, fails_the_workflow);
    // Core finds the error when it applies the next task, for which it read the second part.
    let parts = match nondeterminism {
        Nondeterminism::FoundByCore => 2,
        Nondeterminism::ReportedByLang => 1,
    };
    assert_eq!([runs[0].2, runs[1].2], [4, parts]);
}

/// A task that is on its way to a run, with the places of its first reads among the requests of
/// the Store.
struct InFlight {
    reads: std::ops::Range<usize>,
    /// The activation of the test that is out when the task comes back, or `None` if it comes
    /// back once each task before it has its result.
    at: Option<usize>,
    /// Whether the task comes after a task that the first one doesn't have.
    is_ahead: bool,
}

// The tasks of a run that isn't in the cache each read the first part of its Records before a run
// exists. The first one that comes back makes the run, and each other one meets that run, while
// it replays or later. Each of them still gets its result, with the jobs of the first run.
#[tokio::test]
async fn two_tasks_of_a_run_that_is_not_cached_each_get_their_result() {
    let (mut during_a_replay, mut evicted) = (0, 0);
    for seed in 0..200 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s"]).await;
        let page_records = rng.random_range(1..=3);
        fill(&store, "s", page_records * rng.random_range(2..=6)).await;
        let pages = move |wc: &mut WorkerConfig| {
            wc.stream_runtimes.settings.read_page_records = page_records;
        };
        let (live, mut t, _) = first_run_with_a_timer_at_each_page(&store, pages).await;
        // Another Worker took the task of timer 1, and started timer 2. The server gives out a
        // query before a timer fires, when no Workflow Task is pending.
        let before_timer_1 = t.current_event_id() - 3;
        t.add_workflow_task_completed();
        let started = t.add_timer_started("2".to_string());
        let before_timer_2 = t.current_event_id();
        t.add_timer_fired(started, "2".to_string());
        let last = next_task(&mut t);
        let markers = recorded_markers(&last.history.as_ref().unwrap().events);
        // A part has the Records of one marker, or of some, or of all.
        let first_part = rng.random_range(1..=markers.len());
        let bound = (markers[..first_part].iter())
            .map(|(recorded, _)| recorded)
            .sum::<u64>() as usize;
        // The last Workflow Task and one or two queries, or two or three queries. One query can be
        // from before timer 1, so the run that it makes hasn't seen the task of that timer. Each
        // task comes with the number of activations of its replay.
        let has_workflow_task = rng.random_bool(0.6);
        let queries = rng.random_range(2..=3) - usize::from(has_workflow_task);
        let has_early_query = rng.random_bool(0.3);
        let mut tasks: Vec<(PollWorkflowTaskQueueResponse, usize)> = (0..queries)
            .map(|query| match has_early_query && query == 0 {
                true => (query_before(&last, before_timer_1), live.len() + 1),
                false => (query_before(&last, before_timer_2), live.len() + 2),
            })
            .collect();
        if has_workflow_task {
            tasks.push((last.clone(), live.len() + 2));
        }
        // The seed picks the order in which the first reads of the tasks end, and which
        // activation of the first task is out when each other task comes back.
        for last in (1..tasks.len()).rev() {
            tasks.swap(last, rng.random_range(0..=last));
        }
        let activations_of_first = tasks[0].1;
        let mut at: Vec<Option<usize>> = (1..tasks.len())
            .map(|_| (rng.random_bool(0.6)).then(|| rng.random_range(0..activations_of_first)))
            .collect();
        at.sort_by_key(|at| at.unwrap_or(usize::MAX));
        // A task that comes after a task that the run doesn't have evicts the run. If the run
        // still replays for a query then, lang never gets that query, so it has no answer.
        let seen_by_first = tasks[0].0.previous_started_event_id;
        let is_ahead: Vec<bool> = (tasks.iter())
            .map(|(task, _)| task.previous_started_event_id > seen_by_first)
            .collect();
        let replays_at = |at: &Option<usize>| at.is_some_and(|at| at + 1 < activations_of_first);
        let dropped = (is_ahead[1..].iter().zip(&at)).any(|(ahead, at)| *ahead && replays_at(at));
        evicted += usize::from(dropped);
        during_a_replay += at.iter().flatten().count();

        store.gate_reads(true);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut opts = StartOpts::new(&store);
        opts.tasks = Some(receiver);
        opts.served = None;
        // A Worker has a task slot for each run that its cache can hold, and three tasks come.
        opts.cache = 3;
        opts.queries = queries - usize::from(dropped);
        opts.label = format!("seed {seed}");
        opts.configure = Box::new(move |wc| {
            wc.stream_runtimes.settings.bounds.max_task_record_bytes = bound;
            // Each first part reads its markers at the same time.
            wc.stream_runtimes.settings.replay_concurrency = 64;
            // A read that waits at the gate must not count as a try with no answer.
            wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
        });
        let run = start(t.clone(), opts);
        // Core takes its tasks once lang polls for an activation.
        let mut first = Box::pin(run.worker.poll_workflow_activation());
        assert!(futures_util::poll!(&mut first).is_pending());
        let mut first = Some(first);
        let mut in_flight = VecDeque::new();
        let at = iter::once(None).chain(at);
        for (((task, _), at), is_ahead) in tasks.into_iter().zip(at).zip(is_ahead) {
            let from = store.requests().len();
            let reads = from..from + first_part;
            sender.send(task).unwrap();
            wait_until(|| store.requests().len() == reads.end).await;
            in_flight.push_back(InFlight {
                reads,
                at,
                is_ahead,
            });
        }
        store.gate_reads(false);

        // The jobs of each activation of each run, the tasks that came back, and the one of them
        // that lost its run.
        let mut runs = vec![vec![]];
        let (mut back, mut lost, mut activations) = (0, 0, 0);
        let results = |run: &Run| run.answers.lock().len() + run.sent().len();
        loop {
            if results(&run) + lost == back {
                let Some(next) = in_flight.pop_front() else {
                    break;
                };
                store.let_go(next.reads);
                back += 1;
            }
            let poll = async {
                match first.take() {
                    Some(first) => first.await,
                    None => run.worker.poll_workflow_activation().await,
                }
            };
            let activation = (tokio::time::timeout(Duration::from_secs(10), poll).await)
                .unwrap_or_else(|_| panic!("seed {seed}: a task got no activation"))
                .unwrap();
            if activation.is_only_eviction() {
                let empty = WorkflowActivationCompletion::empty(activation.run_id);
                let _ = run.worker.complete_workflow_activation(empty).await;
                runs.push(vec![]);
                continue;
            }
            // A task that comes back now meets the run before this activation completes.
            let is_due = |next: &InFlight| next.at.is_some_and(|at| at <= activations);
            while in_flight.front().is_some_and(is_due) {
                let next = in_flight.pop_front().unwrap();
                let passed = store.reads_let_go() + next.reads.len();
                store.let_go(next.reads);
                wait_until(|| store.reads_let_go() == passed).await;
                back += 1;
                let replays = activations + 1 < activations_of_first;
                lost = lost.max(usize::from(next.is_ahead && replays));
            }
            activations += 1;
            runs.last_mut().unwrap().push(jobs_of(&activation));
            respond(&run.worker, &activation, |activation| {
                match &activation.jobs[0].variant {
                    Some(Job::FireTimer(fired)) if fired.seq == 1 => vec![timer(2)],
                    _ => read_with_a_timer_at_each_page(activation),
                }
            })
            .await;
        }
        // Each query that wasn't dropped got an answer, and no task failed.
        let answers = run.answers.lock().clone();
        assert_eq!(answers.len(), queries - usize::from(dropped), "seed {seed}");
        let failed = answers.iter().any(|(_, failure)| failure.is_some());
        assert!(!failed, "seed {seed}: {answers:?}");
        assert_eq!(run.failed.lock().clone(), [], "seed {seed}");
        // The Workflow Task completed once, and each run gave the jobs of the first run.
        let completes = vec![CommandType::CompleteWorkflowExecution];
        let sent: Vec<Vec<CommandType>> = run.sent().iter().map(command_types).collect();
        assert_eq!(
            sent,
            vec![completes; usize::from(has_workflow_task)],
            "seed {seed}"
        );
        let fired = |seq| vec![Job::FireTimer(FireTimer { seq })];
        let first_run = [live, vec![fired(1), fired(2)]].concat();
        let is_query = |jobs: &&Vec<Job>| matches!(jobs[..], [Job::QueryWorkflow(_)]);
        for jobs in &runs {
            let replayed: Vec<&Vec<Job>> = jobs.iter().filter(|jobs| !is_query(jobs)).collect();
            let of_first_run: Vec<&Vec<Job>> = first_run.iter().take(replayed.len()).collect();
            assert_eq!(replayed, of_first_run, "seed {seed}");
        }
        // No thread of Core panicked, so the Worker still serves another run.
        let other = history(Duration::from_secs(10));
        sender.send(task_of(&other)).unwrap();
        loop {
            let activation = run.worker.poll_workflow_activation().await.unwrap();
            let run_id = activation.run_id.clone();
            let init = matches!(activation.jobs[0].variant, Some(Job::InitializeWorkflow(_)));
            let completion = WorkflowActivationCompletion::empty(run_id);
            let _ = run.worker.complete_workflow_activation(completion).await;
            if init {
                break;
            }
        }
        run.worker.drain_pollers_and_shutdown_ref().await;
    }
    // Some tasks met a run that still replayed, and some of them evicted it.
    assert!(
        during_a_replay > 0 && evicted > 0,
        "{during_a_replay} {evicted}"
    );
}

/// The read of a replay that gets no answer.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Unanswered {
    /// The first part, which a task reads before its run exists.
    TheFirstPart,
    /// A later part, which a completion of the run waits for.
    ALaterPart,
}

// A replay reads its Records outside the stage that every poll passes, which takes one poll or
// fetch at a time here. So a Store that gives no answer holds up no Workflow that doesn't read it.
#[rstest::rstest]
#[case(Unanswered::TheFirstPart)]
#[case(Unanswered::ALaterPart)]
#[tokio::test]
async fn a_poll_passes_while_a_replay_waits_for_a_store(#[case] unanswered: Unanswered) {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let one_record = |wc: &mut WorkerConfig| wc.stream_runtimes.settings.read_page_records = 1;
    let (_, t, whole) = first_run_with_a_timer_at_each_page(&store, one_record).await;
    let reads = store.reads().len();
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut opts = StartOpts::new(&store);
    opts.tasks = Some(receiver);
    opts.served = None;
    opts.cache = 2;
    opts.configure = Box::new(|wc| {
        wc.stream_runtimes.settings.bounds.max_task_record_bytes = 1;
        wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
    });
    let run = start(t, opts);
    let workflow = read_with_a_timer_at_each_page;
    tasks.send(whole).unwrap();
    let mut next = Box::pin(run.worker.poll_workflow_activation());
    let mut waiting = None;
    if unanswered == Unanswered::TheFirstPart {
        store.hold(Op::Read);
        assert!(futures_util::poll!(&mut next).is_pending());
    } else {
        // The completion of the first activation waits for the second part.
        let init = next.await.unwrap();
        store.hold(Op::Read);
        let commands = workflow(&init);
        let completion = WorkflowActivationCompletion::from_cmds(init.run_id, commands);
        let mut completion = Box::pin(run.worker.complete_workflow_activation(completion));
        assert!(futures_util::poll!(&mut completion).is_pending());
        waiting = Some(completion);
        next = Box::pin(run.worker.poll_workflow_activation());
    }
    wait_until(|| store.reads().len() > reads).await;
    // A Workflow with no Stream call still gets its task.
    let other = history(Duration::from_secs(10));
    tasks.send(task_of(&other)).unwrap();
    let activation = tokio::time::timeout(Duration::from_secs(5), next)
        .await
        .expect("The poll waited for the Store")
        .unwrap();
    assert_eq!(activation.run_id, other.get_orig_run_id());
    let completion = WorkflowActivationCompletion::from_cmds(activation.run_id, vec![complete()]);
    (run.worker.complete_workflow_activation(completion).await).unwrap();
    // Once the Store answers, the replay goes on to its end.
    store.release();
    if let Some(completion) = waiting {
        completion.await.unwrap();
    }
    while run.sent().len() < 2 {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        respond(&run.worker, &activation, workflow).await;
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// The concurrent replay gives the same jobs in the same activations, and reads the same markers in
// each part, as a replay that reads one entry at a time.
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
        let bound = StreamSettings::default().bounds.max_task_record_bytes;
        let parts = check_parts(seed, &markers, &reads, bound);
        assert!(parts[0] > 1, "seed {seed}: {parts:?}");
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
            check_parts(seed, &markers, &reads, bound),
            parts,
            "seed {seed}"
        );
    }
}
