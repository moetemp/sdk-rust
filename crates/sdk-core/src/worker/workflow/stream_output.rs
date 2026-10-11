//! The stream output a Workflow Task publishes: the records lang commits with a completion, and
//! the manifest Core builds from them and records in a `core_external_stream` marker.

use super::WFMachinesError;
use std::collections::HashMap;
use temporalio_common::{
    protos::{
        coresdk::{
            external_data::{
                ExternalOutputSegmentManifest, ExternalOutputStreamManifest,
                ExternalOutputTopicManifest,
            },
            workflow_commands::{OutputClose, OutputRecord},
        },
        temporal::sdk::streams::v1::StreamRecordKind,
    },
    streams::{FINGERPRINT_VERSION, FingerprintRecord, fingerprint},
};

/// The marker version Core writes. Readers must accept every version up to this one.
pub(super) const MARKER_SCHEMA_VERSION: u32 = 1;
const MANIFEST_SCHEMA_VERSION: u32 = 1;
/// The layout a store keeps records in. A store that changes it must refuse older stages.
const STORE_FORMAT_VERSION: u32 = 1;
/// Keeps one output proof well inside the server's per-event payload limits.
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const CONTENT_HASH_BYTES: usize = 32;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// One publishing completion's records, held until its Workflow Task's completion is reported.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutputBatch {
    /// The stage token the completion's marker names.
    pub(crate) token: String,
    pub(crate) history_floor_event_id: i64,
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) first_run_id: String,
    pub(crate) records: Vec<OutputRecord>,
    /// The topics the completion closed, closed in the store once the output is promoted.
    pub(crate) closes: Vec<OutputClose>,
}

/// The stream output a completion carries to the report path.
#[derive(Debug, Default)]
pub(crate) struct ReportedOutput {
    /// To stage before the completion is sent, in commit order.
    pub(crate) output: Vec<OutputBatch>,
    /// What replay proved committed, to promote, in commit order.
    pub(crate) proven: Vec<ProvenStage>,
    /// The chain whose streams to close once the completion is accepted, because it ends the
    /// chain.
    pub(crate) close: Option<ChainToClose>,
}

/// A run chain whose streams a run published to.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChainToClose {
    pub(crate) workflow_id: String,
    pub(crate) first_run_id: String,
}

impl ReportedOutput {
    /// What to report with the next completion, leaving both lists empty, or `None` when there is
    /// nothing.
    pub(crate) fn take(
        output: &mut Vec<OutputBatch>,
        proven: &mut Vec<ProvenStage>,
        close: Option<ChainToClose>,
    ) -> Option<Box<Self>> {
        if output.is_empty() && proven.is_empty() && close.is_none() {
            return None;
        }
        Some(Box::new(Self {
            output: std::mem::take(output),
            proven: std::mem::take(proven),
            close,
        }))
    }
}

/// A stage History proved committed, found while replaying.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProvenStage {
    pub(crate) token: String,
    pub(crate) workflow_id: String,
    pub(crate) first_run_id: String,
    /// In order of first publish, as the recorded manifest lists them.
    pub(crate) topics: Vec<String>,
    /// The topics the replayed commit closed.
    pub(crate) closes: Vec<OutputClose>,
}

/// What the server answered to a completion that carried stream output.
#[cfg(feature = "streams")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionOutcome {
    /// The commands were applied, so each output marker is in History.
    Accepted,
    /// The commands were not applied, so no output marker is in History.
    Refused,
    /// The completion may or may not have been applied.
    Unknown,
}

/// The Worker's side of a Workflow's stream output: stages each completion's records, then
/// promotes or aborts them by what the server answered.
#[cfg(feature = "streams")]
pub(crate) struct OutputStore {
    store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
    namespace: String,
    /// Stages whose completion's outcome is unknown, by run. The run is evicted when that
    /// happens, so the replay before its next completion proves each committed one. Bounded,
    /// since a run that moves to another Worker never comes back here.
    undecided: parking_lot::Mutex<lru::LruCache<String, Vec<OutputBatch>>>,
    /// Tokens this Worker promoted or aborted, so a run replayed again and again doesn't ask the
    /// store about each one every time. Forgetting one only costs a repeat request.
    settled: std::sync::Arc<parking_lot::Mutex<lru::LruCache<String, ()>>>,
    /// Background promotions and topic closes, by run.
    runs: std::sync::Arc<parking_lot::Mutex<HashMap<String, RunWork>>>,
    /// Stops the promotion and close retries when the Worker stops.
    shutdown: tokio_util::sync::CancellationToken,
}

/// A run's promotions and topic closes that have not finished yet.
#[cfg(feature = "streams")]
#[derive(Default)]
struct RunWork {
    /// While a promotion of the run is retried, it and every later one wait here in commit
    /// order, since a promotion appends and readers rely on commit order.
    backlog: Option<std::collections::VecDeque<Promotion>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "streams")]
impl RunWork {
    fn is_idle(&self) -> bool {
        self.backlog.is_none() && self.tasks.iter().all(|task| task.is_finished())
    }
}

#[cfg(feature = "streams")]
const MAX_UNDECIDED_RUNS: usize = 1000;
#[cfg(feature = "streams")]
const MAX_SETTLED_TOKENS: usize = 10_000;
#[cfg(all(feature = "streams", not(test)))]
const RETRY_FIRST: std::time::Duration = std::time::Duration::from_secs(1);
// Keeps the retry tests fast.
#[cfg(all(feature = "streams", test))]
const RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(10);
#[cfg(feature = "streams")]
const RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(60);

/// A committed stage to settle, and the topics its commit closed.
#[cfg(feature = "streams")]
struct Promotion {
    stage: temporalio_streams::proto::StageRef,
    closes: Vec<OutputClose>,
    /// The store no longer holds the stage pending, so only its closes are left.
    settled: bool,
}

#[cfg(feature = "streams")]
fn note_promoted(
    settled: &parking_lot::Mutex<lru::LruCache<String, ()>>,
    stage: &temporalio_streams::proto::StageRef,
    result: temporalio_streams::proto::PromoteResult,
) {
    use temporalio_streams::proto::PromoteOutcome;
    if result.outcome() == PromoteOutcome::Lost {
        warn!(
            token = %stage.token,
            "Stream output a Workflow committed was dropped by retention before it was promoted"
        );
    }
    settled.lock().put(stage.token.clone(), ());
}

/// Closes `closes` on `chain` one after another, each retried with backoff until it lands or
/// `shutdown` fires. A store that can't close topics is logged and given up on.
#[cfg(feature = "streams")]
async fn close_in_order(
    store: &dyn temporalio_streams::StreamStore,
    chain: &temporalio_streams::proto::ChainId,
    closes: &[OutputClose],
    shutdown: &tokio_util::sync::CancellationToken,
) {
    for close in closes {
        let mut delay = RETRY_FIRST;
        loop {
            let error = match store
                .close_topic(chain, &close.topic, close.result.clone())
                .await
            {
                Ok(()) => break,
                Err(error) => error,
            };
            if refused_for_good(&error) {
                warn!(
                    topic = %close.topic,
                    %error,
                    "The store refused to close a stream topic after its output was promoted"
                );
                break;
            }
            warn!(
                topic = %close.topic,
                %error,
                "Could not close a stream topic after its output was promoted; trying again in \
                 {delay:?}"
            );
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => return,
            }
            delay = (delay * 2).min(RETRY_CAP);
        }
    }
}

/// Closes a settled stage's topics in commit order, in the background, each retried with
/// backoff until it lands or the Worker stops.
///
/// Only after the promotion, so a reader that sees a topic end has every record before the
/// close. The close lives only in this Worker, so a failure must not drop it.
#[cfg(feature = "streams")]
fn close_topics(
    store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
    stage: &temporalio_streams::proto::StageRef,
    closes: Vec<OutputClose>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Option<tokio::task::JoinHandle<()>> {
    if closes.is_empty() {
        return None;
    }
    let chain = stage.chain.clone().unwrap_or_default();
    Some(tokio::spawn(async move {
        close_in_order(&*store, &chain, &closes, &shutdown).await;
    }))
}

/// Adds `task` to `run_id`'s unfinished work, and forgets the finished work of every run.
#[cfg(feature = "streams")]
fn track(
    runs: &parking_lot::Mutex<HashMap<String, RunWork>>,
    run_id: &str,
    task: tokio::task::JoinHandle<()>,
) {
    let mut runs = runs.lock();
    runs.retain(|_, work| {
        work.tasks.retain(|task| !task.is_finished());
        !work.is_idle()
    });
    runs.entry(run_id.to_string()).or_default().tasks.push(task);
}

/// Whether the store can never take the call. Every other failure is retried, since the output
/// was committed and lives only in this Worker until the store takes it.
#[cfg(feature = "streams")]
fn refused_for_good(error: &temporalio_streams::StreamError) -> bool {
    error.kind == temporalio_streams::proto::StreamFailureKind::Unsupported
}

#[cfg(feature = "streams")]
impl OutputStore {
    pub(crate) fn new(
        store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
        namespace: String,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            store,
            namespace,
            shutdown,
            undecided: parking_lot::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(MAX_UNDECIDED_RUNS).expect("not zero"),
            )),
            settled: std::sync::Arc::new(parking_lot::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(MAX_SETTLED_TOKENS).expect("not zero"),
            ))),
            runs: Default::default(),
        }
    }

    /// Stages `batches` in commit order, each whole. Nothing is visible until promoted, so a
    /// completion the server refuses publishes nothing.
    pub(crate) async fn stage(
        &self,
        batches: &[OutputBatch],
    ) -> std::result::Result<(), temporalio_streams::StreamError> {
        use prost::Message;
        use temporalio_streams::proto::{StagedBatch, StagedRecord};
        for batch in batches {
            let records = batch
                .records
                .iter()
                .map(|record| {
                    temporalio_streams::stored_output_record(
                        &record.topic,
                        record.kind,
                        record.body.clone(),
                        &record.content_hash,
                        &batch.run_id,
                    )
                    .map(|stored| StagedRecord {
                        topic: record.topic.clone(),
                        record: stored.encode_to_vec(),
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            self.store
                .stage(StagedBatch {
                    chain: Some(self.chain(&batch.workflow_id, &batch.first_run_id)),
                    run_id: batch.run_id.clone(),
                    token: batch.token.clone(),
                    history_floor_event_id: batch.history_floor_event_id,
                    records,
                })
                .await?;
        }
        Ok(())
    }

    /// Promotes the stages replay proved committed, in commit order, each once.
    ///
    /// One read of the chain's pending stages tells which proven stages still need a promotion,
    /// so a long History costs one call, not one per commit. A stage that is no longer pending
    /// still gets its closes, since a reader that settled it knows nothing of them.
    pub(crate) async fn promote_proven(&self, run_id: &str, proven: &[ProvenStage]) {
        let mut unsettled = vec![];
        for stage in proven {
            if let Some(undecided) = self.undecided.lock().get_mut(run_id) {
                undecided.retain(|batch| batch.token != stage.token);
            }
            if !self.settled.lock().contains(&stage.token) {
                unsettled.push(stage);
            }
        }
        let mut pending_by_chain = HashMap::new();
        let mut promotions = vec![];
        for stage in unsettled {
            let chain = self.chain(&stage.workflow_id, &stage.first_run_id);
            let key = (chain.workflow_id.clone(), chain.first_run_id.clone());
            if !pending_by_chain.contains_key(&key) {
                let pending = self.pending_tokens(&chain).await;
                pending_by_chain.insert(key.clone(), pending);
            }
            let settled = pending_by_chain[&key]
                .as_ref()
                .is_some_and(|pending| !pending.contains(&stage.token));
            if settled && stage.closes.is_empty() {
                self.settled.lock().put(stage.token.clone(), ());
                continue;
            }
            promotions.push(Promotion {
                stage: self.stage_ref(
                    &stage.workflow_id,
                    &stage.first_run_id,
                    &stage.token,
                    stage.topics.clone(),
                ),
                closes: stage.closes.clone(),
                settled,
            });
        }
        self.promote_then_close(run_id, promotions).await;
    }

    /// The tokens the store holds pending in `chain`, or `None` when it can't say.
    async fn pending_tokens(
        &self,
        chain: &temporalio_streams::proto::ChainId,
    ) -> Option<std::collections::HashSet<String>> {
        match self.store.pending_stages(chain).await {
            Ok(pending) => Some(pending.into_iter().map(|stage| stage.token).collect()),
            Err(error) => {
                debug!(%error, "Could not list pending stream stages; promoting each proven one");
                None
            }
        }
    }

    /// Promotes or aborts `batches`, staged for one completion, by the server's answer to it.
    ///
    /// An accepted completion also settles the run's stages whose outcome was unknown. Replay
    /// before this completion proved and promoted every one of them that History holds, so the
    /// rest never committed.
    pub(crate) async fn settle(
        &self,
        run_id: &str,
        batches: Vec<OutputBatch>,
        outcome: CompletionOutcome,
    ) {
        match outcome {
            CompletionOutcome::Accepted => {
                let promotions = batches
                    .iter()
                    .map(|batch| Promotion {
                        stage: self.batch_ref(batch),
                        closes: batch.closes.clone(),
                        settled: false,
                    })
                    .collect();
                self.promote_then_close(run_id, promotions).await;
                let undecided = self.undecided.lock().pop(run_id).unwrap_or_default();
                for batch in &undecided {
                    self.abort(self.batch_ref(batch)).await;
                }
            }
            CompletionOutcome::Refused => {
                for batch in &batches {
                    self.abort(self.batch_ref(batch)).await;
                }
            }
            CompletionOutcome::Unknown => {
                if !batches.is_empty() {
                    let mut undecided = self.undecided.lock();
                    match undecided.get_mut(run_id) {
                        Some(held) => held.extend(batches),
                        None => {
                            undecided.put(run_id.to_string(), batches);
                        }
                    }
                }
            }
        }
    }

    /// Closes `chain`'s streams in the background, retrying with backoff until it succeeds or the
    /// Worker stops.
    ///
    /// The chain's latest run decides. A retried or cron run ends with a successor in the same
    /// chain, which keeps the streams open, so the successor's own final task closes them.
    ///
    /// It waits first for `run_id`'s promotions and topic closes that have not finished. A
    /// notifier keeps the first close it hears, so the chain's close must not beat the result a
    /// topic closes with. A promotion or topic close that was given up on still lets the chain
    /// close.
    pub(crate) fn close_after_final_task(
        self: &std::sync::Arc<Self>,
        client: std::sync::Arc<dyn crate::worker::client::WorkerClient>,
        chain: ChainToClose,
        run_id: String,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            this.run_work_done(&run_id).await;
            if shutdown.is_cancelled() {
                return;
            }
            let mut delay = RETRY_FIRST;
            loop {
                match this.close_if_ended(client.as_ref(), &chain).await {
                    Ok(()) => return,
                    Err(error) => warn!(
                        workflow_id = %chain.workflow_id,
                        %error,
                        "Could not close a finished run chain's streams; trying again in {delay:?}"
                    ),
                }
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => return,
                }
                delay = (delay * 2).min(RETRY_CAP);
            }
        });
    }

    /// Waits for `run_id`'s promotions and topic closes, those that start meanwhile too.
    async fn run_work_done(&self, run_id: &str) {
        loop {
            let tasks = {
                let mut runs = self.runs.lock();
                let Some(work) = runs.get_mut(run_id) else {
                    return;
                };
                if work.tasks.is_empty() {
                    runs.remove(run_id);
                    return;
                }
                std::mem::take(&mut work.tasks)
            };
            for task in tasks {
                let _ = task.await;
            }
        }
    }

    async fn close_if_ended(
        &self,
        client: &dyn crate::worker::client::WorkerClient,
        chain: &ChainToClose,
    ) -> std::result::Result<(), String> {
        use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;
        let description = client
            .describe_workflow_execution(chain.workflow_id.clone())
            .await
            .map_err(|status| status.to_string())?;
        if let Some(latest) = description.workflow_execution_info
            && latest.first_run_id == chain.first_run_id
            && matches!(
                latest.status(),
                WorkflowExecutionStatus::Running | WorkflowExecutionStatus::ContinuedAsNew
            )
        {
            return Ok(());
        }
        self.store
            .close_chain(&self.chain(&chain.workflow_id, &chain.first_run_id))
            .await
            .map_err(|error| error.to_string())
    }

    /// Promotes `promotions` of `run_id` in commit order, each followed by its topic closes.
    ///
    /// A promotion that fails hands itself and every later one of the run to a background task
    /// that retries it with backoff until it lands or the Worker stops. Until that task is done,
    /// the run's later promotions queue behind it. A batch keeps its closes until it is settled.
    async fn promote_then_close(&self, run_id: &str, promotions: Vec<Promotion>) {
        let mut promotions = std::collections::VecDeque::from(promotions);
        {
            let mut runs = self.runs.lock();
            if let Some(backlog) = runs.get_mut(run_id).and_then(|work| work.backlog.as_mut()) {
                backlog.extend(promotions);
                return;
            }
        }
        while let Some(promotion) = promotions.pop_front() {
            if !promotion.settled {
                match self.store.promote(&promotion.stage).await {
                    Ok(result) => note_promoted(&self.settled, &promotion.stage, result),
                    Err(error) if refused_for_good(&error) => {
                        warn!(
                            token = %promotion.stage.token,
                            %error,
                            "The store can't promote committed stream output"
                        );
                        continue;
                    }
                    Err(error) => {
                        warn!(
                            token = %promotion.stage.token,
                            %error,
                            "Could not promote committed stream output; trying again"
                        );
                        promotions.push_front(promotion);
                        self.retry_in_background(run_id, promotions);
                        return;
                    }
                }
            }
            self.settled.lock().put(promotion.stage.token.clone(), ());
            let task = close_topics(
                self.store.clone(),
                &promotion.stage,
                promotion.closes,
                self.shutdown.clone(),
            );
            if let Some(task) = task {
                track(&self.runs, run_id, task);
            }
        }
    }

    /// Starts the task that settles `run_id`'s backlog, which begins with `promotions`.
    fn retry_in_background(&self, run_id: &str, promotions: std::collections::VecDeque<Promotion>) {
        let store = self.store.clone();
        let settled = self.settled.clone();
        let runs = self.runs.clone();
        let shutdown = self.shutdown.clone();
        let run = run_id.to_string();
        let mut guard = self.runs.lock();
        guard.entry(run.clone()).or_default().backlog = Some(promotions);
        let task = tokio::spawn(async move {
            let mut delay = RETRY_FIRST;
            loop {
                let promotion = {
                    let mut runs = runs.lock();
                    let Some(work) = runs.get_mut(&run) else {
                        return;
                    };
                    let next = work
                        .backlog
                        .as_mut()
                        .and_then(|backlog| backlog.pop_front());
                    match next {
                        Some(promotion) => promotion,
                        None => {
                            work.backlog = None;
                            return;
                        }
                    }
                };
                let mut promoted = promotion.settled;
                while !promoted {
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = shutdown.cancelled() => return,
                    }
                    match store.promote(&promotion.stage).await {
                        Ok(result) => {
                            note_promoted(&settled, &promotion.stage, result);
                            promoted = true;
                            delay = RETRY_FIRST;
                        }
                        Err(error) if refused_for_good(&error) => {
                            warn!(
                                token = %promotion.stage.token,
                                %error,
                                "The store can't promote committed stream output"
                            );
                            break;
                        }
                        Err(error) => {
                            warn!(
                                token = %promotion.stage.token,
                                %error,
                                "Could not promote committed stream output; trying again in \
                                 {delay:?}"
                            );
                            delay = (delay * 2).min(RETRY_CAP);
                        }
                    }
                }
                if !promoted {
                    continue;
                }
                settled.lock().put(promotion.stage.token.clone(), ());
                let task = close_topics(
                    store.clone(),
                    &promotion.stage,
                    promotion.closes,
                    shutdown.clone(),
                );
                if let Some(task) = task {
                    track(&runs, &run, task);
                }
            }
        });
        guard.get_mut(run_id).expect("just added").tasks.push(task);
    }

    async fn abort(&self, stage: temporalio_streams::proto::StageRef) {
        match self.store.abort(&stage).await {
            Ok(()) => {
                self.settled.lock().put(stage.token, ());
            }
            Err(error) => {
                // The stage stays pending in the store, and a reader settles it from History.
                warn!(token = %stage.token, %error, "Could not abort uncommitted stream output");
            }
        }
    }

    fn chain(&self, workflow_id: &str, first_run_id: &str) -> temporalio_streams::proto::ChainId {
        temporalio_streams::proto::ChainId {
            namespace: self.namespace.clone(),
            workflow_id: workflow_id.to_string(),
            first_run_id: first_run_id.to_string(),
        }
    }

    fn batch_ref(&self, batch: &OutputBatch) -> temporalio_streams::proto::StageRef {
        let mut topics: Vec<String> = vec![];
        for record in &batch.records {
            if !topics.contains(&record.topic) {
                topics.push(record.topic.clone());
            }
        }
        self.stage_ref(
            &batch.workflow_id,
            &batch.first_run_id,
            &batch.token,
            topics,
        )
    }

    fn stage_ref(
        &self,
        workflow_id: &str,
        first_run_id: &str,
        token: &str,
        topics: Vec<String>,
    ) -> temporalio_streams::proto::StageRef {
        temporalio_streams::proto::StageRef {
            chain: Some(self.chain(workflow_id, first_run_id)),
            token: token.to_string(),
            topics,
        }
    }
}

/// Builds the manifest for the records one completion committed.
///
/// `replaying` allows records without bodies, since lang sends no body when nothing is stored
/// again. The run id and stage token are recorded but replay never compares them. Fails the
/// Workflow Task when a record is one no writer stores, when Core can't name the task's history
/// floor, or when the manifest is over its budget. Each failure names what to fix, because the
/// task fails the same way on every retry.
pub(super) fn build_output_manifest(
    records: &[OutputRecord],
    history_floor_event_id: Option<i64>,
    run_id: &str,
    stage_token: String,
    replaying: bool,
) -> Result<ExternalOutputStreamManifest> {
    let Some(history_floor_event_id) = history_floor_event_id.filter(|id| *id > 0) else {
        return Err(refused(
            "Core could not identify the exact event preceding this Workflow Task's scheduled \
             event"
                .to_string(),
        ));
    };
    if records.is_empty() {
        return Err(refused("the commit carried no records".to_string()));
    }
    for (index, record) in records.iter().enumerate() {
        check_record(record, replaying)
            .map_err(|reason| refused(format!("record {index} {reason}")))?;
    }
    let mut topics: Vec<&str> = vec![];
    let mut by_topic: HashMap<&str, Vec<&OutputRecord>> = HashMap::new();
    for record in records {
        let topic = record.topic.as_str();
        by_topic
            .entry(topic)
            .or_insert_with(|| {
                topics.push(topic);
                vec![]
            })
            .push(record);
    }
    let topic_manifests: Vec<ExternalOutputTopicManifest> = topics
        .iter()
        .map(|topic| {
            let topic_records = &by_topic[topic];
            ExternalOutputTopicManifest {
                topic: topic.to_string(),
                record_count: topic_records.len() as u32,
                logical_byte_count: topic_records.iter().map(|r| r.logical_size).sum(),
                logical_fingerprint: fingerprint(topic_records.iter().map(|r| FingerprintRecord {
                    topic,
                    kind: r.kind,
                    content_hash: &r.content_hash,
                }))
                .to_vec(),
                finished: topic_records
                    .iter()
                    .any(|r| r.kind == StreamRecordKind::Finish as i32),
            }
        })
        .collect();
    let manifest = ExternalOutputStreamManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        fingerprint_version: FINGERPRINT_VERSION,
        stage_token,
        history_floor_event_id,
        run_id: run_id.to_string(),
        segments: vec![ExternalOutputSegmentManifest {
            record_counts_by_topic: topic_manifests.iter().map(|t| t.record_count).collect(),
        }],
        topics: topic_manifests,
        provider_id: String::new(),
        provider_format_version: STORE_FORMAT_VERSION,
    };
    let encoded_len = prost::Message::encoded_len(&manifest);
    if encoded_len > MAX_MANIFEST_BYTES {
        return Err(refused(format!(
            "the manifest of {} topic(s) encodes to {encoded_len} bytes, over the {} KiB marker \
             budget; spread the topics across Workflow Tasks",
            manifest.topics.len(),
            MAX_MANIFEST_BYTES / 1024
        )));
    }
    Ok(manifest)
}

/// Compares the manifest Core built from the records lang committed while replaying with the one
/// History recorded.
///
/// The stage token is left out because nothing is staged on replay, so there is no token. The
/// run id is left out because a reset forks the base run's History, so the new run replays
/// markers that name the base run. Everything else derives from the Workflow's own output.
pub(super) fn check_replayed_output_manifest(
    mut replayed: ExternalOutputStreamManifest,
    recorded: Option<ExternalOutputStreamManifest>,
) -> Result<()> {
    let Some(mut recorded) = recorded else {
        return Err(WFMachinesError::Nondeterminism(
            "Lang committed external output while replaying a Workflow Task that recorded none"
                .to_string(),
        ));
    };
    // Only the logical output must replay the same. A reset run replays markers that name its
    // base run, a stage token is new on every attempt, and a provider can be renamed or swapped
    // for a Replayer without changing what the Workflow published.
    for manifest in [&mut replayed, &mut recorded] {
        manifest.stage_token.clear();
        manifest.run_id.clear();
        manifest.provider_id.clear();
    }
    if replayed != recorded {
        return Err(WFMachinesError::Nondeterminism(format!(
            "External output committed while replaying differs from the manifest recorded in \
             History: committed {replayed:?}, recorded {recorded:?}"
        )));
    }
    Ok(())
}

/// Refuses closes the marker could not prove. Each close names a topic once, carries its result,
/// and has its topic's FINISH record among the committed records.
pub(super) fn check_output_closes(records: &[OutputRecord], closes: &[OutputClose]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for close in closes {
        let reason = if close.topic.is_empty() {
            Some("a close names no topic".to_string())
        } else if !seen.insert(close.topic.as_str()) {
            Some(format!("topic {:?} is closed twice", close.topic))
        } else if close.result.is_none() {
            Some(format!(
                "the close of topic {:?} has no result",
                close.topic
            ))
        } else if !records
            .iter()
            .any(|r| r.topic == close.topic && r.kind == StreamRecordKind::Finish as i32)
        {
            Some(format!(
                "the close of topic {:?} has no FINISH record in the same commit",
                close.topic
            ))
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(refused(reason));
        }
    }
    Ok(())
}

fn check_record(record: &OutputRecord, replaying: bool) -> std::result::Result<(), String> {
    if record.topic.is_empty() {
        return Err("has no topic".to_string());
    }
    match StreamRecordKind::try_from(record.kind) {
        Ok(StreamRecordKind::Data) => {
            if record.body.is_none() && !replaying {
                return Err("is DATA without a body".to_string());
            }
            if record.content_hash.len() != CONTENT_HASH_BYTES {
                return Err(format!(
                    "is DATA with a {}-byte content hash, not the {CONTENT_HASH_BYTES}-byte \
                     SHA-256 of its plaintext body",
                    record.content_hash.len()
                ));
            }
        }
        Ok(StreamRecordKind::Finish) => {
            if record.body.is_some() || !record.content_hash.is_empty() || record.logical_size != 0
            {
                return Err("is FINISH with a body".to_string());
            }
        }
        _ => return Err(format!("has kind {}, not DATA or FINISH", record.kind)),
    }
    Ok(())
}

fn refused(reason: String) -> WFMachinesError {
    WFMachinesError::Fatal(format!("Refusing a stream output commit, since {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    const RUN_ID: &str = "run-id";
    const FLOOR: i64 = 4;

    fn data(topic: &str, hash: u8, size: u64) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: b"ciphertext".to_vec(),
                ..Default::default()
            }),
            content_hash: vec![hash; CONTENT_HASH_BYTES],
            logical_size: size,
        }
    }

    fn finish(topic: &str) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Finish as i32,
            ..Default::default()
        }
    }

    fn build(records: &[OutputRecord]) -> Result<ExternalOutputStreamManifest> {
        build_output_manifest(records, Some(FLOOR), RUN_ID, "token".to_string(), false)
    }

    fn rejection(records: &[OutputRecord]) -> String {
        build(records)
            .expect_err("commit must be refused")
            .to_string()
    }

    #[test]
    fn the_manifest_groups_topics_in_order_of_first_publish() {
        let records = [
            data("b", 1, 3),
            data("a", 2, 4),
            data("b", 3, 5),
            finish("b"),
        ];
        let manifest = build(&records).unwrap();
        assert_eq!(manifest.schema_version, 1);
        assert_eq!(manifest.fingerprint_version, 2);
        assert_eq!(manifest.stage_token, "token");
        assert_eq!(manifest.history_floor_event_id, FLOOR);
        assert_eq!(manifest.run_id, RUN_ID);
        assert_eq!(manifest.provider_format_version, 1);
        let topics: Vec<_> = manifest
            .topics
            .iter()
            .map(|t| {
                (
                    t.topic.as_str(),
                    t.record_count,
                    t.logical_byte_count,
                    t.finished,
                )
            })
            .collect();
        assert_eq!(topics, [("b", 3, 8, true), ("a", 1, 4, false)]);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].record_counts_by_topic, [3, 1]);
        let b = [&records[0], &records[2], &records[3]];
        assert_eq!(
            manifest.topics[0].logical_fingerprint,
            fingerprint(b.iter().map(|r| FingerprintRecord {
                topic: "b",
                kind: r.kind,
                content_hash: &r.content_hash,
            }))
        );
    }

    #[test]
    fn the_manifest_never_depends_on_the_encoded_body() {
        let mut reencrypted = data("t", 1, 3);
        reencrypted.body.as_mut().unwrap().data = b"other ciphertext".to_vec();
        let mut replayed = data("t", 1, 3);
        replayed.body = None;
        let live = build(&[data("t", 1, 3)]).unwrap();
        assert_eq!(build(&[reencrypted]).unwrap(), live);
        let replay =
            build_output_manifest(&[replayed], Some(FLOOR), RUN_ID, String::new(), true).unwrap();
        assert_eq!(replay.topics, live.topics);
    }

    #[test]
    fn an_unknown_floor_is_refused_rather_than_guessed() {
        for floor in [None, Some(0)] {
            let error = build_output_manifest(
                &[data("t", 1, 1)],
                floor,
                RUN_ID,
                "token".to_string(),
                false,
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains("exact event"), "{error}");
        }
    }

    #[test]
    fn each_record_no_writer_stores_is_refused_with_its_own_message() {
        let mut no_body = data("t", 1, 1);
        no_body.body = None;
        let mut short_hash = data("t", 1, 1);
        short_hash.content_hash.truncate(3);
        let mut finish_with_body = finish("t");
        finish_with_body.body = Some(Payload::default());
        let unspecified = OutputRecord {
            kind: StreamRecordKind::Unspecified as i32,
            ..data("t", 1, 1)
        };
        for (record, expected) in [
            (data("", 1, 1), "record 1 has no topic"),
            (no_body, "record 1 is DATA without a body"),
            (short_hash, "record 1 is DATA with a 3-byte content hash"),
            (finish_with_body, "record 1 is FINISH with a body"),
            (unspecified, "record 1 has kind 0"),
        ] {
            let message = rejection(&[data("t", 9, 1), record]);
            assert!(message.contains(expected), "{expected}: {message}");
        }
        assert!(rejection(&[]).contains("no records"));
    }

    fn manifest() -> ExternalOutputStreamManifest {
        build(&[data("a", 1, 3), finish("b")]).unwrap()
    }

    #[test]
    fn a_replayed_commit_matches_the_recorded_manifest_whatever_its_token() {
        let mut replayed = manifest();
        replayed.stage_token.clear();
        check_replayed_output_manifest(replayed, Some(manifest())).unwrap();
    }

    #[test]
    fn a_replayed_commit_matches_a_manifest_recorded_by_another_run() {
        let mut recorded = manifest();
        recorded.run_id = "reset-base-run".to_string();
        check_replayed_output_manifest(manifest(), Some(recorded)).unwrap();
    }

    #[test]
    fn a_replayed_commit_matches_a_manifest_recorded_under_another_store_name() {
        let mut recorded = manifest();
        recorded.provider_id = "renamed-store".to_string();
        check_replayed_output_manifest(manifest(), Some(recorded)).unwrap();
    }

    #[test]
    fn a_replayed_commit_in_another_store_format_is_nondeterministic() {
        let mut recorded = manifest();
        recorded.provider_format_version = 2;
        assert!(matches!(
            check_replayed_output_manifest(manifest(), Some(recorded)),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("differs")
        ));
    }

    #[test]
    fn a_replayed_commit_that_differs_from_history_is_nondeterministic() {
        let replayed = build(&[data("a", 2, 3), finish("b")]).unwrap();
        assert!(matches!(
            check_replayed_output_manifest(replayed, Some(manifest())),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("differs")
        ));
    }

    #[test]
    fn a_replayed_commit_with_nothing_recorded_is_nondeterministic() {
        assert!(matches!(
            check_replayed_output_manifest(manifest(), None),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("recorded none")
        ));
    }

    fn close(topic: &str) -> OutputClose {
        OutputClose {
            topic: topic.to_string(),
            result: Some(Payload::default()),
        }
    }

    #[test]
    fn a_close_needs_its_topics_finish_in_the_same_commit() {
        let records = [data("a", 1, 1), finish("a"), data("b", 2, 1)];
        check_output_closes(&records, &[close("a")]).unwrap();
        let mut no_result = close("a");
        no_result.result = None;
        for (closes, expected) in [
            (vec![close("b")], "topic \"b\" has no FINISH"),
            (vec![close("a"), close("a")], "closed twice"),
            (vec![close("")], "names no topic"),
            (vec![no_result], "has no result"),
        ] {
            let message = check_output_closes(&records, &closes)
                .unwrap_err()
                .to_string();
            assert!(message.contains(expected), "{expected}: {message}");
        }
    }

    #[test]
    fn a_manifest_over_its_budget_is_refused() {
        let records: Vec<_> = (0..1200)
            .map(|i| data(&format!("topic-{i:04}-{}", "x".repeat(16)), 1, 1))
            .collect();
        let message = rejection(&records);
        assert!(
            message.contains("over the 64 KiB marker budget"),
            "{message}"
        );
    }

    #[cfg(feature = "streams")]
    mod store {
        use super::{super::*, data};
        use parking_lot::Mutex;
        use std::sync::Arc;
        use temporalio_streams::{
            StreamError, StreamResult, StreamStore,
            proto::{
                ChainId, DeleteOwnerRequest, DeleteOwnerResponse, PendingStage, PromoteOutcome,
                PromoteResult, StageRef, StagedBatch, StoreAppendRequest, StoreAppendResponse,
                StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse,
            },
        };

        /// Logs promotions, aborts and topic closes, and refuses everything else. It can be told
        /// to fail or refuse some promotions and closes, and which stages it holds pending.
        #[derive(Default)]
        struct Log {
            entries: Mutex<Vec<String>>,
            /// How many closes fail for now before one lands.
            close_failures: Mutex<usize>,
            /// Tokens whose promotion is refused every time.
            refused_promotes: Mutex<Vec<String>>,
            /// Topics whose next close is refused.
            refused_closes: Mutex<Vec<String>>,
            /// Topics the store can't close.
            unsupported_closes: Mutex<Vec<String>>,
            /// How many promotions fail for now before one lands.
            promote_failures: Mutex<usize>,
            /// The pending tokens, or `None` when the store can't list them.
            pending: Mutex<Option<Vec<String>>>,
            pending_reads: Mutex<usize>,
        }

        impl Log {
            fn failing_closes(count: usize) -> Self {
                Self {
                    close_failures: Mutex::new(count),
                    ..Default::default()
                }
            }

            fn refusing_promotes_of(token: &str) -> Self {
                Self {
                    refused_promotes: Mutex::new(vec![token.to_string()]),
                    ..Default::default()
                }
            }

            fn failing_promotes(count: usize) -> Self {
                Self {
                    promote_failures: Mutex::new(count),
                    ..Default::default()
                }
            }

            fn refusing_one_close_of(topic: &str) -> Self {
                Self {
                    refused_closes: Mutex::new(vec![topic.to_string()]),
                    ..Default::default()
                }
            }

            fn unable_to_close(topic: &str) -> Self {
                Self {
                    unsupported_closes: Mutex::new(vec![topic.to_string()]),
                    ..Default::default()
                }
            }

            fn holding_pending(tokens: &[&str]) -> Self {
                Self {
                    pending: Mutex::new(Some(tokens.iter().map(|t| t.to_string()).collect())),
                    ..Default::default()
                }
            }

            fn log(&self) -> Vec<String> {
                self.entries.lock().clone()
            }

            /// Waits a bounded time for `count` entries, since closes run in the background.
            async fn entries(&self, count: usize) -> Vec<String> {
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while self.entries.lock().len() < count {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("the store never got the calls");
                self.log()
            }
        }

        #[async_trait::async_trait]
        impl StreamStore for Log {
            fn name(&self) -> &str {
                "log"
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
            async fn record_at(
                &self,
                _: &ChainId,
                _: &str,
                _: &str,
            ) -> StreamResult<Option<Vec<u8>>> {
                Err(StreamError::unsupported("record_at"))
            }
            async fn stage(&self, _: StagedBatch) -> StreamResult<()> {
                Err(StreamError::unsupported("stage"))
            }
            async fn promote(&self, stage: &StageRef) -> StreamResult<PromoteResult> {
                if self.refused_promotes.lock().contains(&stage.token) {
                    return Err(StreamError::refused("this stage can't be promoted"));
                }
                let mut failures = self.promote_failures.lock();
                if *failures > 0 {
                    *failures -= 1;
                    return Err(StreamError::storage("redis failed over"));
                }
                self.entries.lock().push(format!("promote {}", stage.token));
                Ok(PromoteResult {
                    outcome: PromoteOutcome::Promoted as i32,
                    records: 1,
                })
            }
            async fn abort(&self, stage: &StageRef) -> StreamResult<()> {
                self.entries.lock().push(format!("abort {}", stage.token));
                Ok(())
            }
            async fn close_chain(&self, _: &ChainId) -> StreamResult<()> {
                Err(StreamError::unsupported("close_chain"))
            }
            async fn close_topic(
                &self,
                chain: &ChainId,
                topic: &str,
                result: Option<temporalio_streams::proto::Payload>,
            ) -> StreamResult<()> {
                if self.unsupported_closes.lock().iter().any(|t| t == topic) {
                    self.entries.lock().push(format!("unsupported {topic}"));
                    return Err(StreamError::unsupported("close_topic"));
                }
                {
                    let mut refused = self.refused_closes.lock();
                    if let Some(at) = refused.iter().position(|t| t == topic) {
                        refused.remove(at);
                        self.entries.lock().push(format!("refused {topic}"));
                        return Err(StreamError::refused("the replica is read only"));
                    }
                }
                let mut failures = self.close_failures.lock();
                if *failures > 0 {
                    *failures -= 1;
                    return Err(StreamError::storage("redis is down"));
                }
                let result = result.map(|r| String::from_utf8(r.data).unwrap());
                self.entries.lock().push(format!(
                    "close {} {topic} {}",
                    chain.first_run_id,
                    result.unwrap_or_default()
                ));
                Ok(())
            }
            async fn pending_stages(&self, _: &ChainId) -> StreamResult<Vec<PendingStage>> {
                *self.pending_reads.lock() += 1;
                let pending = self.pending.lock().clone();
                let tokens = pending.ok_or_else(|| StreamError::unsupported("pending_stages"))?;
                Ok(tokens
                    .into_iter()
                    .map(|token| PendingStage {
                        token,
                        ..Default::default()
                    })
                    .collect())
            }
            async fn delete_owner(
                &self,
                _: DeleteOwnerRequest,
            ) -> StreamResult<DeleteOwnerResponse> {
                Err(StreamError::unsupported("delete_owner"))
            }
        }

        fn proven(token: &str) -> ProvenStage {
            ProvenStage {
                token: token.to_string(),
                workflow_id: "wf".to_string(),
                first_run_id: "run".to_string(),
                topics: vec!["t".to_string()],
                closes: vec![],
            }
        }

        fn batch(token: &str) -> OutputBatch {
            OutputBatch {
                token: token.to_string(),
                history_floor_event_id: 3,
                workflow_id: "wf".to_string(),
                run_id: "run".to_string(),
                first_run_id: "run".to_string(),
                records: vec![data("t", 1, 1)],
                closes: vec![],
            }
        }

        fn closing(topic: &str, result: &str) -> OutputClose {
            OutputClose {
                topic: topic.to_string(),
                result: Some(temporalio_streams::proto::Payload {
                    data: result.as_bytes().to_vec(),
                    ..Default::default()
                }),
            }
        }

        fn store_on(log: &Arc<Log>) -> OutputStore {
            OutputStore::new(
                log.clone(),
                "ns".to_string(),
                tokio_util::sync::CancellationToken::new(),
            )
        }

        #[tokio::test]
        async fn a_close_follows_its_stage_promotion() {
            let log = Arc::new(Log::default());
            let store = store_on(&log);
            let mut closed = batch("a");
            closed.closes = vec![closing("t", "done"), closing("u", "also done")];
            store
                .settle("run", vec![closed], CompletionOutcome::Accepted)
                .await;
            assert_eq!(
                log.entries(3).await,
                ["promote a", "close run t done", "close run u also done"]
            );
        }

        #[tokio::test]
        async fn a_close_is_retried_until_it_lands() {
            let log = Arc::new(Log::failing_closes(2));
            let store = store_on(&log);
            let mut closed = batch("a");
            closed.closes = vec![closing("t", "done")];
            store
                .settle("run", vec![closed], CompletionOutcome::Accepted)
                .await;
            assert_eq!(log.entries(2).await, ["promote a", "close run t done"]);
            assert_eq!(*log.close_failures.lock(), 0);
        }

        #[tokio::test]
        async fn a_stage_replay_proves_closes_its_topics_too() {
            let log = Arc::new(Log::default());
            let store = store_on(&log);
            let mut stage = proven("a");
            stage.closes = vec![closing("t", "done")];
            store.promote_proven("run", &[stage]).await;
            assert_eq!(log.entries(2).await, ["promote a", "close run t done"]);
        }

        #[tokio::test]
        async fn a_failed_promotion_is_retried_before_its_closes_and_the_next_stage() {
            let log = Arc::new(Log::failing_promotes(1));
            let store = store_on(&log);
            let mut closed = batch("a");
            closed.closes = vec![closing("t", "done")];
            store
                .settle("run", vec![closed, batch("b")], CompletionOutcome::Accepted)
                .await;
            assert_eq!(
                log.entries(3).await,
                ["promote a", "close run t done", "promote b"]
            );
        }

        #[tokio::test]
        async fn a_failed_proven_promotion_is_retried_before_its_closes() {
            let log = Arc::new(Log::failing_promotes(2));
            let store = store_on(&log);
            let mut stage = proven("a");
            stage.closes = vec![closing("t", "done")];
            store.promote_proven("run", &[stage]).await;
            assert_eq!(log.entries(2).await, ["promote a", "close run t done"]);
        }

        #[tokio::test]
        async fn a_close_the_store_cannot_take_is_given_up() {
            let log = Arc::new(Log::unable_to_close("t"));
            let store = store_on(&log);
            let mut closed = batch("a");
            closed.closes = vec![closing("t", "done"), closing("u", "also done")];
            store
                .settle("run", vec![closed], CompletionOutcome::Accepted)
                .await;
            assert_eq!(
                log.entries(3).await,
                ["promote a", "unsupported t", "close run u also done"]
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert_eq!(
                log.log().len(),
                3,
                "a close the store can't take is not retried"
            );
        }

        #[tokio::test]
        async fn a_refused_close_is_retried_until_it_lands() {
            // A replica that turned read only in a failover refuses for a while.
            let log = Arc::new(Log::refusing_one_close_of("t"));
            let store = store_on(&log);
            let mut closed = batch("a");
            closed.closes = vec![closing("t", "done")];
            store
                .settle("run", vec![closed], CompletionOutcome::Accepted)
                .await;
            assert_eq!(
                log.entries(3).await,
                ["promote a", "refused t", "close run t done"]
            );
        }

        #[tokio::test]
        async fn a_later_completion_waits_behind_a_failed_promotion() {
            let log = Arc::new(Log::failing_promotes(1));
            let store = store_on(&log);
            store
                .settle("run", vec![batch("a")], CompletionOutcome::Accepted)
                .await;
            store
                .settle("run", vec![batch("b")], CompletionOutcome::Accepted)
                .await;
            assert_eq!(log.entries(2).await, ["promote a", "promote b"]);
        }

        #[tokio::test]
        async fn replay_promotes_only_the_proven_stages_still_pending() {
            let log = Arc::new(Log::holding_pending(&["c"]));
            let store = store_on(&log);
            store
                .promote_proven("run", &[proven("a"), proven("b"), proven("c")])
                .await;
            assert_eq!(log.log(), ["promote c"]);
            assert_eq!(*log.pending_reads.lock(), 1);
        }

        #[tokio::test]
        async fn a_proven_stage_a_reader_settled_still_closes_its_topics() {
            let log = Arc::new(Log::holding_pending(&[]));
            let store = store_on(&log);
            let mut stage = proven("a");
            stage.closes = vec![closing("t", "done")];
            store.promote_proven("run", &[stage]).await;
            assert_eq!(log.entries(1).await, ["close run t done"]);
        }

        #[tokio::test]
        async fn a_commit_the_server_did_not_apply_closes_nothing() {
            let log = Arc::new(Log::default());
            let store = store_on(&log);
            for outcome in [CompletionOutcome::Refused, CompletionOutcome::Unknown] {
                let mut closed = batch("a");
                closed.closes = vec![closing("t", "done")];
                store.settle("run", vec![closed], outcome).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert_eq!(log.log(), ["abort a"]);
        }

        #[tokio::test]
        async fn a_refused_promotion_holds_back_its_closes_and_the_later_stages() {
            let log = Arc::new(Log::refusing_promotes_of("a"));
            let store = store_on(&log);
            let mut closed = batch("a");
            closed.closes = vec![closing("t", "done")];
            store
                .settle("run", vec![closed, batch("b")], CompletionOutcome::Accepted)
                .await;
            store
                .settle("run", vec![batch("c")], CompletionOutcome::Accepted)
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert!(log.log().is_empty(), "{:?}", log.log());
        }

        #[tokio::test]
        async fn a_proven_stage_is_promoted_once_however_often_the_run_replays() {
            let log = Arc::new(Log::default());
            let store = store_on(&log);
            for _ in 0..3 {
                store
                    .promote_proven("run", &[proven("a"), proven("b")])
                    .await;
            }
            assert_eq!(log.log(), ["promote a", "promote b"]);
        }

        #[tokio::test]
        async fn an_undecided_stage_that_replay_proves_is_not_aborted() {
            let log = Arc::new(Log::default());
            let store = store_on(&log);
            store
                .settle(
                    "run",
                    vec![batch("kept"), batch("lost")],
                    CompletionOutcome::Unknown,
                )
                .await;
            store.promote_proven("run", &[proven("kept")]).await;
            store
                .settle("run", vec![], CompletionOutcome::Accepted)
                .await;
            assert_eq!(log.log(), ["promote kept", "abort lost"]);
        }

        #[tokio::test]
        async fn undecided_stages_belong_to_their_run() {
            let log = Arc::new(Log::default());
            let store = store_on(&log);
            store
                .settle("other", vec![batch("theirs")], CompletionOutcome::Unknown)
                .await;
            store
                .settle("run", vec![batch("mine")], CompletionOutcome::Accepted)
                .await;
            assert_eq!(log.log(), ["promote mine"]);
        }
    }
}
