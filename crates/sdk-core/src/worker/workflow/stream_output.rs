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
    streams::{FINGERPRINT_VERSION, FingerprintRecord, check_topic, fingerprint},
};

/// The marker version Core writes. Readers must accept every version up to this one.
pub(super) const MARKER_SCHEMA_VERSION: u32 = 1;
const MANIFEST_SCHEMA_VERSION: u32 = 1;
/// The layout a store keeps records in, recorded so a reader of History knows how to find them.
const STORE_FORMAT_VERSION: u32 = 1;
/// Keeps one output proof well inside the server's per-event payload limits.
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const CONTENT_HASH_BYTES: usize = 32;
/// The largest record lang may commit, in logical bytes. Lang checks it at the publish call, and
/// Core checks again, since a store call that fails on size fails every retry of the task too.
const MAX_RECORD_BYTES: u64 = 3 * 512 * 1024;
/// The most logical bytes one activation may commit, checked the same way as a record.
const MAX_COMMIT_BYTES: u64 = 16 * 1024 * 1024;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// The records one activation committed, appended when its completion reaches Core.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutputBatch {
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) first_run_id: String,
    /// The run's producer attempt, from History.
    pub(crate) attempt: i64,
    /// One append per topic, in order of first publish.
    pub(crate) topics: Vec<TopicOutput>,
}

/// The records one activation committed to one topic.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TopicOutput {
    pub(crate) topic: String,
    /// The first record's sequence in the run's producer. Records the run committed to the topic
    /// before this activation come first, so a rerun of a task sends the same sequences.
    pub(crate) sequence: i64,
    /// The topic's logical fingerprint from the manifest. It names the batch whatever the codec
    /// made of the bodies, so a repeat of the append always deduplicates.
    pub(crate) digest: Vec<u8>,
    pub(crate) records: Vec<OutputRecord>,
}

/// Splits `records`, which `manifest` describes, into one append per topic, and counts them into
/// `committed`, the records the run committed to each topic so far.
pub(super) fn output_by_topic(
    records: Vec<OutputRecord>,
    manifest: &ExternalOutputStreamManifest,
    committed: &mut HashMap<String, i64>,
) -> Vec<TopicOutput> {
    let mut topics: Vec<TopicOutput> = manifest
        .topics
        .iter()
        .map(|topic| {
            let count = committed.entry(topic.topic.clone()).or_default();
            let sequence = *count + 1;
            *count += i64::from(topic.record_count);
            TopicOutput {
                topic: topic.topic.clone(),
                sequence,
                digest: topic.logical_fingerprint.clone(),
                records: Vec::with_capacity(topic.record_count as usize),
            }
        })
        .collect();
    for record in records {
        if let Some(topic) = topics.iter_mut().find(|t| t.topic == record.topic) {
            topic.records.push(record);
        }
    }
    topics
}

/// Counts the records a replayed commit's `manifest` holds into `committed`, as
/// [output_by_topic] does for a live one.
pub(super) fn count_replayed_output(
    manifest: &ExternalOutputStreamManifest,
    committed: &mut HashMap<String, i64>,
) {
    for topic in &manifest.topics {
        *committed.entry(topic.topic.clone()).or_default() += i64::from(topic.record_count);
    }
}

/// The run chain a run's streams are keyed by.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RunChain {
    pub(crate) workflow_id: String,
    pub(crate) first_run_id: String,
    /// The failure the final task failed the Workflow with, after lang's codec.
    pub(crate) failure: Option<temporalio_common::protos::temporal::api::failure::v1::Failure>,
    /// The details the final task canceled the Workflow with, after lang's codec.
    pub(crate) cancel_details:
        Option<temporalio_common::protos::temporal::api::common::v1::Payloads>,
}

/// The topic closes a completion carries to the report path.
#[derive(Debug)]
// Only a Worker with a stream store closes topics.
#[cfg_attr(not(feature = "streams"), allow(dead_code))]
pub(crate) struct ReportedOutput {
    pub(crate) chain: RunChain,
    /// The task's own closes, in commit order, to close once the server accepts the task.
    pub(crate) closes: Vec<OutputClose>,
    /// The closes of commits replay found in History, in commit order. Only those whose
    /// completion this Worker sent without learning the answer still need closing.
    pub(crate) proven: Vec<OutputClose>,
    /// Whether the completion ends the chain, so its streams close once it is accepted.
    pub(crate) ends_chain: bool,
}

impl ReportedOutput {
    /// What to report with the next completion, leaving both lists empty, or `None` when there is
    /// nothing.
    pub(crate) fn take(
        chain: RunChain,
        closes: &mut Vec<OutputClose>,
        proven: &mut Vec<OutputClose>,
        ends_chain: bool,
    ) -> Option<Box<Self>> {
        if closes.is_empty() && proven.is_empty() && !ends_chain {
            return None;
        }
        Some(Box::new(Self {
            chain,
            closes: std::mem::take(closes),
            proven: std::mem::take(proven),
            ends_chain,
        }))
    }
}

/// What the server answered to a completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionOutcome {
    /// The commands were applied, so each output marker is in History.
    Accepted,
    /// The commands were not applied, so the task runs again.
    Refused,
    /// The completion may or may not have been applied.
    Unknown,
}

/// How often an append that may not have landed is sent again before the task fails.
#[cfg(feature = "streams")]
const APPEND_TRIES: u32 = 4;
#[cfg(all(feature = "streams", not(test)))]
const APPEND_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(100);
// Keeps the retry tests fast.
#[cfg(all(feature = "streams", test))]
const APPEND_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(1);

#[cfg(feature = "streams")]
const MAX_UNDECIDED_RUNS: usize = 1000;
#[cfg(all(feature = "streams", not(test)))]
const RETRY_FIRST: std::time::Duration = std::time::Duration::from_secs(1);
// Keeps the retry tests fast.
#[cfg(all(feature = "streams", test))]
const RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(10);
#[cfg(feature = "streams")]
const RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(60);

/// The Worker's side of a Workflow's stream output: appends what each activation committed, and
/// closes the topics a task closed once the server accepts it.
#[cfg(feature = "streams")]
pub(crate) struct OutputStore {
    store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
    namespace: String,
    /// Closes whose completion's answer was lost, by run. The run is evicted then, so the replay
    /// before its next completion shows which of them History holds. Bounded, since a run that
    /// moves to another Worker never comes back here.
    undecided: parking_lot::Mutex<lru::LruCache<String, Vec<OutputClose>>>,
    /// Background topic closes, by run.
    runs: std::sync::Arc<parking_lot::Mutex<HashMap<String, Vec<tokio::task::JoinHandle<()>>>>>,
    /// Stops the close retries when the Worker stops.
    shutdown: tokio_util::sync::CancellationToken,
}

/// Whether the store can never take the call. Every other failure is retried, since the close
/// lives only in this Worker until the store takes it. `REFUSED` is retried too, since it is an
/// ACL or configuration fault that an operator can fix.
#[cfg(feature = "streams")]
fn refused_for_good(error: &temporalio_streams::StreamError) -> bool {
    error.kind == temporalio_streams::proto::StreamFailureKind::Unsupported
}

/// What a chain's streams close with when its owner ended without closing them itself, and its ID
/// is gone or in another chain. The server's owner check closes with the same failure.
#[cfg(feature = "streams")]
const OWNER_ENDED_FAILURE: &str = "the owner Workflow ended without closing the stream";

/// What a chain's streams close with when its owner's latest run ended with `status`, as a
/// Workflow-run Nexus operation completes for the same status, so a caller sees the same outcome
/// whichever side closes the stream.
#[cfg(feature = "streams")]
fn chain_end_outcome(
    status: temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus,
    chain: &RunChain,
) -> temporalio_streams::CloseOutcome {
    use temporalio_common::protos::temporal::api::{
        enums::v1::WorkflowExecutionStatus,
        failure::v1::{
            CanceledFailureInfo, Failure, TerminatedFailureInfo, TimeoutFailureInfo,
            failure::FailureInfo,
        },
    };
    use temporalio_streams::CloseOutcome;
    let with_info = |message: &str, info: FailureInfo| {
        CloseOutcome::Failure(Failure {
            message: message.to_string(),
            failure_info: Some(info),
            ..Default::default()
        })
    };
    match status {
        WorkflowExecutionStatus::Completed => {
            CloseOutcome::Result(temporalio_streams::empty_close_result())
        }
        WorkflowExecutionStatus::Failed => match &chain.failure {
            Some(failure) => CloseOutcome::Failure(failure.clone()),
            // Another Worker's task may have failed the run.
            None => stream_failure("the owner Workflow failed", "StreamOwnerFailed"),
        },
        WorkflowExecutionStatus::Canceled => with_info(
            "operation canceled",
            FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: chain.cancel_details.clone(),
                ..Default::default()
            }),
        ),
        WorkflowExecutionStatus::Terminated => with_info(
            "operation terminated",
            FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo::default()),
        ),
        WorkflowExecutionStatus::TimedOut => with_info(
            "operation exceeded internal timeout",
            FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo::default()),
        ),
        _ => stream_failure(OWNER_ENDED_FAILURE, ""),
    }
}

/// A non-retryable application failure of type `failure_type`.
#[cfg(feature = "streams")]
fn stream_failure(message: &str, failure_type: &str) -> temporalio_streams::CloseOutcome {
    use temporalio_common::protos::temporal::api::failure::v1::{
        ApplicationFailureInfo, Failure, failure::FailureInfo,
    };
    temporalio_streams::CloseOutcome::Failure(Failure {
        message: message.to_string(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: failure_type.to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    })
}

/// What the operations that handed out `close`'s topic complete with.
#[cfg(feature = "streams")]
fn close_outcome(close: &OutputClose) -> temporalio_streams::CloseOutcome {
    match &close.failure {
        Some(failure) => temporalio_streams::CloseOutcome::Failure(failure.clone()),
        None => temporalio_streams::CloseOutcome::Result(
            close
                .result
                .clone()
                .unwrap_or_else(temporalio_streams::empty_close_result),
        ),
    }
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
                .close_topic_with(chain, &close.topic, close_outcome(close))
                .await
            {
                Ok(()) => break,
                Err(error) => error,
            };
            if refused_for_good(&error) {
                warn!(
                    topic = %close.topic,
                    %error,
                    "The store refused to close a stream topic a Workflow closed"
                );
                break;
            }
            warn!(
                topic = %close.topic,
                %error,
                "Could not close a stream topic a Workflow closed; trying again in {delay:?}"
            );
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => return,
            }
            delay = (delay * 2).min(RETRY_CAP);
        }
    }
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
            undecided: parking_lot::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(MAX_UNDECIDED_RUNS).expect("not zero"),
            )),
            runs: Default::default(),
            shutdown,
        }
    }

    /// Closes the topics a completion closed, by the server's answer to it, in commit order and
    /// in the background.
    ///
    /// The FINISH records are already appended. The close waits for the answer, because a task
    /// the server refuses runs again and appends to the topic under a new attempt, which a closed
    /// topic would refuse. Replay calls nothing, except for closes this Worker kept because the
    /// answer to their completion was lost and replay found their commit.
    pub(crate) fn settle_closes(
        &self,
        run_id: &str,
        output: ReportedOutput,
        outcome: CompletionOutcome,
    ) {
        let mut closes = vec![];
        {
            let mut undecided = self.undecided.lock();
            if let Some(kept) = undecided.get_mut(run_id) {
                for proven in &output.proven {
                    if let Some(index) = kept.iter().position(|k| k.topic == proven.topic) {
                        closes.push(kept.remove(index));
                    }
                }
            }
            match outcome {
                CompletionOutcome::Accepted => {
                    // Replay before this completion found every kept close History holds.
                    undecided.pop(run_id);
                    closes.extend(output.closes);
                }
                CompletionOutcome::Refused => {}
                CompletionOutcome::Unknown => {
                    if !output.closes.is_empty() {
                        match undecided.get_mut(run_id) {
                            Some(kept) => kept.extend(output.closes),
                            None => {
                                undecided.put(run_id.to_string(), output.closes);
                            }
                        }
                    }
                }
            }
        }
        if closes.is_empty() {
            return;
        }
        let store = self.store.clone();
        let chain = self.chain(&output.chain);
        let shutdown = self.shutdown.clone();
        let task = tokio::spawn(async move {
            close_in_order(&*store, &chain, &closes, &shutdown).await;
        });
        let mut runs = self.runs.lock();
        runs.retain(|_, tasks| {
            tasks.retain(|task| !task.is_finished());
            !tasks.is_empty()
        });
        runs.entry(run_id.to_string()).or_default().push(task);
    }

    /// Closes `chain`'s streams in the background, retrying with backoff until it succeeds or the
    /// Worker stops.
    ///
    /// The chain's latest run decides. A retried or cron run ends with a successor in the same
    /// chain, which keeps the streams open, so the successor's own final task closes them.
    ///
    /// It waits first for `run_id`'s topic closes that have not finished. A notifier keeps the
    /// first close it hears, so the chain's close must not beat the result a topic closes with. A
    /// topic close that was given up on still lets the chain close. Every append of the final task
    /// landed before its completion was sent, so before this close.
    pub(crate) fn close_after_final_task(
        self: &std::sync::Arc<Self>,
        client: std::sync::Arc<dyn crate::worker::client::WorkerClient>,
        chain: RunChain,
        run_id: String,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            this.run_closes_done(&run_id).await;
            if this.shutdown.is_cancelled() {
                return;
            }
            let mut delay = RETRY_FIRST;
            loop {
                match this.close_if_ended(client.as_ref(), &chain).await {
                    Ok(()) => return,
                    Err(error) if refused_for_good(&error) => {
                        warn!(
                            workflow_id = %chain.workflow_id,
                            %error,
                            "The store can't close a finished run chain's streams"
                        );
                        return;
                    }
                    Err(error) => warn!(
                        workflow_id = %chain.workflow_id,
                        %error,
                        "Could not close a finished run chain's streams; trying again in {delay:?}"
                    ),
                }
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = this.shutdown.cancelled() => return,
                }
                delay = (delay * 2).min(RETRY_CAP);
            }
        });
    }

    /// Waits for `run_id`'s topic closes, those that start meanwhile too.
    async fn run_closes_done(&self, run_id: &str) {
        loop {
            let Some(tasks) = self.runs.lock().remove(run_id) else {
                return;
            };
            for task in tasks {
                let _ = task.await;
            }
        }
    }

    async fn close_if_ended(
        &self,
        client: &dyn crate::worker::client::WorkerClient,
        chain: &RunChain,
    ) -> std::result::Result<(), temporalio_streams::StreamError> {
        use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;
        let latest = match client
            .describe_workflow_execution(chain.workflow_id.clone())
            .await
        {
            Ok(description) => description.workflow_execution_info,
            // Retention removed the Workflow, so its chain has ended.
            Err(status) if status.code() == tonic::Code::NotFound => None,
            Err(status) => return Err(temporalio_streams::StreamError::storage(status.message())),
        };
        let outcome = match latest.filter(|latest| latest.first_run_id == chain.first_run_id) {
            Some(latest) => match latest.status() {
                WorkflowExecutionStatus::Running
                | WorkflowExecutionStatus::ContinuedAsNew
                | WorkflowExecutionStatus::Paused => return Ok(()),
                status => chain_end_outcome(status, chain),
            },
            None => stream_failure(OWNER_ENDED_FAILURE, ""),
        };
        self.store.end_chain_with(&self.chain(chain), outcome).await
    }

    /// Waits a short while for the stream notifications still out, since they're sent in the
    /// background and a process that stops right after its Worker would drop them.
    pub(crate) async fn flush_notifications(&self) {
        if tokio::time::timeout(
            temporalio_streams::FLUSH_LIMIT,
            self.store.flush_notifications(),
        )
        .await
        .is_err()
        {
            warn!("Stream notifications were still out when the Worker stopped");
        }
    }

    fn chain(&self, chain: &RunChain) -> temporalio_streams::proto::ChainId {
        temporalio_streams::proto::ChainId {
            namespace: self.namespace.clone(),
            workflow_id: chain.workflow_id.clone(),
            first_run_id: chain.first_run_id.clone(),
        }
    }

    /// Appends `batch` as its run's producer, one topic after another.
    ///
    /// A failure that may have landed is sent again with a short backoff. The store answers a
    /// repeat of the attempt's newest batch with its positions, so this never writes twice. Any
    /// other failure, or one that lasts, is returned, and the Workflow Task must fail.
    pub(crate) async fn append(
        &self,
        batch: &OutputBatch,
    ) -> std::result::Result<(), temporalio_streams::StreamError> {
        use prost::Message;
        use temporalio_streams::proto::{ChainId, StoreAppendRequest, StreamFailureKind};
        let producer_id = temporalio_streams::workflow_producer_id(&batch.run_id);
        for topic in &batch.topics {
            let records = topic
                .records
                .iter()
                .zip(topic.sequence..)
                .map(|(record, sequence)| {
                    temporalio_streams::stored_output_record(
                        &record.topic,
                        record.kind,
                        record.body.clone(),
                        &record.content_hash,
                        &batch.run_id,
                        batch.attempt,
                        sequence,
                    )
                    .map(|stored| stored.encode_to_vec())
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let request = StoreAppendRequest {
                chain: Some(ChainId {
                    namespace: self.namespace.clone(),
                    workflow_id: batch.workflow_id.clone(),
                    first_run_id: batch.first_run_id.clone(),
                }),
                topic: topic.topic.clone(),
                producer_id: producer_id.clone(),
                attempt: batch.attempt,
                sequence: topic.sequence,
                digest: topic.digest.clone(),
                records,
            };
            let mut delay = APPEND_RETRY_FIRST;
            let mut tries = 1;
            loop {
                match self.store.append(request.clone()).await {
                    Ok(_) => break,
                    Err(error)
                        if tries < APPEND_TRIES
                            && matches!(
                                error.kind,
                                StreamFailureKind::OutcomeUnknown | StreamFailureKind::Storage
                            ) =>
                    {
                        debug!(topic = %topic.topic, %error, "Appending stream output again");
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                        tries += 1;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

/// Builds the manifest for the records one completion committed.
///
/// `replaying` allows records without bodies, since lang sends no body when nothing is stored
/// again. The run id is recorded but replay never compares it. Fails the
/// Workflow Task when a record is one no writer stores, when Core can't name the task's history
/// floor, or when the manifest is over its budget. Each failure names what to fix, because the
/// task fails the same way on every retry.
pub(super) fn build_output_manifest(
    records: &[OutputRecord],
    history_floor_event_id: Option<i64>,
    run_id: &str,
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
    // Replay takes what History holds.
    if !replaying {
        let committed: u64 = records.iter().map(|r| r.logical_size).sum();
        if committed > MAX_COMMIT_BYTES {
            return Err(refused(format!(
                "the commit holds {committed} logical bytes, over the {} MiB budget of one \
                 activation; publish across activations",
                MAX_COMMIT_BYTES / 1024 / 1024
            )));
        }
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
/// The run id is left out because a reset forks the base run's History, so the new run replays
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
    // base run, and a provider can be renamed, swapped for a Replayer or change its layout without
    // changing what the Workflow published.
    for manifest in [&mut replayed, &mut recorded] {
        manifest.run_id.clear();
        manifest.provider_id.clear();
        manifest.provider_format_version = 0;
    }
    if replayed != recorded {
        return Err(WFMachinesError::Nondeterminism(format!(
            "External output committed while replaying differs from the manifest recorded in \
             History: committed {replayed:?}, recorded {recorded:?}"
        )));
    }
    Ok(())
}

/// Refuses closes the marker could not prove. Each close names a topic once, carries a result or
/// a failure but not both, and has its topic's FINISH record among the committed records.
pub(super) fn check_output_closes(records: &[OutputRecord], closes: &[OutputClose]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for close in closes {
        let reason = if close.topic.is_empty() {
            Some("a close names no topic".to_string())
        } else if !seen.insert(close.topic.as_str()) {
            Some(format!("topic {:?} is closed twice", close.topic))
        } else if close.result.is_none() && close.failure.is_none() {
            Some(format!(
                "the close of topic {:?} has no result or failure",
                close.topic
            ))
        } else if close.result.is_some() && close.failure.is_some() {
            Some(format!(
                "the close of topic {:?} carries both a result and a failure",
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
    check_topic(&record.topic)?;
    if !replaying && record.logical_size > MAX_RECORD_BYTES {
        return Err(format!(
            "holds {} logical bytes, over the 1.5 MiB record budget",
            record.logical_size
        ));
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
        build_output_manifest(records, Some(FLOOR), RUN_ID, false)
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
        let replay = build_output_manifest(&[replayed], Some(FLOOR), RUN_ID, true).unwrap();
        assert_eq!(replay.topics, live.topics);
    }

    #[test]
    fn an_unknown_floor_is_refused_rather_than_guessed() {
        for floor in [None, Some(0)] {
            let error = build_output_manifest(&[data("t", 1, 1)], floor, RUN_ID, false)
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
    fn a_replayed_commit_matches_the_recorded_manifest() {
        check_replayed_output_manifest(manifest(), Some(manifest())).unwrap();
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
    fn a_replayed_commit_matches_a_manifest_recorded_under_another_store_format() {
        // A later Core may lay out records differently. That is the store's concern, not output
        // the Workflow published.
        let mut recorded = manifest();
        recorded.provider_format_version = 2;
        check_replayed_output_manifest(manifest(), Some(recorded)).unwrap();
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

    #[test]
    fn a_topic_no_store_keys_reliably_is_refused() {
        // A store may separate the parts of its keys with a control character.
        let long = "t".repeat(257);
        for (topic, expected) in [
            ("a\u{1f}b", "holds a control character"),
            (long.as_str(), "at most 256 UTF-8 bytes"),
        ] {
            let message = rejection(&[data(topic, 1, 1)]);
            assert!(message.contains(expected), "{expected}: {message}");
        }
        build(&[data(&"t".repeat(256), 1, 1)]).unwrap();
    }

    #[test]
    fn a_record_or_commit_over_its_byte_budget_is_refused() {
        let at_limit = data("t", 1, 3 * 512 * 1024);
        build(std::slice::from_ref(&at_limit)).unwrap();
        let message = rejection(&[data("t", 1, 3 * 512 * 1024 + 1)]);
        assert!(
            message.contains("over the 1.5 MiB record budget"),
            "{message}"
        );

        let commit: Vec<_> = (0..11).map(|i| data("t", i, 3 * 512 * 1024)).collect();
        let message = rejection(&commit);
        assert!(message.contains("over the 16 MiB budget"), "{message}");
    }

    fn close(topic: &str) -> OutputClose {
        OutputClose {
            topic: topic.to_string(),
            result: Some(Payload::default()),
            failure: None,
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

    #[cfg(feature = "streams")]
    #[test]
    fn a_close_without_a_result_carries_a_null_payload() {
        let outcome = close_outcome(&OutputClose {
            topic: "a".to_string(),
            result: None,
            failure: None,
        });
        let temporalio_streams::CloseOutcome::Result(result) = outcome else {
            panic!("a close without a failure carries a result");
        };
        assert_eq!(result.metadata["encoding"], b"binary/null");
    }

    #[test]
    fn a_close_carries_a_result_or_a_failure() {
        let records = [data("a", 1, 1), finish("a")];
        let failed = OutputClose {
            topic: "a".to_string(),
            result: None,
            failure: Some(Default::default()),
        };
        check_output_closes(&records, std::slice::from_ref(&failed)).unwrap();
        let both = OutputClose {
            result: Some(Payload::default()),
            ..failed
        };
        let message = check_output_closes(&records, &[both])
            .unwrap_err()
            .to_string();
        assert!(message.contains("both a result and a failure"), "{message}");
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
    mod closes {
        use super::super::*;
        use std::sync::Arc;
        use temporalio_common::protos::temporal::api::common::v1::Payload;
        use temporalio_streams::{
            MemoryStore, StreamStore,
            proto::{ChainId, StoreReadRequest},
        };

        const RUN: &str = "run-1";

        fn close(topic: &str) -> OutputClose {
            OutputClose {
                topic: topic.to_string(),
                result: Some(Payload::default()),
                failure: None,
            }
        }

        fn chain() -> RunChain {
            RunChain {
                workflow_id: "wf".to_string(),
                first_run_id: RUN.to_string(),
                failure: None,
                cancel_details: None,
            }
        }

        fn reported(closes: &[&str], proven: &[&str]) -> ReportedOutput {
            ReportedOutput {
                chain: chain(),
                closes: closes.iter().map(|t| close(t)).collect(),
                proven: proven.iter().map(|t| close(t)).collect(),
                ends_chain: false,
            }
        }

        fn store() -> (Arc<MemoryStore>, OutputStore) {
            let memory = Arc::new(MemoryStore::new());
            let output = OutputStore::new(memory.clone(), "ns".to_string(), Default::default());
            (memory, output)
        }

        /// Which of `topics` the store holds closed once the background closes are done.
        async fn closed(
            output: &OutputStore,
            memory: &MemoryStore,
            topics: &[&str],
        ) -> Vec<String> {
            let tasks: Vec<_> = output.runs.lock().drain().flat_map(|(_, t)| t).collect();
            for task in tasks {
                task.await.unwrap();
            }
            let mut closed = vec![];
            for topic in topics {
                let read = memory
                    .read(StoreReadRequest {
                        chain: Some(ChainId {
                            namespace: "ns".to_string(),
                            workflow_id: "wf".to_string(),
                            first_run_id: RUN.to_string(),
                        }),
                        topic: topic.to_string(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                if read.closed {
                    closed.push(topic.to_string());
                }
            }
            closed
        }

        #[tokio::test]
        async fn an_accepted_completion_closes_its_topics() {
            let (memory, store) = store();
            store.settle_closes(RUN, closing(&["a", "b"]), CompletionOutcome::Accepted);
            assert_eq!(closed(&store, &memory, &["a", "b"]).await, ["a", "b"]);
        }

        fn closing(closes: &[&str]) -> ReportedOutput {
            reported(closes, &[])
        }

        #[tokio::test]
        async fn a_refused_completion_closes_nothing() {
            let (memory, store) = store();
            store.settle_closes(RUN, closing(&["a"]), CompletionOutcome::Refused);
            assert!(closed(&store, &memory, &["a"]).await.is_empty());
        }

        #[tokio::test]
        async fn a_lost_answer_closes_what_replay_finds() {
            let (memory, store) = store();
            store.settle_closes(RUN, closing(&["a", "b"]), CompletionOutcome::Unknown);
            assert!(closed(&store, &memory, &["a", "b"]).await.is_empty());
            // Replay found the commit that closed `a` only, so the one that closed `b` never
            // reached History.
            store.settle_closes(RUN, reported(&[], &["a"]), CompletionOutcome::Accepted);
            assert_eq!(closed(&store, &memory, &["a", "b"]).await, ["a"]);
            // Nothing is kept once a completion is accepted.
            store.settle_closes(RUN, reported(&[], &["b"]), CompletionOutcome::Accepted);
            assert_eq!(closed(&store, &memory, &["a", "b"]).await, ["a"]);
        }

        #[tokio::test]
        async fn a_close_with_a_failure_closes_its_topic() {
            let (memory, store) = store();
            let mut output = closing(&[]);
            output.closes = vec![OutputClose {
                topic: "a".to_string(),
                result: None,
                failure: Some(
                    temporalio_common::protos::temporal::api::failure::v1::Failure {
                        message: "the agent gave up".to_string(),
                        ..Default::default()
                    },
                ),
            }];
            store.settle_closes(RUN, output, CompletionOutcome::Accepted);
            assert_eq!(closed(&store, &memory, &["a"]).await, ["a"]);
        }

        #[tokio::test]
        async fn replay_on_a_worker_that_kept_nothing_closes_nothing() {
            let (memory, store) = store();
            store.settle_closes(RUN, reported(&[], &["a"]), CompletionOutcome::Accepted);
            assert!(closed(&store, &memory, &["a"]).await.is_empty());
        }
    }

    #[cfg(feature = "streams")]
    mod notifications {
        use super::{super::*, data};
        use parking_lot::Mutex;
        use std::sync::Arc;
        use temporalio_streams::{
            MemoryStore, StreamResult, StreamStore,
            proto::{
                ChainId, DeleteOwnerRequest, DeleteOwnerResponse, Payload, StoreAppendRequest,
                StoreAppendResponse, StoreLatestRequest, StoreLatestResponse, StoreReadRequest,
                StoreReadResponse,
            },
        };

        /// A memory store whose notifications either log their flush or never go out.
        #[derive(Default)]
        struct Flushing {
            inner: MemoryStore,
            flushes: Mutex<usize>,
            stuck: bool,
        }

        #[async_trait::async_trait]
        impl StreamStore for Flushing {
            fn name(&self) -> &str {
                "flushing"
            }
            async fn append(&self, r: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
                self.inner.append(r).await
            }
            async fn read(&self, r: StoreReadRequest) -> StreamResult<StoreReadResponse> {
                self.inner.read(r).await
            }
            async fn latest(&self, r: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
                self.inner.latest(r).await
            }
            async fn record_at(
                &self,
                c: &ChainId,
                t: &str,
                p: &str,
            ) -> StreamResult<Option<Vec<u8>>> {
                self.inner.record_at(c, t, p).await
            }
            async fn close_chain(&self, c: &ChainId) -> StreamResult<()> {
                self.inner.close_chain(c).await
            }
            async fn close_topic(
                &self,
                c: &ChainId,
                t: &str,
                r: Option<Payload>,
            ) -> StreamResult<()> {
                self.inner.close_topic(c, t, r).await
            }
            async fn delete_owner(
                &self,
                r: DeleteOwnerRequest,
            ) -> StreamResult<DeleteOwnerResponse> {
                self.inner.delete_owner(r).await
            }
            async fn flush_notifications(&self) {
                if self.stuck {
                    std::future::pending::<()>().await;
                }
                *self.flushes.lock() += 1;
            }
        }

        /// Records the stream notifications it was sent.
        #[derive(Default)]
        struct Recorder(Mutex<Vec<temporalio_streams::StreamNotification>>);

        #[async_trait::async_trait]
        impl temporalio_streams::NotifierClient for Recorder {
            async fn notify(
                &self,
                notification: temporalio_streams::StreamNotification,
            ) -> Result<(), temporalio_streams::NotifyError> {
                self.0.lock().push(notification);
                Ok(())
            }
        }

        fn batch(topic: &str) -> OutputBatch {
            OutputBatch {
                workflow_id: "wf".to_string(),
                run_id: "run".to_string(),
                first_run_id: "run".to_string(),
                attempt: 1,
                topics: vec![TopicOutput {
                    topic: topic.to_string(),
                    sequence: 1,
                    digest: vec![1; 32],
                    records: vec![data(topic, 1, 3)],
                }],
            }
        }

        #[tokio::test]
        async fn an_append_notifies_each_topic_it_made_visible() {
            let recorder = Arc::new(Recorder::default());
            let notifying = Arc::new(temporalio_streams::NotifyingStore::new(
                Arc::new(MemoryStore::new()),
                Arc::new(temporalio_streams::Notifier::new(recorder.clone(), 10)),
            ));
            let store = OutputStore::new(notifying, "ns".to_string(), Default::default());
            store.append(&batch("t")).await.unwrap();
            store.flush_notifications().await;
            let sent = recorder.0.lock().clone();
            assert_eq!(sent.len(), 1);
            assert_eq!((sent[0].topic.as_str(), sent[0].counter), ("t", 1));
            assert_eq!(sent[0].chain.first_run_id, "run");
        }

        /// A Worker's output store over memory that notifies `recorder`.
        fn notifying_output(recorder: &Arc<Recorder>) -> OutputStore {
            let notifying = Arc::new(temporalio_streams::NotifyingStore::new(
                Arc::new(temporalio_streams::MemoryStore::new()),
                Arc::new(temporalio_streams::Notifier::new(recorder.clone(), 10)),
            ));
            OutputStore::new(notifying, "ns".to_string(), Default::default())
        }

        /// Waits until the background store close has sent its notifier close, then until every
        /// notification is out.
        async fn closed_on_the_server(store: &OutputStore, recorder: &Recorder) {
            for _ in 0..500 {
                if recorder.0.lock().iter().any(|n| n.close_result().is_some()) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            store.flush_notifications().await;
        }

        fn progress_and_closes(recorder: &Recorder) -> Vec<(String, i64, Option<Vec<u8>>)> {
            recorder
                .0
                .lock()
                .iter()
                .map(|n| {
                    let result = n.close_result().as_ref().map(|result| result.data.clone());
                    (n.topic.clone(), n.counter, result)
                })
                .collect()
        }

        fn closing(topic: &str, result: &str) -> OutputClose {
            OutputClose {
                topic: topic.to_string(),
                result: Some(Payload {
                    data: result.as_bytes().to_vec(),
                    ..Default::default()
                }),
                failure: None,
            }
        }

        fn reported(closes: Vec<OutputClose>, proven: Vec<OutputClose>) -> ReportedOutput {
            ReportedOutput {
                chain: RunChain {
                    workflow_id: "wf".to_string(),
                    first_run_id: "run".to_string(),
                    failure: None,
                    cancel_details: None,
                },
                closes,
                proven,
                ends_chain: false,
            }
        }

        #[tokio::test]
        async fn a_workflow_close_ends_the_stream_on_the_server_after_its_output() {
            let recorder = Arc::new(Recorder::default());
            let store = notifying_output(&recorder);
            store.append(&batch("t")).await.unwrap();
            store.settle_closes(
                "run",
                reported(vec![closing("t", "done")], vec![]),
                CompletionOutcome::Accepted,
            );
            closed_on_the_server(&store, &recorder).await;
            assert_eq!(
                progress_and_closes(&recorder),
                [
                    ("t".to_string(), 1, None),
                    ("t".to_string(), 2, Some(b"done".to_vec())),
                ],
                "the close comes after the output's progress and outranks it"
            );
        }

        #[tokio::test]
        async fn a_close_replay_proves_ends_the_stream_on_the_server() {
            let recorder = Arc::new(Recorder::default());
            let store = notifying_output(&recorder);
            store.append(&batch("t")).await.unwrap();
            // The answer to the completion was lost, so the close waits for replay.
            store.settle_closes(
                "run",
                reported(vec![closing("t", "done")], vec![]),
                CompletionOutcome::Unknown,
            );
            store.settle_closes(
                "run",
                reported(vec![], vec![closing("t", "done")]),
                CompletionOutcome::Accepted,
            );
            closed_on_the_server(&store, &recorder).await;
            assert_eq!(
                progress_and_closes(&recorder),
                [
                    ("t".to_string(), 1, None),
                    ("t".to_string(), 2, Some(b"done".to_vec())),
                ]
            );
        }

        #[tokio::test]
        async fn a_stopping_worker_waits_for_its_stream_notifications() {
            let flushing = Arc::new(Flushing::default());
            OutputStore::new(flushing.clone(), "ns".to_string(), Default::default())
                .flush_notifications()
                .await;
            assert_eq!(*flushing.flushes.lock(), 1);
        }

        #[tokio::test(start_paused = true)]
        async fn a_stopping_worker_waits_for_its_stream_notifications_only_so_long() {
            let store = OutputStore::new(
                Arc::new(Flushing {
                    stuck: true,
                    ..Default::default()
                }),
                "ns".to_string(),
                Default::default(),
            );
            let started = tokio::time::Instant::now();
            store.flush_notifications().await;
            assert_eq!(started.elapsed(), temporalio_streams::FLUSH_LIMIT);
        }
    }
}
