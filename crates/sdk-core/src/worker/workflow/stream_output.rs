//! The stream records a Workflow Task publishes: lang commits them with an activation completion,
//! and the Worker appends them to its stream store before it reports the completion.

use super::WFMachinesError;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use temporalio_common::{
    protos::{
        coresdk::{
            activity_result::{ActivityResolution, activity_resolution},
            workflow_commands::OutputRecord,
        },
        temporal::{api::failure::v1::Failure, sdk::streams::v1::StreamRecordKind},
    },
    streams::check_topic,
};

const CONTENT_HASH_BYTES: usize = 32;

/// The `ScheduleActivity` header that marks an Activity as the owner of its own streams. The
/// scheduling Workflow's Core closes them when it resolves the Activity. The command holds it, so
/// replay rebuilds the mark with no History read, and the Activity's Worker sees it too.
pub(crate) const STREAM_OWNER_HEADER: &str = "__temporal_stream_owner";

/// The streams of one Workflow Activity to close, with the outcome its scheduling Workflow saw.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ActivityStreamClose {
    pub(crate) workflow_id: String,
    /// The run that scheduled the Activity, which keys its streams.
    pub(crate) run_id: String,
    pub(crate) activity_id: String,
    /// `None` for a completed Activity, which closes with a null result.
    pub(crate) failure: Option<Failure>,
}

impl ActivityStreamClose {
    /// The close for `resolution`, or `None` for one that does not end the Activity.
    pub(crate) fn of(
        workflow_id: &str,
        run_id: &str,
        activity_id: &str,
        resolution: Option<&ActivityResolution>,
    ) -> Option<Self> {
        let failure = match resolution?.status.as_ref()? {
            activity_resolution::Status::Completed(_) => None,
            // A resolution with no failure still ends the Activity, so its streams close with a
            // failure of their own.
            activity_resolution::Status::Failed(failed) => Some(
                failed
                    .failure
                    .clone()
                    .unwrap_or_else(|| no_failure("failed")),
            ),
            activity_resolution::Status::Cancelled(cancelled) => Some(
                cancelled
                    .failure
                    .clone()
                    .unwrap_or_else(|| no_failure("was cancelled")),
            ),
            // Only Local Activities back off, and they own no streams.
            activity_resolution::Status::Backoff(_) => return None,
        };
        Some(Self {
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            failure,
        })
    }
}

fn no_failure(what: &str) -> Failure {
    Failure {
        message: format!("The Activity {what} with no failure"),
        ..Default::default()
    }
}

/// The records one activation committed, appended when its completion reaches Core.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutputBatch {
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) first_run_id: String,
    /// The run's producer attempt for the current Workflow Task, from History.
    pub(crate) attempt: i64,
    /// The current task's WorkflowTaskStarted event id.
    pub(crate) task_started: i64,
    /// The last completed tasks' WorkflowTaskStarted event ids, newest first, from History.
    pub(crate) completed_tasks: Vec<i64>,
    /// One append per topic, in order of first publish.
    pub(crate) topics: Vec<TopicOutput>,
}

/// The records one activation committed to one topic.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TopicOutput {
    pub(crate) topic: String,
    /// The first record's sequence in the attempt. Each attempt has its own dedupe session in the
    /// store, so the count starts at 1 per attempt and goes on across the task's activations.
    pub(crate) sequence: i64,
    pub(crate) records: Vec<OutputRecord>,
}

/// The dedupe sessions of attempts that will not write again, to drop from the store once the
/// server accepts the Workflow Task.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RetiredSessions {
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) first_run_id: String,
    pub(crate) sessions: BTreeMap<i64, BTreeSet<String>>,
    /// The run's attempt, when the task ends the run's chain. The chain closes once the server
    /// accepts the task, since a command the server doesn't take can still keep the run open.
    pub(crate) chain_end: Option<i64>,
}

/// The most record bytes one append carries when the store declares no limit. A larger commit goes
/// as several appends per topic, so no activation is too large to append and lang never fails a
/// task for its size.
pub(crate) const MAX_APPEND_BYTES: usize = 4 << 20;

/// What the store adds to each record besides its body, topic and hash, counted high: the
/// producer id, the attempt, the sequence, the hash's metadata entry and the completed tasks.
const ENVELOPE_BYTES: usize = 256;

/// Splits `records` into appends per topic, in order of first publish, each of at most
/// `max_batch_bytes` past its first record, and counts them into `committed`, the records the
/// attempt committed to each topic so far.
///
/// Fails the Workflow Task when a record is one no writer stores. Each failure names what to
/// fix, since the task fails the same way on every retry.
pub(super) fn output_by_topic(
    records: Vec<OutputRecord>,
    committed: &mut HashMap<String, i64>,
    max_batch_bytes: usize,
) -> Result<Vec<TopicOutput>, WFMachinesError> {
    for (index, record) in records.iter().enumerate() {
        check_record(record).map_err(|reason| {
            WFMachinesError::Fatal(format!(
                "Refusing a stream output commit, since record {index} {reason}"
            ))
        })?;
    }
    let mut topics: Vec<TopicOutput> = vec![];
    for record in records {
        match topics.iter_mut().find(|t| t.topic == record.topic) {
            Some(topic) => topic.records.push(record),
            None => topics.push(TopicOutput {
                topic: record.topic.clone(),
                sequence: 0,
                records: vec![record],
            }),
        }
    }
    let mut appends = vec![];
    for topic in topics {
        let mut bytes = 0;
        for record in topic.records {
            let size = prost::Message::encoded_len(&record) + ENVELOPE_BYTES;
            let full = appends.last().is_some_and(|last: &TopicOutput| {
                last.topic == topic.topic && bytes + size > max_batch_bytes
            });
            if full || appends.last().is_none_or(|last| last.topic != topic.topic) {
                appends.push(TopicOutput {
                    topic: topic.topic.clone(),
                    sequence: 0,
                    records: vec![],
                });
                bytes = 0;
            }
            bytes += size;
            appends.last_mut().unwrap().records.push(record);
        }
    }
    for append in &mut appends {
        let count = committed.entry(append.topic.clone()).or_default();
        append.sequence = *count + 1;
        *count += append.records.len() as i64;
    }
    Ok(appends)
}

fn check_record(record: &OutputRecord) -> Result<(), String> {
    if record.topic.is_empty() {
        return Err("has no topic".to_string());
    }
    check_topic(&record.topic)?;
    match StreamRecordKind::try_from(record.kind) {
        Ok(StreamRecordKind::Data) => {
            if record.body.is_none() {
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
            if record.body.is_some() || !record.content_hash.is_empty() {
                return Err("is FINISH with a body".to_string());
            }
        }
        _ => return Err(format!("has kind {}, not DATA or FINISH", record.kind)),
    }
    Ok(())
}

/// How often an append that may not have landed is sent again before the task fails.
#[cfg(feature = "streams")]
const APPEND_TRIES: u32 = 4;
#[cfg(all(feature = "streams", not(test)))]
const APPEND_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(100);
// Keeps the retry tests fast.
#[cfg(all(feature = "streams", test))]
const APPEND_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(1);

/// The Worker's side of a Workflow's stream output: appends what each activation committed.
#[cfg(feature = "streams")]
pub(crate) struct OutputStore {
    store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
    namespace: String,
    /// Store calls that run after the task that caused them. A Worker shutdown waits for them.
    tasks: tokio_util::task::TaskTracker,
}

#[cfg(feature = "streams")]
impl OutputStore {
    pub(crate) fn new(
        store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
        namespace: String,
    ) -> Self {
        Self {
            store,
            namespace,
            tasks: tokio_util::task::TaskTracker::new(),
        }
    }

    fn chain(&self, workflow_id: &str, first_run_id: &str) -> temporalio_streams::proto::ChainId {
        temporalio_streams::proto::ChainId {
            namespace: self.namespace.clone(),
            workflow_id: workflow_id.to_string(),
            first_run_id: first_run_id.to_string(),
            ..Default::default()
        }
    }

    /// Appends `batch` as its run's producer, one topic after another, and answers the topics
    /// whose records it dropped.
    ///
    /// A failure that may have landed is sent again with a short backoff. The store answers a
    /// repeat of the attempt's newest batch with its positions, so this never writes twice. A
    /// topic the store has closed refuses every retry of the task too, so its records are
    /// dropped and the other topics go on. That holds when the run closed the topic itself
    /// before its task reran. Any other failure, or one that lasts, is returned, and the
    /// Workflow Task must fail.
    pub(crate) async fn append(
        &self,
        batch: &OutputBatch,
    ) -> Result<Vec<String>, temporalio_streams::StreamError> {
        use prost::Message;
        use temporalio_streams::proto::{AppendRecord, StoreAppendRequest, StreamFailureKind};
        let producer_id = temporalio_streams::workflow_producer_id(&batch.run_id);
        let mut dropped = vec![];
        for topic in &batch.topics {
            let records = topic
                .records
                .iter()
                .zip(topic.sequence..)
                .map(|(record, sequence)| {
                    let append = AppendRecord {
                        kind: record.kind,
                        body: record.body.clone(),
                        content_hash: record.content_hash.clone(),
                    };
                    temporalio_streams::stored_append_record(
                        &topic.topic,
                        &producer_id,
                        batch.attempt,
                        sequence,
                        &append,
                    )
                    .map(|mut stored| {
                        // The attempt names both, so a repeat of the batch stores the same.
                        stored.task_started_event_id = batch.task_started;
                        if sequence == topic.sequence {
                            stored.completed_tasks = batch.completed_tasks.clone();
                        }
                        stored.encode_to_vec()
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            temporalio_streams::check_record_limit(&records, &self.store.capabilities())?;
            let request = StoreAppendRequest {
                chain: Some(self.chain(&batch.workflow_id, &batch.first_run_id)),
                topic: topic.topic.clone(),
                producer_id: producer_id.clone(),
                attempt: batch.attempt,
                sequence: topic.sequence,
                digest: temporalio_streams::workflow_batch_digest(
                    &topic.topic,
                    topic
                        .records
                        .iter()
                        .map(|r| (r.kind, r.content_hash.as_slice())),
                ),
                records,
            };
            let mut delay = APPEND_RETRY_FIRST;
            let mut tries = 1;
            loop {
                match temporalio_streams::append_batch(&*self.store, request.clone()).await {
                    Ok(_) => break,
                    Err(error) if error.kind == StreamFailureKind::Closed => {
                        if !dropped.contains(&topic.topic) {
                            dropped.push(topic.topic.clone());
                        }
                        break;
                    }
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
        Ok(dropped)
    }

    /// Closes every topic each Workflow Activity in `closes` wrote, in the background. A close
    /// that fails is logged and dropped, since it must not fail the Workflow Task. The owner
    /// checks then close the streams with a generic failure.
    pub(crate) fn close_activity_streams(&self, closes: Vec<ActivityStreamClose>) {
        if closes.is_empty() {
            return;
        }
        let store = self.store.clone();
        let namespace = self.namespace.clone();
        // Replay never sends the close again, so a Worker shutdown waits for it.
        self.tasks.spawn(async move {
            for close in closes {
                close_one(store.as_ref(), &namespace, close).await;
            }
        });
    }

    /// Drops the store's dedupe sessions of attempts that will not write again, in the
    /// background, so the next activation doesn't wait a store round trip per session. A
    /// failure only leaves a session in the store until retention, so it is logged. Then closes
    /// the chain when the run ended it. A failed close leaves each reader to end on the owner's
    /// status and each producer at its owner check.
    pub(crate) fn retire(&self, retired: RetiredSessions) {
        let chain = self.chain(&retired.workflow_id, &retired.first_run_id);
        let producer_id = temporalio_streams::workflow_producer_id(&retired.run_id);
        let store = self.store.clone();
        // A store without producer state keeps no sessions to drop.
        let sessions = if store.capabilities().producer_state {
            retired.sessions
        } else {
            Default::default()
        };
        self.tasks.spawn(async move {
            for (attempt, topics) in &sessions {
                for topic in topics {
                    if let Err(error) = store
                        .retire_producer(&chain, topic, &producer_id, *attempt)
                        .await
                    {
                        debug!(%topic, attempt, %error, "Could not retire a stream output session");
                    }
                }
            }
            if let Some(attempt) = retired.chain_end
                && let Err(error) =
                    temporalio_streams::close_chain(&*store, &chain, &producer_id, attempt).await
            {
                warn!(%error, "Could not close the streams of a run that ended its chain");
            }
        });
    }

    /// Waits for the store calls still running in the background.
    pub(crate) async fn flush(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

#[cfg(feature = "streams")]
async fn close_one(
    store: &dyn temporalio_streams::StreamStore,
    namespace: &str,
    close: ActivityStreamClose,
) {
    use temporalio_streams::proto::{ChainId, StoredClose, StreamOwnerKind};
    let chain = ChainId {
        namespace: namespace.to_string(),
        workflow_id: close.workflow_id,
        first_run_id: close.run_id,
        owner_kind: StreamOwnerKind::WorkflowActivity as i32,
        activity_id: close.activity_id,
    };
    let closed_by = format!("{}@{}", chain.activity_id, chain.first_run_id);
    let (result, failure) = match close.failure {
        None => (Some(temporalio_streams::empty_close_result()), None),
        Some(failure) => (None, Some(fit_failure(failure, closed_by.len()))),
    };
    let topics = match store.written_topics(&chain).await {
        Ok(topics) => topics,
        Err(error) => {
            warn!(
                workflow_id = %chain.workflow_id,
                activity_id = %chain.activity_id,
                %error,
                "Could not list the topics of an Activity's streams to close them"
            );
            return;
        }
    };
    for topic in topics {
        let stored = StoredClose {
            result: result.clone(),
            failure: failure.clone(),
            closed_by: closed_by.clone(),
        };
        if let Err(error) = store.close_stream(&chain, &topic, stored).await {
            warn!(
                workflow_id = %chain.workflow_id,
                activity_id = %chain.activity_id,
                %topic,
                %error,
                "Could not close an Activity's stream when its Workflow resolved it"
            );
        }
    }
}

/// `failure` cut to what a stored close holds. The causes and the stack trace go first, then the
/// details, and a failure still too large keeps only the start of its message.
#[cfg(feature = "streams")]
fn fit_failure(failure: Failure, closed_by_bytes: usize) -> Failure {
    use prost::Message;
    // Room for the close's other fields and their tags.
    let limit = temporalio_streams::MAX_CLOSE_BYTES - closed_by_bytes - 64;
    if failure.encoded_len() <= limit {
        return failure;
    }
    let mut cut = Failure {
        cause: None,
        stack_trace: String::new(),
        ..failure
    };
    if cut.encoded_len() <= limit {
        return cut;
    }
    cut.encoded_attributes = None;
    cut.failure_info = None;
    if cut.encoded_len() <= limit {
        return cut;
    }
    let mut end = limit / 2;
    while !cut.message.is_char_boundary(end) {
        end -= 1;
    }
    cut.message.truncate(end);
    Failure {
        message: cut.message,
        source: cut.source,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    #[test]
    fn a_failed_or_cancelled_resolution_with_no_failure_still_closes() {
        use temporalio_common::protos::coresdk::activity_result::{
            Cancellation, Failure as Failed,
        };
        for status in [
            activity_resolution::Status::Failed(Failed {
                failure: None,
                ..Default::default()
            }),
            activity_resolution::Status::Cancelled(Cancellation { failure: None }),
        ] {
            let resolution = ActivityResolution {
                status: Some(status),
            };
            let close = ActivityStreamClose::of("wf", "run-1", "tool", Some(&resolution)).unwrap();
            assert!(close.failure.is_some());
        }
    }

    fn data(topic: &str, hash: u8) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: b"ciphertext".to_vec(),
                ..Default::default()
            }),
            content_hash: vec![hash; CONTENT_HASH_BYTES],
        }
    }

    fn finish(topic: &str) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Finish as i32,
            ..Default::default()
        }
    }

    #[test]
    fn a_large_commit_goes_as_several_appends_per_topic() {
        let big = |topic: &str, hash: u8| OutputRecord {
            body: Some(Payload {
                data: vec![7; 3 << 20],
                ..Default::default()
            }),
            ..data(topic, hash)
        };
        let mut committed = HashMap::new();
        let appends = output_by_topic(
            vec![big("a", 1), big("a", 2), big("a", 3), data("b", 4)],
            &mut committed,
            MAX_APPEND_BYTES,
        )
        .unwrap();
        let shape: Vec<(&str, i64, usize)> = appends
            .iter()
            .map(|append| (append.topic.as_str(), append.sequence, append.records.len()))
            .collect();
        assert_eq!(shape, [("a", 1, 1), ("a", 2, 1), ("a", 3, 1), ("b", 1, 1)]);
        assert_eq!(committed["a"], 3);
    }

    #[test]
    fn a_commit_splits_by_the_stores_batch_limit() {
        let mut committed = HashMap::new();
        let records = (0..4).map(|n| data("a", n)).collect();
        let appends = output_by_topic(records, &mut committed, 700).unwrap();
        let sizes: Vec<usize> = appends.iter().map(|append| append.records.len()).collect();
        assert_eq!(sizes, [2, 2]);
        assert_eq!((appends[1].sequence, committed["a"]), (3, 4));
    }

    #[test]
    fn topics_keep_their_order_of_first_publish_and_sequences_go_on() {
        let mut committed = HashMap::new();
        let first = output_by_topic(
            vec![data("b", 1), data("a", 2), finish("b")],
            &mut committed,
            MAX_APPEND_BYTES,
        )
        .unwrap();
        let shape: Vec<_> = first
            .iter()
            .map(|t| (t.topic.as_str(), t.sequence, t.records.len()))
            .collect();
        assert_eq!(shape, [("b", 1, 2), ("a", 1, 1)]);
        let second = output_by_topic(vec![data("a", 3)], &mut committed, MAX_APPEND_BYTES).unwrap();
        assert_eq!((second[0].topic.as_str(), second[0].sequence), ("a", 2));
    }

    #[test]
    fn each_record_no_writer_stores_is_refused_with_its_own_message() {
        let mut no_body = data("t", 1);
        no_body.body = None;
        let mut short_hash = data("t", 1);
        short_hash.content_hash.truncate(3);
        let mut finish_with_body = finish("t");
        finish_with_body.body = Some(Payload::default());
        let unspecified = OutputRecord {
            kind: StreamRecordKind::Unspecified as i32,
            ..data("t", 1)
        };
        for (record, expected) in [
            (data("", 1), "record 1 has no topic"),
            (data("a\u{1f}b", 1), "control character"),
            (no_body, "record 1 is DATA without a body"),
            (short_hash, "record 1 is DATA with a 3-byte content hash"),
            (finish_with_body, "record 1 is FINISH with a body"),
            (unspecified, "record 1 has kind 0"),
        ] {
            let message = output_by_topic(
                vec![data("t", 9), record],
                &mut HashMap::new(),
                MAX_APPEND_BYTES,
            )
            .unwrap_err()
            .to_string();
            assert!(message.contains(expected), "{expected}: {message}");
        }
    }
}
