//! The stream records a Workflow Task publishes: lang commits them with an activation completion,
//! and the Worker appends them to its stream store before it reports the completion.

use super::WFMachinesError;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use temporalio_common::{
    protos::{
        coresdk::workflow_commands::OutputRecord, temporal::sdk::streams::v1::StreamRecordKind,
    },
    streams::check_topic,
};

const CONTENT_HASH_BYTES: usize = 32;

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
}

/// Splits `records` into one append per topic, in order of first publish, and counts them into
/// `committed`, the records the attempt committed to each topic so far.
///
/// Fails the Workflow Task when a record is one no writer stores. Each failure names what to
/// fix, since the task fails the same way on every retry.
pub(super) fn output_by_topic(
    records: Vec<OutputRecord>,
    committed: &mut HashMap<String, i64>,
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
    for topic in &mut topics {
        let count = committed.entry(topic.topic.clone()).or_default();
        topic.sequence = *count + 1;
        *count += topic.records.len() as i64;
    }
    Ok(topics)
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
                match self.store.append(request.clone()).await {
                    Ok(_) => break,
                    Err(error) if error.kind == StreamFailureKind::Closed => {
                        dropped.push(topic.topic.clone());
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

    /// Drops the store's dedupe sessions of attempts that will not write again, in the
    /// background, so the next activation doesn't wait a store round trip per session. A
    /// failure only leaves a session in the store until retention, so it is logged.
    pub(crate) fn retire(&self, retired: RetiredSessions) {
        let chain = self.chain(&retired.workflow_id, &retired.first_run_id);
        let producer_id = temporalio_streams::workflow_producer_id(&retired.run_id);
        let store = self.store.clone();
        self.tasks.spawn(async move {
            for (attempt, topics) in &retired.sessions {
                for topic in topics {
                    if let Err(error) = store
                        .retire_producer(&chain, topic, &producer_id, *attempt)
                        .await
                    {
                        debug!(%topic, attempt, %error, "Could not retire a stream output session");
                    }
                }
            }
        });
    }

    /// Waits for the store calls still running in the background.
    pub(crate) async fn flush(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

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
    fn topics_keep_their_order_of_first_publish_and_sequences_go_on() {
        let mut committed = HashMap::new();
        let first = output_by_topic(
            vec![data("b", 1), data("a", 2), finish("b")],
            &mut committed,
        )
        .unwrap();
        let shape: Vec<_> = first
            .iter()
            .map(|t| (t.topic.as_str(), t.sequence, t.records.len()))
            .collect();
        assert_eq!(shape, [("b", 1, 2), ("a", 1, 1)]);
        let second = output_by_topic(vec![data("a", 3)], &mut committed).unwrap();
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
            let message = output_by_topic(vec![data("t", 9), record], &mut HashMap::new())
                .unwrap_err()
                .to_string();
            assert!(message.contains(expected), "{expected}: {message}");
        }
    }
}
