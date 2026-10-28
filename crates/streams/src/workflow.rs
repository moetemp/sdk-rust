//! The Workflow write calls, the bodies of the append Local Activity and the close Activity of
//! Workflow code. Workflow code builds each request, and never calls the crate itself.

use crate::{
    Reason, StreamError, StreamResult, derive_producer,
    epoch::{task_key, task_of},
    ids,
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamResponse, DeriveProducerRequest,
        DeriveProducerResponse, Producer, ProducerPurpose, PublishOutcome, StreamFailure,
        StreamProducerKind, WorkflowAppendPendingResponse, WorkflowAppendRequest,
        WorkflowAppendResponse, WorkflowCloseRequest, publish_outcome::Outcome,
    },
    record::data_record,
    runtime::Shared,
    store::{MAX_BATCH_BYTES, MAX_RECORD_BYTES},
};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// A run that ends while its append retries leaves its entry, so the oldest entries go when the
/// kept batches pass this size.
const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

/// The namespace, Stream id and Producer id of an append.
type Key = (String, String, String);

/// The batches of one append, each with its publishes and Records, and the answer of each batch
/// that the Store took.
struct Batches {
    batches: Vec<(Vec<usize>, Vec<AppendRecord>)>,
    counts: Vec<i64>,
    size: usize,
    answers: Mutex<HashMap<usize, Answer>>,
}

#[derive(Clone)]
enum Answer {
    LastOffset(i64),
    Failure(StreamFailure),
}

struct Entry {
    /// The task key of the Producer's epoch.
    epoch: String,
    batches: Arc<Batches>,
    order: u64,
}

/// The appends that haven't succeeded, one for each Producer.
pub(crate) struct Pending {
    limit: usize,
    state: Mutex<PendingState>,
}

#[derive(Default)]
struct PendingState {
    size: usize,
    next: u64,
    entries: HashMap<Key, Entry>,
    by_age: BTreeMap<u64, Key>,
}

impl Default for Pending {
    fn default() -> Self {
        Self::new(MAX_PENDING_BYTES)
    }
}

impl Pending {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::default(),
        }
    }

    fn get(&self, key: &Key, epoch: &str) -> Option<Arc<Batches>> {
        let state = self.state.lock();
        let entry = state.entries.get(key)?;
        (entry.epoch == epoch).then(|| entry.batches.clone())
    }

    fn put(&self, key: Key, epoch: String, batches: Arc<Batches>) {
        let mut state = self.state.lock();
        if let Some(found) = state.entries.get(&key) {
            // The Store fences this write, since the Producer has a later epoch.
            if found.epoch > epoch {
                return;
            }
            let found = state.entries.remove(&key).expect("found above");
            state.forget(&found);
        }
        state.next += 1;
        let order = state.next;
        state.size += batches.size;
        state.by_age.insert(order, key.clone());
        state.entries.insert(
            key,
            Entry {
                epoch,
                batches,
                order,
            },
        );
        while state.size > self.limit && state.entries.len() > 1 {
            let Some((_, oldest)) = state.by_age.pop_first() else {
                break;
            };
            if let Some(entry) = state.entries.remove(&oldest) {
                state.size -= entry.batches.size;
            }
        }
    }

    fn pop(&self, key: &Key, epoch: &str) {
        let mut state = self.state.lock();
        if state
            .entries
            .get(key)
            .is_some_and(|entry| entry.epoch == epoch)
        {
            let entry = state.entries.remove(key).expect("found above");
            state.forget(&entry);
        }
    }
}

impl PendingState {
    fn forget(&mut self, entry: &Entry) {
        self.size -= entry.batches.size;
        self.by_age.remove(&entry.order);
    }
}

/// Splits the merged Records into batches. They stay one batch unless the codec grew them past
/// the Store's limits. Then each publish goes alone, as with no merge.
fn split(producer: &Producer, records: Vec<AppendRecord>, counts: &[i64]) -> Batches {
    let sizes: Vec<usize> = records
        .iter()
        .map(|record| data_record(producer, record.clone()).len())
        .collect();
    let size = sizes.iter().sum();
    let batches = if size <= MAX_BATCH_BYTES && sizes.iter().all(|s| *s <= MAX_RECORD_BYTES) {
        vec![((0..counts.len()).collect(), records)]
    } else {
        let mut records = records.into_iter();
        counts
            .iter()
            .enumerate()
            .map(|(call, count)| (vec![call], records.by_ref().take(*count as usize).collect()))
            .collect()
    };
    Batches {
        batches,
        counts: counts.to_vec(),
        size,
        answers: Mutex::default(),
    }
}

/// The Producer of a Workflow write, and the key and epoch of its pending batches.
fn workflow_write(
    request: &WorkflowAppendRequest,
) -> StreamResult<(DeriveProducerResponse, Key, String)> {
    let derived = derive_producer(DeriveProducerRequest {
        context: request.context.clone(),
        purpose: ProducerPurpose::WorkflowWrite as i32,
    })?;
    let producer = derived.producer.as_ref().expect("derived");
    let stream = request
        .append
        .as_ref()
        .and_then(|append| append.stream.as_ref())
        .ok_or_else(|| StreamError::invalid("The append has no Stream"))?;
    let key = (
        request.namespace.clone(),
        stream.id.clone(),
        producer.id.clone(),
    );
    let epoch = task_key(&task_of(&producer.epoch.unwrap_or_default()));
    Ok((derived, key, epoch))
}

impl Shared {
    pub(crate) fn workflow_append_pending(
        &self,
        request: &WorkflowAppendRequest,
    ) -> StreamResult<WorkflowAppendPendingResponse> {
        let (_, key, epoch) = workflow_write(request)?;
        Ok(WorkflowAppendPendingResponse {
            pending: self.pending.get(&key, &epoch).is_some(),
        })
    }

    pub(crate) async fn workflow_append(
        &self,
        request: WorkflowAppendRequest,
    ) -> StreamResult<WorkflowAppendResponse> {
        let (derived, key, epoch) = workflow_write(&request)?;
        let producer = derived.producer.expect("derived");
        let append = request.append.expect("checked");
        let batches = match self.pending.get(&key, &epoch) {
            Some(batches) => batches,
            None if append.records.is_empty() => {
                return Err(StreamError::storage(
                    "The runtime no longer keeps the batches of this append, so the next \
                     attempt sends the Records again",
                ));
            }
            None => {
                let counts: Vec<i64> = request.counts.iter().map(|c| *c as i64).collect();
                if counts.iter().any(|c| *c < 1)
                    || counts.iter().sum::<i64>() != append.records.len() as i64
                {
                    return Err(StreamError::invalid(
                        "The counts must be 1 or more and add up to the Records",
                    ));
                }
                let batches = Arc::new(split(&producer, append.records, &counts));
                self.pending
                    .put(key.clone(), epoch.clone(), batches.clone());
                batches
            }
        };
        let service = self.service().await?;
        let mut outcomes = vec![PublishOutcome::default(); batches.counts.len()];
        for (index, (calls, records)) in batches.batches.iter().enumerate() {
            let answer = batches.answers.lock().get(&index).cloned();
            let answer = match answer {
                Some(answer) => answer,
                None => {
                    // The first batch that the Store takes withdraws the previous run.
                    let taken = {
                        let answers = batches.answers.lock();
                        (0..index).any(|i| matches!(answers.get(&i), Some(Answer::LastOffset(_))))
                    };
                    let sent = service
                        .append_records(AppendRecordsRequest {
                            namespace: request.namespace.clone(),
                            stream: append.stream.clone(),
                            producer: Some(producer.clone()),
                            sequence: index as i64 + 1,
                            records: records.clone(),
                            previous_completed_task: derived.previous_completed_task,
                            failed_tasks: derived.failed_tasks.clone(),
                            withdraw_run: if taken {
                                String::new()
                            } else {
                                append.withdraw_run.clone()
                            },
                            ..Default::default()
                        })
                        .await;
                    let answer = match sent {
                        Ok(response) => Answer::LastOffset(response.last_offset),
                        Err(error) if error.reason == Reason::TooLarge => {
                            Answer::Failure(error.failure())
                        }
                        Err(error) => {
                            // A retry can't succeed, so its Records go again with a new codec run.
                            if error.reason != Reason::Storage {
                                self.pending.pop(&key, &epoch);
                            }
                            return Err(error);
                        }
                    };
                    batches.answers.lock().insert(index, answer.clone());
                    answer
                }
            };
            // A batch has consecutive offsets, so a publish's last Record is the batch's last
            // one less the Records after it.
            let mut after: i64 = calls.iter().map(|call| batches.counts[*call]).sum();
            for call in calls {
                after -= batches.counts[*call];
                outcomes[*call].outcome = Some(match &answer {
                    Answer::LastOffset(last) => Outcome::LastOffset(last - after),
                    Answer::Failure(failure) => Outcome::Failure(failure.clone()),
                });
            }
        }
        self.pending.pop(&key, &epoch);
        Ok(WorkflowAppendResponse { outcomes })
    }

    pub(crate) async fn workflow_close(
        &self,
        request: WorkflowCloseRequest,
    ) -> StreamResult<CloseStreamResponse> {
        let context = request
            .context
            .ok_or_else(|| StreamError::invalid("The request has no Activity context"))?;
        if !context.in_workflow || context.is_local || context.workflow_id.is_empty() {
            return Err(StreamError::invalid(
                "The close runs as a regular Activity of the Workflow",
            ));
        }
        let mut close = request
            .close
            .ok_or_else(|| StreamError::invalid("The request has no close"))?;
        let stream = close.stream.as_ref();
        let owner = stream
            .and_then(|stream| stream.owner.as_ref())
            .map(|owner| owner.business_id.as_str());
        if owner != Some(context.workflow_id.as_str()) {
            return Err(StreamError::new(
                Reason::NotOwner,
                format!(
                    "The Workflow {} doesn't own the Stream {}",
                    context.workflow_id,
                    stream.map(|stream| stream.id.as_str()).unwrap_or_default()
                ),
            ));
        }
        // The Store refuses an Activity Producer's close of a Workflow's Stream, so the close
        // writes as the run that scheduled it.
        let run = context.workflow_run_id;
        close.namespace = request.namespace;
        close.producer = Some(Producer {
            kind: StreamProducerKind::Workflow as i32,
            id: ids::workflow_producer_id(&run),
            epoch: None,
            run_id: run,
        });
        self.service().await?.close_stream(close).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batches(size: usize) -> Arc<Batches> {
        Arc::new(Batches {
            batches: vec![],
            counts: vec![],
            size,
            answers: Mutex::default(),
        })
    }

    fn key(stream: &str) -> Key {
        ("ns".into(), stream.into(), "la:r:1".into())
    }

    #[test]
    fn the_pending_batches_drop_the_oldest_past_the_limit() {
        let pending = Pending::new(100);
        pending.put(key("a"), "1".into(), batches(40));
        pending.put(key("b"), "1".into(), batches(40));
        pending.put(key("c"), "1".into(), batches(40));
        assert!(pending.get(&key("a"), "1").is_none());
        assert!(pending.get(&key("b"), "1").is_some());
        assert!(pending.get(&key("c"), "2").is_none());
        // One entry stays even when it alone passes the limit.
        pending.put(key("d"), "1".into(), batches(500));
        assert!(pending.get(&key("d"), "1").is_some());
        assert_eq!(pending.state.lock().entries.len(), 1);
    }

    #[test]
    fn a_lower_epoch_doesnt_replace_a_higher_one() {
        let pending = Pending::new(100);
        pending.put(key("a"), "2".into(), batches(10));
        pending.put(key("a"), "1".into(), batches(10));
        assert!(pending.get(&key("a"), "2").is_some());
        pending.put(key("a"), "3".into(), batches(10));
        assert!(pending.get(&key("a"), "2").is_none());
        pending.pop(&key("a"), "2");
        assert!(pending.get(&key("a"), "3").is_some());
        pending.pop(&key("a"), "3");
        assert_eq!(pending.state.lock().size, 0);
    }
}
