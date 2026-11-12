//! Writers for Activities and clients. `write` appends its Records at once and returns their
//! offsets, as an awaited publish needs. A writer keeps one Producer epoch and one `sequence`
//! order, so that the Store writes a retried batch only once. Lang can buffer Records on its own
//! and write each batch with one call.
//!
//! The bridge holds each writer as an object, and dropping it releases it. The writers of one
//! Producer epoch on one Stream share a lane, which keeps their batches in `sequence` order.

use crate::{
    Reason, StreamError, StreamResult,
    epoch::{EPOCH_KEY_LEN, ProducerKind, epoch_key},
    ids::{check_producer, check_required, check_stream_id},
    metrics::Metrics,
    offset::{offset_from_wire, offset_to_wire},
    proto::{
        AppendRecord, AppendRecordsRequest, CloseWriterResponse, OpenWriterRequest, Producer,
        StreamRef, WriteRecordsRequest, WriteRecordsResponse, producer::Kind,
    },
    record::check_records,
    runtime::duration,
    store::{MAX_BATCH_BYTES, MAX_BATCH_RECORDS, MAX_RECORD_BYTES, Store},
};
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

const DEFAULT_RETRY: Duration = Duration::from_secs(10);
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

/// The last `sequence` of any batch in this process. Every writer numbers its batches from it, so
/// that a writer that opens again after its close continues above its earlier batches.
static LAST_SEQUENCE: AtomicI64 = AtomicI64::new(0);

fn next_sequence() -> i64 {
    LAST_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1
}

/// What the writers of one epoch share beyond the order of their batches.
#[derive(Default)]
struct LaneState {
    /// The epoch's empty batch reached the Store, or the Stream takes no more Records.
    fenced: bool,
}

/// The ordering lock of one Producer epoch on one Stream. A write holds it from its first
/// `sequence` to its last retry, so the batches of the epoch reach the Store in order. An open
/// holds it while it sends the epoch's empty batch, so the writers of the epoch see one result.
#[derive(Default)]
pub(crate) struct Lane(tokio::sync::Mutex<LaneState>);

/// The Stream id, Producer id and epoch key of a lane.
type LaneKey = (String, String, [u8; EPOCH_KEY_LEN]);

/// The lanes of one runtime, while a writer holds them.
#[derive(Default)]
pub(crate) struct Lanes(Mutex<HashMap<LaneKey, Weak<Lane>>>);

impl Lanes {
    /// The lane of `key`. It first drops the lanes that no writer holds, so a writer's drop never
    /// reaches back into the runtime.
    fn get(&self, key: LaneKey) -> Arc<Lane> {
        let mut lanes = self.0.lock();
        lanes.retain(|_, lane| lane.strong_count() > 0);
        if let Some(lane) = lanes.get(&key).and_then(Weak::upgrade) {
            return lane;
        }
        let lane = Arc::new(Lane::default());
        lanes.insert(key, Arc::downgrade(&lane));
        lane
    }
}

#[derive(Default)]
struct State {
    last_offset: i64,
    /// A write that the Store refused, such as on a closed Stream or a newer epoch. Every later
    /// call fails with it. A Store failure fails only its own write.
    failed: Option<StreamError>,
    closed: bool,
}

/// A writer of one Producer epoch on one Stream, for an Activity, a Local Activity or a client.
/// The bridge holds it, and dropping it releases it. A write in flight still completes.
pub struct StreamWriter {
    /// The namespace of the open, which each batch sends.
    namespace: String,
    stream_ref: StreamRef,
    /// The open's Producer, with its epoch, which each batch sends.
    producer: Producer,
    max_batch: usize,
    max_retry: Duration,
    store: Arc<dyn Store>,
    metrics: Metrics,
    lane: Arc<Lane>,
    state: Mutex<State>,
}

impl std::fmt::Debug for StreamWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamWriter")
            .field("stream", &self.stream_ref.id)
            .field("producer", &self.producer.id)
            .finish_non_exhaustive()
    }
}

fn closed(message: &str) -> StreamError {
    StreamError::new(Reason::Closed, message)
}

impl StreamWriter {
    /// Opens a writer on the lane of its Producer epoch. A writer of a Local Activity, or of an
    /// Activity attempt above 1, first sends an empty batch unless an open of its epoch already
    /// did, so the new epoch sets its fence even when it publishes nothing. On a closed or expired
    /// Stream the open still succeeds, and the first write fails with the reason.
    pub(crate) async fn open(
        store: &Arc<dyn Store>,
        metrics: &Metrics,
        lanes: &Lanes,
        request: OpenWriterRequest,
    ) -> StreamResult<Self> {
        let stream = request
            .r#ref
            .ok_or_else(|| StreamError::invalid("The writer has no Stream"))?;
        check_stream_id(&stream.id)?;
        let producer = request
            .producer
            .ok_or_else(|| StreamError::invalid("The writer has no Producer"))?;
        check_producer(&producer)?;
        check_required(&producer)?;
        if ProducerKind::try_from(&producer)? == ProducerKind::Workflow {
            return Err(StreamError::invalid("Workflow code doesn't open a writer"));
        }
        // A Local Activity that runs again in a later task has attempt 1 again, so its writer
        // always sends the empty batch.
        let retry = match producer.kind.as_ref() {
            Some(Kind::LocalActivity(_)) => true,
            Some(Kind::WorkflowActivity(activity) | Kind::StandaloneActivity(activity)) => {
                activity.epoch.map_or(0, |e| e.attempt) > 1
            }
            _ => false,
        };
        let key = (
            stream.id.clone(),
            producer.id.clone(),
            epoch_key(producer.kind.as_ref()),
        );
        let lane = lanes.get(key);
        let writer = Self {
            namespace: request.namespace,
            stream_ref: stream,
            producer,
            // The Store refuses a batch over its Record limit, so no batch can be larger.
            max_batch: match request.max_batch_size {
                n if n > 0 => (n as usize).min(MAX_BATCH_RECORDS),
                _ => MAX_BATCH_RECORDS,
            },
            max_retry: duration(request.max_retry_duration).unwrap_or(DEFAULT_RETRY),
            store: store.clone(),
            metrics: metrics.clone(),
            lane,
            state: Mutex::default(),
        };
        if retry {
            // The flag changes only after the Store answers, so a failed or cancelled open leaves
            // the fence to the next open of the epoch.
            let mut lane = writer.lane.0.lock().await;
            if !lane.fenced {
                match writer.send(vec![], next_sequence()).await {
                    Ok(_) => lane.fenced = true,
                    // A closed or expired Stream accepts no more Records, and a retry that only
                    // reads it must still run.
                    Err(error) if matches!(error.reason, Reason::Closed | Reason::Expired) => {
                        lane.fenced = true;
                    }
                    // A Stream with no key has no fence to set yet, so a later open tries again.
                    Err(error) if error.reason == Reason::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(writer)
    }

    /// Appends Records at once and answers with the offsets of the first and the last one. Records
    /// past the Store's batch limit go in more batches, in order. A Record that the Store would
    /// refuse fails the write before the Store, and the writer goes on.
    pub async fn write(&self, request: WriteRecordsRequest) -> StreamResult<WriteRecordsResponse> {
        let records = request.records;
        {
            let state = self.state.lock();
            if state.closed {
                return Err(closed("The writer is closed"));
            }
            if let Some(error) = &state.failed {
                return Err(error.clone());
            }
        }
        check_records(&records)?;
        check_sizes(&self.producer, &records)?;
        let (first, last) = self.write_batches(records).await?;
        Ok(WriteRecordsResponse {
            first_offset: offset_to_wire(first),
            last_offset: offset_to_wire(last),
        })
    }

    /// Closes the writer. It waits for a write in flight, then answers with the offset of the
    /// newest Record that the writer wrote, or fails with the writer's error. It doesn't close the
    /// Stream, and a later write fails as closed.
    pub async fn close(&self) -> StreamResult<CloseWriterResponse> {
        self.state.lock().closed = true;
        let _lane = self.lane.0.lock().await;
        let state = self.state.lock();
        match &state.failed {
            Some(error) => Err(error.clone()),
            None => Ok(CloseWriterResponse {
                last_offset: offset_to_wire(state.last_offset),
            }),
        }
    }

    /// The number of `records` in the next batch. The Store refuses a batch over its limit, so a
    /// batch ends at the count limit or the byte limit, whichever comes first.
    fn batch_len<'a>(&self, records: impl Iterator<Item = &'a AppendRecord>) -> usize {
        let mut count = 0;
        let mut bytes = 0;
        for record in records.take(self.max_batch) {
            let size = stored_size(&self.producer, record);
            if count > 0 && bytes + size > MAX_BATCH_BYTES {
                break;
            }
            bytes += size;
            count += 1;
        }
        count
    }

    /// Appends `records` in batches on the writer's lane, and returns the offsets of the first and
    /// the last one.
    async fn write_batches(&self, mut records: Vec<AppendRecord>) -> StreamResult<(i64, i64)> {
        let _lane = self.lane.0.lock().await;
        // A close that started while this write waited has closed the writer.
        {
            let state = self.state.lock();
            if state.closed {
                return Err(closed("The writer closed while the write waited"));
            }
            // A write that failed while this one waited fails it too, also when it has no Record.
            if let Some(error) = &state.failed {
                return Err(error.clone());
            }
        }
        let (mut first, mut last) = (0, 0);
        while !records.is_empty() {
            let count = self.batch_len(records.iter());
            let rest = records.split_off(count);
            let sequence = next_sequence();
            let (batch_first, batch_last) = match self.send(records, sequence).await {
                Ok(offsets) => offsets,
                // The next write takes a higher `sequence`, so the Store refuses a late copy of
                // this batch after it.
                Err(error) if error.reason == Reason::StorageUnavailable => return Err(error),
                Err(error) => {
                    self.state.lock().failed = Some(error.clone());
                    return Err(error);
                }
            };
            if first == 0 {
                first = batch_first;
            }
            last = batch_last;
            records = rest;
        }
        let mut state = self.state.lock();
        state.last_offset = state.last_offset.max(last);
        Ok((first, last))
    }

    /// The append of one batch.
    fn append(&self, records: Vec<AppendRecord>, sequence: i64) -> AppendRecordsRequest {
        AppendRecordsRequest {
            namespace: self.namespace.clone(),
            r#ref: Some(self.stream_ref.clone()),
            producer: Some(self.producer.clone()),
            sequence,
            records,
        }
    }

    /// Appends one batch and returns its first and last offset. After a Store failure it sends the
    /// batch again with the same `sequence`, so the dedupe key stops a second write.
    async fn send(&self, records: Vec<AppendRecord>, sequence: i64) -> StreamResult<(i64, i64)> {
        let request = self.append(records, sequence);
        // A retry window too long for the clock has no end.
        let until = Instant::now().checked_add(self.max_retry);
        let mut backoff = FIRST_BACKOFF;
        loop {
            match crate::runtime::append(&*self.store, &self.metrics, request.clone()).await {
                Ok(response) => {
                    return Ok((
                        offset_from_wire(&response.first_offset)?,
                        offset_from_wire(&response.last_offset)?,
                    ));
                }
                Err(error)
                    if error.reason == Reason::StorageUnavailable
                        && until.is_none_or(|until| Instant::now() + backoff < until) =>
                {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// The size of a Record once stored, with a margin for the envelope's own fields.
fn stored_size(producer: &Producer, record: &AppendRecord) -> usize {
    record.encoded_len() + producer.encoded_len() + 32
}

/// Refuses a Record over the Store's Record limit, before it reaches the Store.
fn check_sizes(producer: &Producer, records: &[AppendRecord]) -> StreamResult<()> {
    if let Some(big) = records
        .iter()
        .map(|record| stored_size(producer, record))
        .find(|size| *size > MAX_RECORD_BYTES)
    {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!("A Record has about {big} bytes, more than {MAX_RECORD_BYTES}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        StreamRuntime,
        ids::{activity_producer_id, client_producer_id},
        memory::MemoryStore,
        proto::{ActivityEpoch, ActivityProducer, ClientProducer, CreateStreamRequest},
    };
    use prost_types::Timestamp;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    #[tokio::test]
    async fn an_append_of_a_writer_carries_the_namespace_of_its_open() {
        let store = Arc::new(MemoryStore::new(Duration::from_secs(60), None));
        let runtime = StreamRuntime::new("test", "", store, None);
        let request = OpenWriterRequest {
            namespace: "ns-1".into(),
            r#ref: Some(StreamRef {
                id: "s".into(),
                ..Default::default()
            }),
            producer: Some(Producer {
                id: client_producer_id("me", "p"),
                kind: Some(Kind::Client(ClientProducer {})),
            }),
            ..Default::default()
        };
        let writer = runtime.open_writer(request).await.unwrap();
        assert_eq!(writer.append(vec![], 1).namespace, "ns-1");
    }

    // An open of a retry Producer sends its empty batch in the same retry window as a write.
    #[tokio::test]
    async fn a_retry_window_too_long_for_the_clock_has_no_end() {
        let store = Arc::new(MemoryStore::new(Duration::from_secs(60), None));
        let runtime = StreamRuntime::new("test", "", store, None);
        let stream_ref = Some(StreamRef {
            id: "s".into(),
            ..Default::default()
        });
        let create = CreateStreamRequest {
            stream_ref: stream_ref.clone(),
            ..Default::default()
        };
        runtime.create_stream(create).await.unwrap();
        let retry = Producer {
            id: activity_producer_id("r", "a", "s"),
            kind: Some(Kind::WorkflowActivity(ActivityProducer {
                run_id: "r".into(),
                epoch: Some(ActivityEpoch {
                    attempt_scheduled_time: Some(Timestamp::default()),
                    attempt: 2,
                }),
            })),
        };
        let request = OpenWriterRequest {
            r#ref: stream_ref,
            producer: Some(retry),
            max_retry_duration: Some(prost_types::Duration {
                seconds: i64::MAX,
                nanos: 999_999_999,
            }),
            ..Default::default()
        };
        let writer = runtime.open_writer(request).await.unwrap();
        let records = vec![AppendRecord {
            body: Some(Payload::default()),
        }];
        writer.write(WriteRecordsRequest { records }).await.unwrap();
    }
}
