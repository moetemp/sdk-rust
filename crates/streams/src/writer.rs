//! Writers for Activities and clients. `write` appends its Records at once and returns their
//! offsets, as an awaited publish needs. A writer keeps one Producer epoch and one `sequence`
//! order, so that the Store writes a retried batch only once. Lang can buffer Records on its own
//! and write each batch with one call.
//!
//! The handles of one Producer epoch on one Stream share one writer. Each open adds a handle and
//! each close or free releases one. The last release drops the writer.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{
        AppendRecord, AppendRecordsRequest, OpenWriterRequest, Producer, StreamProducerKind,
        StreamRef, WorkflowTaskEpoch,
    },
    service::StreamService,
    store::{MAX_BATCH_BYTES, MAX_RECORD_BYTES},
};
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

const DEFAULT_BATCH: usize = 1000;
const DEFAULT_RETRY: Duration = Duration::from_secs(10);
const LONGEST_RETRY: Duration = Duration::from_secs(24 * 60 * 60);
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

/// The last `sequence` of any batch in this process. Every writer numbers its batches from it, so
/// that a writer that opens again after its close continues above its earlier batches.
static LAST_SEQUENCE: AtomicI64 = AtomicI64::new(0);

fn next_sequence() -> i64 {
    LAST_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1
}

#[derive(Default)]
struct State {
    last_offset: i64,
    /// A write that the Store refused, such as on a closed Stream or a newer epoch. Every later
    /// call fails with it. A Store failure fails only its own write.
    failed: Option<StreamError>,
    closed: bool,
}

struct Writer {
    namespace: String,
    stream: StreamRef,
    producer: Producer,
    previous_completed_task: Option<WorkflowTaskEpoch>,
    failed_tasks: Vec<i64>,
    max_batch: usize,
    max_retry: Duration,
    service: Arc<StreamService>,
    state: Mutex<State>,
    /// Allows one write at a time, so that the batches of one epoch reach the Store in order.
    writing: tokio::sync::Mutex<()>,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

impl Writer {
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

    /// Appends `records` now and returns the offsets of the first and the last one. Records past
    /// the Store's batch limit go in more batches, in order.
    async fn write(&self, mut records: Vec<AppendRecord>) -> Written {
        let _writing = self.writing.lock().await;
        let mut written = Written::default();
        // A close that started while this write waited has ended the writer.
        if self.state.lock().closed {
            written.error = Some(StreamError::new(
                Reason::UnknownWriter,
                "The writer closed while the write waited",
            ));
            return written;
        }
        while !records.is_empty() && written.error.is_none() {
            let count = self.batch_len(records.iter());
            let rest = records.split_off(count);
            if let Some(error) = &self.state.lock().failed {
                written.error = Some(error.clone());
                break;
            }
            let sequence = next_sequence();
            match self.send(records, sequence).await {
                Ok((batch_first, batch_last)) => {
                    if written.first == 0 {
                        written.first = batch_first;
                    }
                    written.last = batch_last;
                    written.records += count;
                }
                // The next write takes a higher `sequence`, so the Store refuses a late copy of
                // this batch after it.
                Err(error) if error.reason == Reason::Storage => written.error = Some(error),
                Err(error) => {
                    self.state.lock().failed = Some(error.clone());
                    written.error = Some(error);
                }
            }
            records = rest;
        }
        let mut state = self.state.lock();
        state.last_offset = state.last_offset.max(written.last);
        written
    }

    /// Appends one batch and returns its first and last offset. After a Store failure it sends the
    /// batch again with the same `sequence`, so the dedupe key stops a second write.
    async fn send(&self, records: Vec<AppendRecord>, sequence: i64) -> StreamResult<(i64, i64)> {
        let request = AppendRecordsRequest {
            namespace: self.namespace.clone(),
            stream: Some(self.stream.clone()),
            producer: Some(self.producer.clone()),
            sequence,
            records,
            previous_completed_task: self.previous_completed_task,
            failed_tasks: self.failed_tasks.clone(),
            ..Default::default()
        };
        let until = Instant::now() + self.max_retry;
        let mut backoff = FIRST_BACKOFF;
        loop {
            match self.service.append_records(request.clone()).await {
                Ok(response) => return Ok((response.first_offset, response.last_offset)),
                Err(error)
                    if error.reason == Reason::Storage && Instant::now() + backoff < until =>
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

/// What a write wrote. A write that failed after some batches has their Records and offsets.
#[derive(Default)]
pub(crate) struct Written {
    pub(crate) first: i64,
    pub(crate) last: i64,
    pub(crate) records: usize,
    pub(crate) error: Option<StreamError>,
}

/// The namespace, Stream id, Producer id and epoch key of a writer.
type WriterKey = (String, String, String, String);

/// One open writer and the handles that share it.
struct Open {
    writer: Arc<Writer>,
    key: WriterKey,
    /// The handles of each holder. Lang's `CloseWriters` releases lang's only.
    lang: usize,
    scoped: usize,
}

impl Open {
    fn handles(&mut self, holder: Holder) -> &mut usize {
        match holder {
            Holder::Lang => &mut self.lang,
            Holder::Scope => &mut self.scoped,
        }
    }
}

/// Who holds a handle of a writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Holder {
    /// Lang, through `OpenWriter`.
    Lang,
    /// A Producer scope.
    Scope,
}

#[derive(Default)]
struct Table {
    open: HashMap<u64, Open>,
    /// The writer of each key, so a second open shares its sequence instead of reusing it.
    by_producer: HashMap<WriterKey, u64>,
}

impl Table {
    /// Releases one handle of a writer. Returns the writer when it has no handle left, so that the
    /// caller ends it.
    fn release(&mut self, id: u64, holder: Holder) -> StreamResult<Option<Arc<Writer>>> {
        let unknown = || StreamError::new(Reason::UnknownWriter, format!("No writer {id}"));
        let open = self.open.get_mut(&id).ok_or_else(unknown)?;
        // A holder with no handle left, such as lang after `CloseWriters`, takes none of another.
        let handles = open.handles(holder);
        *handles = handles.checked_sub(1).ok_or_else(unknown)?;
        if open.lang + open.scoped > 0 {
            return Ok(None);
        }
        Ok(Some(self.remove(id)))
    }

    fn add_handle(&mut self, key: &WriterKey, holder: Holder) -> Option<u64> {
        let id = self.by_producer.get(key).copied()?;
        *self.open.get_mut(&id).expect("indexed").handles(holder) += 1;
        Some(id)
    }

    fn remove(&mut self, id: u64) -> Arc<Writer> {
        let open = self.open.remove(&id).expect("found by the caller");
        self.by_producer.remove(&open.key);
        open.writer
    }
}

/// The open writers of one runtime.
#[derive(Default)]
pub(crate) struct Writers {
    next: AtomicU64,
    table: Mutex<Table>,
}

impl Writers {
    /// Opens a writer, or adds a handle to the one that this Producer epoch has on the Stream. A
    /// writer of a Local Activity, or of an Activity attempt above 1, first sends an empty batch.
    /// So the new epoch withdraws the earlier one's Records even when it publishes nothing. On a
    /// closed or expired Stream the open still succeeds, and the first write fails with the reason.
    pub(crate) async fn open(
        &self,
        service: &Arc<StreamService>,
        request: OpenWriterRequest,
        holder: Holder,
    ) -> StreamResult<u64> {
        let stream = request
            .stream
            .ok_or_else(|| StreamError::invalid("The writer has no Stream"))?;
        crate::ids::check_stream_id(&stream.id)?;
        let producer = request
            .producer
            .ok_or_else(|| StreamError::invalid("The writer has no Producer"))?;
        crate::ids::check_producer(&producer)?;
        if producer.kind == StreamProducerKind::Workflow as i32 {
            return Err(StreamError::invalid("Workflow code doesn't open a writer"));
        }
        let epoch = producer.epoch.unwrap_or_default();
        let key = (
            request.namespace.clone(),
            stream.id.clone(),
            producer.id.clone(),
            crate::epoch::epoch_key(&epoch),
        );
        if let Some(id) = self.share(&key, holder) {
            return Ok(id);
        }
        let writer = Arc::new(Writer {
            namespace: request.namespace,
            stream,
            producer,
            previous_completed_task: request.previous_completed_task,
            failed_tasks: request.failed_tasks,
            max_batch: match request.max_batch_size {
                n if n > 0 => n as usize,
                _ => DEFAULT_BATCH,
            },
            // A retry of more than a day counts as a day, so that `Instant` math can't overflow.
            max_retry: (duration(request.max_retry_duration).unwrap_or(DEFAULT_RETRY))
                .min(LONGEST_RETRY),
            service: service.clone(),
            state: Mutex::default(),
            writing: tokio::sync::Mutex::new(()),
        });
        // A Local Activity that runs again in a later task has attempt 1 again, so its writer
        // always sends the empty batch.
        let retry = match StreamProducerKind::try_from(writer.producer.kind).unwrap_or_default() {
            StreamProducerKind::LocalActivity => true,
            StreamProducerKind::Activity | StreamProducerKind::StandaloneActivity => {
                epoch.attempt > 1
            }
            _ => false,
        };
        if retry {
            match writer.send(vec![], next_sequence()).await {
                // A Stream with no key has nothing to withdraw yet. A closed or expired one accepts
                // no more Records, and a retry that only reads it must still run.
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.reason,
                        Reason::NotFound | Reason::Closed | Reason::Expired
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        let mut table = self.table.lock();
        if let Some(id) = table.add_handle(&key, holder) {
            return Ok(id);
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        table.by_producer.insert(key.clone(), id);
        table.open.insert(
            id,
            Open {
                writer,
                key,
                lang: usize::from(holder == Holder::Lang),
                scoped: usize::from(holder == Holder::Scope),
            },
        );
        Ok(id)
    }

    #[cfg(test)]
    pub(crate) fn open_count(&self) -> usize {
        self.table.lock().open.len()
    }

    /// Adds a handle to the open writer of `key`, if there is one.
    fn share(&self, key: &WriterKey, holder: Holder) -> Option<u64> {
        self.table.lock().add_handle(key, holder)
    }

    fn get(&self, id: u64) -> StreamResult<Arc<Writer>> {
        self.table
            .lock()
            .open
            .get(&id)
            .map(|open| open.writer.clone())
            .ok_or_else(|| StreamError::new(Reason::UnknownWriter, format!("No writer {id}")))
    }

    /// Appends Records at once and returns the first and the last offset. A retry after a Store
    /// failure resends the same batch, so that the Store writes it once.
    pub(crate) async fn write(
        &self,
        id: u64,
        records: Vec<AppendRecord>,
    ) -> StreamResult<(i64, i64)> {
        let written = self.write_counted(id, records).await;
        match written.error {
            Some(error) => Err(error),
            None => Ok((written.first, written.last)),
        }
    }

    /// Writes as [Self::write] does, and also tells how many Records went in before a failure.
    pub(crate) async fn write_counted(&self, id: u64, records: Vec<AppendRecord>) -> Written {
        let refused = |error| Written {
            error: Some(error),
            ..Default::default()
        };
        let writer = match self.get(id) {
            Ok(writer) => writer,
            Err(error) => return refused(error),
        };
        {
            let state = writer.state.lock();
            if let Some(error) = &state.failed {
                return refused(error.clone());
            }
            if state.closed {
                return refused(StreamError::new(
                    Reason::UnknownWriter,
                    format!("Writer {id} is closed"),
                ));
            }
        }
        if let Err(error) = check_sizes(&writer.producer, &records) {
            return refused(error);
        }
        writer.write(records).await
    }

    /// Releases every handle that lang holds, and ends each writer with no handle left. Returns
    /// how many of those had failed. Scopes release their own handles when they end.
    pub(crate) async fn close_all(&self) -> usize {
        let ended: Vec<Arc<Writer>> = {
            let mut table = self.table.lock();
            let ids: Vec<u64> = table.open.keys().copied().collect();
            let mut ended = vec![];
            for id in ids {
                let open = table.open.get_mut(&id).expect("listed");
                open.lang = 0;
                if open.scoped == 0 {
                    let writer = table.remove(id);
                    // Closed at once, so a dropped call still ends every writer it took.
                    writer.state.lock().closed = true;
                    ended.push(writer);
                }
            }
            ended
        };
        let mut failed = 0;
        for writer in ended {
            if end(&writer).await.is_err() {
                failed += 1;
            }
        }
        failed
    }

    /// Releases one handle. The last one waits for a write in flight, then drops the writer.
    /// Returns its last offset, or the error it failed with.
    pub(crate) async fn close(&self, id: u64, holder: Holder) -> StreamResult<i64> {
        let writer = self.get(id)?;
        if self.table.lock().release(id, holder)?.is_none() {
            let state = writer.state.lock();
            return match &state.failed {
                Some(error) => Err(error.clone()),
                None => Ok(state.last_offset),
            };
        }
        end(&writer).await
    }

    /// Releases one handle with no wait, for a handle that lang drops. A write in flight still
    /// completes, and a write that waits behind it is refused once no handle is left.
    pub(crate) fn free(&self, id: u64, holder: Holder) -> StreamResult<()> {
        if let Some(writer) = self.table.lock().release(id, holder)? {
            writer.state.lock().closed = true;
        }
        Ok(())
    }
}

/// Ends a writer that no handle uses. It waits for a write in flight, then returns its last offset
/// or the error it failed with.
async fn end(writer: &Writer) -> StreamResult<i64> {
    writer.state.lock().closed = true;
    let _writing = writer.writing.lock().await;
    let state = writer.state.lock();
    match &state.failed {
        Some(error) => Err(error.clone()),
        None => Ok(state.last_offset),
    }
}
