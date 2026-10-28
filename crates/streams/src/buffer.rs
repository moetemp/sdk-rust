//! Buffered publishers. The runtime keeps the batching rules, and lang keeps only the codec, which
//! runs once per batch, since a remote codec costs a round trip per call.

use crate::{
    Reason, StreamError, StreamResult,
    ids::check_stream_id,
    proto::{
        AppendRecord, BufferBatch, BufferFlushResponse, BufferWriteRequest, OpenBufferRequest,
        StreamFailure, StreamRef, buffer_write_request::Body,
    },
    runtime::Shared,
    scope::Scope,
    writer::Written,
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

const DEFAULT_DELAY: Duration = Duration::from_millis(100);
const LONGEST_DELAY: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const DEFAULT_BATCH: usize = 1000;

pub(crate) struct Buffer {
    scope: Arc<Scope>,
    /// The runtime's own pump takes the batches, so lang's pump calls are refused.
    encoded: bool,
    stream: StreamRef,
    max_delay: Duration,
    max_batch: usize,
    state: Mutex<State>,
    /// Wakes the pump and the flushes after each change of the state.
    changed: Notify,
}

#[derive(Default)]
struct State {
    /// The values that no batch took yet.
    pending: Vec<AppendRecord>,
    /// When the oldest pending value arrived.
    first_at: Option<Instant>,
    in_flight: Option<InFlight>,
    last_batch: u64,
    pushed: u64,
    /// The values whose batch was written or failed.
    settled: u64,
    /// A batch is due at once while fewer values than this are settled, so a flush doesn't wait.
    flush_to: u64,
    error: Option<StreamError>,
    /// Lang got the error from a push or a flush.
    reported: bool,
    last_offset: i64,
    unwritten: u64,
    /// Set when the scope's close starts to flush the buffer. Then a push fails.
    closing: bool,
    freed: bool,
}

struct InFlight {
    id: u64,
    count: u64,
    /// The values of a batch that lang's pump took, until a write starts. The next
    /// `BufferNextBatch` offers them again, so a dropped pump doesn't strand the batch.
    records: Option<Vec<AppendRecord>>,
}

fn unknown_buffer() -> StreamError {
    StreamError::new(Reason::UnknownWriter, "No buffer has the id")
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

impl Buffer {
    fn push(&self, records: Vec<AppendRecord>) -> StreamResult<()> {
        let mut state = self.state.lock();
        if let Some(error) = &state.error {
            let error = error.clone();
            state.reported = true;
            return Err(error);
        }
        if state.freed || state.closing {
            return Err(unknown_buffer());
        }
        if records.is_empty() {
            return Err(StreamError::invalid("A push takes one or more values"));
        }
        if state.pending.is_empty() {
            state.first_at = Some(Instant::now());
        }
        state.pushed += records.len() as u64;
        state.pending.extend(records);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    /// Waits for the next due batch and takes it. `None` once the buffer is freed or failed.
    async fn next_batch(&self) -> Option<(u64, Vec<AppendRecord>)> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let due_at = {
                let mut state = self.state.lock();
                if state.freed || state.error.is_some() {
                    return None;
                }
                if let Some(InFlight {
                    id,
                    records: Some(records),
                    ..
                }) = &state.in_flight
                {
                    return Some((*id, records.clone()));
                }
                match state.first_at {
                    Some(first_at) if state.in_flight.is_none() => {
                        let due_at = first_at + self.max_delay;
                        let full = state.pending.len() >= self.max_batch;
                        if full || state.flush_to > state.settled || due_at <= Instant::now() {
                            return Some(state.take(self.max_batch, !self.encoded));
                        }
                        Some(due_at)
                    }
                    _ => None,
                }
            };
            match due_at {
                Some(at) => {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(at) => {}
                    }
                }
                None => notified.await,
            }
        }
    }

    /// Starts the write of the batch in flight, once. Records of another count than the batch's
    /// fail the call and the buffer, as a codec error does.
    fn start_write(&self, batch_id: u64, records: Option<usize>) -> StreamResult<()> {
        let mut state = self.state.lock();
        // A free already counted the batch as unwritten.
        if state.freed {
            return Err(unknown_buffer());
        }
        let Some(in_flight) = state
            .in_flight
            .as_mut()
            .filter(|in_flight| in_flight.id == batch_id && in_flight.records.is_some())
        else {
            return Err(StreamError::invalid(format!(
                "Batch {batch_id} isn't a batch in flight that nobody writes"
            )));
        };
        in_flight.records = None;
        if records.is_some_and(|count| count as u64 != in_flight.count) {
            let error = StreamError::invalid(format!(
                "Batch {batch_id} has {} values, and the codec must give one Record for each",
                in_flight.count
            ));
            drop(state);
            let written = Written {
                error: Some(error.clone()),
                ..Default::default()
            };
            self.settle(batch_id, written);
            return Err(error);
        }
        Ok(())
    }

    /// Ends the batch in flight with the outcome of its write.
    fn settle(&self, batch_id: u64, written: Written) {
        let mut state = self.state.lock();
        let count = match &state.in_flight {
            Some(in_flight) if in_flight.id == batch_id => in_flight.count,
            _ => return,
        };
        state.in_flight = None;
        state.settled += count;
        state.last_offset = state.last_offset.max(written.last);
        if let Some(error) = written.error {
            // A failed batch fails the later values too, so a reader sees no gap.
            let left = state.pending.len() as u64;
            state.pending.clear();
            state.first_at = None;
            state.settled += left;
            state.unwritten += count.saturating_sub(written.records as u64) + left;
            state.error = Some(error);
        }
        drop(state);
        self.changed.notify_waiters();
    }

    /// Waits until every value pushed before the call is settled.
    async fn flush(&self) -> BufferFlushResponse {
        let target = {
            let mut state = self.state.lock();
            state.flush_to = state.flush_to.max(state.pushed);
            state.pushed
        };
        self.changed.notify_waiters();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock();
                if state.settled >= target || state.freed {
                    let mut failure = state.error.as_ref().map(StreamError::failure);
                    if failure.is_none() && state.settled < target {
                        failure = Some(StreamError::failure(&StreamError::new(
                            Reason::UnknownWriter,
                            "The buffer was freed before it wrote every value",
                        )));
                    }
                    state.reported |= failure.is_some();
                    return BufferFlushResponse {
                        last_offset: state.last_offset,
                        unwritten: state.unwritten as i64,
                        failure,
                    };
                }
            }
            notified.await;
        }
    }

    fn free(&self) {
        let mut state = self.state.lock();
        state.freed = true;
        state.unwritten += state.pending.len() as u64;
        state.pending.clear();
        // Lang's pump can't write a batch that it took once the buffer is gone.
        if let Some(InFlight {
            count,
            records: Some(_),
            ..
        }) = state.in_flight
        {
            state.unwritten += count;
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

impl State {
    /// Takes the next batch. With `keep`, it keeps its values until lang's write starts.
    fn take(&mut self, max: usize, keep: bool) -> (u64, Vec<AppendRecord>) {
        let rest = self.pending.split_off(self.pending.len().min(max));
        let batch = std::mem::replace(&mut self.pending, rest);
        // The values left behind waited since the batch's first value, so they're due at once.
        if self.pending.is_empty() {
            self.first_at = None;
        }
        self.last_batch += 1;
        self.in_flight = Some(InFlight {
            id: self.last_batch,
            count: batch.len() as u64,
            records: keep.then(|| batch.clone()),
        });
        (self.last_batch, batch)
    }
}

/// Fails the batch in flight when the write task ends early, such as by a panic. After the write
/// settled the batch, the settle here finds no batch and does nothing.
struct SettleOnDrop {
    buffer: Arc<Buffer>,
    batch_id: u64,
}

impl Drop for SettleOnDrop {
    fn drop(&mut self) {
        let error = StreamError::storage("The write of a batch ended before it answered");
        let written = Written {
            error: Some(error),
            ..Default::default()
        };
        self.buffer.settle(self.batch_id, written);
    }
}

#[derive(Default)]
pub(crate) struct Buffers {
    next: AtomicU64,
    table: Mutex<HashMap<u64, Arc<Buffer>>>,
}

impl Buffers {
    fn get(&self, id: u64) -> StreamResult<Arc<Buffer>> {
        self.table
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(unknown_buffer)
    }

    fn lang_pumped(&self, id: u64) -> StreamResult<Arc<Buffer>> {
        let buffer = self.get(id)?;
        if buffer.encoded {
            return Err(StreamError::invalid(
                "The runtime writes the batches of an encoded buffer",
            ));
        }
        Ok(buffer)
    }

    pub(crate) fn free(&self, id: u64) -> StreamResult<()> {
        let buffer = self.table.lock().remove(&id).ok_or_else(unknown_buffer)?;
        buffer.scope.remove_buffer(id);
        buffer.free();
        Ok(())
    }

    /// Frees every buffer, so that the runtime's pumps stop.
    pub(crate) fn free_all(&self) {
        for (_, buffer) in self.table.lock().drain() {
            buffer.free();
        }
    }
}

impl Shared {
    pub(crate) fn open_buffer(self: &Arc<Self>, request: OpenBufferRequest) -> StreamResult<u64> {
        let scope = self.scopes.get(request.scope_id)?;
        let stream = request
            .stream
            .ok_or_else(|| StreamError::invalid("The request has no Stream"))?;
        check_stream_id(&stream.id)?;
        let handle = match request.encoded {
            true => Some(self.tokio_handle().ok_or_else(|| {
                StreamError::invalid("An encoded buffer needs a Tokio runtime to write on")
            })?),
            false => None,
        };
        let buffer = Arc::new(Buffer {
            scope: scope.clone(),
            encoded: request.encoded,
            stream,
            // A delay of more than a year counts as a year, so that `Instant` math can't overflow.
            max_delay: (duration(request.max_delay).unwrap_or(DEFAULT_DELAY)).min(LONGEST_DELAY),
            max_batch: match request.max_batch {
                n if n > 0 => n as usize,
                _ => DEFAULT_BATCH,
            },
            state: Mutex::default(),
            changed: Notify::new(),
        });
        let id = self.buffers.next.fetch_add(1, Ordering::Relaxed) + 1;
        self.buffers.table.lock().insert(id, buffer.clone());
        if let Err(error) = scope.add_buffer(id) {
            self.buffers.free(id)?;
            return Err(error);
        }
        if let Some(handle) = handle {
            // The pump holds a weak reference, so it doesn't keep a dropped runtime alive.
            let shared = Arc::downgrade(self);
            handle.spawn(async move {
                while let Some((batch_id, records)) = buffer.next_batch().await {
                    let guard = SettleOnDrop {
                        buffer: buffer.clone(),
                        batch_id,
                    };
                    let Some(shared) = shared.upgrade() else {
                        return;
                    };
                    let written = shared.write_batch(&guard.buffer, records).await;
                    guard.buffer.settle(batch_id, written);
                }
            });
        }
        Ok(id)
    }

    pub(crate) fn buffer_push(&self, id: u64, records: Vec<AppendRecord>) -> StreamResult<()> {
        self.buffers.get(id)?.push(records)
    }

    pub(crate) async fn buffer_next_batch(&self, id: u64) -> StreamResult<BufferBatch> {
        let buffer = match self.buffers.lang_pumped(id) {
            // A freed buffer is gone from the table, and its pump stops.
            Err(error) if error.reason == Reason::UnknownWriter => {
                return Ok(BufferBatch {
                    done: true,
                    ..Default::default()
                });
            }
            buffer => buffer?,
        };
        Ok(match buffer.next_batch().await {
            Some((batch_id, records)) => BufferBatch {
                batch_id,
                records,
                done: false,
            },
            None => BufferBatch {
                done: true,
                ..Default::default()
            },
        })
    }

    pub(crate) async fn buffer_write(
        self: &Arc<Self>,
        request: BufferWriteRequest,
    ) -> StreamResult<()> {
        let buffer = self.buffers.lang_pumped(request.buffer_id)?;
        let body = request
            .body
            .ok_or_else(|| StreamError::invalid("The write has no records"))?;
        let handle = self
            .tokio_handle()
            .ok_or_else(|| StreamError::invalid("A buffer needs a Tokio runtime to write on"))?;
        let batch_id = request.batch_id;
        let count = match &body {
            Body::Records(batch) => Some(batch.records.len()),
            Body::CodecError(_) => None,
        };
        buffer.start_write(batch_id, count)?;
        let records = match body {
            Body::Records(batch) => batch.records,
            Body::CodecError(message) => {
                let failed = format!("The codec failed on a batch: {message}");
                let error = Some(StreamError::invalid(failed));
                buffer.settle(
                    batch_id,
                    Written {
                        error,
                        ..Default::default()
                    },
                );
                return Ok(());
            }
        };
        // The write runs on a task of its own, so a dropped call doesn't leave the batch in flight.
        // Nobody may await that task, so the guard settles the batch if the task ends early, or if
        // a runtime that shut down never runs it.
        let (shared, guard) = (self.clone(), SettleOnDrop { buffer, batch_id });
        let write = handle.spawn(async move {
            let written = shared.write_batch(&guard.buffer, records).await;
            guard.buffer.settle(batch_id, written);
        });
        let _ = write.await;
        Ok(())
    }

    pub(crate) async fn buffer_flush(&self, id: u64) -> StreamResult<BufferFlushResponse> {
        Ok(self.buffers.get(id)?.flush().await)
    }

    /// Flushes and frees a scope's buffer. Returns the failure that lang didn't get yet, also for
    /// values that a free during the flush dropped.
    pub(crate) async fn end_buffer(&self, id: u64) -> Option<StreamFailure> {
        let buffer = self.buffers.get(id).ok()?;
        let reported = {
            let mut state = buffer.state.lock();
            state.closing = true;
            state.reported
        };
        let flushed = buffer.flush().await;
        let _ = self.buffers.free(id);
        flushed.failure.filter(|_| !reported)
    }

    async fn write_batch(&self, buffer: &Buffer, records: Vec<AppendRecord>) -> Written {
        match buffer.scope.writer(self, &buffer.stream).await {
            Ok(writer) => self.writers.write_counted(writer, records).await,
            Err(error) => Written {
                error: Some(error),
                ..Default::default()
            },
        }
    }
}
