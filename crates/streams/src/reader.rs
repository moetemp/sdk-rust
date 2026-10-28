//! Readers. A reader keeps the state of one read of a Stream, so lang only decodes each item.

use crate::{
    Reason, StreamError, StreamResult,
    epoch::{epoch_key, withdraws_producer},
    proto::{
        OpenReaderRequest, Producer, ReadNextRequest, ReadRecordsRequest, ReadRecordsResponse,
        ReaderItem, ReaderPage, StreamProducerKind, StreamReadStart, Supersession,
        stream_record::Item,
    },
    runtime::Shared,
    service::MAX_WAIT,
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
use tokio::time::Instant;

// An empty page that comes back in less than half of its wait is followed by a pause, so a Store
// that ignores the wait isn't asked again at once. The pause doubles up to the last one.
const FIRST_PAUSE: Duration = Duration::from_millis(50);
const LAST_PAUSE: Duration = Duration::from_secs(2);

/// The Producer fields that the withdrawal rule reads.
type ProducerKey = (i32, String, String, Option<String>);

struct State {
    /// The request of the next page.
    request: ReadRecordsRequest,
    wait_new_records: bool,
    create_wait: Option<Duration>,
    /// Set by the first `ReadNext`, so that the create wait starts when lang starts to read.
    create_until: Option<Instant>,
    /// The resume point, zero before the first page of a read from END or with `last`.
    position: i64,
    /// The offsets of the Records that the read gave, as runs, by Producer.
    shown: HashMap<ProducerKey, (Producer, Vec<(i64, i64)>)>,
    done: bool,
}

pub(crate) struct Reader {
    /// Allows one `ReadNext` at a time.
    state: tokio::sync::Mutex<State>,
}

#[derive(Default)]
pub(crate) struct Readers {
    next: AtomicU64,
    table: Mutex<HashMap<u64, Arc<Reader>>>,
}

impl Readers {
    fn get(&self, id: u64) -> StreamResult<Arc<Reader>> {
        self.table
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(unknown_reader)
    }

    pub(crate) fn free(&self, id: u64) -> StreamResult<()> {
        self.table
            .lock()
            .remove(&id)
            .map(|_| ())
            .ok_or_else(unknown_reader)
    }

    pub(crate) fn clear(&self) {
        self.table.lock().clear();
    }
}

fn unknown_reader() -> StreamError {
    StreamError::new(Reason::UnknownWriter, "No reader has the id")
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// The time `wait` from now. A wait of more than a year counts as a year, so it can't overflow.
fn after(wait: Duration) -> Instant {
    Instant::now() + wait.min(Duration::from_secs(365 * 24 * 60 * 60))
}

fn proto_duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: value.as_secs() as i64,
        nanos: value.subsec_nanos() as i32,
    }
}

fn producer_key(producer: &Producer) -> ProducerKey {
    (
        producer.kind,
        producer.id.clone(),
        producer.run_id.clone(),
        producer.epoch.as_ref().map(epoch_key),
    )
}

/// The page of a read that ended with no new item.
fn ended() -> ReadRecordsResponse {
    ReadRecordsResponse {
        done: true,
        ..Default::default()
    }
}

impl State {
    /// Moves the resume point over a page and returns its items. A Superseded item takes the
    /// offsets of the Records that it withdraws from this read.
    fn take(&mut self, page: ReadRecordsResponse) -> Vec<ReaderItem> {
        let mut items = vec![];
        for record in page.records {
            let offset = record.offset;
            let mut withdrawn_offsets = vec![];
            match &record.item {
                // The first Record of its batch has the same offset and comes next.
                Some(Item::Superseded(supersession)) => {
                    self.position = offset;
                    withdrawn_offsets = self.withdraw(supersession);
                }
                Some(Item::Data(data)) => {
                    self.position = self.position.max(offset + 1);
                    // Nothing withdraws a client's Records, so a long read of them keeps nothing.
                    let producer = data.producer.as_ref();
                    if let Some(producer) =
                        producer.filter(|p| p.kind != StreamProducerKind::Client as i32)
                    {
                        let (_, runs) = self
                            .shown
                            .entry(producer_key(producer))
                            .or_insert_with(|| (producer.clone(), vec![]));
                        match runs.last_mut() {
                            Some((_, last)) if *last + 1 == offset => *last = offset,
                            _ => runs.push((offset, offset)),
                        }
                    }
                }
                Some(Item::Close(_)) => {
                    self.position = self.position.max(offset + 1);
                    self.done = true;
                }
                None => self.position = self.position.max(offset + 1),
            }
            items.push(ReaderItem {
                record: Some(record),
                next_offset: self.position,
                withdrawn_offsets,
            });
            if self.done {
                return items;
            }
        }
        self.position = self.position.max(page.next_offset);
        self.done |= page.done;
        items
    }

    fn withdraw(&mut self, supersession: &Supersession) -> Vec<i64> {
        let mut offsets = vec![];
        // A withdrawn Record stays withdrawn, so the read forgets it.
        self.shown.retain(|_, (producer, runs)| {
            if !withdraws_producer(supersession, producer) {
                return true;
            }
            for (first, last) in runs.iter() {
                offsets.extend(*first..=*last);
            }
            false
        });
        offsets.sort_unstable();
        offsets
    }
}

impl Shared {
    pub(crate) fn open_reader(&self, request: OpenReaderRequest) -> StreamResult<u64> {
        if request.from_offset < 0 || request.last < 0 || request.max_bytes < 0 {
            return Err(StreamError::invalid(
                "from_offset, last and max_bytes must be 0 or more",
            ));
        }
        let from_end = request.from_offset == 0 && request.start == StreamReadStart::End as i32;
        if request.last > 0 && (request.from_offset > 0 || from_end) {
            return Err(StreamError::invalid(
                "A read with last takes no from_offset and no END",
            ));
        }
        let state = State {
            request: ReadRecordsRequest {
                namespace: request.namespace,
                stream: request.stream,
                from_offset: request.from_offset,
                start: if request.last > 0 { 0 } else { request.start },
                last: request.last,
                max_bytes: request.max_bytes,
                ..Default::default()
            },
            wait_new_records: request.wait_new_records,
            create_wait: duration(request.create_wait),
            create_until: None,
            position: if request.last > 0 || from_end {
                0
            } else {
                request.from_offset
            },
            shown: HashMap::new(),
            done: false,
        };
        let id = self.readers.next.fetch_add(1, Ordering::Relaxed) + 1;
        let reader = Arc::new(Reader {
            state: tokio::sync::Mutex::new(state),
        });
        self.readers.table.lock().insert(id, reader);
        // A shutdown that already cleared the table doesn't see this reader.
        if self.is_shut_down() {
            let _ = self.readers.free(id);
            return Err(crate::runtime::shut_down());
        }
        Ok(id)
    }

    pub(crate) async fn read_next(&self, request: ReadNextRequest) -> StreamResult<ReaderPage> {
        let reader = self.readers.get(request.reader_id)?;
        let mut state = reader.state.lock().await;
        if !state.done {
            let create_until = state.create_until.or_else(|| state.create_wait.map(after));
            let deadline = after(duration(request.max_wait).unwrap_or(MAX_WAIT));
            // The state changes only after the last await, so a dropped call changes nothing.
            let paged = self.next_page(&state, create_until, deadline).await;
            state.create_until = create_until;
            let (page, next) = paged?;
            state.request = next;
            let items = state.take(page);
            return Ok(ReaderPage {
                items,
                next_offset: state.position,
                done: state.done,
            });
        }
        Ok(ReaderPage {
            next_offset: state.position,
            done: true,
            ..Default::default()
        })
    }

    /// The next page with items, or the one at the deadline or at the end of the read, and the
    /// request of the page after it.
    async fn next_page(
        &self,
        state: &State,
        create_until: Option<Instant>,
        deadline: Instant,
    ) -> StreamResult<(ReadRecordsResponse, ReadRecordsRequest)> {
        let service = self.service().await?;
        let mut request = state.request.clone();
        let mut pause = FIRST_PAUSE;
        loop {
            let now = Instant::now();
            let left = deadline.saturating_duration_since(now);
            request.create_wait = create_until
                .map(|until| until.saturating_duration_since(now).min(left))
                .filter(|wait| !wait.is_zero())
                .map(proto_duration);
            // The newest Records come in one page that never waits.
            let last = request.last > 0;
            let wait = (state.wait_new_records && !last)
                .then(|| left.clamp(Duration::from_millis(1), MAX_WAIT));
            request.wait = wait.map(proto_duration);
            let page = match service.read_records(request.clone()).await {
                Ok(page) => page,
                Err(error)
                    if error.reason == Reason::NotFound
                        && create_until.is_some_and(|until| until > Instant::now()) =>
                {
                    if Instant::now() >= deadline {
                        return Ok((ReadRecordsResponse::default(), request));
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            let from = request.from_offset;
            if page.next_offset > 0 {
                (request.from_offset, request.last, request.start) = (page.next_offset, 0, 0);
            }
            if last {
                // A read that waits goes on after the newest Records.
                let end = page.done || !state.wait_new_records || page.next_offset == 0;
                if !page.records.is_empty() || end {
                    let done = page.done || end;
                    return Ok((ReadRecordsResponse { done, ..page }, request));
                }
                continue;
            }
            if !page.records.is_empty() || page.done {
                return Ok((page, request));
            }
            if !state.wait_new_records {
                // An empty page that didn't move is at the newest Record.
                if page.next_offset == 0 || page.next_offset <= from {
                    return Ok((
                        ReadRecordsResponse {
                            next_offset: page.next_offset,
                            ..ended()
                        },
                        request,
                    ));
                }
                continue;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok((page, request));
            }
            if now.elapsed() < wait.unwrap_or_default() / 2 {
                tokio::time::sleep(pause.min(left)).await;
                pause = (pause * 2).min(LAST_PAUSE);
            } else {
                pause = FIRST_PAUSE;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamRecord;

    #[test]
    fn an_item_of_an_unknown_kind_moves_the_resume_point() {
        let mut state = State {
            request: ReadRecordsRequest::default(),
            wait_new_records: false,
            create_wait: None,
            create_until: None,
            position: 5,
            shown: HashMap::new(),
            done: false,
        };
        let page = ReadRecordsResponse {
            records: vec![StreamRecord {
                offset: 9,
                item: None,
            }],
            ..Default::default()
        };
        let items = state.take(page);
        assert_eq!((items[0].next_offset, state.position), (10, 10));
    }
}
