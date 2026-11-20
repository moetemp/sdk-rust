//! Reads the Records of History's read entries again for a replay. The entry of a read without
//! `records_in_history` has only a count and a digest, so the replay reads that many Records after
//! the entry's offset and checks them against the digest.

use super::{
    StreamRuntimes,
    digest::Digest,
    manager::{cut, is_outage, next_wait, retry_waits},
    marker::peeked_marker,
};
use crate::{
    AnyReason, Reason,
    proto::{ReadRecordsRequest, StreamRecord, StreamRef},
    record::field_len,
};
use futures_util::{
    StreamExt,
    future::{self, Either},
    stream,
};
use prost::Message;
use std::{
    collections::{HashMap, VecDeque},
    pin::pin,
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::external_data::StreamReadEntry, temporal::api::history::v1::HistoryEvent,
};
use tokio::{sync::watch, time::Instant};

/// Why a replay can't give Workflow code the Records of a read entry.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplayError {
    /// The Store has fewer Records after the entry's offset than the first run got, or other ones.
    Gone(String),
    /// The Store gave no answer, or this Worker can't read from it, so another attempt may still
    /// read the Records.
    Unavailable(String),
}

/// The Records that a replay read for the read entries of one marker, by the index of each entry.
/// An entry whose marker holds its Records, or that has none, has no result.
pub type MarkerReads = Vec<Option<Result<Vec<StreamRecord>, ReplayError>>>;

/// The Records that a replay read for the markers of a History, by the event id of each marker.
#[derive(Debug, Default)]
pub struct ReplayReads {
    reads: HashMap<i64, MarkerReads>,
    unread: Option<i64>,
}

impl ReplayReads {
    /// Takes the results of the read entries of the marker event `event_id`.
    pub fn take(&mut self, event_id: i64) -> MarkerReads {
        self.reads.remove(&event_id).unwrap_or_default()
    }

    /// Returns the event id of the first marker whose Records the replay hasn't read yet.
    pub fn unread(&self) -> Option<i64> {
        self.unread
    }

    /// Adds the Records of the next part, which also has the first marker that is still unread.
    pub fn extend(&mut self, part: ReplayReads) {
        self.reads.extend(part.reads);
        self.unread = part.unread;
    }
}

/// Returns true if the replay of `entry` needs its Records from the Store.
pub(crate) fn needs_records(entry: &StreamReadEntry) -> bool {
    entry.records.is_empty() && record_count(entry) > 0
}

fn record_count(entry: &StreamReadEntry) -> u64 {
    entry
        .page_records
        .iter()
        .map(|count| u64::from(*count))
        .sum()
}

/// A marker of History whose Records a replay hasn't read yet.
#[derive(Debug)]
struct UnreadMarker {
    event_id: i64,
    /// The number of read entries of the marker.
    len: usize,
    /// The read entries that need Records from the Store.
    entries: Vec<UnreadEntry>,
}

impl UnreadMarker {
    /// Returns the bytes of Records that the entries recorded. History sets them, so they can be
    /// any size.
    fn recorded_bytes(&self) -> usize {
        let bytes =
            (self.entries.iter()).fold(0, |sum: u64, entry| sum.saturating_add(entry.record_bytes));
        usize::try_from(bytes).unwrap_or(usize::MAX)
    }
}

/// What a replay needs of a read entry to read its Records again.
#[derive(Debug)]
struct UnreadEntry {
    /// The index of the entry among the read entries of its marker.
    index: usize,
    stream: StreamRef,
    after_offset: Vec<u8>,
    /// The number of Records.
    count: u64,
    digest: Vec<u8>,
    record_bytes: u64,
}

/// A reader of the Records of read entries, for the replay of one run. It keeps the markers of
/// History that it hasn't read yet, and reads them a part at a time.
#[derive(Debug)]
pub struct ReplayReader {
    runtimes: StreamRuntimes,
    unread: VecDeque<UnreadMarker>,
}

impl ReplayReader {
    /// Creates a reader with the Stream runtimes of a Worker.
    pub fn new(runtimes: StreamRuntimes) -> Self {
        let unread = VecDeque::new();
        Self { runtimes, unread }
    }

    /// Queues each Stream marker in `events` that has a read entry with no Records in History.
    pub fn queue<'a>(&mut self, events: impl Iterator<Item = &'a HistoryEvent>) {
        for event in events {
            let Some(Ok(data)) = peeked_marker(event) else {
                continue;
            };
            let unread = |(index, entry): (usize, &StreamReadEntry)| UnreadEntry {
                index,
                stream: data.streams[entry.stream as usize].clone(),
                after_offset: entry.after_offset.clone(),
                count: record_count(entry),
                digest: entry.digest.clone(),
                record_bytes: entry.record_bytes,
            };
            let entries: Vec<UnreadEntry> = (data.reads.iter().enumerate())
                .filter(|(_, entry)| needs_records(entry))
                .map(unread)
                .collect();
            if !entries.is_empty() {
                self.unread.push_back(UnreadMarker {
                    event_id: event.event_id,
                    len: data.reads.len(),
                    entries,
                });
            }
        }
    }

    /// Returns no Records, with the first marker of the queue as the unread one. A page of History
    /// that comes after the first part starts with them.
    pub fn waiting(&self) -> ReplayReads {
        ReplayReads {
            reads: HashMap::new(),
            unread: self.unread.front().map(|marker| marker.event_id),
        }
    }

    /// Returns true while a queued marker has no Records yet.
    pub fn has_unread(&self) -> bool {
        !self.unread.is_empty()
    }

    /// Reads the Records of the next part of the queue. A part is the next markers while their
    /// recorded bytes stay within `max_task_record_bytes`, and at least one marker.
    pub async fn read_next(&mut self) -> ReplayReads {
        let max_bytes = self.runtimes.settings.max_task_record_bytes;
        let mut reads = ReplayReads::default();
        // The read takes markers by their recorded bytes before any Store call, so that the
        // entries of all the markers that it takes can read at the same time.
        let (mut entries, mut taken_bytes) = (vec![], 0_usize);
        while let Some(marker) = self.unread.pop_front() {
            let marker_bytes = marker.recorded_bytes();
            if !reads.reads.is_empty() && taken_bytes.saturating_add(marker_bytes) > max_bytes {
                self.unread.push_front(marker);
                break;
            }
            taken_bytes = taken_bytes.saturating_add(marker_bytes);
            (reads.reads).insert(marker.event_id, vec![None; marker.len]);
            entries.extend((marker.entries.into_iter()).map(|entry| (marker.event_id, entry)));
        }
        reads.unread = self.waiting().unread;
        // Each Store has its own deadline, so the outage of one Store ends no read of another.
        let deadlines: HashMap<String, Deadline> = (entries.iter())
            .map(|(_, entry)| (entry.stream.store.clone(), Deadline::default()))
            .collect();
        let (reader, deadlines) = (&*self, &deadlines);
        let mut read = stream::iter(entries)
            .map(|(event_id, entry)| async move {
                let deadline = &deadlines[&entry.stream.store];
                (
                    event_id,
                    entry.index,
                    reader.read_entry(&entry, deadline).await,
                )
            })
            .buffer_unordered(self.runtimes.settings.replay_concurrency);
        while let Some((event_id, index, result)) = read.next().await {
            let marker = (reads.reads.get_mut(&event_id)).expect("Each entry has its marker");
            marker[index] = Some(result);
        }
        reads
    }

    /// Reads the Records of one entry, and checks them against its count and its digest.
    async fn read_entry(
        &self,
        entry: &UnreadEntry,
        deadline: &Deadline,
    ) -> Result<Vec<StreamRecord>, ReplayError> {
        let (stream, count) = (&entry.stream, entry.count);
        let range = || {
            format!(
                "the {count} Records of Stream `{}` in Store `{}` after {}",
                stream.id,
                stream.store,
                offset_text(&entry.after_offset)
            )
        };
        let Some(runtime) = self.runtimes.get(&stream.store) else {
            return Err(ReplayError::Unavailable(cut(format!(
                "This Worker has no Stream runtime for the Store `{}`, so it can't read {}",
                stream.store,
                range()
            ))));
        };
        let gone = |why: String| {
            ReplayError::Gone(cut(format!("The Store doesn't have {}: {why}", range())))
        };
        let settings = self.runtimes.settings;
        let recorded = usize::try_from(entry.record_bytes).unwrap_or(usize::MAX);
        let mut records: Vec<StreamRecord> = vec![];
        let mut bytes = 0;
        let mut cursor = entry.after_offset.clone();
        let mut waits = retry_waits(&settings);
        while (records.len() as u64) < count {
            let left = count - records.len() as u64;
            let request = ReadRecordsRequest {
                stream_id: stream.id.clone(),
                from_offset: cursor.clone(),
                max_records: i32::try_from(left).unwrap_or(i32::MAX),
                // Past the recorded bytes the hint is 0, which asks for no cap, so a replay whose
                // Records grew reads the rest in pages of the Store's size.
                max_bytes: i64::try_from(recorded.saturating_sub(bytes)).unwrap_or(i64::MAX),
                ..Default::default()
            };
            let read = tokio::time::timeout(settings.try_deadline, runtime.read_records(request));
            // Once the deadline passed, no try starts, and one in flight stops.
            let answer = match future::select(pin!(deadline.passed()), pin!(read)).await {
                Either::Left((first_error, _)) => {
                    let bound = settings.replay_retry_bound;
                    return Err(ReplayError::Unavailable(cut(format!(
                        "The Store is unavailable, so the replay can't read {} within {bound:?} \
                         of its first failed try: {first_error}",
                        range()
                    ))));
                }
                Either::Right((answer, _)) => answer,
            };
            let error = match answer {
                Ok(Ok(page)) => {
                    waits = retry_waits(&settings);
                    // An answer with no Records that moves the cursor passed entries that aren't
                    // Records, so it doesn't mean that Records are missing.
                    if page.records.is_empty() && (page.done || page.last_offset == cursor) {
                        return Err(gone(format!("it has only {} of them", records.len())));
                    }
                    let page_bytes = page
                        .records
                        .iter()
                        .map(|record| field_len(record.encoded_len()));
                    bytes += page_bytes.sum::<usize>();
                    records.extend(page.records);
                    if page.done && (records.len() as u64) < count {
                        return Err(gone(format!("it has only {} of them", records.len())));
                    }
                    cursor = page.last_offset;
                    continue;
                }
                // These say what this Worker can't do, not what the Store lost, so another Worker
                // may still read the Records.
                Ok(Err(error))
                    if matches!(
                        error.reason,
                        Reason::Any(
                            AnyReason::FormatTooNew
                                | AnyReason::InvalidArgument
                                | AnyReason::Unsupported
                        )
                    ) =>
                {
                    return Err(ReplayError::Unavailable(cut(format!(
                        "This Worker can't read {}: {error}",
                        range()
                    ))));
                }
                Ok(Err(error)) if !is_outage(&error) => {
                    return Err(gone(error.to_string()));
                }
                Ok(Err(error)) => error.to_string(),
                Err(_) => format!("The Store gave no answer in {:?}", settings.try_deadline),
            };
            let left = deadline.start(settings.replay_retry_bound, error);
            let wait = next_wait(&mut waits);
            tokio::time::sleep(wait.min(left)).await;
        }
        let mut digest = Digest::new();
        digest.add(&records);
        if digest.finish() != entry.digest {
            return Err(gone("it has other Records".to_string()));
        }
        Ok(records)
    }
}

/// The deadline of a replay's tries on one Store, as the time of the first try that failed, the
/// bound after it, and the error of that try. A bound past the end of the clock never passes.
#[derive(Default)]
struct Deadline(watch::Sender<Option<(Instant, Duration, String)>>);

impl Deadline {
    /// Starts the deadline `bound` from now, unless a try failed before, and returns the time left.
    fn start(&self, bound: Duration, error: String) -> Duration {
        self.0.send_if_modified(|start| {
            let unset = start.is_none();
            start.get_or_insert_with(|| (Instant::now(), bound, error));
            unset
        });
        let start = self.0.borrow();
        let (from, bound, _) = start.as_ref().expect("The deadline is set");
        bound.saturating_sub(from.elapsed())
    }

    /// Returns the error of the first failed try once the deadline passed.
    async fn passed(&self) -> String {
        let mut deadline = self.0.subscribe();
        let start = (deadline.wait_for(Option::is_some).await)
            .ok()
            .and_then(|start| start.clone());
        let (from, bound, error) = start.expect("The deadline holds its sender");
        // A sleep of zero waits for the next tick of the timer, so a deadline that passed must
        // return immediately, or a try that fails immediately would win each time.
        let left = bound.saturating_sub(from.elapsed());
        if !left.is_zero() {
            tokio::time::sleep(left).await;
        }
        error
    }
}

/// Returns an offset as hex, for a message.
pub(crate) fn offset_text(offset: &[u8]) -> String {
    if offset.is_empty() {
        return "the start".to_string();
    }
    let hex: String = offset.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("offset {hex}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ReadError, ReadReason, StreamError, StreamRuntime,
        record::StoreLimits,
        test_store::{Entry, Fault, Model, ModelStore, client},
        workflow::{StreamSettings, marker::marker_details},
    };
    use parking_lot::Mutex;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::{Arc, atomic::Ordering};
    use temporalio_common::protos::{
        constants::STREAM_MARKER_NAME,
        coresdk::external_data::{StreamMarkerData, StreamReadEnded},
        temporal::api::{
            common::v1::Payload,
            history::v1::{MarkerRecordedEventAttributes, history_event},
        },
    };

    /// How the Store of a replay differs from the Store of the first run.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Store {
        Same,
        /// The Stream lost its last entries.
        Fewer,
        /// A Record has another body of the same size.
        Other,
        /// Each Record has a larger body.
        Larger,
        /// The Stream is gone.
        Gone,
        /// Reads fail with `StoreUnavailable`, or get no answer, at random.
        Outage,
        /// The Store refuses reads for what this Worker can't do, such as a newer format or a call
        /// that the Store doesn't support.
        Refuses,
        /// The Worker has no runtime for the Store.
        NoRuntime,
    }

    fn record(offset: &[u8], data: u8, size: usize) -> StreamRecord {
        StreamRecord {
            offset: offset.to_vec(),
            body: Some(Payload {
                data: vec![data; size],
                ..Default::default()
            }),
            producer: Some(client()),
        }
    }

    /// Returns a text of at most `bytes` bytes, and of fewer than 4 less, with characters of one
    /// to four bytes.
    fn text(rng: &mut StdRng, bytes: usize) -> String {
        let mut text = String::new();
        loop {
            let char = ['x', 'é', '€', '𝄞'][rng.random_range(0..4)];
            if text.len() + char.len_utf8() > bytes {
                return text;
            }
            text.push(char);
        }
    }

    /// Returns a Stream id or a Store name, which is sometimes as long as an id may be.
    fn name(rng: &mut StdRng, short: &str) -> String {
        if rng.random_bool(0.8) {
            return short.to_string();
        }
        let bytes = rng.random_range(4000..=4096);
        text(rng, bytes)
    }

    /// Checks that `message` fits the bound of a failure message, and returns true if it was cut.
    fn fits(message: &str, seed: u64) -> bool {
        assert!(
            message.len() <= 4096,
            "seed {seed}: {} bytes",
            message.len()
        );
        message.ends_with("...")
    }

    /// When each try of a Store came, and when it failed if it got `StoreUnavailable` or no answer.
    type Tries = Arc<Mutex<Vec<(Instant, Option<Instant>)>>>;

    /// A Store of a replay, with the Stream that the first run read from it.
    struct Replayed {
        kind: Store,
        stream: StreamRef,
        /// The Records that the first run read.
        records: Vec<StreamRecord>,
        model: Arc<ModelStore>,
        tries: Tries,
    }

    /// Makes a Stream, and the Store `store_name` of a random kind that has it now.
    fn replayed(
        rng: &mut StdRng,
        seed: u64,
        store_name: &str,
        settings: StreamSettings,
    ) -> Replayed {
        let kind = [
            Store::Same,
            Store::Fewer,
            Store::Other,
            Store::Larger,
            Store::Gone,
            Store::Outage,
            Store::Refuses,
            Store::NoRuntime,
        ][rng.random_range(0..8)];
        // A long Stream gives entries with more Records than one read can ask for.
        let len = if rng.random_bool(0.05) {
            rng.random_range(1000..1500u16)
        } else {
            rng.random_range(1..60u16)
        };
        let mut entries: Vec<Entry> = (0..len)
            .map(|index| {
                let offset = (index * 2 + 1).to_be_bytes().to_vec();
                let size = match rng.random_bool(0.9) {
                    true => rng.random_range(0..100),
                    false => rng.random_range(100..4000),
                };
                let record = rng.random_bool(0.7).then(|| record(&offset, 0, size));
                let at = Duration::ZERO;
                Entry { offset, at, record }
            })
            .collect();
        let records = entries.iter().filter_map(|e| e.record.clone()).collect();
        let stream = StreamRef {
            id: name(rng, "s"),
            store: name(rng, store_name),
        };
        let refusal = [
            AnyReason::FormatTooNew,
            AnyReason::InvalidArgument,
            AnyReason::Unsupported,
        ][rng.random_range(0..3)];
        let bytes = rng.random_range(0..6000);
        let message = text(rng, bytes);
        match kind {
            Store::Fewer => entries.truncate(rng.random_range(0..entries.len())),
            Store::Other => {
                let at = rng.random_range(0..entries.len());
                let size = (entries[at].record.as_ref())
                    .map_or(0, |record| record.body.as_ref().unwrap().data.len());
                entries[at].record = Some(record(&entries[at].offset, 1, size));
            }
            Store::Larger => {
                let more = rng.random_range(1..2000);
                for record in entries.iter_mut().filter_map(|entry| entry.record.as_mut()) {
                    record.body.as_mut().unwrap().data.extend(vec![0; more]);
                }
            }
            _ => {}
        }
        let tries = Tries::default();
        let kept = tries.clone();
        let fault = move |rng: &mut StdRng, _: &ReadRecordsRequest| {
            let now = Instant::now();
            let fails = |error| Some(Fault::Fails(error));
            let (fault, failed) = match kind {
                Store::Gone => {
                    let gone = ReadError::operation(ReadReason::NotFound, &message);
                    (fails(gone), None)
                }
                Store::Outage if rng.random_bool(0.3) => {
                    (fails(StreamError::store_unavailable(&message)), Some(now))
                }
                Store::Outage if rng.random_bool(0.2) => {
                    (Some(Fault::Silent), Some(now + settings.try_deadline))
                }
                Store::Refuses => (fails(StreamError::new(refusal, &message)), None),
                _ => (None, None),
            };
            kept.lock().push((now, failed));
            fault
        };
        let model = Model {
            rng: StdRng::seed_from_u64(seed),
            entries,
            closed: rng.random_bool(0.5),
            fault: Box::new(fault),
            ignores_wait: false,
            reading: (0, 0),
            moved_to: None,
            empty_until: None,
            answered_early: 0,
            violations: vec![],
        };
        Replayed {
            kind,
            stream,
            records,
            model: Arc::new(ModelStore::new(model)),
            tries,
        }
    }

    // Only a Store that gives no answer, or one that this Worker can't read or has no runtime for,
    // leaves the Records unknown. Fewer Records or other ones are gone. An entry gets the result
    // of its own Store, whatever the other Store does.
    #[tokio::test(start_paused = true)]
    async fn a_replay_read_gives_the_records_or_its_cause() {
        let (mut together, mut stopped, mut cut_off, mut timed_out) = (0, 0, 0, 0);
        let (mut markers_together, mut many_together, mut beside_an_outage) = (0, 0, 0);
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let settings = StreamSettings {
                retry_initial_interval: Duration::from_millis(rng.random_range(1..20)),
                retry_max_interval: Duration::from_millis(rng.random_range(20..50)),
                try_deadline: Duration::from_millis(rng.random_range(1..50)),
                replay_retry_bound: Duration::from_millis(rng.random_range(0..100)),
                replay_concurrency: rng.random_range(1..=4),
                ..Default::default()
            };
            let stores = ["mem", "disk"].map(|name| replayed(&mut rng, seed, name, settings));
            // The markers of the first run, each with read entries over the two Streams. An entry
            // of few Records lets a part take several markers.
            let (mut events, mut datas) = (vec![], vec![]);
            let mut expected = vec![];
            let markers = rng.random_range(1..=4);
            for event_id in 1..=markers {
                let mut data = StreamMarkerData {
                    streams: stores.iter().map(|store| store.stream.clone()).collect(),
                    ..Default::default()
                };
                for seq in 1..=rng.random_range(1..=4) {
                    let of = rng.random_range(0..2);
                    let records = &stores[of].records;
                    let from = rng.random_range(0..=records.len());
                    let count = match rng.random_bool(0.5) {
                        true => rng.random_range(0..=(records.len() - from).min(3)),
                        false => rng.random_range(0..=records.len() - from),
                    };
                    let after_offset = match from {
                        0 => vec![],
                        at => records[at - 1].offset.clone(),
                    };
                    let given = records[from..from + count].to_vec();
                    let in_history = rng.random_bool(0.2);
                    let mut digest = Digest::new();
                    digest.add(&given);
                    let size = |record: &StreamRecord| field_len(record.encoded_len());
                    let bytes = given.iter().map(size).sum::<usize>() as u64;
                    let pages = vec![1; count];
                    // An entry with no page has no digest, and an end.
                    data.reads.push(StreamReadEntry {
                        seq,
                        stream: of as u32,
                        after_offset: after_offset.clone(),
                        page_activations: (0..count as u64).collect(),
                        page_records: pages,
                        digest: match in_history || count == 0 {
                            true => vec![],
                            false => digest.finish(),
                        },
                        records: if in_history { given.clone() } else { vec![] },
                        end: (count == 0).then(StreamReadEnded::default),
                        record_bytes: if in_history { 0 } else { bytes },
                    });
                    let read = (!in_history && count > 0).then_some(given);
                    expected.push((event_id, seq as usize - 1, of, after_offset, read));
                }
                events.push(HistoryEvent {
                    event_id,
                    attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                        MarkerRecordedEventAttributes {
                            marker_name: STREAM_MARKER_NAME.to_string(),
                            details: marker_details(&data),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                });
                datas.push(data);
            }
            let served = (stores.iter())
                .filter(|store| store.kind != Store::NoRuntime)
                .map(|store| {
                    let runtime = StreamRuntime::new(store.model.clone(), StoreLimits::default());
                    (store.stream.store.clone(), Arc::new(runtime))
                });
            let mut runtimes = StreamRuntimes::new(served);
            runtimes.settings = settings;
            // The markers with an entry that needs Records, each with its recorded bytes.
            let needing: Vec<(i64, usize)> = (1..=markers)
                .zip(&datas)
                .filter(|(_, data)| data.reads.iter().any(needs_records))
                .map(|(event_id, data)| {
                    let bytes = data.reads.iter().map(|read| read.record_bytes).sum::<u64>();
                    (event_id, bytes as usize)
                })
                .collect();
            let recorded: usize = needing.iter().map(|(_, bytes)| bytes).sum();
            runtimes.settings.max_task_record_bytes = rng.random_range(1..=2 * recorded + 1);
            let max_bytes = runtimes.settings.max_task_record_bytes;
            let mut reader = ReplayReader::new(runtimes);
            reader.queue(events.iter());
            // The reader gives each marker in one part, in the order of History.
            let mut results = HashMap::<i64, MarkerReads>::new();
            let mut next = 0;
            while reader.has_unread() {
                let seen = stores.each_ref().map(|store| store.tries.lock().len());
                let mut part = reader.read_next().await;
                // On each Store, the deadline stops each try, and no try starts after it.
                let end = Instant::now();
                let bound = settings.replay_retry_bound;
                let mut deadlines = vec![];
                for (store, seen) in stores.iter().zip(seen) {
                    let tries = store.tries.lock()[seen..].to_vec();
                    let Some(first) = tries.iter().filter_map(|(_, failed)| *failed).min() else {
                        continue;
                    };
                    let deadline = first + bound;
                    let early =
                        |at: &Instant| *at < deadline || (bound.is_zero() && *at == deadline);
                    assert!(tries.iter().all(|(at, _)| early(at)), "seed {seed}");
                    deadlines.push(deadline);
                }
                if let Some(last) = deadlines.iter().max() {
                    assert!(end <= *last, "seed {seed}: {:?} late", end - *last);
                }
                // A part takes at least one marker, and those that it takes fit the bound unless
                // it takes only one. The next one would pass the bound.
                let taken: Vec<&(i64, usize)> = (needing[next..].iter())
                    .take_while(|(event_id, _)| {
                        let marker = part.take(*event_id);
                        let read = !marker.is_empty();
                        results.insert(*event_id, marker);
                        read
                    })
                    .collect();
                let taken_bytes: usize = taken.iter().map(|(_, bytes)| bytes).sum();
                assert!(!taken.is_empty(), "seed {seed}");
                assert!(taken.len() == 1 || taken_bytes <= max_bytes, "seed {seed}");
                next += taken.len();
                let unread = needing.get(next);
                assert_eq!(
                    part.unread(),
                    unread.map(|(event_id, _)| *event_id),
                    "seed {seed}"
                );
                let passes = |(_, bytes): &(i64, usize)| taken_bytes + bytes > max_bytes;
                assert!(unread.is_none_or(passes), "seed {seed}");
                for event_id in 1..=markers {
                    assert!(part.take(event_id).is_empty(), "seed {seed}: {event_id}");
                }
                stopped += usize::from(unread.is_some());
                markers_together += usize::from(taken.len() > 1);
                // Markers of more than 10 small Records each fit the bound together.
                let many = |(event_id, _): &&(i64, usize)| {
                    let entries = datas[*event_id as usize - 1].reads.iter();
                    entries.map(record_count).sum::<u64>() > 10
                };
                many_together += usize::from(taken.len() > 1 && taken.iter().all(many));
            }
            assert_eq!(next, needing.len(), "seed {seed}");
            // A marker holds no more than its recorded bytes.
            for (event_id, max) in &needing {
                let records = (results[event_id].iter().flatten()).filter_map(|r| r.as_ref().ok());
                let held: usize = records.flatten().map(|r| field_len(r.encoded_len())).sum();
                assert!(held <= *max, "seed {seed}: {held} of {max}");
            }
            for (of, store) in stores.iter().enumerate() {
                let read: Vec<&StreamReadEntry> = (datas.iter())
                    .flat_map(|data| &data.reads)
                    .filter(|entry| entry.stream as usize == of && needs_records(entry))
                    .collect();
                let largest = (store.model.records_after(&[]).iter())
                    .map(|record| field_len(record.encoded_len()))
                    .max()
                    .unwrap_or(0);
                // Each entry gets its recorded bytes and one Record more, or, if its Records
                // grew, at most its count of them.
                let answered = store.model.answered.load(Ordering::Relaxed);
                let recorded = read.iter().map(|entry| entry.record_bytes).sum::<u64>() as usize;
                let bound = match store.kind {
                    Store::Larger => {
                        read.iter().map(|entry| record_count(entry)).sum::<u64>() as usize
                    }
                    _ => read.len(),
                } * largest
                    + recorded;
                assert!(answered <= bound, "seed {seed}: {answered} of {bound}");
                let (_, peak) = store.model.model.lock().reading;
                assert!(peak <= settings.replay_concurrency, "seed {seed}: {peak}");
                together += usize::from(peak > 1);
            }
            for (event_id, index, of, after_offset, given) in expected {
                let store = &stores[of];
                let kind = store.kind;
                let result = (results.get_mut(&event_id))
                    .and_then(|marker| marker.get_mut(index))
                    .and_then(Option::take);
                // The Records that the Store of the replay has where the entry read.
                let unchanged = given.as_ref().is_some_and(|given| {
                    let now = store.model.records_after(&after_offset);
                    now.len() >= given.len() && now[..given.len()] == given[..]
                }) && kind != Store::Gone;
                match (given, result) {
                    // An entry with its Records in History, or with none, reads no Store.
                    (None, None) => {}
                    (Some(given), Some(Ok(records))) => {
                        assert!(
                            unchanged && kind != Store::NoRuntime,
                            "seed {seed}: {kind:?}"
                        );
                        assert_eq!(records, given, "seed {seed}");
                        let other = stores[1 - of].kind;
                        beside_an_outage += usize::from(other == Store::Outage);
                    }
                    (Some(_), Some(Err(ReplayError::Gone(message)))) => {
                        assert!(!unchanged, "seed {seed}: {kind:?}");
                        cut_off += usize::from(fits(&message, seed));
                    }
                    (Some(_), Some(Err(ReplayError::Unavailable(message)))) => {
                        let unknown = matches!(kind, Store::Outage | Store::Refuses);
                        assert!(unknown || kind == Store::NoRuntime, "seed {seed}: {kind:?}");
                        cut_off += usize::from(fits(&message, seed));
                        timed_out += usize::from(kind == Store::Outage);
                    }
                    other => panic!("seed {seed}: {kind:?}: {other:?}"),
                }
            }
        }
        let reached = [
            together,
            stopped,
            cut_off,
            timed_out,
            markers_together,
            many_together,
            beside_an_outage,
        ];
        assert!(reached.iter().all(|count| *count > 0), "{reached:?}");
    }

    /// Replays a marker with one read entry, of one Record that the entry recorded `less` bytes
    /// short of, from a Store whose reads get `fault`. Returns the Record and the entry's result.
    async fn replay_one_record(
        less: u64,
        limits: StoreLimits,
        settings: StreamSettings,
        fault: impl FnMut(&mut StdRng, &ReadRecordsRequest) -> Option<Fault> + Send + 'static,
    ) -> (StreamRecord, MarkerReads) {
        let given = record(&[1], 0, 10);
        let mut digest = Digest::new();
        digest.add(std::slice::from_ref(&given));
        let stream = StreamRef {
            id: "s".to_string(),
            store: "mem".to_string(),
        };
        let data = StreamMarkerData {
            streams: vec![stream.clone()],
            reads: vec![StreamReadEntry {
                seq: 1,
                page_records: vec![1],
                page_activations: vec![0],
                digest: digest.finish(),
                record_bytes: field_len(given.encoded_len()) as u64 - less,
                ..Default::default()
            }],
            ..Default::default()
        };
        let event = HistoryEvent {
            event_id: 1,
            attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                MarkerRecordedEventAttributes {
                    marker_name: STREAM_MARKER_NAME.to_string(),
                    details: marker_details(&data),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let entry = Entry {
            offset: vec![1],
            at: Duration::ZERO,
            record: Some(given.clone()),
        };
        let model = ModelStore::new(Model {
            rng: StdRng::seed_from_u64(0),
            entries: vec![entry],
            closed: false,
            fault: Box::new(fault),
            ignores_wait: false,
            reading: (0, 0),
            moved_to: None,
            empty_until: None,
            answered_early: 0,
            violations: vec![],
        });
        let runtime = StreamRuntime::new(Arc::new(model), limits);
        let mut runtimes = StreamRuntimes::new([(stream.store, Arc::new(runtime))]);
        runtimes.settings = settings;
        let mut reader = ReplayReader::new(runtimes);
        reader.queue([event].iter());
        (given, reader.read_next().await.take(1))
    }

    // A runtime whose Record size is below the Record leaves it unknown, since another Worker may
    // read it.
    #[tokio::test]
    async fn only_the_count_and_the_digest_of_an_entry_tell_that_its_records_are_gone() {
        let small = StoreLimits::default().with_max_record_bytes(5).unwrap();
        for limits in [StoreLimits::default(), small] {
            let settings = StreamSettings::default();
            let (given, result) = replay_one_record(1, limits, settings, |_, _| None).await;
            match (limits == small, result.as_slice()) {
                (false, [Some(Ok(records))]) => assert_eq!(records, &[given]),
                (true, [Some(Err(ReplayError::Unavailable(_)))]) => {}
                (_, other) => panic!("{limits:?}: {other:?}"),
            }
        }
    }

    // A replay with no bound in time on its tries, or on the wait between them, tries again after
    // an outage.
    #[tokio::test(start_paused = true)]
    async fn a_replay_with_no_bound_in_time_tries_again() {
        let settings = StreamSettings {
            retry_initial_interval: Duration::MAX,
            retry_max_interval: Duration::MAX,
            replay_retry_bound: Duration::MAX,
            ..Default::default()
        };
        let mut failed = false;
        let fault = move |_: &mut StdRng, _: &ReadRecordsRequest| {
            let down = StreamError::store_unavailable("Down");
            (!std::mem::replace(&mut failed, true)).then_some(Fault::Fails(down))
        };
        let limits = StoreLimits::default();
        let (given, result) = replay_one_record(0, limits, settings, fault).await;
        assert_eq!(result, vec![Some(Ok(vec![given]))]);
    }
}
