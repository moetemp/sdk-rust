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
use std::{collections::HashMap, pin::pin, time::Duration};
use temporalio_common::protos::{
    coresdk::external_data::{StreamMarkerData, StreamReadEntry},
    temporal::api::history::v1::HistoryEvent,
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

    /// Returns the event id of the first marker that the read left, since its Records could pass
    /// the bound together with those of the markers before it.
    pub fn unread(&self) -> Option<i64> {
        self.unread
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

/// Returns the bytes of Records that the read entries of a marker recorded. History sets them, so
/// they can be any size.
fn recorded_bytes(data: &StreamMarkerData) -> usize {
    let bytes =
        (data.reads.iter()).fold(0, |sum: u64, entry| sum.saturating_add(entry.record_bytes));
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// A reader of the Records of read entries, for the replays of Workflows.
#[derive(Clone, Debug)]
pub struct ReplayReader {
    runtimes: StreamRuntimes,
}

impl ReplayReader {
    /// Creates a reader with the Stream runtimes of a Worker.
    pub fn new(runtimes: StreamRuntimes) -> Self {
        Self { runtimes }
    }

    /// Reads the Records of the Stream markers in `events` after the event `after`, from the first
    /// marker on while their recorded bytes stay within `max_task_record_bytes`.
    pub async fn read<'a>(
        &self,
        events: impl Iterator<Item = &'a HistoryEvent> + Send,
        after: i64,
    ) -> ReplayReads {
        let max_bytes = self.runtimes.settings.max_task_record_bytes;
        let mut reads = ReplayReads::default();
        // The read takes markers by their recorded bytes before any Store call, so that the
        // entries of all the markers that it takes can read at the same time.
        let (mut entries, mut taken_bytes) = (vec![], 0_usize);
        for event in events {
            if event.event_id <= after {
                continue;
            }
            let Some(Ok(data)) = peeked_marker(event) else {
                continue;
            };
            let marker_bytes = recorded_bytes(&data);
            if marker_bytes == 0 {
                continue;
            }
            if !reads.reads.is_empty() && taken_bytes.saturating_add(marker_bytes) > max_bytes {
                reads.unread = Some(event.event_id);
                break;
            }
            taken_bytes = taken_bytes.saturating_add(marker_bytes);
            (reads.reads).insert(event.event_id, vec![None; data.reads.len()]);
            for (index, entry) in data.reads.iter().enumerate() {
                if needs_records(entry) {
                    let stream = data.streams[entry.stream as usize].clone();
                    entries.push((event.event_id, index, stream, entry.clone()));
                }
            }
        }
        let deadline = &Deadline(watch::Sender::new(None));
        let mut read = stream::iter(entries)
            .map(|(event_id, index, stream, entry)| async move {
                let result = self.read_entry(&stream, &entry, deadline).await;
                (event_id, index, result)
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
        stream: &StreamRef,
        entry: &StreamReadEntry,
        deadline: &Deadline,
    ) -> Result<Vec<StreamRecord>, ReplayError> {
        let count = record_count(entry);
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
        let mut outage = None;
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
                Either::Left(_) => {
                    let bound = settings.replay_retry_bound;
                    let why = outage.map_or_else(String::new, |error| format!(": {error}"));
                    return Err(ReplayError::Unavailable(cut(format!(
                        "The Store is unavailable, so the replay can't read {} within {bound:?} \
                         of its first failed try{why}",
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
            let left = deadline.start(settings.replay_retry_bound);
            let wait = next_wait(&mut waits);
            outage = Some(error);
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

/// The deadline of the tries of one replay read, as the time of the first try that failed and the
/// bound after it. A bound past the end of the clock never passes.
struct Deadline(watch::Sender<Option<(Instant, Duration)>>);

impl Deadline {
    /// Starts the deadline `bound` from now, unless a try failed before, and returns the time left.
    fn start(&self, bound: Duration) -> Duration {
        self.0.send_if_modified(|start| {
            let unset = start.is_none();
            start.get_or_insert_with(|| (Instant::now(), bound));
            unset
        });
        let (from, bound) = self.0.borrow().expect("The deadline is set");
        bound.saturating_sub(from.elapsed())
    }

    /// Returns once the deadline passed.
    async fn passed(&self) {
        let mut deadline = self.0.subscribe();
        let start = (deadline.wait_for(Option::is_some).await)
            .ok()
            .and_then(|start| *start);
        // A sleep of zero waits for the next tick of the timer, so a deadline that passed must
        // return immediately, or a try that fails immediately would win each time.
        let left = start.map(|(from, bound)| bound.saturating_sub(from.elapsed()));
        if let Some(left) = left.filter(|left| !left.is_zero()) {
            tokio::time::sleep(left).await;
        }
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
        coresdk::external_data::StreamReadEnded,
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

    // Only a Store that gives no answer, or one that this Worker can't read or has no runtime for,
    // leaves the Records unknown. Fewer Records or other ones are gone.
    #[tokio::test(start_paused = true)]
    async fn a_replay_read_gives_the_records_or_its_cause() {
        let (mut together, mut stopped, mut cut_off, mut timed_out) = (0, 0, 0, 0);
        let (mut markers_together, mut many_together) = (0, 0);
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            // A long Stream gives entries with more Records than one read can ask for.
            let len = if rng.random_bool(0.05) {
                rng.random_range(1000..1500u16)
            } else {
                rng.random_range(1..60u16)
            };
            let entries: Vec<Entry> = (0..len)
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
            let records: Vec<StreamRecord> =
                entries.iter().filter_map(|e| e.record.clone()).collect();
            let stream = StreamRef {
                id: name(&mut rng, "s"),
                store: name(&mut rng, "mem"),
            };
            // The markers of the first run, each with read entries over the Stream. An entry of
            // few Records lets the read take several markers.
            let (mut events, mut datas) = (vec![], vec![]);
            let mut expected = vec![];
            let markers = rng.random_range(1..=4);
            for event_id in 1..=markers {
                let mut data = StreamMarkerData {
                    streams: vec![stream.clone()],
                    ..Default::default()
                };
                for seq in 1..=rng.random_range(1..=4) {
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
                        ..Default::default()
                    });
                    let read = (!in_history && count > 0).then_some(given);
                    expected.push((event_id, seq as usize - 1, after_offset, read));
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
            let store = [
                Store::Same,
                Store::Fewer,
                Store::Other,
                Store::Larger,
                Store::Gone,
                Store::Outage,
                Store::Refuses,
                Store::NoRuntime,
            ][rng.random_range(0..8)];
            let refusal = [
                AnyReason::FormatTooNew,
                AnyReason::InvalidArgument,
                AnyReason::Unsupported,
            ][rng.random_range(0..3)];
            let bytes = rng.random_range(0..6000);
            let message = text(&mut rng, bytes);
            let mut entries = entries;
            match store {
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
            let settings = StreamSettings {
                retry_initial_interval: Duration::from_millis(rng.random_range(1..20)),
                retry_max_interval: Duration::from_millis(rng.random_range(20..50)),
                try_deadline: Duration::from_millis(rng.random_range(1..50)),
                replay_retry_bound: Duration::from_millis(rng.random_range(0..100)),
                replay_concurrency: rng.random_range(1..=4),
                ..Default::default()
            };
            // When each try came, and when it failed if it got `StoreUnavailable` or no answer.
            let tries = Arc::new(Mutex::new(vec![]));
            let kept = tries.clone();
            let fault = move |rng: &mut StdRng, _: &ReadRecordsRequest| {
                let now = Instant::now();
                let fails = |error| Some(Fault::Fails(error));
                let (fault, failed) = match store {
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
            let model = Arc::new(ModelStore::new(model));
            let runtime = StreamRuntime::new(model.clone(), StoreLimits::default());
            let served =
                (store != Store::NoRuntime).then(|| (stream.store.clone(), Arc::new(runtime)));
            let mut runtimes = StreamRuntimes::new(served);
            runtimes.settings = settings;
            let after = rng.random_range(0..=markers);
            let recorded: usize = datas.iter().map(recorded_bytes).sum();
            runtimes.settings.max_task_record_bytes = rng.random_range(1..=2 * recorded + 1);
            let max_bytes = runtimes.settings.max_task_record_bytes;
            let reader = ReplayReader::new(runtimes);
            let mut reads = reader.read(events.iter(), after).await;
            // The deadline stops each try, and no try starts after it.
            let end = Instant::now();
            let bound = settings.replay_retry_bound;
            let tries = tries.lock().clone();
            if let Some(first) = tries.iter().filter_map(|(_, failed)| *failed).min() {
                let deadline = first + bound;
                assert!(end <= deadline, "seed {seed}: {:?} late", end - deadline);
                let early = |at: &Instant| *at < deadline || (bound.is_zero() && *at == deadline);
                assert!(tries.iter().all(|(at, _)| early(at)), "seed {seed}");
            }
            // The read takes at least one marker, and those that it takes fit the bound unless it
            // takes only one. The next one would pass the bound.
            let needing: Vec<(i64, usize)> = (after + 1..=markers)
                .map(|event_id| (event_id, recorded_bytes(&datas[event_id as usize - 1])))
                .filter(|(_, max)| *max > 0)
                .collect();
            let mut results = HashMap::<i64, MarkerReads>::new();
            for (event_id, _) in &needing {
                let marker = reads.take(*event_id);
                if marker.is_empty() {
                    break;
                }
                results.insert(*event_id, marker);
            }
            let (taken, left) = needing.split_at(results.len());
            let taken_bytes: usize = taken.iter().map(|(_, max)| max).sum();
            assert!(needing.is_empty() || !taken.is_empty(), "seed {seed}");
            assert!(taken.len() <= 1 || taken_bytes <= max_bytes, "seed {seed}");
            let next = left.first();
            assert_eq!(
                reads.unread(),
                next.map(|(event_id, _)| *event_id),
                "seed {seed}"
            );
            let passes = |(_, max): &(i64, usize)| taken_bytes + max > max_bytes;
            assert!(next.is_none_or(passes), "seed {seed}");
            for event_id in (1..=markers).filter(|id| !results.contains_key(id)) {
                assert!(reads.take(event_id).is_empty(), "seed {seed}: {event_id}");
            }
            // A marker holds no more than its recorded bytes.
            for (event_id, max) in taken {
                let records = (results[event_id].iter().flatten()).filter_map(|r| r.as_ref().ok());
                let held: usize = records.flatten().map(|r| field_len(r.encoded_len())).sum();
                assert!(held <= *max, "seed {seed}: {held} of {max}");
            }
            let entries_of = |event_id: &i64| &datas[*event_id as usize - 1].reads;
            let read: Vec<&StreamReadEntry> = (taken.iter())
                .flat_map(|(event_id, _)| entries_of(event_id))
                .filter(|entry| needs_records(entry))
                .collect();
            let largest = (model.records_after(&[]).iter())
                .map(|record| field_len(record.encoded_len()))
                .max()
                .unwrap_or(0);
            // Each entry gets its recorded bytes and one Record more, or, if its Records grew, at
            // most its count of them.
            let answered = model.answered.load(Ordering::Relaxed);
            let bound = match store {
                Store::Larger => read.iter().map(|entry| record_count(entry)).sum::<u64>() as usize,
                _ => read.len(),
            } * largest
                + taken_bytes;
            assert!(answered <= bound, "seed {seed}: {answered} of {bound}");
            stopped += usize::from(next.is_some());
            markers_together += usize::from(taken.len() > 1);
            // Markers of more than 10 small Records each fit the bound together.
            let many = |(event_id, _): &(i64, usize)| {
                let entries = entries_of(event_id).iter();
                entries.map(record_count).sum::<u64>() > 10
            };
            many_together += usize::from(taken.len() > 1 && taken.iter().all(many));
            for (event_id, index, after_offset, given) in expected {
                let result = (results.get_mut(&event_id))
                    .and_then(|marker| marker.get_mut(index))
                    .and_then(Option::take);
                // The Records that the Store of the replay has where the entry read.
                let unchanged = given.as_ref().is_some_and(|given| {
                    let now = model.records_after(&after_offset);
                    now.len() >= given.len() && now[..given.len()] == given[..]
                }) && store != Store::Gone;
                match (given, result) {
                    // An entry with its Records in History, or with none, reads no Store.
                    (None, None) => {}
                    (Some(_), None) => {
                        assert!(!results.contains_key(&event_id), "seed {seed}: {event_id}");
                    }
                    (Some(given), Some(Ok(records))) => {
                        assert!(
                            unchanged && store != Store::NoRuntime,
                            "seed {seed}: {store:?}"
                        );
                        assert_eq!(records, given, "seed {seed}");
                    }
                    (Some(_), Some(Err(ReplayError::Gone(message)))) => {
                        assert!(!unchanged, "seed {seed}: {store:?}");
                        cut_off += usize::from(fits(&message, seed));
                    }
                    (Some(_), Some(Err(ReplayError::Unavailable(message)))) => {
                        let unknown = matches!(store, Store::Outage | Store::Refuses);
                        assert!(unknown || store == Store::NoRuntime, "{seed}");
                        cut_off += usize::from(fits(&message, seed));
                        timed_out += usize::from(store == Store::Outage);
                    }
                    other => panic!("seed {seed}: {store:?}: {other:?}"),
                }
            }
            let (_, peak) = model.model.lock().reading;
            assert!(peak <= settings.replay_concurrency, "seed {seed}: {peak}");
            together += usize::from(peak > 1);
        }
        let reached = [
            together,
            stopped,
            cut_off,
            timed_out,
            markers_together,
            many_together,
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
        let reader = ReplayReader::new(runtimes);
        let mut reads = reader.read([event].iter(), 0).await;
        (given, reads.take(1))
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
