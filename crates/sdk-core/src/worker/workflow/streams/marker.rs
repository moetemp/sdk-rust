//! The `core_stream` marker of a Workflow Task, which records the results and the close intents
//! of the task's Stream calls.

use super::{
    StreamSettings,
    digest::{DIGEST_LEN, digest},
};
use crate::worker::workflow::WFMachinesError;
use prost::{
    Message,
    encoding::{encoded_len_varint, key_len, message::encoded_len},
};
use std::collections::HashMap;
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        external_data::{
            StreamAppendOffsets, StreamAppendResult, StreamCloseIntent, StreamCloseResult,
            StreamMarkerData, StreamReadPage, StreamReadResult, stream_append_result,
            stream_read_result,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, ResolveStreamRead, StreamAppended, StreamPage,
            resolve_stream_append, resolve_stream_read,
        },
    },
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::Failure,
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};
use temporalio_streams::{
    proto::{ReadRecordsResponse, StreamRecord, StreamRef},
    record,
};

/// The maximum activation index that Core takes from a marker entry. A replay adds one to the
/// index of an entry for each later activation, and the bound keeps that sum from an overflow.
const MAX_ACTIVATION: u64 = u32::MAX as u64;

/// The key of the marker's details that holds its data. History has markers with this key, so it
/// can't change.
const MARKER_DATA_KEY: &str = "data";

/// The field numbers of the entries in [`StreamMarkerData`].
const STREAMS: u32 = 1;
const CLOSE_INTENTS: u32 = 2;
const APPENDS: u32 = 3;
const CLOSES: u32 = 4;
const READS: u32 = 5;

/// A live result in the form that a marker records it. `len` is its encoded size before the
/// marker gives it an activation and a Stream index, so a size check doesn't have to encode it.
#[derive(Debug)]
pub(crate) struct Done<T> {
    entry: T,
    len: usize,
}

impl Done<StreamAppendResult> {
    /// Converts the result of append `seq` into its marker entry.
    pub(crate) fn append(seq: u32, status: resolve_stream_append::Status) -> Self {
        let result = match status {
            resolve_stream_append::Status::Appended(appended) => {
                stream_append_result::Result::Appended(StreamAppendOffsets {
                    first_offset: appended.first_offset,
                    last_offset: appended.last_offset,
                })
            }
            resolve_stream_append::Status::Failed(failure) => {
                stream_append_result::Result::Failure(failure)
            }
        };
        let entry = StreamAppendResult {
            seq,
            result: Some(result),
            ..Default::default()
        };
        let len = entry.encoded_len();
        Self { entry, len }
    }

    /// Returns the job that gives Workflow code this result.
    pub(crate) fn job(&self) -> ResolveStreamAppend {
        ResolveStreamAppend {
            seq: self.entry.seq,
            status: self.entry.result.clone().map(status_of),
        }
    }
}

impl Done<StreamCloseResult> {
    /// Converts the result of close `seq` into its marker entry.
    pub(crate) fn close(seq: u32, failure: Option<Failure>) -> Self {
        let entry = StreamCloseResult {
            seq,
            failure,
            ..Default::default()
        };
        let len = entry.encoded_len();
        Self { entry, len }
    }

    /// Returns the job that gives Workflow code this result.
    pub(crate) fn job(&self) -> ResolveStreamClose {
        ResolveStreamClose {
            seq: self.entry.seq,
            failed: self.entry.failure.clone(),
        }
    }
}

/// The live result of a read, as its marker entry and the Records of its page.
#[derive(Debug)]
pub(crate) struct ReadDone {
    /// The entry, whose page has no Records until a marker records them.
    entry: StreamReadResult,
    /// The encoded size of `entry`, before the marker gives it an activation and a Stream index.
    len: usize,
    records: Vec<StreamRecord>,
    /// The size of `records`, as the page of a read encodes them.
    bytes: usize,
    records_in_history: bool,
}

impl ReadDone {
    /// Converts the result of read `seq` into its marker entry. The entry of a page gets the
    /// digest of its Records, unless a marker holds them.
    pub(crate) fn new(
        seq: u32,
        from_offset: Vec<u8>,
        result: Result<ReadRecordsResponse, Failure>,
        records_in_history: bool,
    ) -> Self {
        let page = match result {
            Ok(page) => page,
            Err(failure) => {
                let result = stream_read_result::Result::Failure(failure);
                return Self::of(seq, from_offset, result, vec![], false);
            }
        };
        let mut entry = StreamReadPage {
            record_count: page.records.len() as u32,
            done: page.done,
            ..Default::default()
        };
        if !records_in_history && !page.records.is_empty() {
            entry.digest = digest(&page.records);
            entry.record_bytes = record_bytes(&page.records) as u64;
        }
        let result = stream_read_result::Result::Page(entry);
        Self::of(seq, from_offset, result, page.records, records_in_history)
    }

    /// Returns the result of read `seq` that Workflow code cancelled before it had one.
    pub(crate) fn cancelled(seq: u32, from_offset: Vec<u8>) -> Self {
        let result = stream_read_result::Result::Cancelled(());
        Self::of(seq, from_offset, result, vec![], false)
    }

    fn of(
        seq: u32,
        from_offset: Vec<u8>,
        result: stream_read_result::Result,
        records: Vec<StreamRecord>,
        records_in_history: bool,
    ) -> Self {
        let entry = StreamReadResult {
            seq,
            from_offset,
            result: Some(result),
            ..Default::default()
        };
        Self {
            len: entry.encoded_len(),
            entry,
            bytes: record_bytes(&records),
            records,
            records_in_history,
        }
    }
}

/// Returns the size of `records` as the page of a read encodes them, which is the measure of the
/// `max_bytes` of a read.
fn record_bytes(records: &[StreamRecord]) -> usize {
    (records.iter())
        .map(|record| record::field_len(record.encoded_len()))
        .sum()
}

/// Returns the job of a read result in a marker, with the Records of its page. A replay and a live
/// run both build their jobs with it, so both give Workflow code the same job for the same entry.
pub(crate) fn read_job(
    seq: u32,
    result: Option<stream_read_result::Result>,
    records: Vec<StreamRecord>,
) -> ResolveStreamRead {
    let status = result.map(|result| match result {
        stream_read_result::Result::Page(page) => {
            let done = page.done;
            resolve_stream_read::Status::Page(StreamPage { records, done })
        }
        stream_read_result::Result::Failure(failure) => {
            resolve_stream_read::Status::Failed(failure)
        }
        stream_read_result::Result::Cancelled(()) => resolve_stream_read::Status::Cancelled(()),
    });
    ResolveStreamRead { seq, status }
}

/// Returns the job status of an append result in a marker. A replay and a live run both build
/// their jobs with it, so both give Workflow code the same job for the same entry.
pub(crate) fn status_of(result: stream_append_result::Result) -> resolve_stream_append::Status {
    match result {
        stream_append_result::Result::Appended(offsets) => {
            resolve_stream_append::Status::Appended(StreamAppended {
                first_offset: offsets.first_offset,
                last_offset: offsets.last_offset,
            })
        }
        stream_append_result::Result::Failure(failure) => {
            resolve_stream_append::Status::Failed(failure)
        }
    }
}

/// The marker of the task in hand. Its results of each kind are in the order that Core took them,
/// and its intents are in the order of their seqs. Each caller stops at the first entry that
/// doesn't fit, so the marker never records an entry while an earlier one waits.
#[derive(Debug)]
pub(crate) struct MarkerBuilder {
    data: StreamMarkerData,
    streams: HashMap<(String, String), u32>,
    /// The encoded size of `data`, apart from the Records of its reads.
    bytes: usize,
    /// The bytes of the Records that the task gave Workflow code, and of those that `data` holds.
    records: (usize, usize),
    settings: StreamSettings,
}

impl MarkerBuilder {
    /// Creates an empty marker with the sizes of `settings`.
    pub(crate) fn new(settings: StreamSettings) -> Self {
        Self {
            data: StreamMarkerData::default(),
            streams: HashMap::new(),
            bytes: 0,
            records: (0, 0),
            settings,
        }
    }

    /// Records the result of an append on `stream`, given to Workflow code at `activation`.
    /// Returns false if the marker has no room for it.
    pub(crate) fn append_result(
        &mut self,
        stream: &StreamRef,
        done: &Done<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        if !self.fits(self.append_len(stream, done, activation)) {
            return false;
        }
        let entry = StreamAppendResult {
            stream: self.index_of(stream),
            activation,
            ..done.entry.clone()
        };
        self.note_stream(stream);
        self.data.appends.push(entry);
        true
    }

    /// Returns true if the marker has room for the result of an append, without recording it.
    pub(crate) fn has_room_for_append(
        &self,
        stream: &StreamRef,
        done: &Done<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        self.has_room(self.append_len(stream, done, activation))
    }

    /// Records the intent of close `seq` on `stream`. Returns false if the marker has no room.
    pub(crate) fn close_intent(&mut self, stream: &StreamRef, seq: u32) -> bool {
        let entry = StreamCloseIntent {
            seq,
            stream: self.index_of(stream),
        };
        if !self.fits(encoded_len(CLOSE_INTENTS, &entry) + self.table_len(stream)) {
            return false;
        }
        self.note_stream(stream);
        self.data.close_intents.push(entry);
        true
    }

    /// Records the result of a close, given at `activation`. Returns false if the marker has no
    /// room for it.
    pub(crate) fn close_result(&mut self, done: &Done<StreamCloseResult>, activation: u64) -> bool {
        if !self.fits(close_len(done, activation)) {
            return false;
        }
        let entry = StreamCloseResult {
            activation,
            ..done.entry.clone()
        };
        self.data.closes.push(entry);
        true
    }

    /// Returns true if the marker has room for the result of a close, without recording it.
    pub(crate) fn has_room_for_close(
        &self,
        done: &Done<StreamCloseResult>,
        activation: u64,
    ) -> bool {
        self.has_room(close_len(done, activation))
    }

    /// Records the result of a read on `stream`, given to Workflow code at `activation`, and
    /// returns its job. The caller checked that the marker and the task have room for it.
    pub(crate) fn read_result(
        &mut self,
        stream: &StreamRef,
        done: ReadDone,
        activation: u64,
    ) -> ResolveStreamRead {
        self.bytes += self.read_len(stream, &done, activation);
        let ReadDone {
            mut entry,
            records,
            bytes,
            records_in_history,
            ..
        } = done;
        entry.stream = self.index_of(stream);
        entry.activation = activation;
        self.note_stream(stream);
        let job = entry.result.clone();
        self.records.0 += bytes;
        if records_in_history
            && let Some(stream_read_result::Result::Page(page)) = &mut entry.result
        {
            page.records.clone_from(&records);
            self.records.1 += bytes;
        }
        let seq = entry.seq;
        self.data.reads.push(entry);
        read_job(seq, job, records)
    }

    /// Returns true if the marker and the task have room for the result of a read, without
    /// recording it. An empty task or marker takes any page, since a read asks for at most what
    /// an empty one holds, and a larger page is one Record.
    pub(crate) fn has_room_for_read(
        &self,
        stream: &StreamRef,
        done: &ReadDone,
        activation: u64,
    ) -> bool {
        let (task, marker) = self.records;
        let max_task_record_bytes = self.settings.max_task_record_bytes();
        let max_marker_record_bytes = self.settings.max_marker_record_bytes();
        if done.bytes > 0
            && (has_no_room(task, done.bytes, max_task_record_bytes)
                || (done.records_in_history
                    && has_no_room(marker, done.bytes, max_marker_record_bytes)))
        {
            return false;
        }
        self.has_room(self.read_len(stream, done, activation))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    /// Returns the marker, if it has an entry, and leaves an empty builder for the next task.
    pub(crate) fn take(&mut self) -> Option<StreamMarkerData> {
        let builder = std::mem::replace(self, Self::new(self.settings));
        debug_assert_eq!(builder.bytes, {
            let mut bare = builder.data.clone();
            for read in &mut bare.reads {
                if let Some(stream_read_result::Result::Page(page)) = &mut read.result {
                    page.records.clear();
                }
            }
            bare.encoded_len()
        });
        (!builder.is_empty()).then_some(builder.data)
    }

    /// An empty marker takes any entry, since each input of an entry has a bound.
    fn has_room(&self, len: usize) -> bool {
        self.is_empty() || self.bytes + len <= self.settings.max_marker_entry_bytes()
    }

    fn fits(&mut self, len: usize) -> bool {
        if !self.has_room(len) {
            return false;
        }
        self.bytes += len;
        true
    }

    fn append_len(
        &self,
        stream: &StreamRef,
        done: &Done<StreamAppendResult>,
        activation: u64,
    ) -> usize {
        let placed = StreamAppendResult {
            stream: self.index_of(stream),
            activation,
            ..Default::default()
        };
        field_len(APPENDS, done.len + placed.encoded_len()) + self.table_len(stream)
    }

    fn read_len(&self, stream: &StreamRef, done: &ReadDone, activation: u64) -> usize {
        let placed = StreamReadResult {
            stream: self.index_of(stream),
            activation,
            ..Default::default()
        };
        field_len(READS, done.len + placed.encoded_len()) + self.table_len(stream)
    }

    fn index_of(&self, stream: &StreamRef) -> u32 {
        match self.streams.get(&key(stream)) {
            Some(index) => *index,
            None => self.data.streams.len() as u32,
        }
    }

    fn table_len(&self, stream: &StreamRef) -> usize {
        if self.streams.contains_key(&key(stream)) {
            0
        } else {
            encoded_len(STREAMS, stream)
        }
    }

    fn note_stream(&mut self, stream: &StreamRef) {
        if let std::collections::hash_map::Entry::Vacant(vacant) = self.streams.entry(key(stream)) {
            vacant.insert(self.data.streams.len() as u32);
            self.data.streams.push(stream.clone());
        }
    }
}

fn close_len(done: &Done<StreamCloseResult>, activation: u64) -> usize {
    let placed = StreamCloseResult {
        activation,
        ..Default::default()
    };
    field_len(CLOSES, done.len + placed.encoded_len())
}

/// Returns true if a page of `records` bytes doesn't fit after `current` bytes under `max`. With no
/// bytes taken yet, any page fits, so one Record larger than `max` still goes in.
fn has_no_room(current: usize, records: usize, max: usize) -> bool {
    current > 0 && (current > max || records > max - current)
}

/// Returns the encoded size of a message field whose message has `len` bytes. An absent field and
/// a field with a default value encode to nothing, so the sizes of disjoint fields add up.
fn field_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

fn key(stream: &StreamRef) -> (String, String) {
    (stream.store.clone(), stream.id.clone())
}

/// Returns the details of the marker command that records `data`.
pub(crate) fn marker_details(data: &StreamMarkerData) -> HashMap<String, Payloads> {
    let payload = Payload {
        metadata: HashMap::from([
            ("encoding".to_string(), b"binary/protobuf".to_vec()),
            (
                "messageType".to_string(),
                b"coresdk.external_data.StreamMarkerData".to_vec(),
            ),
        ]),
        data: data.encode_to_vec(),
        ..Default::default()
    };
    HashMap::from([(
        MARKER_DATA_KEY.to_string(),
        Payloads {
            payloads: vec![payload],
        },
    )])
}

/// Returns true if `event` is a `core_stream` marker.
pub(crate) fn is_stream_marker(event: &HistoryEvent) -> bool {
    marker_of(event).is_some()
}

/// Decodes a `core_stream` marker event, and returns `None` for any other event. A marker that no
/// Core could have written is an error.
pub(crate) fn peeked_marker(
    event: &HistoryEvent,
) -> Option<Result<StreamMarkerData, WFMachinesError>> {
    marker_of(event).map(decode)
}

fn marker_of(event: &HistoryEvent) -> Option<&MarkerRecordedEventAttributes> {
    match event.attributes.as_ref()? {
        history_event::Attributes::MarkerRecordedEventAttributes(marker)
            if marker.marker_name == STREAM_MARKER_NAME =>
        {
            Some(marker)
        }
        _ => None,
    }
}

fn decode(marker: &MarkerRecordedEventAttributes) -> Result<StreamMarkerData, WFMachinesError> {
    let bad = |why: String| WFMachinesError::Fatal(format!("The Stream marker {why}"));
    let payload = marker
        .details
        .get(MARKER_DATA_KEY)
        .and_then(|payloads| payloads.payloads.first())
        .ok_or_else(|| bad("has no data".to_string()))?;
    let data = StreamMarkerData::decode(payload.data.as_slice())
        .map_err(|error| bad(format!("doesn't decode: {error}")))?;
    let streams = data.streams.len();
    let indexes = (data.close_intents.iter().map(|intent| intent.stream))
        .chain(data.appends.iter().map(|append| append.stream))
        .chain(data.reads.iter().map(|read| read.stream));
    if let Some(index) = indexes.into_iter().find(|index| *index as usize >= streams) {
        return Err(bad(format!(
            "has an entry of Stream {index}, and its table has {streams}"
        )));
    }
    if let Some(append) = data.appends.iter().find(|append| append.result.is_none()) {
        return Err(bad(format!("has no result of append {}", append.seq)));
    }
    // A replay sets its activation index to the activation of an entry, and each later activation
    // of the task adds one to it.
    let activations = (data.appends.iter().map(|append| append.activation))
        .chain(data.closes.iter().map(|close| close.activation))
        .chain(data.reads.iter().map(|read| read.activation));
    if let Some(activation) = (activations.into_iter()).find(|at| *at > MAX_ACTIVATION) {
        return Err(bad(format!(
            "has an entry at activation {activation}, above the maximum of {MAX_ACTIVATION}"
        )));
    }
    for read in &data.reads {
        let seq = read.seq;
        let page = match &read.result {
            Some(stream_read_result::Result::Page(page)) => page,
            Some(_) => continue,
            None => return Err(bad(format!("has no result of read {seq}"))),
        };
        // A page has its Records, or a digest of them with their bytes. A page with no Record has
        // neither.
        let (count, records) = (page.record_count as usize, page.records.len());
        let (digest, bytes) = (page.digest.len(), page.record_bytes);
        let shape = if records > 0 && records != count {
            Some(format!(
                "counts {count} Records in the page of read {seq}, and has {records}"
            ))
        } else if count == 0 && digest > 0 {
            Some(format!(
                "has a digest of read {seq}, whose page has no Record"
            ))
        } else if count > 0 && records == 0 && digest != DIGEST_LEN {
            Some(format!(
                "has a digest of {digest} bytes for the page of read {seq}, and no Records"
            ))
        } else if records > 0 && digest > 0 {
            Some(format!(
                "has both the Records of read {seq} and their digest"
            ))
        } else if digest > 0 && bytes == 0 {
            Some(format!(
                "has a digest of read {seq}, and no bytes of its Records"
            ))
        } else if digest == 0 && bytes > 0 {
            Some(format!(
                "has bytes of the Records of read {seq}, and no digest"
            ))
        } else {
            None
        };
        if let Some(why) = shape {
            return Err(bad(why));
        }
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    fn event(details: HashMap<String, Payloads>) -> HistoryEvent {
        HistoryEvent {
            attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                MarkerRecordedEventAttributes {
                    marker_name: STREAM_MARKER_NAME.to_string(),
                    details,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    /// Returns a marker that Core could have written, with random entries of each kind.
    fn valid_marker(rng: &mut StdRng) -> StreamMarkerData {
        let streams: Vec<StreamRef> = (0..rng.random_range(1..=4))
            .map(|index| StreamRef {
                id: format!("s{index}"),
                store: "mem".to_string(),
            })
            .collect();
        let count = streams.len();
        let index = |rng: &mut StdRng| rng.random_range(0..count) as u32;
        let activation = |rng: &mut StdRng| match rng.random_bool(0.9) {
            true => rng.random_range(0..10),
            false => MAX_ACTIVATION,
        };
        let close_intents = (1..=rng.random_range(0..3))
            .map(|seq| StreamCloseIntent {
                seq,
                stream: index(rng),
            })
            .collect();
        let appends = (1..=rng.random_range(0..4))
            .map(|seq| StreamAppendResult {
                seq,
                stream: index(rng),
                activation: activation(rng),
                result: Some(match rng.random_bool(0.5) {
                    true => stream_append_result::Result::Appended(StreamAppendOffsets {
                        first_offset: vec![1],
                        last_offset: vec![2],
                    }),
                    false => stream_append_result::Result::Failure(Failure::default()),
                }),
            })
            .collect();
        let closes = (1..=rng.random_range(0..3))
            .map(|seq| StreamCloseResult {
                seq,
                activation: activation(rng),
                failure: rng.random_bool(0.5).then(Failure::default),
            })
            .collect();
        let reads = (1..=rng.random_range(0..3))
            .map(|seq| {
                // A page has its Records or a digest of them with their bytes, and a page with no
                // Record has neither.
                let count = rng.random_range(0..3);
                let in_history = rng.random_bool(0.5);
                let digest = !in_history && count > 0;
                let page = StreamReadPage {
                    record_count: count as u32,
                    done: rng.random_bool(0.5),
                    records: match in_history {
                        true => vec![Default::default(); count],
                        false => vec![],
                    },
                    digest: match digest {
                        true => vec![7; DIGEST_LEN],
                        false => vec![],
                    },
                    record_bytes: match digest {
                        true => rng.random_range(1..=4 << 20),
                        false => 0,
                    },
                };
                let result = match rng.random_range(0..4) {
                    0 => stream_read_result::Result::Failure(Failure::default()),
                    1 => stream_read_result::Result::Cancelled(()),
                    _ => stream_read_result::Result::Page(page),
                };
                StreamReadResult {
                    seq,
                    stream: index(rng),
                    activation: activation(rng),
                    from_offset: vec![seq as u8],
                    result: Some(result),
                }
            })
            .collect();
        StreamMarkerData {
            streams,
            close_intents,
            appends,
            closes,
            reads,
        }
    }

    /// Breaks one rule of `marker`. Returns the details of the marker event and the reason that
    /// the decode gives.
    fn corrupt(
        rng: &mut StdRng,
        mut marker: StreamMarkerData,
    ) -> (HashMap<String, Payloads>, String) {
        let streams = marker.streams.len() as u32;
        // Adds read 99 with `page`, which breaks a rule of a page.
        let mut add = |page: StreamReadPage| {
            marker.reads.push(StreamReadResult {
                seq: 99,
                result: Some(stream_read_result::Result::Page(page)),
                ..Default::default()
            });
        };
        let digest = vec![7; DIGEST_LEN];
        let why = match rng.random_range(0..11) {
            0 => return (HashMap::new(), "has no data".to_string()),
            1 => {
                let garbage = Payloads {
                    payloads: vec![Payload {
                        data: vec![0xff],
                        ..Default::default()
                    }],
                };
                let details = HashMap::from([(MARKER_DATA_KEY.to_string(), garbage)]);
                let why = "doesn't decode: failed to decode Protobuf message: invalid varint";
                return (details, why.to_string());
            }
            2 => {
                let index = streams + rng.random_range(0..3);
                match rng.random_range(0..3) {
                    0 if !marker.appends.is_empty() => marker.appends[0].stream = index,
                    1 if !marker.reads.is_empty() => marker.reads[0].stream = index,
                    _ => marker.close_intents.push(StreamCloseIntent {
                        seq: 99,
                        stream: index,
                    }),
                }
                format!("has an entry of Stream {index}, and its table has {streams}")
            }
            3 => {
                let read = rng.random_bool(0.5);
                match read {
                    true => marker.reads.push(StreamReadResult {
                        seq: 99,
                        ..Default::default()
                    }),
                    false => marker.appends.push(StreamAppendResult {
                        seq: 99,
                        ..Default::default()
                    }),
                }
                let kind = if read { "read" } else { "append" };
                format!("has no result of {kind} 99")
            }
            4 => {
                let activation = rng.random_range(MAX_ACTIVATION + 1..=u64::MAX);
                match rng.random_range(0..3) {
                    0 => marker.appends.push(StreamAppendResult {
                        seq: 99,
                        activation,
                        result: Some(stream_append_result::Result::Failure(Failure::default())),
                        ..Default::default()
                    }),
                    1 => marker.closes.push(StreamCloseResult {
                        seq: 99,
                        activation,
                        failure: None,
                    }),
                    _ => marker.reads.push(StreamReadResult {
                        seq: 99,
                        activation,
                        result: Some(stream_read_result::Result::Cancelled(())),
                        ..Default::default()
                    }),
                }
                format!("has an entry at activation {activation}, above the maximum of 4294967295")
            }
            5 => {
                add(StreamReadPage {
                    record_count: 2,
                    records: vec![Default::default()],
                    ..Default::default()
                });
                "counts 2 Records in the page of read 99, and has 1".to_string()
            }
            6 => {
                add(StreamReadPage {
                    digest,
                    ..Default::default()
                });
                "has a digest of read 99, whose page has no Record".to_string()
            }
            7 => {
                let digest = [0, 1, DIGEST_LEN - 1, DIGEST_LEN + 1, 32][rng.random_range(0..5)];
                add(StreamReadPage {
                    record_count: 1,
                    digest: vec![7; digest],
                    ..Default::default()
                });
                format!("has a digest of {digest} bytes for the page of read 99, and no Records")
            }
            8 => {
                add(StreamReadPage {
                    record_count: 1,
                    records: vec![Default::default()],
                    digest,
                    ..Default::default()
                });
                "has both the Records of read 99 and their digest".to_string()
            }
            9 => {
                add(StreamReadPage {
                    record_count: 1,
                    digest,
                    ..Default::default()
                });
                "has a digest of read 99, and no bytes of its Records".to_string()
            }
            _ => {
                let records = rng.random_range(0..2);
                add(StreamReadPage {
                    record_count: records as u32,
                    records: vec![Default::default(); records],
                    record_bytes: rng.random_range(1..=1000),
                    ..Default::default()
                });
                "has bytes of the Records of read 99, and no digest".to_string()
            }
        };
        (marker_details(&marker), why)
    }

    #[test]
    fn a_bad_marker_is_fatal() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let valid = valid_marker(&mut rng);
            let decoded = peeked_marker(&event(marker_details(&valid)));
            assert_eq!(decoded, Some(Ok(valid.clone())), "seed {seed}");
            let (details, why) = corrupt(&mut rng, valid);
            let error = WFMachinesError::Fatal(format!("The Stream marker {why}"));
            assert_eq!(
                peeked_marker(&event(details)),
                Some(Err(error)),
                "seed {seed}"
            );
        }
    }
}
