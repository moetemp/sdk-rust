//! The `core_stream` marker of a Workflow Task, which records the results and the close intents
//! of the task's Stream calls.

use super::{
    MAX_MARKER_ONE_RECORD_BYTES, StreamSettings,
    digest::digest,
    manager::{failure_of, recorded},
};
use crate::worker::workflow::WFMachinesError;
use prost::Message;
use serde::Serialize;
use std::{collections::HashMap, sync::LazyLock};
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        AsJsonPayloadExt, FromJsonPayloadExt,
        external_data::{
            StreamAppendResult, StreamCloseIntent, StreamCloseResult, StreamFailure,
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
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};
use temporalio_streams::{
    AnyError, AnyReason,
    proto::{ReadRecordsResponse, StreamErrorReason, StreamRecord, StreamRef},
    record,
};

/// The maximum activation index that Core takes from a marker entry. A replay adds one to the
/// index of an entry for each later activation, and the bound prevents an overflow of that sum.
const MAX_ACTIVATION: u64 = u32::MAX as u64;

/// The key of the marker's details that holds its data. It can't change, because History keeps
/// the markers that earlier releases wrote.
const MARKER_DATA_KEY: &str = "data";

/// The keys of the marker's details that hold the Records of the reads with `records_in_history`.
/// The first has the body of each Record, and the second has the rest of each Record.
const MARKER_RECORDS_KEY: &str = "records";
const MARKER_RECORD_INFO_KEY: &str = "record_info";

/// The length of the JSON of a marker's data with no entry. Each new run and each failed task
/// makes an empty marker, so Core measures it once.
static EMPTY_DATA_BYTES: LazyLock<usize> =
    LazyLock::new(|| json(&StreamMarkerData::default()).data.len());

/// A Stream marker, as Core builds it and as it reads one from History.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StreamMarker {
    pub(crate) data: StreamMarkerData,
    /// The Records that the marker holds for each entry of `data.reads`, by the index of the
    /// entry. Only the page of a read with `records_in_history` has its Records here.
    pub(crate) records: Vec<Vec<StreamRecord>>,
}

/// The entry of a result that waits for an activation, as a marker records it, with the length of
/// its JSON. So the room check of each activation doesn't write JSON.
#[derive(Debug)]
pub(crate) struct Waiting<T> {
    /// The entry, which gets its activation when a marker records it.
    pub(crate) entry: T,
    json_len: usize,
}

impl<T: Serialize> Waiting<T> {
    pub(crate) fn new(entry: T) -> Self {
        let json_len = json(&entry).data.len();
        Self { entry, json_len }
    }

    /// Returns the bytes that the entry adds to the JSON of a marker's data at `activation`, as
    /// the next item of `list`. Each item but the first has a comma before it.
    fn len_at(&self, activation: u64, list: &[T]) -> usize {
        // The entry was measured with activation 0, whose JSON has one digit.
        self.json_len + activation.to_string().len() - 1 + usize::from(!list.is_empty())
    }
}

/// The live result of a read, as its marker entry and the Records of its page.
#[derive(Debug)]
pub(crate) struct ReadDone {
    entry: Waiting<StreamReadResult>,
    records: Vec<StreamRecord>,
    /// The size of `records`, as the page of a read encodes them.
    bytes: usize,
    records_in_history: bool,
}

impl ReadDone {
    /// Converts the result of read `seq` of `stream` into its marker entry. The entry of a page
    /// gets the digest of its Records, unless a marker holds them.
    pub(crate) fn new(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        result: Result<ReadRecordsResponse, StreamFailure>,
        records_in_history: bool,
    ) -> Self {
        let page = match result {
            Ok(page) => page,
            Err(failure) => {
                let result = stream_read_result::Result::Failure(failure);
                return Self::of(stream, seq, from_offset, result, vec![], false);
            }
        };
        // A marker must stay under the blob size limit of the server, whatever Record size the
        // limits of the Store's runtime have.
        let too_large = (page.records.iter().map(stored_len).enumerate())
            .find(|(_, len)| records_in_history && *len > MAX_MARKER_ONE_RECORD_BYTES);
        if let Some((at, len)) = too_large {
            let message = format!(
                "Record {at} of the page has {len} bytes, and a read with records_in_history takes \
                 at most {MAX_MARKER_ONE_RECORD_BYTES}"
            );
            let failure = recorded(&AnyError::new(AnyReason::Unsupported, message));
            let result = stream_read_result::Result::Failure(failure);
            return Self::of(stream, seq, from_offset, result, vec![], false);
        }
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
        Self::of(
            stream,
            seq,
            from_offset,
            result,
            page.records,
            records_in_history,
        )
    }

    /// Returns the result of read `seq` of `stream` that Workflow code cancelled before it got
    /// the job of the read.
    pub(crate) fn cancelled(stream: &StreamRef, seq: u32, from_offset: Vec<u8>) -> Self {
        let result = stream_read_result::Result::Cancelled(());
        Self::of(stream, seq, from_offset, result, vec![], false)
    }

    fn of(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        result: stream_read_result::Result,
        records: Vec<StreamRecord>,
        records_in_history: bool,
    ) -> Self {
        let entry = StreamReadResult {
            seq,
            stream_id: stream.id.clone(),
            store: stream.store.clone(),
            activation_index: 0,
            from_offset,
            result: Some(result),
        };
        Self {
            entry: Waiting::new(entry),
            bytes: record_bytes(&records),
            records,
            records_in_history,
        }
    }
}

/// Returns the size of `record` in its stored form, which is the measure of a Record size limit.
fn stored_len(record: &StreamRecord) -> usize {
    let producer = record.producer.as_ref().map(Message::encoded_len);
    let body = record.body.as_ref().map(Message::encoded_len);
    (producer.into_iter().chain(body))
        .map(record::field_len)
        .sum()
}

/// Returns the size of `records` as the page of a read encodes them, which is the measure of the
/// `max_bytes` of a read.
fn record_bytes(records: &[StreamRecord]) -> usize {
    (records.iter())
        .map(|record| record::field_len(record.encoded_len()))
        .sum()
}

/// Returns the job that gives Workflow code the result of an append in a marker. A replay and a
/// live run both build their jobs from the entry, so both give Workflow code the same job for it.
pub(crate) fn append_job(entry: &StreamAppendResult) -> ResolveStreamAppend {
    let status = entry.result.as_ref().map(|result| match result {
        stream_append_result::Result::Appended(offsets) => {
            resolve_stream_append::Status::Appended(StreamAppended {
                first_offset: offsets.first_offset.clone(),
                last_offset: offsets.last_offset.clone(),
            })
        }
        stream_append_result::Result::Failure(failure) => {
            resolve_stream_append::Status::Failed(failure_of(failure))
        }
    });
    ResolveStreamAppend {
        seq: entry.seq,
        status,
    }
}

/// Returns the job that gives Workflow code the result of a close in a marker.
pub(crate) fn close_job(entry: &StreamCloseResult) -> ResolveStreamClose {
    ResolveStreamClose {
        seq: entry.seq,
        failed: entry.failure.as_ref().map(failure_of),
    }
}

/// Returns the job that gives Workflow code the result of a read in a marker, with the Records
/// of its page.
pub(crate) fn read_job(entry: &StreamReadResult, records: Vec<StreamRecord>) -> ResolveStreamRead {
    let status = entry.result.as_ref().map(|result| match result {
        stream_read_result::Result::Page(page) => {
            let done = page.done;
            resolve_stream_read::Status::Page(StreamPage { records, done })
        }
        stream_read_result::Result::Failure(failure) => {
            resolve_stream_read::Status::Failed(failure_of(failure))
        }
        stream_read_result::Result::Cancelled(()) => resolve_stream_read::Status::Cancelled(()),
    });
    ResolveStreamRead {
        seq: entry.seq,
        status,
    }
}

/// The marker of the current task. Its results of each kind are in the order that Core took them,
/// and its intents are in `seq` order. Each caller stops at the first entry that doesn't fit, so
/// the marker never records an entry while an earlier one waits.
#[derive(Debug)]
pub(crate) struct MarkerBuilder {
    data: StreamMarkerData,
    /// The Records that the marker holds for each entry of `data.reads`.
    held: Vec<Vec<StreamRecord>>,
    /// The length of the JSON of `data`.
    bytes: usize,
    /// The bytes of the Records that the task gave Workflow code, and of those in `held`.
    records: (usize, usize),
    settings: StreamSettings,
}

impl MarkerBuilder {
    /// Creates an empty marker with the sizes of `settings`.
    pub(crate) fn new(settings: StreamSettings) -> Self {
        Self {
            bytes: *EMPTY_DATA_BYTES,
            data: StreamMarkerData::default(),
            held: vec![],
            records: (0, 0),
            settings,
        }
    }

    /// Records the result of an append, given to Workflow code at `activation`. Returns false if
    /// the marker has no room for it.
    pub(crate) fn append_result(
        &mut self,
        done: &Waiting<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        if !self.fits(done.len_at(activation, &self.data.appends)) {
            return false;
        }
        self.data.appends.push(StreamAppendResult {
            activation_index: activation,
            ..done.entry.clone()
        });
        true
    }

    /// Returns true if the marker has room for the result of an append, without recording it.
    pub(crate) fn has_room_for_append(
        &self,
        done: &Waiting<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        self.has_room(done.len_at(activation, &self.data.appends))
    }

    /// Records the intent of close `seq` on `stream`. Returns false if the marker has no room.
    pub(crate) fn close_intent(&mut self, stream: &StreamRef, seq: u32) -> bool {
        let entry = StreamCloseIntent {
            seq,
            stream_id: stream.id.clone(),
            store: stream.store.clone(),
        };
        if !self.fits(item_len(&entry, &self.data.close_intents)) {
            return false;
        }
        self.data.close_intents.push(entry);
        true
    }

    /// Records the result of a close, given at `activation`. Returns false if the marker has no
    /// room for it.
    pub(crate) fn close_result(
        &mut self,
        done: &Waiting<StreamCloseResult>,
        activation: u64,
    ) -> bool {
        if !self.fits(done.len_at(activation, &self.data.closes)) {
            return false;
        }
        self.data.closes.push(StreamCloseResult {
            activation_index: activation,
            ..done.entry.clone()
        });
        true
    }

    /// Returns true if the marker has room for the result of a close, without recording it.
    pub(crate) fn has_room_for_close(
        &self,
        done: &Waiting<StreamCloseResult>,
        activation: u64,
    ) -> bool {
        self.has_room(done.len_at(activation, &self.data.closes))
    }

    /// Records the result of a read, given to Workflow code at `activation`, and returns its job.
    /// The caller checked that the marker and the task have room for it.
    pub(crate) fn read_result(&mut self, done: ReadDone, activation: u64) -> ResolveStreamRead {
        self.bytes += done.entry.len_at(activation, &self.data.reads);
        let entry = StreamReadResult {
            activation_index: activation,
            ..done.entry.entry
        };
        self.records.0 += done.bytes;
        let mut held = vec![];
        if done.records_in_history {
            self.records.1 += done.bytes;
            held.clone_from(&done.records);
        }
        self.held.push(held);
        let job = read_job(&entry, done.records);
        self.data.reads.push(entry);
        job
    }

    /// Returns true if the marker and the task have room for the result of a read, without
    /// recording it. An empty task or marker takes any page. A read asks for at most the bytes
    /// that an empty task or marker holds, and a page over that size has a single Record.
    pub(crate) fn has_room_for_read(&self, done: &ReadDone, activation: u64) -> bool {
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
        self.has_room(done.entry.len_at(activation, &self.data.reads))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data == StreamMarkerData::default()
    }

    /// Returns the marker, if it has an entry, and leaves an empty builder for the next task.
    pub(crate) fn take(&mut self) -> Option<StreamMarker> {
        if self.is_empty() {
            return None;
        }
        let builder = std::mem::replace(self, Self::new(self.settings));
        debug_assert_eq!(builder.bytes, json(&builder.data).data.len());
        Some(StreamMarker {
            data: builder.data,
            records: builder.held,
        })
    }

    /// An empty marker accepts any entry, because each input of an entry has a bound.
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
}

/// Returns `value` as the JSON payload of a marker. The data of a marker has no map, so its JSON
/// can always be written.
fn json(value: &impl Serialize) -> Payload {
    (value.as_json_payload()).expect("The data of a Stream marker has a JSON form")
}

/// Returns the bytes that `entry` adds to the JSON of a marker's data as the next item of `list`.
/// Each item but the first has a comma before it.
fn item_len<T: Serialize>(entry: &T, list: &[T]) -> usize {
    json(entry).data.len() + usize::from(!list.is_empty())
}

/// Returns true if a page of `records` bytes doesn't fit after `current` bytes under `max`. With no
/// bytes taken yet, any page fits, so one Record larger than `max` still goes in.
fn has_no_room(current: usize, records: usize, max: usize) -> bool {
    current > 0 && (current > max || records > max - current)
}

/// Returns the details of the marker command that records `marker`.
pub(crate) fn marker_details(marker: StreamMarker) -> HashMap<String, Payloads> {
    let mut details = HashMap::from([(MARKER_DATA_KEY.to_string(), json(&marker.data).into())]);
    let (mut bodies, mut info) = (vec![], vec![]);
    for mut record in marker.records.into_iter().flatten() {
        // The runtime gives no Record without a body. A marker that lacked one would fail the
        // count check of `decode`.
        bodies.extend(record.body.take());
        info.extend(record.encode_length_delimited_to_vec());
    }
    if !bodies.is_empty() {
        let bodies = Payloads { payloads: bodies };
        details.insert(MARKER_RECORDS_KEY.to_string(), bodies);
        details.insert(MARKER_RECORD_INFO_KEY.to_string(), info.into());
    }
    details
}

/// Returns true if `event` is a `core_stream` marker.
pub(crate) fn is_stream_marker(event: &HistoryEvent) -> bool {
    marker_of(event).is_some()
}

/// Decodes a `core_stream` marker event, and returns `None` for any other event. A marker that
/// doesn't decode, or that has a result which Core can't give Workflow code, is an error.
pub(crate) fn peeked_marker(event: &HistoryEvent) -> Option<Result<StreamMarker, WFMachinesError>> {
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

fn decode(marker: &MarkerRecordedEventAttributes) -> Result<StreamMarker, WFMachinesError> {
    let bad = |why: String| WFMachinesError::Fatal(format!("The Stream marker {why}"));
    let payloads = |key: &str| (marker.details.get(key)).map_or(&[][..], |all| &all.payloads[..]);
    let payload = (payloads(MARKER_DATA_KEY).first()).ok_or_else(|| bad("has no data".into()))?;
    let data = StreamMarkerData::from_json_payload(payload)
        .map_err(|error| bad(format!("doesn't decode: {error}")))?;
    if let Some(append) = data.appends.iter().find(|append| append.result.is_none()) {
        return Err(bad(format!("has no result of append {}", append.seq)));
    }
    if let Some(read) = data.reads.iter().find(|read| read.result.is_none()) {
        return Err(bad(format!("has no result of read {}", read.seq)));
    }
    // Core builds the failure of a call from its reason, and a later version can have a reason
    // that this one doesn't know.
    let failed_appends = (data.appends.iter()).filter_map(|append| match &append.result {
        Some(stream_append_result::Result::Failure(failure)) => Some(failure),
        _ => None,
    });
    let failed_reads = (data.reads.iter()).filter_map(|read| match &read.result {
        Some(stream_read_result::Result::Failure(failure)) => Some(failure),
        _ => None,
    });
    let failed_closes = (data.closes.iter()).filter_map(|close| close.failure.as_ref());
    let mut failures = failed_appends.chain(failed_closes).chain(failed_reads);
    if let Some(failure) =
        failures.find(|failure| StreamErrorReason::from_str_name(&failure.reason).is_none())
    {
        return Err(bad(format!(
            "has a failure with the reason `{}`, which this Worker doesn't know",
            failure.reason
        )));
    }
    // A replay sets its activation index to the activation of an entry, and each later activation
    // of the task adds one to it.
    let activations = (data.appends.iter().map(|append| append.activation_index))
        .chain(data.closes.iter().map(|close| close.activation_index))
        .chain(data.reads.iter().map(|read| read.activation_index));
    if let Some(activation) = (activations.into_iter()).find(|at| *at > MAX_ACTIVATION) {
        return Err(bad(format!(
            "has an entry at activation {activation}, above the maximum of {MAX_ACTIVATION}"
        )));
    }
    let bodies = payloads(MARKER_RECORDS_KEY);
    let info = payloads(MARKER_RECORD_INFO_KEY).first();
    let mut info = info.map_or(&[][..], |payload| &payload.data[..]);
    let mut held = vec![];
    while !info.is_empty() {
        let record = StreamRecord::decode_length_delimited(&mut info)
            .map_err(|error| bad(format!("has Records that don't decode: {error}")))?;
        held.push(record);
    }
    // A page with no digest has its Records in the marker, and takes the next ones of the lists.
    let counts = data.reads.iter().map(|read| match &read.result {
        Some(stream_read_result::Result::Page(page)) if page.digest.is_empty() => page.record_count,
        _ => 0,
    });
    let counted: u64 = counts.clone().map(u64::from).sum();
    if counted != bodies.len() as u64 || bodies.len() != held.len() {
        return Err(bad(format!(
            "counts {counted} Records in its entries, and has the bodies of {} and the rest of {}",
            bodies.len(),
            held.len()
        )));
    }
    let mut held = held.into_iter().zip(bodies).map(|(mut record, body)| {
        record.body = Some(body.clone());
        record
    });
    let records = counts
        .map(|count| held.by_ref().take(count as usize).collect())
        .collect();
    Ok(StreamMarker { data, records })
}

#[cfg(test)]
mod tests {
    use super::super::manager::recorded;
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use temporalio_common::protos::coresdk::external_data::StreamAppendOffsets;
    use temporalio_streams::{
        AppendError, AppendReason, StreamError,
        proto::{
            ClientProducer, LocalActivityProducer, Producer, StandaloneActivityProducer,
            WorkflowActivityProducer, WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
        },
    };

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

    fn append_done(
        stream: &StreamRef,
        seq: u32,
        result: stream_append_result::Result,
    ) -> Waiting<StreamAppendResult> {
        Waiting::new(StreamAppendResult {
            seq,
            stream_id: stream.id.clone(),
            store: stream.store.clone(),
            activation_index: 0,
            result: Some(result),
        })
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    // History keeps the markers of earlier releases, so the JSON of the data, the keys of the
    // details and the bytes of `record_info` can't change.
    #[test]
    fn the_details_of_a_known_marker_never_change() {
        let stream = |id: &str| StreamRef {
            id: id.to_string(),
            store: "redis".to_string(),
        };
        let offset = |at: u64| at.to_be_bytes().to_vec();
        let record = |at: u64, text: &str| StreamRecord {
            producer: Some(Producer {
                kind: Some(Kind::Client(ClientProducer {
                    namespace: "default".to_string(),
                    id: "feed".to_string(),
                })),
            }),
            body: Some(json(&HashMap::from([("text", text)]))),
            offset: offset(at),
        };
        let mut builder = MarkerBuilder::new(StreamSettings::default());
        let appended = stream_append_result::Result::Appended(StreamAppendOffsets {
            first_offset: offset(42),
            last_offset: offset(43),
        });
        assert!(builder.append_result(&append_done(&stream("orders"), 3, appended), 0));
        let closed = AppendError::operation(AppendReason::Closed, "The Stream is closed");
        let failed = stream_append_result::Result::Failure(recorded(&closed));
        assert!(builder.append_result(&append_done(&stream("audit"), 4, failed), 1));
        let close = Waiting::new(StreamCloseResult {
            seq: 1,
            ..Default::default()
        });
        assert!(builder.close_result(&close, 1));
        let prices = ReadRecordsResponse {
            records: vec![record(8, "1.5"), record(9, "1.6"), record(10, "1.4")],
            ..Default::default()
        };
        let prices = ReadDone::new(&stream("prices"), 1, offset(7), Ok(prices), false);
        builder.read_result(prices, 0);
        let replies = vec![record(1, "hello"), record(2, "bye")];
        let page = ReadRecordsResponse {
            records: replies.clone(),
            done: true,
            ..Default::default()
        };
        let in_history = ReadDone::new(&stream("replies"), 2, vec![], Ok(page), true);
        builder.read_result(in_history, 1);
        builder.read_result(ReadDone::cancelled(&stream("prices"), 3, offset(10)), 1);
        assert!(builder.close_intent(&stream("orders"), 2));
        let marker = builder.take().unwrap();

        let details = marker_details(marker.clone());
        let mut keys: Vec<&str> = details.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["data", "record_info", "records"]);
        let encoding = |name: &str| HashMap::from([("encoding".to_string(), name.into())]);
        let [data] = &details["data"].payloads[..] else {
            panic!("The data is one payload");
        };
        assert_eq!(data.metadata, encoding("json/plain"));
        let json = [
            r#"{"close_intents":[{"seq":2,"stream_id":"orders","store":"redis"}],"#,
            r#""appends":[{"seq":3,"stream_id":"orders","store":"redis","activation_index":0,"#,
            r#""result":{"appended":{"first_offset":"000000000000002a","#,
            r#""last_offset":"000000000000002b"}}},"#,
            r#"{"seq":4,"stream_id":"audit","store":"redis","activation_index":1,"#,
            r#""result":{"failure":{"reason":"STREAM_ERROR_REASON_CLOSED","#,
            r#""message":"The Stream is closed"}}}],"#,
            r#""closes":[{"seq":1,"activation_index":1,"failure":null}],"#,
            r#""reads":[{"seq":1,"stream_id":"prices","store":"redis","activation_index":0,"#,
            r#""from_offset":"0000000000000007","result":{"page":{"record_count":3,"done":false,"#,
            r#""digest":"1be96426707f0f4fff8300217b932d4d","record_bytes":219}}},"#,
            r#"{"seq":2,"stream_id":"replies","store":"redis","activation_index":1,"#,
            r#""from_offset":"","result":{"page":{"record_count":2,"done":true,"digest":"","#,
            r#""record_bytes":0}}},"#,
            r#"{"seq":3,"stream_id":"prices","store":"redis","activation_index":1,"#,
            r#""from_offset":"000000000000000a","result":{"cancelled":null}}]}"#,
        ];
        assert_eq!(std::str::from_utf8(&data.data).unwrap(), json.concat());
        let bodies: Vec<Payload> = (replies.iter())
            .map(|record| record.body.clone().unwrap())
            .collect();
        assert_eq!(details["records"].payloads, bodies);
        let [info] = &details["record_info"].payloads[..] else {
            panic!("The rest of the Records is one payload");
        };
        assert_eq!(info.metadata, encoding("binary/plain"));
        // A length, the Producer and the offset of each Record.
        let rest = [
            "1d0a112a0f0a0764656661756c741204666565641a080000000000000001",
            "1d0a112a0f0a0764656661756c741204666565641a080000000000000002",
        ];
        assert_eq!(hex(&info.data), rest.concat());
        assert_eq!(peeked_marker(&event(details)), Some(Ok(marker)));
    }

    // A marker records a failed call by the name of its reason, and a replay reads that name.
    #[test]
    fn the_names_of_the_reasons_never_change() {
        let names: Vec<&str> = (0..)
            .map_while(|number| StreamErrorReason::try_from(number).ok())
            .map(|reason| reason.as_str_name())
            .collect();
        let pinned = [
            "STREAM_ERROR_REASON_UNSPECIFIED",
            "STREAM_ERROR_REASON_NOT_FOUND",
            "STREAM_ERROR_REASON_EXPIRED",
            "STREAM_ERROR_REASON_CLOSED",
            "STREAM_ERROR_REASON_FENCED",
            "STREAM_ERROR_REASON_FORMAT_TOO_NEW",
            "STREAM_ERROR_REASON_OFFSET_TOO_HIGH",
            "STREAM_ERROR_REASON_TOO_LARGE",
            "STREAM_ERROR_REASON_INVALID_ARGUMENT",
            "STREAM_ERROR_REASON_STORE_UNAVAILABLE",
            "STREAM_ERROR_REASON_UNSUPPORTED",
            "STREAM_ERROR_REASON_ALREADY_EXISTS",
            "STREAM_ERROR_REASON_STORE_CORRUPT",
        ];
        assert_eq!(names, pinned);
    }

    /// Returns text of 1 to `max` characters that JSON escapes or has in several bytes, such as a
    /// quote, a control character or a character of another script.
    fn text(rng: &mut StdRng, max: usize) -> String {
        let chars = [
            'a', '"', '\\', '/', '\n', '\0', '\u{7f}', 'é', '€', '𝄞', '\u{2028}',
        ];
        (0..rng.random_range(1..=max))
            .map(|_| chars[rng.random_range(0..chars.len())])
            .collect()
    }

    fn stream(rng: &mut StdRng) -> StreamRef {
        StreamRef {
            id: text(rng, 12),
            store: text(rng, 12),
        }
    }

    /// Returns an offset of 0 to 1024 bytes of any value.
    fn offset(rng: &mut StdRng) -> Vec<u8> {
        let len = match rng.random_range(0..10) {
            0 => 0,
            1 => 1024,
            _ => rng.random_range(1..=16),
        };
        (0..len).map(|_| rng.random()).collect()
    }

    /// Returns the failure of a call, as the manager records it for an error of any reason.
    fn failure(rng: &mut StdRng) -> StreamFailure {
        let reasons: Vec<StreamErrorReason> = (0..)
            .map_while(|number| StreamErrorReason::try_from(number).ok())
            .collect();
        let reason = reasons[rng.random_range(0..reasons.len())];
        let message = match rng.random_bool(0.9) {
            true => text(rng, 40),
            false => text(rng, 5000),
        };
        recorded(&StreamError { reason, message })
    }

    /// Returns a Record as a Store gives it, with a Producer of any kind and a body.
    fn record(rng: &mut StdRng) -> StreamRecord {
        let id = text(rng, 8);
        let kind = match rng.random_range(0..5) {
            0 => Kind::Workflow(WorkflowProducer {
                run_id: id,
                attempt: Some(WorkflowTaskAttempt {
                    started_event_id: rng.random_range(0..i64::MAX),
                    attempt: rng.random_range(1..9),
                    started_time: None,
                }),
                ..Default::default()
            }),
            1 => Kind::LocalActivity(LocalActivityProducer {
                activity_id: id,
                ..Default::default()
            }),
            2 => Kind::WorkflowActivity(WorkflowActivityProducer {
                activity_id: id,
                ..Default::default()
            }),
            3 => Kind::StandaloneActivity(StandaloneActivityProducer {
                activity_id: id,
                ..Default::default()
            }),
            _ => Kind::Client(ClientProducer {
                namespace: "n".to_string(),
                id,
            }),
        };
        let mut body = match rng.random_range(0..3) {
            0 => Payload::default(),
            1 => json(&text(rng, 30)),
            _ => Payload::from(offset(rng)),
        };
        if rng.random_bool(0.3) {
            let reference = Default::default();
            body.external_payloads = vec![reference; rng.random_range(1..3)];
        }
        StreamRecord {
            producer: Some(Producer { kind: Some(kind) }),
            body: Some(body),
            offset: offset(rng),
        }
    }

    /// Returns a marker with random entries of each kind, as the builder of a task makes it.
    fn valid_marker(rng: &mut StdRng) -> StreamMarker {
        let mut builder = MarkerBuilder::new(StreamSettings::default());
        let activation = |rng: &mut StdRng| match rng.random_bool(0.9) {
            true => rng.random_range(0..10),
            false => MAX_ACTIVATION,
        };
        for seq in 1..=rng.random_range(1..3) {
            builder.close_intent(&stream(rng), seq);
        }
        for seq in 1..=rng.random_range(0..4) {
            let result = match rng.random_bool(0.5) {
                true => stream_append_result::Result::Appended(StreamAppendOffsets {
                    first_offset: offset(rng),
                    last_offset: offset(rng),
                }),
                false => stream_append_result::Result::Failure(failure(rng)),
            };
            builder.append_result(&append_done(&stream(rng), seq, result), activation(rng));
        }
        for seq in 1..=rng.random_range(0..3) {
            let close = Waiting::new(StreamCloseResult {
                seq,
                failure: rng.random_bool(0.5).then(|| failure(rng)),
                ..Default::default()
            });
            builder.close_result(&close, activation(rng));
        }
        for seq in 1..=rng.random_range(0..5) {
            let page = ReadRecordsResponse {
                records: (0..rng.random_range(0..4)).map(|_| record(rng)).collect(),
                done: rng.random_bool(0.5),
                ..Default::default()
            };
            let stream = stream(rng);
            let done = match rng.random_range(0..5) {
                0 => ReadDone::new(&stream, seq, offset(rng), Err(failure(rng)), false),
                1 => ReadDone::cancelled(&stream, seq, offset(rng)),
                _ => ReadDone::new(&stream, seq, offset(rng), Ok(page), rng.random_bool(0.6)),
            };
            builder.read_result(done, activation(rng));
        }
        builder.take().expect("The marker has an intent")
    }

    #[test]
    fn a_marker_reads_back_as_it_was_written() {
        let mut with_records = 0;
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let marker = valid_marker(&mut rng);
            let holding = marker.records.iter().filter(|held| !held.is_empty());
            with_records += usize::from(holding.count() > 1);
            let details = marker_details(marker.clone());
            // Core bounds the Records of a marker by their size in the pages of their reads, so
            // the details must take no more bytes for the Records than those pages do.
            let in_pages: usize = marker.records.iter().map(|held| record_bytes(held)).sum();
            let bodies = details.get(MARKER_RECORDS_KEY);
            let info = details.get(MARKER_RECORD_INFO_KEY);
            let in_marker = bodies.map_or(0, Payloads::encoded_len)
                + info.map_or(0, |info| info.payloads[0].data.len());
            assert!(in_marker <= in_pages, "seed {seed}: {in_marker}");
            let read_back = peeked_marker(&event(details));
            assert_eq!(read_back, Some(Ok(marker)), "seed {seed}");
        }
        assert!(with_records > 0, "No marker had the Records of two reads");
    }

    /// Breaks one rule of `marker`. Returns the details of the marker event and the reason that
    /// the decode gives, or the start of it where the rest is the text of the JSON parser.
    fn corrupt(rng: &mut StdRng, mut marker: StreamMarker) -> (HashMap<String, Payloads>, String) {
        let why = match rng.random_range(0..7) {
            0 => return (HashMap::new(), "has no data".to_string()),
            1 => {
                let mut details = marker_details(marker);
                let payload = &mut details.get_mut(MARKER_DATA_KEY).unwrap().payloads[0];
                let text = String::from_utf8(payload.data.clone()).unwrap();
                // One more character makes the hex of an offset odd, and a `g` isn't hex.
                let has_offset = "\"first_offset\":\"";
                match rng.random_range(0..3) {
                    0 => payload.metadata.clear(),
                    1 if text.contains(has_offset) => {
                        let not_hex = ["\"first_offset\":\"0", "\"first_offset\":\"0g"];
                        let not_hex = not_hex[rng.random_range(0..2)];
                        payload.data = text.replacen(has_offset, not_hex, 1).into_bytes();
                    }
                    _ => payload.data.truncate(text.len() - 1),
                }
                return (details, "doesn't decode: ".to_string());
            }
            2 => {
                let read = rng.random_bool(0.5);
                match read {
                    true => marker.data.reads.push(StreamReadResult {
                        seq: 99,
                        ..Default::default()
                    }),
                    false => marker.data.appends.push(StreamAppendResult {
                        seq: 99,
                        ..Default::default()
                    }),
                }
                let kind = if read { "read" } else { "append" };
                format!("has no result of {kind} 99")
            }
            3 => {
                let activation = rng.random_range(MAX_ACTIVATION + 1..=u64::MAX);
                match rng.random_range(0..3) {
                    0 => marker.data.appends.push(StreamAppendResult {
                        seq: 99,
                        activation_index: activation,
                        result: Some(stream_append_result::Result::Appended(Default::default())),
                        ..Default::default()
                    }),
                    1 => marker.data.closes.push(StreamCloseResult {
                        seq: 99,
                        activation_index: activation,
                        failure: None,
                    }),
                    _ => marker.data.reads.push(StreamReadResult {
                        seq: 99,
                        activation_index: activation,
                        result: Some(stream_read_result::Result::Cancelled(())),
                        ..Default::default()
                    }),
                }
                format!("has an entry at activation {activation}, above the maximum of 4294967295")
            }
            4 => {
                let later = StreamFailure {
                    reason: "STREAM_ERROR_REASON_LATER".to_string(),
                    message: "A later version of Core has this reason".to_string(),
                };
                match rng.random_range(0..3) {
                    0 => marker.data.appends.push(StreamAppendResult {
                        seq: 99,
                        result: Some(stream_append_result::Result::Failure(later)),
                        ..Default::default()
                    }),
                    1 => marker.data.closes.push(StreamCloseResult {
                        seq: 99,
                        failure: Some(later),
                        ..Default::default()
                    }),
                    _ => marker.data.reads.push(StreamReadResult {
                        seq: 99,
                        result: Some(stream_read_result::Result::Failure(later)),
                        ..Default::default()
                    }),
                }
                "has a failure with the reason `STREAM_ERROR_REASON_LATER`, which this Worker \
                 doesn't know"
                    .to_string()
            }
            5 => {
                // The entries count one Record more than the marker has, or one fewer, or the
                // marker lacks the body of one.
                let held: usize = marker.records.iter().map(Vec::len).sum();
                let (counted, bodies, rest) = match rng.random_range(0..3) {
                    0 => {
                        let page = StreamReadPage {
                            record_count: 1,
                            ..Default::default()
                        };
                        marker.data.reads.push(StreamReadResult {
                            seq: 99,
                            result: Some(stream_read_result::Result::Page(page)),
                            ..Default::default()
                        });
                        (held + 1, held, held)
                    }
                    1 if held > 0 => (held, held - 1, held),
                    _ => {
                        marker.records.push(vec![record(rng)]);
                        (held, held + 1, held + 1)
                    }
                };
                let mut details = marker_details(marker);
                if bodies < rest {
                    details.get_mut(MARKER_RECORDS_KEY).unwrap().payloads.pop();
                }
                let why = format!(
                    "counts {counted} Records in its entries, and has the bodies of {bodies} and \
                     the rest of {rest}"
                );
                return (details, why);
            }
            _ => {
                let mut details = marker_details(marker);
                let info = details.entry(MARKER_RECORD_INFO_KEY.to_string());
                let payload = &mut info.or_insert_with(|| vec![].into()).payloads[0];
                payload.data.push(0xff);
                let why = "has Records that don't decode: failed to decode Protobuf message: \
                           invalid varint";
                return (details, why.to_string());
            }
        };
        (marker_details(marker), why)
    }

    #[test]
    fn a_bad_marker_is_fatal() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let valid = valid_marker(&mut rng);
            let (details, why) = corrupt(&mut rng, valid);
            let Some(Err(WFMachinesError::Fatal(error))) = peeked_marker(&event(details)) else {
                panic!("seed {seed}: the marker decoded");
            };
            let expected = format!("The Stream marker {why}");
            match why.ends_with(": ") {
                true => assert!(error.starts_with(&expected), "seed {seed}: {error}"),
                false => assert_eq!(error, expected, "seed {seed}"),
            }
        }
    }
}
