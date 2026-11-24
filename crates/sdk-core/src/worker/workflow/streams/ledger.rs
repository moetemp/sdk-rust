//! The ledger of a Workflow Task, which has the results and the close intents of the task's Stream
//! calls. Core puts it on the completion of the task, and History keeps it in the
//! `WorkflowTaskCompleted` event.

use super::{StreamSettings, digest::digest, manager::failure_of};
use crate::worker::workflow::WFMachinesError;
use prost::{Message, encoding::encoded_len_varint};
use temporalio_common::protos::{
    coresdk::{
        external_data::{
            StreamAppendResult, StreamCloseIntent, StreamCloseResult, StreamFailure, StreamLedger,
            StreamReadPage, StreamReadResult, stream_append_result, stream_read_result,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, ResolveStreamRead, StreamAppended, StreamPage,
            resolve_stream_append, resolve_stream_read,
        },
    },
    temporal::api::history::v1::{HistoryEvent, history_event},
};
use temporalio_streams::{
    proto::{ReadRecordsResponse, StreamErrorReason, StreamRecord, StreamRef},
    record::field_len,
};

/// The maximum activation index that Core takes from a ledger entry. A replay adds one to the
/// index of an entry for each later activation, and the bound prevents an overflow of that sum.
const MAX_ACTIVATION: u64 = u32::MAX as u64;

/// The entry of a result that waits for an activation, as a ledger records it, with its encoded
/// size.
#[derive(Debug)]
pub(crate) struct Waiting<T> {
    /// The entry, which gets its activation when a ledger records it.
    pub(crate) entry: T,
    len: usize,
}

impl<T: Message> Waiting<T> {
    pub(crate) fn new(entry: T) -> Self {
        let len = entry.encoded_len();
        Self { entry, len }
    }

    /// Returns the bytes that the entry adds to a ledger at `activation`.
    fn len_at(&self, activation: u64) -> usize {
        // The entry was measured with activation 0, which the encoding leaves out. Another
        // activation adds its one-byte tag and its varint.
        let activation_len = match activation {
            0 => 0,
            _ => 1 + encoded_len_varint(activation),
        };
        field_len(self.len + activation_len)
    }
}

/// The live result of a read, as its ledger entry and the Records of its page.
#[derive(Debug)]
pub(crate) struct ReadDone {
    entry: Waiting<StreamReadResult>,
    records: Vec<StreamRecord>,
    /// The size of `records`, as the page of a read encodes them.
    bytes: usize,
}

impl ReadDone {
    /// Converts the result of read `seq` of `stream` into its ledger entry. The entry of a page
    /// gets the digest of its Records.
    pub(crate) fn new(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        result: Result<ReadRecordsResponse, StreamFailure>,
    ) -> Self {
        let page = match result {
            Ok(page) => page,
            Err(failure) => {
                let result = stream_read_result::Result::Failure(failure);
                return Self::of(stream, seq, from_offset, result, vec![]);
            }
        };
        let mut entry = StreamReadPage {
            record_count: page.records.len() as u32,
            done: page.done,
            last_offset: page.last_offset,
            ..Default::default()
        };
        if !page.records.is_empty() {
            entry.digest = digest(&page.records);
            entry.record_bytes = record_bytes(&page.records) as u64;
        }
        let result = stream_read_result::Result::Page(entry);
        Self::of(stream, seq, from_offset, result, page.records)
    }

    /// Returns the result of read `seq` of `stream` that Workflow code cancelled before it got
    /// the job of the read.
    pub(crate) fn cancelled(stream: &StreamRef, seq: u32, from_offset: Vec<u8>) -> Self {
        let result = stream_read_result::Result::Cancelled(());
        Self::of(stream, seq, from_offset, result, vec![])
    }

    fn of(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        result: stream_read_result::Result,
        records: Vec<StreamRecord>,
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
        }
    }
}

/// Returns the size of `records` as the page of a read encodes them, which is the measure of the
/// `max_bytes` of a read.
fn record_bytes(records: &[StreamRecord]) -> usize {
    (records.iter())
        .map(|record| field_len(record.encoded_len()))
        .sum()
}

/// Returns the job that gives Workflow code the result of an append in a ledger. A replay and a
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

/// Returns the job that gives Workflow code the result of a close in a ledger.
pub(crate) fn close_job(entry: &StreamCloseResult) -> ResolveStreamClose {
    ResolveStreamClose {
        seq: entry.seq,
        failed: entry.failure.as_ref().map(failure_of),
    }
}

/// Returns the job that gives Workflow code the result of a read in a ledger, with the Records
/// of its page.
pub(crate) fn read_job(entry: &StreamReadResult, records: Vec<StreamRecord>) -> ResolveStreamRead {
    let status = entry.result.as_ref().map(|result| match result {
        stream_read_result::Result::Page(page) => resolve_stream_read::Status::Page(StreamPage {
            records,
            done: page.done,
            last_offset: page.last_offset.clone(),
        }),
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

/// The ledger of the current task. Its results of each kind are in the order that Core took them,
/// and its intents are in `seq` order. Each caller stops at the first entry that doesn't fit, so
/// the ledger never records an entry while an earlier one waits.
#[derive(Debug)]
pub(crate) struct LedgerBuilder {
    data: StreamLedger,
    /// The encoded size of `data`.
    bytes: usize,
    /// The bytes of the Records that the task gave Workflow code.
    records: usize,
    settings: StreamSettings,
}

impl LedgerBuilder {
    /// Creates an empty ledger with the sizes of `settings`.
    pub(crate) fn new(settings: StreamSettings) -> Self {
        Self {
            bytes: 0,
            data: StreamLedger::default(),
            records: 0,
            settings,
        }
    }

    /// Records the result of an append, given to Workflow code at `activation`. Returns false if
    /// the ledger has no room for it.
    pub(crate) fn append_result(
        &mut self,
        done: &Waiting<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        if !self.fits(done.len_at(activation)) {
            return false;
        }
        self.data.appends.push(StreamAppendResult {
            activation_index: activation,
            ..done.entry.clone()
        });
        true
    }

    /// Returns true if the ledger has room for the result of an append, without recording it.
    pub(crate) fn has_room_for_append(
        &self,
        done: &Waiting<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        self.has_room(done.len_at(activation))
    }

    /// Records the intent of close `seq` on `stream`. Returns false if the ledger has no room.
    pub(crate) fn close_intent(&mut self, stream: &StreamRef, seq: u32) -> bool {
        let entry = StreamCloseIntent {
            seq,
            stream_id: stream.id.clone(),
            store: stream.store.clone(),
        };
        if !self.fits(field_len(entry.encoded_len())) {
            return false;
        }
        self.data.close_intents.push(entry);
        true
    }

    /// Records the result of a close, given at `activation`. Returns false if the ledger has no
    /// room for it.
    pub(crate) fn close_result(
        &mut self,
        done: &Waiting<StreamCloseResult>,
        activation: u64,
    ) -> bool {
        if !self.fits(done.len_at(activation)) {
            return false;
        }
        self.data.closes.push(StreamCloseResult {
            activation_index: activation,
            ..done.entry.clone()
        });
        true
    }

    /// Returns true if the ledger has room for the result of a close, without recording it.
    pub(crate) fn has_room_for_close(
        &self,
        done: &Waiting<StreamCloseResult>,
        activation: u64,
    ) -> bool {
        self.has_room(done.len_at(activation))
    }

    /// Records the result of a read, given to Workflow code at `activation`, and returns its job.
    /// The caller checked that the ledger and the task have room for it.
    pub(crate) fn read_result(&mut self, done: ReadDone, activation: u64) -> ResolveStreamRead {
        self.bytes += done.entry.len_at(activation);
        let entry = StreamReadResult {
            activation_index: activation,
            ..done.entry.entry
        };
        self.records += done.bytes;
        let job = read_job(&entry, done.records);
        self.data.reads.push(entry);
        job
    }

    /// Returns true if the ledger and the task have room for the result of a read, without
    /// recording it. An empty task takes any page. A read asks for at most the bytes that an empty
    /// task holds, and a page over that size has a single Record.
    pub(crate) fn has_room_for_read(&self, done: &ReadDone, activation: u64) -> bool {
        let max_task_record_bytes = self.settings.max_task_record_bytes();
        if done.bytes > 0 && has_no_room(self.records, done.bytes, max_task_record_bytes) {
            return false;
        }
        self.has_room(done.entry.len_at(activation))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data == StreamLedger::default()
    }

    /// Returns the ledger, if it has an entry, and leaves an empty builder for the next task.
    pub(crate) fn take(&mut self) -> Option<StreamLedger> {
        if self.is_empty() {
            return None;
        }
        let builder = std::mem::replace(self, Self::new(self.settings));
        debug_assert_eq!(builder.bytes, builder.data.encoded_len());
        Some(builder.data)
    }

    /// An empty ledger accepts any entry, because each input of an entry has a bound.
    fn has_room(&self, len: usize) -> bool {
        self.is_empty() || self.bytes + len <= self.settings.max_ledger_bytes()
    }

    fn fits(&mut self, len: usize) -> bool {
        if !self.has_room(len) {
            return false;
        }
        self.bytes += len;
        true
    }
}

/// Returns true if a page of `records` bytes doesn't fit after `current` bytes under `max`. With no
/// bytes taken yet, any page fits, so one Record larger than `max` still goes in.
fn has_no_room(current: usize, records: usize, max: usize) -> bool {
    current > 0 && (current > max || records > max - current)
}

/// Returns true if `event` is the completion of a Workflow Task with a ledger.
pub(crate) fn has_ledger(event: &HistoryEvent) -> bool {
    ledger_of(event).is_some()
}

/// Decodes the ledger of a `WorkflowTaskCompleted` event, and returns `None` for an event with no
/// ledger. A ledger that doesn't decode, or that has a result which Core can't give Workflow code,
/// is an error.
pub(crate) fn peeked_ledger(event: &HistoryEvent) -> Option<Result<StreamLedger, WFMachinesError>> {
    ledger_of(event).map(decode)
}

/// Returns the encoded ledger of a `WorkflowTaskCompleted` event. A completion has one only if
/// its ledger has an entry, which is never empty when encoded.
fn ledger_of(event: &HistoryEvent) -> Option<&[u8]> {
    match event.attributes.as_ref()? {
        history_event::Attributes::WorkflowTaskCompletedEventAttributes(completed) => {
            let ledger = &completed.sdk_metadata.as_ref()?.core_stream_calls;
            (!ledger.is_empty()).then_some(&ledger[..])
        }
        _ => None,
    }
}

fn decode(encoded: &[u8]) -> Result<StreamLedger, WFMachinesError> {
    let bad = |why: String| WFMachinesError::Fatal(format!("The Stream ledger {why}"));
    let data =
        StreamLedger::decode(encoded).map_err(|error| bad(format!("doesn't decode: {error}")))?;
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
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::super::manager::recorded;
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::collections::HashMap;
    use temporalio_common::protos::{
        coresdk::{AsJsonPayloadExt, external_data::StreamAppendOffsets},
        temporal::api::{
            common::v1::Payload, history::v1::WorkflowTaskCompletedEventAttributes,
            sdk::v1::WorkflowTaskCompletedMetadata,
        },
    };
    use temporalio_streams::{
        AppendError, AppendReason, StreamError,
        proto::{
            ClientProducer, LocalActivityProducer, Producer, StandaloneActivityProducer,
            WorkflowActivityProducer, WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
        },
    };

    /// Returns the completion of a Workflow Task with the ledger `encoded`.
    fn event(encoded: Vec<u8>) -> HistoryEvent {
        let sdk_metadata = WorkflowTaskCompletedMetadata {
            core_stream_calls: encoded,
            ..Default::default()
        };
        HistoryEvent {
            attributes: Some(
                history_event::Attributes::WorkflowTaskCompletedEventAttributes(
                    WorkflowTaskCompletedEventAttributes {
                        sdk_metadata: Some(sdk_metadata),
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        }
    }

    /// Returns `value` as a JSON payload, as the body of a Record.
    fn json(value: &impl serde::Serialize) -> Payload {
        value.as_json_payload().unwrap()
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

    // History keeps the ledgers of earlier releases, so the encoding of a known ledger can't
    // change.
    #[test]
    fn the_encoding_of_a_known_ledger_never_changes() {
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
        let mut builder = LedgerBuilder::new(StreamSettings::default());
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
            last_offset: offset(10),
            ..Default::default()
        };
        let prices = ReadDone::new(&stream("prices"), 1, offset(7), Ok(prices));
        builder.read_result(prices, 0);
        let replies = ReadRecordsResponse {
            records: vec![record(1, "hello"), record(2, "bye")],
            last_offset: offset(2),
            done: true,
        };
        let replies = ReadDone::new(&stream("replies"), 2, vec![], Ok(replies));
        builder.read_result(replies, 1);
        builder.read_result(ReadDone::cancelled(&stream("prices"), 3, offset(10)), 1);
        // The Store passed entries that aren't Records, so a page with none ends past its start.
        let passed = ReadRecordsResponse {
            last_offset: offset(14),
            ..Default::default()
        };
        let passed = ReadDone::new(&stream("prices"), 4, offset(10), Ok(passed));
        builder.read_result(passed, 2);
        assert!(builder.close_intent(&stream("orders"), 2));
        let ledger = builder.take().unwrap();

        let encoded = ledger.encode_to_vec();
        let pinned = [
            // The intent of close 2 on `orders`.
            "0a11080212066f72646572731a057265646973",
            // Append 3 on `orders`, at activation 0, with its offsets.
            "1227080312066f72646572731a0572656469732a140a08000000000000002a1208000000000000002b",
            // Append 4 on `audit`, at activation 1, with its failure.
            "12460804120561756469741a057265646973200132320a1a53545245414d5f4552524f525f524541534f",
            "4e5f434c4f53454412145468652053747265616d20697320636c6f736564",
            // Close 1, at activation 1.
            "1a0408011001",
            // Read 1 of `prices`, with its digest and its last offset.
            "223e080112067072696365731a0572656469732a080000000000000007322108031a101be96426707f0f",
            "4fff8300217b932d4d20db012a08000000000000000a",
            // Read 2 of `replies`, with its digest and its last offset.
            "2239080212077265706c6965731a05726564697320013223080210011a1057e53d263ef48dd39e62ca29",
            "4ef6a2de2094012a080000000000000002",
            // Read 3 of `prices`, cancelled.
            "221f080312067072696365731a05726564697320012a08000000000000000a4200",
            // Read 4 of `prices`, with no Record and a last offset past its start.
            "2229080412067072696365731a05726564697320022a08000000000000000a320a2a0800000000000000",
            "0e",
        ];
        assert_eq!(hex(&encoded), pinned.concat());
        assert_eq!(peeked_ledger(&event(encoded)), Some(Ok(ledger)));
    }

    // A ledger records a failed call by the name of its reason, and a replay reads that name.
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

    /// Returns text of 1 to `max` characters, such as a quote, a control character or a character
    /// of several bytes.
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

    /// Returns a ledger with random entries of each kind, as the builder of a task makes it.
    fn valid_ledger(rng: &mut StdRng) -> StreamLedger {
        let mut builder = LedgerBuilder::new(StreamSettings::default());
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
                last_offset: offset(rng),
                done: rng.random_bool(0.5),
            };
            let stream = stream(rng);
            let done = match rng.random_range(0..5) {
                0 => ReadDone::new(&stream, seq, offset(rng), Err(failure(rng))),
                1 => ReadDone::cancelled(&stream, seq, offset(rng)),
                _ => ReadDone::new(&stream, seq, offset(rng), Ok(page)),
            };
            builder.read_result(done, activation(rng));
        }
        builder.take().expect("The ledger has an intent")
    }

    #[test]
    fn a_ledger_reads_back_as_it_was_written() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let ledger = valid_ledger(&mut rng);
            let read_back = peeked_ledger(&event(ledger.encode_to_vec()));
            assert_eq!(read_back, Some(Ok(ledger)), "seed {seed}");
        }
        // A completion with no Stream call has no ledger.
        assert!(!has_ledger(&event(vec![])));
    }

    /// Breaks one rule of `ledger`. Returns the encoded ledger and the reason that the decode
    /// gives, or the start of it where the rest is the text of the protobuf decoder.
    fn corrupt(rng: &mut StdRng, mut ledger: StreamLedger) -> (Vec<u8>, String) {
        let why = match rng.random_range(0..4) {
            0 => {
                // A field whose length passes the end of the ledger.
                let mut encoded = ledger.encode_to_vec();
                encoded.extend([0x0a, 0x7f]);
                return (encoded, "doesn't decode: ".to_string());
            }
            1 => {
                let read = rng.random_bool(0.5);
                match read {
                    true => ledger.reads.push(StreamReadResult {
                        seq: 99,
                        ..Default::default()
                    }),
                    false => ledger.appends.push(StreamAppendResult {
                        seq: 99,
                        ..Default::default()
                    }),
                }
                let kind = if read { "read" } else { "append" };
                format!("has no result of {kind} 99")
            }
            2 => {
                let activation = rng.random_range(MAX_ACTIVATION + 1..=u64::MAX);
                match rng.random_range(0..3) {
                    0 => ledger.appends.push(StreamAppendResult {
                        seq: 99,
                        activation_index: activation,
                        result: Some(stream_append_result::Result::Appended(Default::default())),
                        ..Default::default()
                    }),
                    1 => ledger.closes.push(StreamCloseResult {
                        seq: 99,
                        activation_index: activation,
                        failure: None,
                    }),
                    _ => ledger.reads.push(StreamReadResult {
                        seq: 99,
                        activation_index: activation,
                        result: Some(stream_read_result::Result::Cancelled(())),
                        ..Default::default()
                    }),
                }
                format!("has an entry at activation {activation}, above the maximum of 4294967295")
            }
            _ => {
                let later = StreamFailure {
                    reason: "STREAM_ERROR_REASON_LATER".to_string(),
                    message: "A later version of Core has this reason".to_string(),
                };
                match rng.random_range(0..3) {
                    0 => ledger.appends.push(StreamAppendResult {
                        seq: 99,
                        result: Some(stream_append_result::Result::Failure(later)),
                        ..Default::default()
                    }),
                    1 => ledger.closes.push(StreamCloseResult {
                        seq: 99,
                        failure: Some(later),
                        ..Default::default()
                    }),
                    _ => ledger.reads.push(StreamReadResult {
                        seq: 99,
                        result: Some(stream_read_result::Result::Failure(later)),
                        ..Default::default()
                    }),
                }
                "has a failure with the reason `STREAM_ERROR_REASON_LATER`, which this Worker \
                 doesn't know"
                    .to_string()
            }
        };
        (ledger.encode_to_vec(), why)
    }

    #[test]
    fn a_bad_ledger_is_fatal() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let valid = valid_ledger(&mut rng);
            let (encoded, why) = corrupt(&mut rng, valid);
            let Some(Err(WFMachinesError::Fatal(error))) = peeked_ledger(&event(encoded)) else {
                panic!("seed {seed}: the ledger decoded");
            };
            let expected = format!("The Stream ledger {why}");
            match why.ends_with(": ") {
                true => assert!(error.starts_with(&expected), "seed {seed}: {error}"),
                false => assert_eq!(error, expected, "seed {seed}"),
            }
        }
    }
}
