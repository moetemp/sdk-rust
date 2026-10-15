//! The stream service's Nexus operations, served by Core.
//!
//! Lang forwards each `append` and `read` call as N0a's JSON transfer form and gets back a
//! [NexusStep]. A call whose bodies need lang's payload codec answers first with [CodecWork], and
//! lang resumes it with the results. Core runs every rule of the contract, so each SDK's handler
//! keeps only its authorizer and its codec. The state a reader carries makes the endpoint
//! stateless, so a read goes on at any handler Worker.

mod failure;
mod wire;

use crate::{
    StreamError, Streams,
    handle::CarriedRead,
    proto::{
        AppendRecord, AppendRequest, CodecResult, CodecWork, LatestRequest, NamedProducer,
        NexusCall, NexusFailure, NexusResumeRequest, NexusStep, ReadRecord, ReadRequest,
        StreamAddress, StreamOwnerKind, StreamRecord, StreamRecordKind, append_request::Producer,
        codec_result, nexus_step, read_record,
    },
    reader::ReadState,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use tokio::time::Instant;
use wire::{
    AppendInput, AppendOutput, Base64, ReadInput, ReadOutput, RecordWire, StreamRef, Supersession,
    crossing_size, json_size,
};

/// The most bytes one read answer takes as JSON past its first record, with its tokens and state.
/// A read answer is a sync Nexus result recorded in the caller's History, so it stays well below
/// the server's blob size limit (2 MiB by default).
pub const DEFAULT_MAX_ANSWER_BYTES: u64 = 1 << 20;
/// The most bytes one record may take in an answer. One record must fit an answer on its own, so
/// a larger one can never be read through the service.
pub const DEFAULT_MAX_RECORD_BYTES: u64 = 3 << 19;
/// The most bytes the state a reader carries may take. It rides in the caller's History on each
/// call of a read.
pub const MAX_STATE_BYTES: usize = crate::MAX_STATE_BYTES;

/// What a stored record holds beside its body, topic and producer id: the kind, attempt and
/// sequence, the field tags, and the content hash metadata.
const RECORD_FIXED_BYTES: usize = 192;
/// Room left inside the request deadline for the answer to travel back.
const DEADLINE_MARGIN: Duration = Duration::from_millis(500);
const DEFAULT_MAX_RECORDS: i64 = 100;
const MAX_RECORDS: i64 = 1000;
const MAX_WAIT_MS: i64 = 60_000;
/// The class a codec failure on an append crosses as. No stream condition covers it, since the
/// failure is lang's.
const CODEC_FAILURE: &str = "PayloadCodecError";

/// How the Nexus calls behave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NexusOptions {
    /// How long a call waits for lang's codec results. A lang call that was cancelled never
    /// resumes, so its continuation is dropped after this.
    pub continuation_ttl: Duration,
    /// How many calls may wait for lang's codec at once.
    pub max_continuations: usize,
    /// How many body bytes the calls waiting for lang's codec may hold together. One call goes
    /// on alone whatever its size, so no call is refused for good.
    pub max_continuation_bytes: usize,
}

impl Default for NexusOptions {
    fn default() -> Self {
        Self {
            continuation_ttl: Duration::from_secs(60),
            max_continuations: 1024,
            max_continuation_bytes: 256 << 20,
        }
    }
}

/// The stream service's Nexus operations over one stream layer.
pub struct NexusCalls {
    streams: Arc<Streams>,
    options: NexusOptions,
    waiting: Arc<Mutex<Waiting>>,
    next: AtomicU64,
}

impl std::fmt::Debug for NexusCalls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusCalls")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

/// The calls waiting for lang's codec.
#[derive(Default)]
struct Waiting {
    calls: HashMap<u64, Pending>,
    /// Set while a task drops the calls that wait too long.
    sweeping: bool,
}

impl Waiting {
    fn purge(&mut self) {
        let now = Instant::now();
        self.calls.retain(|_, pending| pending.expires > now);
    }
}

/// A call waiting for lang's codec.
struct Pending {
    expires: Instant,
    bytes: usize,
    work: Work,
}

enum Work {
    Append {
        address: StreamAddress,
        producer_id: String,
        attempt: i64,
        sequence: i64,
        plaintext: Vec<Vec<u8>>,
    },
    Read {
        read: Box<CarriedRead>,
        limits: Limits,
        /// Which records have a body, in the order the codec work lists them. Their bodies
        /// went to the codec, so the records hold none.
        bodies: Vec<usize>,
        /// The answer's byte budget left out records of the page before the codec step.
        cut: bool,
    },
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    answer: usize,
    record: usize,
}

impl Limits {
    fn of(call: &NexusCall) -> Self {
        let (answer, record) = call.limits.as_ref().map_or((0, 0), |limits| {
            (limits.max_answer_bytes, limits.max_record_bytes)
        });
        let pick = |value: u64, default: u64| {
            usize::try_from(if value == 0 { default } else { value }).unwrap_or(usize::MAX)
        };
        Self {
            answer: pick(answer, DEFAULT_MAX_ANSWER_BYTES),
            record: pick(record, DEFAULT_MAX_RECORD_BYTES),
        }
    }
}

impl NexusCalls {
    /// The Nexus operations over `streams`.
    pub fn new(streams: Arc<Streams>, options: NexusOptions) -> Self {
        Self {
            streams,
            options,
            waiting: Arc::default(),
            next: AtomicU64::new(1),
        }
    }

    /// How many calls wait for lang's codec now.
    pub fn waiting_calls(&self) -> usize {
        self.waiting.lock().unwrap().calls.len()
    }

    /// Serves the `append` operation: one batch for one producer attempt, or its finish.
    ///
    /// Every check runs before lang's codec, so a refused call never encodes. The content hash
    /// and the batch digest are taken over the payload bytes as the caller sent them, so a retry
    /// deduplicates at any handler Worker, whatever codec nonce it uses.
    pub async fn append(&self, call: NexusCall) -> NexusStep {
        let limits = Limits::of(&call);
        let input: AppendInput = match serde_json::from_slice(&call.input) {
            Ok(input) => input,
            Err(error) => return failed(failure::input_mistake(input_error("append", error))),
        };
        let address = match address(&call.namespace, &input.stream) {
            Ok(address) => address,
            Err(failure) => return failed(*failure),
        };
        if input.producer_id.is_empty() {
            return failed(failure::input_mistake("producer_id must not be empty"));
        }
        if input.attempt < 1 || input.sequence < 1 {
            return failed(failure::input_mistake(format!(
                "attempt and sequence start at one, not {} and {}",
                input.attempt, input.sequence
            )));
        }
        let payloads: Vec<Vec<u8>> = input
            .payloads
            .unwrap_or_default()
            .into_iter()
            .map(|payload| payload.0)
            .collect();
        let finish = input.finish.unwrap_or(false);
        if finish && !payloads.is_empty() {
            return failed(failure::input_mistake(
                "a finishing append carries no payloads; send the batch first, then finish at \
                 the next sequence",
            ));
        }
        if !finish && payloads.is_empty() {
            return failed(failure::input_mistake(
                "an append carries payloads or finishes",
            ));
        }
        let overhead = input.stream.topic.len() + input.producer_id.len() + RECORD_FIXED_BYTES;
        let mut parsed = Vec::with_capacity(payloads.len());
        for (index, body) in payloads.iter().enumerate() {
            let size = crossing_size(body.len() + overhead);
            if size > limits.record {
                return failed(failure::input_mistake(format!(
                    "payload {index} would take {size} bytes in a read result, above the \
                     per-record limit of {}",
                    limits.record
                )));
            }
            match Payload::decode(body.as_slice()) {
                Ok(payload) => parsed.push(payload),
                Err(error) => {
                    return failed(failure::input_mistake(format!(
                        "payload {index} is not a serialized Payload: {error}"
                    )));
                }
            }
        }
        let bytes = payloads.iter().map(Vec::len).sum();
        let work = Work::Append {
            address,
            producer_id: input.producer_id,
            attempt: input.attempt,
            sequence: input.sequence,
            plaintext: payloads,
        };
        if finish {
            return self.finish(work, Vec::new()).await;
        }
        self.ask_codec(
            work,
            bytes,
            true,
            &call.namespace,
            &input.stream.workflow_id,
            parsed,
        )
    }

    /// Serves the `read` operation: the records after the caller's cursor, waiting for some.
    ///
    /// The answer carries the read's state as it stood after the last record kept, also when the
    /// byte budget cut it short, so a reader's marks stay exact across calls and Workers. A record
    /// the store's reader can't parse crosses as an error at its cursor, which the reader goes on
    /// past.
    pub async fn read(&self, call: NexusCall) -> NexusStep {
        let limits = Limits::of(&call);
        let input: ReadInput = match serde_json::from_slice(&call.input) {
            Ok(input) => input,
            Err(error) => return failed(failure::input_mistake(input_error("read", error))),
        };
        let address = match address(&call.namespace, &input.stream) {
            Ok(address) => address,
            Err(failure) => return failed(*failure),
        };
        let max_records = input.max_records.unwrap_or(DEFAULT_MAX_RECORDS);
        if !(1..=MAX_RECORDS).contains(&max_records) {
            return failed(failure::input_mistake(format!(
                "max_records is from 1 to {MAX_RECORDS}, not {max_records}"
            )));
        }
        let wait_ms = input.wait_ms.unwrap_or(0);
        if !(0..=MAX_WAIT_MS).contains(&wait_ms) {
            return failed(failure::input_mistake(format!(
                "wait_ms is from 0 to {MAX_WAIT_MS}, not {wait_ms}"
            )));
        }
        let after = input.after_token.unwrap_or_default();
        if input.latest_only.unwrap_or(false) {
            if !after.is_empty() {
                return failed(failure::input_mistake(
                    "latest_only starts a read at the newest record; it takes no after_token",
                ));
            }
            return match self
                .streams
                .latest(LatestRequest {
                    stream: Some(address),
                })
                .await
            {
                Ok(latest) => output(&ReadOutput {
                    records: Vec::new(),
                    next_token: latest.cursor,
                    done: false,
                    state: None,
                }),
                Err(error) => failed(failure::of_stream(&error)),
            };
        }
        // A deadline past what the clock holds is as good as none.
        let deadline = call.deadline.as_ref().and_then(|deadline| {
            SystemTime::UNIX_EPOCH.checked_add(Duration::new(
                u64::try_from(deadline.seconds).unwrap_or(0),
                u32::try_from(deadline.nanos).unwrap_or(0),
            ))
        });
        let wait = shortened(Duration::from_millis(wait_ms as u64), deadline);
        let state = match input.state {
            None => Vec::new(),
            Some(text) if text.len() > crossing_size(MAX_STATE_BYTES) => {
                return failed(failure::of_stream(&StreamError::cursor(format!(
                    "the read state takes {} bytes, above the {} of any state a read answers, so \
                     no earlier call of this read returned it",
                    text.len(),
                    crossing_size(MAX_STATE_BYTES)
                ))));
            }
            Some(text) => match wire::base64_bytes(&text) {
                Some(state) => state,
                None => return failed(failure::input_mistake("state must be base64-encoded")),
            },
        };
        // A ref's run only picks the chain, so the read names the chain by its state and follows
        // it past that run's end.
        let (address, state) = match self.chain_by_state(address, state).await {
            Ok(picked) => picked,
            Err(error) => return failed(failure::of_stream(&error)),
        };
        let request = ReadRequest {
            stream: Some(address.clone()),
            after,
            wait: wait.try_into().ok(),
            max_records: max_records as u32,
            state: state.clone(),
            floor_holder: String::new(),
        };
        let mut read = match self.streams.read_carried(request).await {
            Ok(read) => read,
            Err(error) => {
                let state = if state.is_empty() {
                    self.first_state(&address).await
                } else {
                    state
                };
                return read_failure(error, state);
            }
        };
        // Cut by the stored bodies first, so the codec decodes only what can cross. A codec that
        // grows a body is caught by the cut after the decode.
        let mut budget = Budget::new(limits, &read);
        let mut keep = read.response.records.len();
        for (index, record) in read.response.records.iter().enumerate() {
            let size = match &record.record {
                Some(read_record::Record::Stored(stored)) => {
                    json_size(&stored_wire(
                        record.cursor.clone(),
                        Base64::default(),
                        record.stale,
                    )) + crossing_size(stored.encoded_len())
                }
                Some(read_record::Record::Superseded(superseded)) => {
                    json_size(&superseded_wire(record.cursor.clone(), superseded))
                }
                None => continue,
            };
            if !budget.admits(
                size,
                matches!(record.record, Some(read_record::Record::Stored(_))),
            ) {
                keep = index;
                break;
            }
        }
        let cut = keep < read.response.records.len();
        read.response.records.truncate(keep);
        let mut bodies = Vec::new();
        let mut payloads = Vec::new();
        let mut bytes = 0;
        for (index, record) in read.response.records.iter_mut().enumerate() {
            if let Some(read_record::Record::Stored(stored)) = &mut record.record
                && let Some(body) = stored.body.take()
            {
                bytes += body.encoded_len();
                bodies.push(index);
                payloads.push(body);
            }
        }
        let work = Work::Read {
            read: Box::new(read),
            limits,
            bodies,
            cut,
        };
        if payloads.is_empty() {
            return self.finish(work, Vec::new()).await;
        }
        self.ask_codec(
            work,
            bytes,
            false,
            &call.namespace,
            &input.stream.workflow_id,
            payloads,
        )
    }

    /// `address` without its run, and a state that names the chain the run picked. A state the
    /// caller passed back already names its chain.
    async fn chain_by_state(
        &self,
        mut address: StreamAddress,
        state: Vec<u8>,
    ) -> Result<(StreamAddress, Vec<u8>), StreamError> {
        if address.run_id.is_empty() {
            return Ok((address, state));
        }
        let state = if state.is_empty() {
            ReadState::of_chain(self.streams.first_run_of(&address).await?).encode_to_vec()
        } else {
            state
        };
        address.run_id.clear();
        Ok((address, state))
    }

    /// The state of a first call that failed, so the reader's next call follows the chain this
    /// one resolved, not one that took the Workflow id since.
    async fn first_state(&self, address: &StreamAddress) -> Vec<u8> {
        match self.streams.first_run_of(address).await {
            Ok(first_run_id) => ReadState::of_chain(first_run_id).encode_to_vec(),
            Err(_) => Vec::new(),
        }
    }

    /// Goes on with a call that asked for codec work, with lang's results.
    pub async fn resume(&self, request: NexusResumeRequest) -> NexusStep {
        let pending = {
            let mut waiting = self.waiting.lock().unwrap();
            waiting.purge();
            waiting.calls.remove(&request.continuation)
        };
        let Some(pending) = pending else {
            // The call waited too long, or lang resumed it twice. Nothing was written, so the
            // caller's retry starts it over.
            return failed(failure::failure(
                "StreamStorageError",
                failure::UNAVAILABLE,
                true,
                "the call waited too long for the payload codec, so it was dropped",
            ));
        };
        self.finish(pending.work, request.results).await
    }

    fn ask_codec(
        &self,
        work: Work,
        bytes: usize,
        encode: bool,
        namespace: &str,
        workflow_id: &str,
        payloads: Vec<Payload>,
    ) -> NexusStep {
        let continuation = self.next.fetch_add(1, Ordering::Relaxed);
        let start_sweep = {
            let mut waiting = self.waiting.lock().unwrap();
            waiting.purge();
            let held: usize = waiting.calls.values().map(|pending| pending.bytes).sum();
            let full = waiting.calls.len() >= self.options.max_continuations
                || (!waiting.calls.is_empty()
                    && held.saturating_add(bytes) > self.options.max_continuation_bytes);
            if full {
                return failed(failure::failure(
                    "StreamStorageError",
                    failure::UNAVAILABLE,
                    true,
                    "too many stream calls are waiting for the payload codec",
                ));
            }
            waiting.calls.insert(
                continuation,
                Pending {
                    expires: Instant::now() + self.options.continuation_ttl,
                    bytes,
                    work,
                },
            );
            !std::mem::replace(&mut waiting.sweeping, true)
        };
        if start_sweep {
            tokio::spawn(sweep(Arc::downgrade(&self.waiting)));
        }
        NexusStep {
            step: Some(nexus_step::Step::Codec(CodecWork {
                encode,
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                payloads,
                continuation,
            })),
        }
    }

    async fn finish(&self, work: Work, results: Vec<CodecResult>) -> NexusStep {
        match work {
            Work::Append {
                address,
                producer_id,
                attempt,
                sequence,
                plaintext,
            } => {
                self.finish_append(address, producer_id, attempt, sequence, plaintext, results)
                    .await
            }
            Work::Read {
                read,
                limits,
                bodies,
                cut,
            } => finish_read(*read, limits, &bodies, cut, results),
        }
    }

    async fn finish_append(
        &self,
        address: StreamAddress,
        producer_id: String,
        attempt: i64,
        sequence: i64,
        plaintext: Vec<Vec<u8>>,
        results: Vec<CodecResult>,
    ) -> NexusStep {
        if results.len() != plaintext.len() {
            return failed(wrong_count(results.len(), plaintext.len()));
        }
        let mut records = Vec::with_capacity(plaintext.len().max(1));
        for (index, (plain, result)) in plaintext.iter().zip(results).enumerate() {
            let body = match result.result {
                Some(codec_result::Result::Payload(body)) => body,
                Some(codec_result::Result::Error(error)) => {
                    return failed(failure::failure(
                        CODEC_FAILURE,
                        failure::INTERNAL,
                        true,
                        &format!("the payload codec failed on payload {index}: {error}"),
                    ));
                }
                None => {
                    return failed(failure::failure(
                        CODEC_FAILURE,
                        failure::INTERNAL,
                        true,
                        &format!("the payload codec answered nothing for payload {index}"),
                    ));
                }
            };
            records.push(AppendRecord {
                kind: StreamRecordKind::Data as i32,
                body: Some(body),
                content_hash: Sha256::digest(plain).to_vec(),
                ..Default::default()
            });
        }
        if records.is_empty() {
            records.push(AppendRecord {
                kind: StreamRecordKind::Finish as i32,
                ..Default::default()
            });
        }
        let request = AppendRequest {
            stream: Some(address),
            producer: Some(Producer::Named(NamedProducer {
                producer_id,
                attempt,
            })),
            sequence,
            digest: digest(&records, &plaintext),
            records,
        };
        match self.streams.append(request).await {
            Ok(landed) => output(&AppendOutput {
                cursor: landed.last_cursor,
            }),
            Err(error) => failed(failure::of_stream(&error)),
        }
    }
}

/// The batch digest over each record's kind and the payload bytes as the caller sent them,
/// length-prefixed, never over a protobuf serialization, whose map order differs per language.
fn digest(records: &[AppendRecord], plaintext: &[Vec<u8>]) -> Vec<u8> {
    let mut digest = Sha256::new();
    for (index, record) in records.iter().enumerate() {
        let body = plaintext.get(index).map(Vec::as_slice).unwrap_or_default();
        digest.update(record.kind.to_be_bytes());
        digest.update((body.len() as u64).to_be_bytes());
        digest.update(body);
    }
    digest.finalize().to_vec()
}

/// A codec answer with the wrong number of results is a fault of lang's codec loop, and it fails
/// the same way on each retry.
fn wrong_count(results: usize, payloads: usize) -> NexusFailure {
    failure::failure(
        CODEC_FAILURE,
        failure::INTERNAL,
        false,
        &format!("the payload codec answered {results} results for {payloads} payloads"),
    )
}

/// What is left of a read answer's bytes for its records, after the members every answer has.
struct Budget {
    left: usize,
    /// Set once a stored record is in, since the first one crosses whatever its size.
    stored: bool,
}

impl Budget {
    fn new(limits: Limits, read: &CarriedRead) -> Self {
        let envelope = json_size(&ReadOutput {
            records: Vec::new(),
            next_token: read.response.cursor.clone(),
            done: false,
            state: Some(Base64::default()),
        });
        let state = crossing_size(read.state.encoded_len().min(MAX_STATE_BYTES));
        Self {
            left: limits.answer.saturating_sub(envelope + state),
            stored: false,
        }
    }

    /// Takes a wire of `size` bytes, or answers that the answer is cut before it.
    fn admits(&mut self, size: usize, stored: bool) -> bool {
        // One more for the comma between records.
        let size = size + 1;
        if self.stored && size > self.left {
            return false;
        }
        self.left = self.left.saturating_sub(size);
        self.stored |= stored;
        true
    }
}

fn finish_read(
    read: CarriedRead,
    limits: Limits,
    bodies: &[usize],
    cut: bool,
    results: Vec<CodecResult>,
) -> NexusStep {
    if results.len() != bodies.len() {
        return failed(wrong_count(results.len(), bodies.len()));
    }
    let mut decoded: HashMap<usize, Result<Payload, String>> = HashMap::new();
    for (index, result) in bodies.iter().zip(results) {
        decoded.insert(
            *index,
            match result.result {
                Some(codec_result::Result::Payload(body)) => Ok(body),
                Some(codec_result::Result::Error(error)) => Err(error),
                None => Err("the payload codec answered nothing".to_string()),
            },
        );
    }
    let mut budget = Budget::new(limits, &read);
    let CarriedRead {
        response,
        mut state,
        before,
    } = read;
    let mut cut = cut;
    let mut wires: Vec<RecordWire> = Vec::with_capacity(response.records.len());
    let mut kept: Vec<StreamRecord> = Vec::new();
    for (index, record) in response.records.into_iter().enumerate() {
        let ReadRecord {
            cursor,
            record,
            stale,
        } = record;
        let (wire, stored) = match record {
            Some(read_record::Record::Superseded(superseded)) => {
                (superseded_wire(cursor, &superseded), None)
            }
            Some(read_record::Record::Stored(stored)) => {
                let wire = record_wire(cursor, &stored, decoded.remove(&index), stale, limits);
                (wire, Some(stored))
            }
            None => continue,
        };
        if !budget.admits(json_size(&wire), stored.is_some()) {
            cut = true;
            break;
        }
        wires.push(wire);
        kept.extend(stored);
    }
    let (next_token, done) = if cut {
        // A supersession belongs to the record after it, so it goes into the next answer too.
        if wires.last().is_some_and(|wire| wire.superseded.is_some()) {
            wires.pop();
        }
        state = state.at_cut(&before, &kept);
        let last = wires
            .last()
            .map(|wire| wire.token.clone())
            .unwrap_or_default();
        (last, false)
    } else {
        (response.cursor, response.done)
    };
    state.fit_within(MAX_STATE_BYTES);
    output(&ReadOutput {
        records: wires,
        next_token,
        done,
        state: Some(Base64(state.encode_to_vec())),
    })
}

fn superseded_wire(token: String, superseded: &crate::proto::Supersession) -> RecordWire {
    RecordWire {
        token,
        record: Base64::default(),
        error: None,
        stale: None,
        superseded: Some(Supersession {
            producer_id: superseded.producer_id.clone(),
            previous_attempt: superseded.previous_attempt,
            attempt: superseded.attempt,
        }),
    }
}

fn stored_wire(token: String, record: Base64, stale: bool) -> RecordWire {
    RecordWire {
        token,
        record,
        error: None,
        stale: stale.then_some(true),
        superseded: None,
    }
}

/// One stored record as it crosses, with its body decoded, or the reason it can't cross. An
/// error keeps the `stale` mark, so a reader can skip a stale record it can't decode.
fn record_wire(
    token: String,
    stored: &StreamRecord,
    decoded: Option<Result<Payload, String>>,
    stale: bool,
    limits: Limits,
) -> RecordWire {
    let error = |message: String| RecordWire {
        error: Some(failure::capped(&message)),
        ..stored_wire(token.clone(), Base64::default(), stale)
    };
    let mut crossing = stored.clone();
    match decoded {
        Some(Ok(body)) => crossing.body = Some(body),
        Some(Err(failure)) => {
            return error(format!("the record's body could not be decoded: {failure}"));
        }
        None => {}
    }
    let bytes = crossing.encode_to_vec();
    let size = crossing_size(bytes.len());
    if size > limits.record {
        // No read answer can carry it, so the reader is told why at its cursor and goes on.
        return error(format!(
            "the record would take {size} bytes in a read result, above the per-record limit of \
             {}",
            limits.record
        ));
    }
    stored_wire(token, Base64(bytes), stale)
}

/// How a read's failure crosses. A record the store's reader can't parse ends no read: it
/// crosses as an error at its cursor, and the reader goes on past it with the state it had.
fn read_failure(error: StreamError, state: Vec<u8>) -> NexusStep {
    match (error.kind, error.cursor.clone()) {
        (crate::proto::StreamFailureKind::Record, Some(cursor)) => output(&ReadOutput {
            records: vec![RecordWire {
                error: Some(failure::capped(&error.message)),
                ..stored_wire(cursor.clone(), Base64::default(), false)
            }],
            next_token: cursor,
            done: false,
            state: (!state.is_empty()).then_some(Base64(state)),
        }),
        _ => failed(failure::of_stream(&error)),
    }
}

/// The stream address a call names. An owner kind this release can't open is refused before any
/// codec work.
fn address(namespace: &str, stream: &StreamRef) -> Result<StreamAddress, Box<NexusFailure>> {
    if stream.kind != "workflow" {
        return Err(Box::new(failure::of_stream(&StreamError::unsupported(
            format!(
                "this release opens streams of Workflows only, not of kind {}",
                failure::named(&stream.kind)
            ),
        ))));
    }
    if let Err(reason) = temporalio_common::streams::check_topic(&stream.topic) {
        return Err(Box::new(failure::input_mistake(failure::without_values(
            &reason,
        ))));
    }
    if stream.workflow_id.is_empty() {
        return Err(Box::new(failure::input_mistake(
            "a stream needs its owner's Workflow id",
        )));
    }
    Ok(StreamAddress {
        namespace: namespace.to_string(),
        owner_kind: StreamOwnerKind::Workflow as i32,
        workflow_id: stream.workflow_id.clone(),
        run_id: stream.chain_run_id.clone().unwrap_or_default(),
        topic: stream.topic.clone(),
        ..Default::default()
    })
}

/// `wait`, shortened so the answer can travel back before `deadline`.
fn shortened(wait: Duration, deadline: Option<SystemTime>) -> Duration {
    let Some(deadline) = deadline else {
        return wait;
    };
    let left = deadline
        .duration_since(SystemTime::now())
        .unwrap_or_default()
        .saturating_sub(DEADLINE_MARGIN);
    wait.min(left)
}

/// Drops the calls that waited too long for lang's codec, also while no other call comes in, so
/// their plaintext doesn't outlive the wait.
async fn sweep(waiting: Weak<Mutex<Waiting>>) {
    loop {
        let next = {
            let Some(waiting) = waiting.upgrade() else {
                return;
            };
            let mut waiting = waiting.lock().unwrap();
            waiting.purge();
            match waiting.calls.values().map(|pending| pending.expires).min() {
                Some(next) => next,
                None => {
                    waiting.sweeping = false;
                    return;
                }
            }
        };
        tokio::time::sleep_until(next).await;
    }
}

fn input_error(operation: &str, error: serde_json::Error) -> String {
    format!(
        "the {operation} input doesn't match the contract: {}",
        failure::without_values(&error.to_string())
    )
}

fn output(value: &impl serde::Serialize) -> NexusStep {
    NexusStep {
        step: Some(nexus_step::Step::Output(
            serde_json::to_vec(value).expect("a contract model always serializes"),
        )),
    }
}

fn failed(failure: NexusFailure) -> NexusStep {
    NexusStep {
        step: Some(nexus_step::Step::Failure(failure)),
    }
}
