//! The Stream service operations on the Store, for Activities, clients and the Local Activity that
//! runs each Workflow call.

use crate::{
    Reason, StreamError, StreamResult,
    digest::batch_digest,
    ids::{check_producer, check_stream_id},
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, StreamProducerKind, StreamRef, StreamStatus,
    },
    record::{caller_status, close_record, data_records, supersession},
    store::{
        Batch, Capabilities, CreateOutcome, MAX_RECORD_BYTES, MAX_SEQUENCE, MAX_TTL, Owner, Store,
        Withdrawal, owner_chain,
    },
};
use std::{sync::Arc, time::Duration};

/// The Stream service on one Store.
pub(crate) struct StreamService {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) metrics: Metrics,
    /// The backend name for the refs the service builds.
    pub(crate) backend: String,
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// The Retention of a create, `None` for the Store default. A Store computes expiry times from it,
/// so a value past [MAX_TTL] would overflow them after the create's first write.
fn retention(value: Option<prost_types::Duration>) -> StreamResult<Option<Duration>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let out_of_range = || {
        StreamError::invalid(format!(
            "The Retention must be from 0 to {} s",
            MAX_TTL.as_secs()
        ))
    };
    let (Ok(seconds), Ok(nanos)) = (u64::try_from(value.seconds), u32::try_from(value.nanos))
    else {
        return Err(out_of_range());
    };
    if nanos >= 1_000_000_000 || Duration::new(seconds, nanos) > MAX_TTL {
        return Err(out_of_range());
    }
    Ok(duration(Some(value)))
}

fn proto_duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: value.as_secs() as i64,
        nanos: value.subsec_nanos() as i32,
    }
}

fn stream_id(stream: Option<&StreamRef>) -> StreamResult<&str> {
    let id = stream.map(|stream| stream.id.as_str()).unwrap_or_default();
    check_stream_id(id)?;
    Ok(id)
}

/// The `Superseded` facts of a create that withdraws a retry run's previous run. It has no epoch,
/// so a Reader withdraws only that run's Records.
fn run_supersession(owner: Option<&Producer>, withdraw_run: &str) -> Vec<u8> {
    let producer = Producer {
        epoch: None,
        ..owner.cloned().unwrap_or_default()
    };
    supersession(&producer, withdraw_run)
}

/// Checks a Producer that writes outside Workflow code.
fn outside_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    if producer.kind == StreamProducerKind::Workflow as i32 {
        return Err(StreamError::invalid(
            "A Workflow writes with the Producer of its Local Activity",
        ));
    }
    // Without its task, a re-dispatch in a later task has the same epoch as the first dispatch.
    let task = producer
        .epoch
        .map_or(0, |epoch| epoch.task_started_event_id);
    if producer.kind == StreamProducerKind::LocalActivity as i32 && task <= 0 {
        return Err(StreamError::invalid(
            "A Local Activity Producer's epoch must name the Workflow Task that dispatched it",
        ));
    }
    Ok(producer)
}

/// Checks the owner of a create, which must be a Workflow run with a ref of its run chain. A
/// Store stores that chain and checks every later create and close against it.
fn create_owner<'a>(owner: &'a Producer, stream: &StreamRef) -> StreamResult<&'a Producer> {
    check_producer(owner)?;
    if owner.kind != StreamProducerKind::Workflow as i32 {
        return Err(StreamError::invalid(
            "The owner of a create is a Workflow Producer",
        ));
    }
    if owner_chain(stream).is_empty() {
        return Err(StreamError::invalid(
            "A create with an owner needs a ref whose owner is the Workflow's run chain",
        ));
    }
    Ok(owner)
}

/// Checks the Producer of a close. A Workflow's close Activity closes as the run's Workflow
/// Producer, so a Workflow Producer may close.
fn close_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    Ok(producer)
}

impl StreamService {
    pub(crate) async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        check_stream_id(&request.stream_id)?;
        let retention = retention(request.retention)?;
        if request
            .r#ref
            .as_ref()
            .is_some_and(|stream| stream.id != request.stream_id)
        {
            return Err(StreamError::invalid(
                "The create's ref names another Stream than its stream_id",
            ));
        }
        let stream = request.r#ref.clone().unwrap_or_else(|| StreamRef {
            id: request.stream_id.clone(),
            ..Default::default()
        });
        // A Workflow's call from outside Workflow code, such as from a Local Activity, sets the
        // owner run. Then the Stream has the safety TTL of a Workflow-owned Stream.
        let owner = match &request.owner {
            Some(owner) => {
                create_owner(owner, &stream)?;
                self.store
                    .capabilities()
                    .require(Capabilities::SAFETY_TTL)?;
                Owner::Workflow
            }
            None => Owner::Client,
        };
        if !request.withdraw_run.is_empty() {
            // The `Superseded` item names its Producer, which here can only be the owner. Any other
            // caller could withdraw and fence an unrelated run, as on the append.
            let Some(owner) = &request.owner else {
                return Err(StreamError::invalid(
                    "A create with withdraw_run needs the owner run",
                ));
            };
            if request.withdraw_run == owner.run_id {
                return Err(StreamError::invalid("A run doesn't withdraw itself"));
            }
            self.store.capabilities().require(Capabilities::RUN_FENCE)?;
        }
        let bytes = run_supersession(request.owner.as_ref(), &request.withdraw_run);
        let withdrawal = Withdrawal {
            withdraw_run: &request.withdraw_run,
            supersession: &bytes,
            tasks: None,
        };
        let created = self
            .store
            .create(&request.namespace, &stream, retention, owner, &withdrawal)
            .await?;
        if created.flagged {
            self.metrics.superseded();
        }
        Ok(CreateStreamResponse {
            stream: Some(StreamRef {
                id: request.stream_id,
                owner: stream.owner.filter(|_| owner == Owner::Workflow),
                backend: self.backend.clone(),
            }),
            created: created.outcome == CreateOutcome::Created,
        })
    }

    pub(crate) async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        stream_id(request.stream.as_ref())?;
        let stream = request.stream.clone().unwrap_or_default();
        let producer = outside_producer(request.producer.as_ref())?;
        if !(1..=MAX_SEQUENCE).contains(&request.sequence) {
            return Err(StreamError::invalid(
                "The sequence must be from 1 to 2^53 - 1",
            ));
        }
        // Without the fence and the flag, a copy or a stale batch would reach Readers.
        self.store
            .capabilities()
            .require(Capabilities::PRODUCER_FENCE.union(Capabilities::WITHDRAWAL))?;
        if !request.withdraw_run.is_empty() {
            // Only a retry run's Local Activity withdraws its previous run, as only the owner's
            // create does. Any other Producer could withdraw and fence an unrelated run.
            if producer.kind != StreamProducerKind::LocalActivity as i32
                || request.withdraw_run == producer.run_id
            {
                return Err(StreamError::invalid(
                    "Only a Local Activity of a later run withdraws a run",
                ));
            }
            self.store.capabilities().require(Capabilities::RUN_FENCE)?;
        }
        let digest = batch_digest(&request.records);
        let records = data_records(producer, request.records)?;
        let bytes = supersession(producer, &request.withdraw_run);
        let batch = Batch {
            producer,
            sequence: request.sequence,
            records: &records,
            digest: &digest,
            withdrawal: Withdrawal {
                withdraw_run: &request.withdraw_run,
                supersession: &bytes,
                tasks: None,
            },
        };
        let appended = self.store.append(&request.namespace, &stream, &batch).await;
        let appended = self.count(appended)?;
        let (first_offset, last_offset) = appended.offsets.unwrap_or_default();
        Ok(AppendRecordsResponse {
            first_offset,
            last_offset,
        })
    }

    /// Counts a fence or a flag of an append on the metrics.
    pub(crate) fn count<T: Flagged>(&self, result: StreamResult<T>) -> StreamResult<T> {
        match &result {
            Ok(value) if value.flagged() => self.metrics.superseded(),
            Err(error) if error.reason == Reason::Fenced => self.metrics.fenced(),
            _ => {}
        }
        result
    }

    pub(crate) async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        stream_id(request.stream.as_ref())?;
        let stream = request.stream.clone().unwrap_or_default();
        let producer = close_producer(request.producer.as_ref())?;
        self.store.capabilities().require(Capabilities::CLOSE)?;
        let status = caller_status(request.status, request.failure.as_ref())?;
        let record = close_record(producer, request.result, request.failure, status);
        // Every read past the close returns the close Record again.
        if record.len() > MAX_RECORD_BYTES {
            return Err(StreamError::new(
                Reason::TooLarge,
                format!(
                    "The close Record has {} bytes, more than {MAX_RECORD_BYTES}",
                    record.len()
                ),
            ));
        }
        self.store
            .close(&request.namespace, &stream, producer, &record)
            .await?;
        Ok(CloseStreamResponse {})
    }

    pub(crate) async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let id = stream_id(request.stream.as_ref())?;
        let (state, _) = self.store.read(&request.namespace, id, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        // A closed Stream that expired has lost its close Record, and every read fails.
        let status = if state.expired() {
            StreamStatus::Expired
        } else if state.closed_at.is_some() {
            StreamStatus::Closed
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: state.newest,
            retention: Some(proto_duration(state.retention)),
        })
    }
}

/// A Store answer that may have withdrawn Records.
pub(crate) trait Flagged {
    fn flagged(&self) -> bool;
}

impl Flagged for crate::store::Appended {
    fn flagged(&self) -> bool {
        self.flagged
    }
}
