//! The stored Records and the `Superseded` facts of a batch.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{
        AppendRecord, Producer, StreamClose, StreamCloseStatus, StreamData, StreamRecord,
        Supersession, stream_record::Item,
    },
    store::{MAX_BATCH_BYTES, MAX_RECORD_BYTES},
};
use prost::Message;
use temporalio_common::protos::temporal::api::{
    common::v1::Payload,
    failure::v1::{Failure, failure::FailureInfo},
};

/// One stored data Record.
pub(crate) fn data_record(producer: &Producer, record: AppendRecord) -> Vec<u8> {
    StreamRecord {
        item: Some(Item::Data(StreamData {
            producer: Some(producer.clone()),
            body: record.body,
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// The stored Records of one batch from outside Workflow code. A batch over the Store's limits
/// fails, since the crate doesn't split a batch it didn't build.
pub(crate) fn data_records(
    producer: &Producer,
    records: Vec<AppendRecord>,
) -> StreamResult<Vec<Vec<u8>>> {
    let records: Vec<_> = records
        .into_iter()
        .map(|record| data_record(producer, record))
        .collect();
    if let Some(big) = records
        .iter()
        .find(|record| record.len() > MAX_RECORD_BYTES)
    {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!(
                "A Record has {} bytes, more than {MAX_RECORD_BYTES}",
                big.len()
            ),
        ));
    }
    let total: usize = records.iter().map(Vec::len).sum();
    if total > MAX_BATCH_BYTES {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!("The batch has {total} bytes, more than {MAX_BATCH_BYTES}"),
        ));
    }
    Ok(records)
}

/// The stored close Record.
pub(crate) fn close_record(
    producer: &Producer,
    result: Option<Payload>,
    failure: Option<Failure>,
    status: StreamCloseStatus,
) -> Vec<u8> {
    StreamRecord {
        item: Some(Item::Close(StreamClose {
            producer: Some(producer.clone()),
            result,
            failure,
            status: status as i32,
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// The status of a caller's close. Only the owner's end on the server terminates or times out a
/// Stream, so a reader can trust those two.
pub(crate) fn caller_status(
    status: i32,
    failure: Option<&Failure>,
) -> StreamResult<StreamCloseStatus> {
    let failed = failure.is_some();
    let canceled = failure.is_some_and(|failure| {
        matches!(
            failure.failure_info,
            Some(FailureInfo::CanceledFailureInfo(_))
        )
    });
    match StreamCloseStatus::try_from(status) {
        // The SDK APIs send no status, so a failure with `CanceledFailureInfo` gives canceled.
        Ok(StreamCloseStatus::Unspecified) if canceled => Ok(StreamCloseStatus::Canceled),
        Ok(StreamCloseStatus::Unspecified) if failed => Ok(StreamCloseStatus::Failed),
        Ok(StreamCloseStatus::Unspecified) => Ok(StreamCloseStatus::Completed),
        Ok(StreamCloseStatus::Terminated | StreamCloseStatus::TimedOut) => Err(
            StreamError::invalid("Only the owner's end terminates or times out a Stream"),
        ),
        Ok(StreamCloseStatus::Completed) if failed => {
            Err(StreamError::invalid("A completed close has no failure"))
        }
        Ok(StreamCloseStatus::Failed) if !failed => {
            Err(StreamError::invalid("A failed close needs its failure"))
        }
        Ok(status) => Ok(status),
        Err(_) => Err(StreamError::invalid(format!(
            "The close status {status} is unknown"
        ))),
    }
}

/// The facts a `Superseded` item of this batch gives the Reader.
pub(crate) fn supersession(producer: &Producer, withdraw_run: &str) -> Vec<u8> {
    Supersession {
        producer_kind: producer.kind,
        producer_id: producer.id.clone(),
        epoch: producer.epoch,
        withdraw_run: withdraw_run.to_string(),
        run_id: producer.run_id.clone(),
        ..Default::default()
    }
    .encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_over_the_limits_fails() {
        let producer = Producer::default();
        let record = |bytes: usize| AppendRecord {
            body: Some(Payload {
                data: vec![0; bytes],
                ..Default::default()
            }),
        };
        let error = data_records(&producer, vec![record(MAX_RECORD_BYTES)]).unwrap_err();
        assert_eq!(error.reason, Reason::TooLarge);
        let many = (0..4)
            .map(|_| record(MAX_RECORD_BYTES / 2 + 1))
            .collect::<Vec<_>>();
        let many = [many.clone(), many].concat();
        assert_eq!(
            data_records(&producer, many).unwrap_err().reason,
            Reason::TooLarge
        );
        assert_eq!(data_records(&producer, vec![record(10)]).unwrap().len(), 1);
    }

    #[test]
    fn an_empty_status_with_a_canceled_failure_is_canceled() {
        let canceled = Failure {
            failure_info: Some(FailureInfo::CanceledFailureInfo(Default::default())),
            ..Default::default()
        };
        let other = Failure::default();
        let empty = StreamCloseStatus::Unspecified as i32;
        let status = |status: i32, failure| caller_status(status, failure).unwrap();
        assert_eq!(status(empty, Some(&canceled)), StreamCloseStatus::Canceled);
        assert_eq!(status(empty, Some(&other)), StreamCloseStatus::Failed);
        assert_eq!(status(empty, None), StreamCloseStatus::Completed);
        assert_eq!(
            status(StreamCloseStatus::Failed as i32, Some(&canceled)),
            StreamCloseStatus::Failed
        );
    }
}
