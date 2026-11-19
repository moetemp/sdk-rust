//! The stored form of a Record, and the limits of a Store. The stored form is the encoded
//! [`StreamRecord`](crate::proto::StreamRecord) of the Record with an empty offset.

use crate::{
    AppendError, AppendReason,
    proto::{AppendRecord, Producer},
};
use prost::{Message, length_delimiter_len};
use temporalio_common::protos::temporal::api::common::v1::Payload;

pub(crate) const DEFAULT_MAX_RECORD_BYTES: usize = 1536 * 1024;
const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_BATCH_RECORDS: usize = 1000;

/// The limits of a Store on the size of a batch. A batch is measured on the stored form of its
/// Records, which counts the Producer of each. The runtime refuses a request over these limits,
/// and an answer with a Record over the Record size. The limits are:
///
/// - the size of one stored Record, 1.5 MiB
/// - the size of one stored batch, 4 MiB
/// - the number of Records in one batch, 1000
///
/// A Record can end up in a marker in Workflow History, and the server rejects a payload over
/// 2 MiB by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreLimits {
    pub(crate) max_record_bytes: usize,
    pub(crate) max_batch_bytes: usize,
    pub(crate) max_batch_records: usize,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_batch_records: DEFAULT_MAX_BATCH_RECORDS,
        }
    }
}

impl StoreLimits {
    /// Checks a batch against the limits. It computes the stored size of each Record without
    /// encoding it, since a batch can be megabytes long.
    pub(crate) fn check(
        &self,
        producer: &Producer,
        records: &[AppendRecord],
    ) -> Result<(), AppendError> {
        if records.len() > self.max_batch_records {
            return Err(too_large(format!(
                "The batch has {} Records, more than {}",
                records.len(),
                self.max_batch_records
            )));
        }
        let producer_len = producer_len(producer);
        let mut total: usize = 0;
        for (at, record) in records.iter().enumerate() {
            let size = stored_len(producer_len, record.body.as_ref());
            if size > self.max_record_bytes {
                return Err(too_large(format!(
                    "Record {at} of the batch has {size} bytes, more than {}",
                    self.max_record_bytes
                )));
            }
            total = total.checked_add(size).ok_or_else(|| {
                too_large(format!(
                    "The batch has more than {} bytes",
                    self.max_batch_bytes
                ))
            })?;
        }
        if total > self.max_batch_bytes {
            return Err(too_large(format!(
                "The batch has {total} bytes, more than {}",
                self.max_batch_bytes
            )));
        }
        Ok(())
    }
}

fn too_large(message: String) -> AppendError {
    AppendError::operation(AppendReason::TooLarge, message)
}

/// Returns the encoded size of a length-delimited field whose content has `len` bytes. The field
/// number must be below 16, so that its tag is one byte.
pub fn field_len(len: usize) -> usize {
    1 + length_delimiter_len(len) + len
}

pub(crate) fn producer_len(producer: &Producer) -> usize {
    field_len(producer.encoded_len())
}

pub(crate) fn stored_len(producer_len: usize, body: Option<&Payload>) -> usize {
    producer_len + body.map(Message::encoded_len).map_or(0, field_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::{ClientProducer, StreamRecord, producer::Kind},
        test_store::{activity, client, local_activity, standalone_activity, task, workflow},
    };

    fn data_record(producer: &Producer, record: AppendRecord) -> Vec<u8> {
        StreamRecord {
            producer: Some(producer.clone()),
            body: record.body,
            offset: Vec::new(),
        }
        .encode_to_vec()
    }

    fn record(data: usize) -> AppendRecord {
        AppendRecord {
            body: Some(Payload {
                data: vec![7; data],
                ..Default::default()
            }),
        }
    }

    #[test]
    fn the_measured_size_of_a_record_is_its_stored_size() {
        let task = task(300, 2, 1_760_000_000);
        // A 400-byte id makes the length prefix of the Producer two bytes long.
        let long_id = Kind::Client(ClientProducer {
            id: "id".repeat(200),
            ..Default::default()
        });
        let producers = [
            workflow(Some(task)),
            local_activity(task, 4),
            activity(1_760_000_001, 999_999_999, 3),
            standalone_activity(1_760_000_001, 999_999_999, 3),
            client(),
            Producer {
                kind: Some(long_id),
            },
            Producer::default(),
        ];
        let with_metadata = AppendRecord {
            body: Some(Payload {
                metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
                data: vec![7; 200],
                ..Default::default()
            }),
        };
        let records = [
            record(DEFAULT_MAX_RECORD_BYTES / 2),
            with_metadata,
            record(10),
            record(0),
            AppendRecord::default(),
        ];
        for producer in &producers {
            for (at, record) in records.iter().enumerate() {
                let stored = data_record(producer, record.clone()).len();
                let measured = stored_len(producer_len(producer), record.body.as_ref());
                assert_eq!(measured, stored, "Record {at} of {producer:?}");
            }
        }
    }

    #[test]
    fn a_store_limit_decides_which_batches_it_takes() {
        let producer = client();
        let over_default = vec![record(0); DEFAULT_MAX_BATCH_RECORDS + 1];
        let default = StoreLimits::default();
        let larger = StoreLimits {
            max_batch_records: DEFAULT_MAX_BATCH_RECORDS + 1,
            ..default
        };
        assert!(default.check(&producer, &over_default).is_err());
        assert_eq!(larger.check(&producer, &over_default), Ok(()));
        let size = |data| data_record(&producer, record(data)).len();
        let cases = [
            (
                StoreLimits {
                    max_batch_records: 2,
                    ..default
                },
                vec![record(10); 2],
                vec![record(10); 3],
                "The batch has 3 Records, more than 2".to_string(),
            ),
            (
                StoreLimits {
                    max_record_bytes: size(10),
                    ..default
                },
                vec![record(10)],
                vec![record(10), record(11)],
                format!(
                    "Record 1 of the batch has {} bytes, more than {}",
                    size(11),
                    size(10)
                ),
            ),
            (
                StoreLimits {
                    max_batch_bytes: 2 * size(10),
                    ..default
                },
                vec![record(10); 2],
                vec![record(10), record(11)],
                format!(
                    "The batch has {} bytes, more than {}",
                    size(10) + size(11),
                    2 * size(10)
                ),
            ),
        ];
        for (limits, at, over, message) in cases {
            assert_eq!(limits.check(&producer, &at), Ok(()));
            assert_eq!(default.check(&producer, &over), Ok(()));
            let error = AppendError::operation(AppendReason::TooLarge, message);
            assert_eq!(limits.check(&producer, &over), Err(error));
        }
    }
}
