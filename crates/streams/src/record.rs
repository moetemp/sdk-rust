//! Building the records a store keeps, and naming who wrote them.

use crate::{
    StreamError, StreamResult,
    proto::{ActivityProducer, AppendRecord, StreamRecord, StreamRecordKind},
};
use temporalio_common::{
    protos::temporal::api::common::v1::Payload,
    streams::{CONTENT_HASH_KEY, METADATA_ENCODING, RUN_ID_KEY, content_hash_text},
};

/// The owner kind a Workflow's streams hash under in their cursors.
pub const WORKFLOW_OWNER_KIND: &str = "workflow";

const HASH_LENGTH: usize = 32;

/// The producer id an Activity attempt writes as.
///
/// The scheduling run is part of it, so the same Activity id in another run of the chain is a
/// different producer, and a retry of the Activity keeps its id and raises the attempt.
pub fn activity_producer_id(activity: &ActivityProducer) -> String {
    format!("{}@{}", activity.activity_id, activity.run_id)
}

/// The producer id a Workflow run writes its own records as.
///
/// It is an Activity's id form with no Activity id. An Activity id is never empty, so no Activity
/// writes as a run. Each run of a chain is a producer of its own, so Continue-as-New and a reset
/// start a new producer rather than superseding the earlier run.
pub fn workflow_producer_id(run_id: &str) -> String {
    format!("@{run_id}")
}

/// Refuses an append batch digest that isn't the 32-byte SHA-256 lang takes before the codec.
///
/// Stores compare it unchanged when a producer repeats its newest batch, so a digest Core made
/// another way would turn a retry into a divergent write.
pub fn check_append_digest(digest: &[u8]) -> StreamResult<()> {
    if digest.len() != HASH_LENGTH {
        return Err(StreamError::invalid_argument(format!(
            "an append needs the {HASH_LENGTH}-byte digest lang takes over the batch before the \
             codec, not {} bytes",
            digest.len()
        )));
    }
    Ok(())
}

/// The record a store keeps for one appended record of a producer.
///
/// Fails as an invalid argument when the record is one no writer stores: a kind other than DATA
/// or FINISH, a DATA record without a body or a 32-byte content hash, or a FINISH record with a
/// body.
pub fn stored_append_record(
    topic: &str,
    producer_id: &str,
    attempt: i64,
    sequence: i64,
    record: &AppendRecord,
) -> StreamResult<StreamRecord> {
    let mut stored = stored_record(
        topic,
        record.kind,
        record.body.clone(),
        &record.content_hash,
    )?;
    stored.producer_id = producer_id.to_string();
    stored.attempt = attempt;
    stored.sequence = sequence;
    Ok(stored)
}

/// The record a store keeps for one record a Workflow run published, as its run's producer at
/// `attempt` and `sequence`.
///
/// The run id is also stamped, since a stream follows the run chain and readers tell a successor
/// run or a reset branch apart by it. Fails like [stored_append_record].
pub fn stored_output_record(
    topic: &str,
    kind: i32,
    body: Option<Payload>,
    content_hash: &[u8],
    run_id: &str,
    attempt: i64,
    sequence: i64,
) -> StreamResult<StreamRecord> {
    let mut stored = stored_record(topic, kind, body, content_hash)?;
    stored.producer_id = workflow_producer_id(run_id);
    stored.attempt = attempt;
    stored.sequence = sequence;
    stored
        .metadata
        .insert(RUN_ID_KEY.to_string(), metadata_payload(run_id.as_bytes()));
    Ok(stored)
}

fn stored_record(
    topic: &str,
    kind: i32,
    body: Option<Payload>,
    content_hash: &[u8],
) -> StreamResult<StreamRecord> {
    if topic.is_empty() {
        return Err(StreamError::invalid_argument(
            "a stream record needs a topic",
        ));
    }
    let mut stored = StreamRecord {
        topic: topic.to_string(),
        kind,
        ..Default::default()
    };
    match StreamRecordKind::try_from(kind) {
        Ok(StreamRecordKind::Data) => {
            let Some(body) = body else {
                return Err(StreamError::invalid_argument("a DATA record needs a body"));
            };
            if content_hash.len() != HASH_LENGTH {
                return Err(StreamError::invalid_argument(format!(
                    "a DATA record needs the {HASH_LENGTH}-byte SHA-256 of its plaintext body, \
                     not {} bytes",
                    content_hash.len()
                )));
            }
            stored.metadata.insert(
                CONTENT_HASH_KEY.to_string(),
                metadata_payload(content_hash_text(content_hash).as_bytes()),
            );
            stored.body = Some(body);
        }
        Ok(StreamRecordKind::Finish) => {
            if body.is_some() || !content_hash.is_empty() {
                return Err(StreamError::invalid_argument(
                    "a FINISH record carries no body",
                ));
            }
        }
        _ => {
            return Err(StreamError::invalid_argument(format!(
                "a writer stores DATA or FINISH records, not kind {kind}"
            )));
        }
    }
    Ok(stored)
}

fn metadata_payload(data: &[u8]) -> Payload {
    Payload {
        metadata: [("encoding".to_string(), METADATA_ENCODING.to_vec())].into(),
        data: data.to_vec(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StreamFailureKind;

    fn body() -> Payload {
        Payload {
            metadata: [("encoding".to_string(), b"binary/encrypted".to_vec())].into(),
            data: b"ciphertext".to_vec(),
            ..Default::default()
        }
    }

    fn data(hash: u8) -> AppendRecord {
        AppendRecord {
            kind: StreamRecordKind::Data as i32,
            body: Some(body()),
            content_hash: vec![hash; HASH_LENGTH],
        }
    }

    fn finish() -> AppendRecord {
        AppendRecord {
            kind: StreamRecordKind::Finish as i32,
            ..Default::default()
        }
    }

    #[test]
    fn an_activity_writes_as_its_id_at_its_run() {
        let activity = ActivityProducer {
            workflow_id: "wf".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "fetch".to_string(),
            attempt: 3,
        };
        assert_eq!(activity_producer_id(&activity), "fetch@run-1");
    }

    #[test]
    fn a_workflow_run_writes_as_no_activity_can() {
        let activity = ActivityProducer {
            activity_id: "x".to_string(),
            run_id: "run-1".to_string(),
            ..Default::default()
        };
        assert_eq!(workflow_producer_id("run-1"), "@run-1");
        assert_ne!(
            workflow_producer_id("run-1"),
            activity_producer_id(&activity)
        );
    }

    #[test]
    fn a_stored_data_record_carries_its_plaintext_hash() {
        let stored = stored_append_record("out", "p", 2, 5, &data(0xab)).unwrap();
        assert_eq!(stored.body, Some(body()));
        assert_eq!(
            stored.metadata[CONTENT_HASH_KEY].data,
            "ab".repeat(HASH_LENGTH).into_bytes()
        );
        assert_eq!(
            stored.metadata[CONTENT_HASH_KEY].metadata["encoding"],
            b"binary/plain"
        );
        assert_eq!(
            (stored.producer_id.as_str(), stored.attempt, stored.sequence),
            ("p", 2, 5)
        );
        assert!(!stored.metadata.contains_key(RUN_ID_KEY));
    }

    #[test]
    fn a_workflow_record_is_written_by_its_run() {
        let stored = stored_output_record(
            "out",
            StreamRecordKind::Finish as i32,
            None,
            &[],
            "run-7",
            2,
            9,
        )
        .unwrap();
        assert_eq!(stored.metadata[RUN_ID_KEY].data, b"run-7");
        assert_eq!(
            (stored.producer_id.as_str(), stored.attempt, stored.sequence),
            ("@run-7", 2, 9)
        );
        assert!(stored.body.is_none());
        assert!(!stored.metadata.contains_key(CONTENT_HASH_KEY));
    }

    #[test]
    fn records_no_writer_stores_are_refused() {
        let mut no_body = data(1);
        no_body.body = None;
        let mut short_hash = data(1);
        short_hash.content_hash.truncate(4);
        let mut finish_with_body = finish();
        finish_with_body.body = Some(body());
        let unspecified = AppendRecord {
            kind: StreamRecordKind::Unspecified as i32,
            ..data(1)
        };
        for (record, expected) in [
            (no_body, "needs a body"),
            (short_hash, "not 4 bytes"),
            (finish_with_body, "carries no body"),
            (unspecified, "not kind 0"),
        ] {
            let error = stored_append_record("out", "p", 1, 1, &record).unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
            assert!(error.message.contains(expected), "{expected}: {error}");
        }
        let error = stored_append_record("", "p", 1, 1, &data(1)).unwrap_err();
        assert!(error.message.contains("needs a topic"));
    }

    #[test]
    fn an_append_needs_a_full_digest() {
        assert_eq!(check_append_digest(&[9; HASH_LENGTH]), Ok(()));
        for digest in [vec![], vec![9; 16]] {
            let error = check_append_digest(&digest).unwrap_err();
            assert_eq!(error.kind, StreamFailureKind::InvalidArgument);
            assert!(error.message.contains("32-byte digest"), "{error}");
        }
    }
}
