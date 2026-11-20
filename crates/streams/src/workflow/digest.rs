//! The digest of the Records that a read gave Workflow code in one Workflow Task. A marker keeps it
//! in place of the Records, and a replay checks the Records that it reads again against it.

use crate::{
    attempt::{attempt_key, producer_key},
    proto::StreamRecord,
};
use sha2::{Digest as _, Sha256};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The length of a digest in a marker, in bytes. It's a fact of the marker's format.
pub(crate) const DIGEST_LEN: usize = 16;

/// The first byte of the hashed input, so the digest differs from other hashes of the same bytes.
/// It's a fact of the digest's format.
const RECORDS_TAG: u8 = b'R';

/// The first 16 bytes of a SHA-256 over Records in their order, which takes them one page at a
/// time. History keeps the digest for good, so it hashes a fixed encoding that this crate owns. A
/// field that a later version adds to a message doesn't change it.
///
/// The input starts with the byte `R`, and each Record adds:
///
/// - its offset,
/// - its Producer, which is the byte 0 if it has none, or else the byte 1, its [`producer_key`]
///   with its kind and its ids (empty for a Producer with no kind) and its [`attempt_key`] (40
///   bytes),
/// - its body, which is the byte 0 if it has none, or else the byte 1, the count of its metadata,
///   each key and value in key order, its data, the count of its external payloads and the size
///   of each (a big-endian `i64`).
///
/// An offset, a Producer key, a metadata key or value, and the data have their length before them.
/// A length or a count is a big-endian `u64`.
#[derive(Clone, Debug)]
pub(crate) struct Digest(Sha256);

impl Digest {
    /// Creates the digest of no Records.
    pub(crate) fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update([RECORDS_TAG]);
        Self(hasher)
    }

    /// Adds `records` after the Records that the digest already has.
    pub(crate) fn add(&mut self, records: &[StreamRecord]) {
        let hasher = &mut self.0;
        for record in records {
            part(hasher, &record.offset);
            match &record.producer {
                Some(producer) => {
                    hasher.update([1]);
                    part(hasher, &producer_key(producer).unwrap_or_default());
                    hasher.update(attempt_key(producer.kind.as_ref()));
                }
                None => hasher.update([0]),
            }
            body(hasher, record.body.as_ref());
        }
    }

    /// Returns the digest.
    pub(crate) fn finish(self) -> Vec<u8> {
        self.0.finalize()[..DIGEST_LEN].to_vec()
    }
}

fn body(hasher: &mut Sha256, body: Option<&Payload>) {
    // A Record with no body and one with an empty body are two different stored Records.
    let Some(body) = body else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    let mut metadata: Vec<_> = body.metadata.iter().collect();
    metadata.sort();
    hasher.update((metadata.len() as u64).to_be_bytes());
    for (key, value) in metadata {
        part(hasher, key.as_bytes());
        part(hasher, value);
    }
    part(hasher, &body.data);
    hasher.update((body.external_payloads.len() as u64).to_be_bytes());
    for external in &body.external_payloads {
        hasher.update(external.size_bytes.to_be_bytes());
    }
}

fn part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_store::workflow;
    use temporalio_common::protos::temporal::api::{
        common::v1::payload::ExternalPayloadDetails, stream::v1::WorkflowTaskAttempt,
    };

    fn record(offset: u8, started_event_id: Option<i64>, body: Option<Payload>) -> StreamRecord {
        let task_attempt = |started_event_id| WorkflowTaskAttempt {
            started_event_id,
            attempt: 1,
            started_time: None,
        };
        StreamRecord {
            offset: vec![offset],
            producer: started_event_id.map(|id| workflow(Some(task_attempt(id)))),
            body,
        }
    }

    // History keeps these digests, so a change to them breaks the replay of existing Workflows.
    #[test]
    fn the_digest_of_known_records_never_changes() {
        let hello = Some(Payload {
            data: b"hello".to_vec(),
            ..Default::default()
        });
        // Prost keeps metadata in a `HashMap`, which is unlikely to give six keys in key order.
        let six_keys = Some(Payload {
            metadata: [
                ("type", "Greeting"),
                ("encoding", "json/plain"),
                ("zone", "eu"),
                ("app", "shop"),
                ("lang", "en"),
                ("id", "42"),
            ]
            .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
            .into(),
            data: b"hi".to_vec(),
            external_payloads: vec![ExternalPayloadDetails { size_bytes: 1024 }],
        });
        // A Producer of a kind that this build doesn't know.
        let no_kind = StreamRecord {
            producer: Some(Default::default()),
            ..record(2, None, None)
        };
        let cases = [
            (vec![], "8c2574892063f995fdf756bce07f46c1"),
            (
                vec![record(1, Some(3), hello.clone())],
                "c65ec4f6a6c8ac497c05ccc7b7b71433",
            ),
            (
                vec![record(2, Some(3), hello.clone())],
                "947f076d8990d24b84de0e768aab97e8",
            ),
            (
                vec![record(1, Some(9), hello.clone())],
                "51c910ffa73cdaf304e2b0dddcac6d45",
            ),
            (
                vec![record(2, None, None)],
                "fdc49188bf705cbb99846b2b09d1d243",
            ),
            (vec![no_kind], "e96275103899b9814dc13cb5586f68ee"),
            (
                vec![record(1, Some(3), six_keys.clone())],
                "b7adace12409efc8371e091d8bcb35ab",
            ),
            (
                vec![record(1, Some(3), hello), record(9, Some(3), six_keys)],
                "324ecb8f98eff4e0e97a7be0eedd7738",
            ),
        ];
        for (records, expected) in cases {
            let mut whole = Digest::new();
            whole.add(&records);
            let whole = whole.finish();
            let hex: String = whole.iter().map(|byte| format!("{byte:02x}")).collect();
            assert_eq!(hex, expected, "{records:?}");
            // Two pages give the digest of one.
            for cut in 0..=records.len() {
                let mut pages = Digest::new();
                pages.add(&records[..cut]);
                pages.add(&records[cut..]);
                assert_eq!(pages.finish(), whole, "{records:?} cut at {cut}");
            }
        }
    }
}
