//! The content digest of a batch. Each dedupe entry stores it, so that a resend with the same
//! `sequence` and other content fails instead of being dropped without an error. The Store compares
//! the digest that this function gives, and `testdata/digests.json` has its expected values.

use crate::proto::AppendRecord;
use sha2::{Digest, Sha256};

/// SHA-256 over the Records in order, as 64 lowercase hex characters. Each part has its length
/// before it, so that two different batches never hash the same bytes. The metadata is hashed in
/// key order, since a map has no order on the wire.
pub fn batch_digest(records: &[AppendRecord]) -> String {
    let mut hasher = Sha256::new();
    for record in records {
        // A Record with no body and one with an empty body are two different stored Records.
        let Some(body) = &record.body else {
            hasher.update([0]);
            continue;
        };
        hasher.update([1]);
        let mut metadata: Vec<_> = body.metadata.iter().collect();
        metadata.sort();
        hasher.update((metadata.len() as u64).to_be_bytes());
        for (key, value) in metadata {
            part(&mut hasher, key.as_bytes());
            part(&mut hasher, value);
        }
        part(&mut hasher, &body.data);
        hasher.update((body.external_payloads.len() as u64).to_be_bytes());
        for external in &body.external_payloads {
            hasher.update(external.size_bytes.to_be_bytes());
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}
