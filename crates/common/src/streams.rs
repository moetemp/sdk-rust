//! What every SDK and store must compute the same way for stream records: their stored metadata
//! keys and the fingerprint that names a batch of them.

use sha2::{Digest, Sha256};

/// The record metadata key a body's plaintext hash is stored under, as a `binary/plain` payload
/// whose data is the hex SHA-256.
pub const CONTENT_HASH_KEY: &str = "temporal.io/content-hash";

/// The record metadata key the publishing run id is stored under, as a `binary/plain` payload, on
/// every record a Workflow publishes.
pub const RUN_ID_KEY: &str = "temporal.io/run-id";

/// The encoding of the payloads stored under [CONTENT_HASH_KEY] and [RUN_ID_KEY].
pub const METADATA_ENCODING: &[u8] = b"binary/plain";

/// The longest topic, in UTF-8 bytes, every store keys reliably.
pub const MAX_TOPIC_BYTES: usize = 256;

/// Refuses a topic no store keys reliably, with the reason. A control character could be a
/// separator a store uses.
pub fn check_topic(topic: &str) -> Result<(), String> {
    if topic.is_empty() {
        return Err("a stream needs a topic".to_string());
    }
    if topic.len() > MAX_TOPIC_BYTES {
        return Err(format!(
            "a topic is at most {MAX_TOPIC_BYTES} UTF-8 bytes, not {}",
            topic.len()
        ));
    }
    if topic.chars().any(char::is_control) {
        return Err(format!("topic {topic:?} holds a control character"));
    }
    Ok(())
}

/// The fingerprint version [fingerprint] computes.
pub const FINGERPRINT_VERSION: u32 = 2;

/// What one record contributes to a fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FingerprintRecord<'a> {
    /// The topic the record is on.
    pub topic: &'a str,
    /// The `temporal.sdk.streams.v1.StreamRecordKind` value.
    pub kind: i32,
    /// The SHA-256 of the body as the payload converter made it, or empty for a record with no
    /// body.
    pub content_hash: &'a [u8],
}

/// The version 2 fingerprint of `records`, in order.
///
/// Each record is its topic, its kind and its body's plaintext hash, so the fingerprint never
/// depends on the payload codec. A codec that encrypts with a fresh nonce would otherwise make a
/// retry look like different content. The layout is explicit rather than a protobuf
/// serialization, because map order makes those differ between SDKs. Every part is
/// length-delimited, so a batch split or joined differently can't collide with this one.
pub fn fingerprint<'a>(records: impl IntoIterator<Item = FingerprintRecord<'a>>) -> [u8; 32] {
    let mut digest = Sha256::new();
    for record in records {
        let mut entry = Vec::with_capacity(28 + record.topic.len() + record.content_hash.len());
        entry.extend_from_slice(&(record.topic.len() as u64).to_be_bytes());
        entry.extend_from_slice(record.topic.as_bytes());
        entry.extend_from_slice(&(record.kind as u32).to_be_bytes());
        entry.extend_from_slice(&(record.content_hash.len() as u64).to_be_bytes());
        entry.extend_from_slice(record.content_hash);
        digest.update((entry.len() as u64).to_be_bytes());
        digest.update(&entry);
    }
    digest.finalize().into()
}

/// The hex text a content hash is stored as under [CONTENT_HASH_KEY].
pub fn content_hash_text(content_hash: &[u8]) -> String {
    content_hash.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: i32 = 1;
    const FINISH: i32 = 2;

    fn hex(bytes: &[u8]) -> String {
        content_hash_text(bytes)
    }

    #[test]
    fn the_fingerprint_matches_the_published_vector() {
        // Go and Java compute this by hand from the wire contract, so its value is fixed.
        let hash = [7u8; 32];
        let records = [
            FingerprintRecord {
                topic: "out",
                kind: DATA,
                content_hash: &hash,
            },
            FingerprintRecord {
                topic: "out",
                kind: FINISH,
                content_hash: &[],
            },
        ];
        assert_eq!(
            hex(&fingerprint(records)),
            "f27b597233a351a424a541ef15a5ef7680865c1b6c53d56918eaeffdab3eda0e"
        );
    }

    #[test]
    fn the_fingerprint_depends_on_order_kind_topic_and_hash() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let base = fingerprint([
            FingerprintRecord {
                topic: "t",
                kind: DATA,
                content_hash: &a,
            },
            FingerprintRecord {
                topic: "t",
                kind: DATA,
                content_hash: &b,
            },
        ]);
        let swapped = fingerprint([
            FingerprintRecord {
                topic: "t",
                kind: DATA,
                content_hash: &b,
            },
            FingerprintRecord {
                topic: "t",
                kind: DATA,
                content_hash: &a,
            },
        ]);
        let other_kind = fingerprint([
            FingerprintRecord {
                topic: "t",
                kind: FINISH,
                content_hash: &a,
            },
            FingerprintRecord {
                topic: "t",
                kind: DATA,
                content_hash: &b,
            },
        ]);
        let other_topic = fingerprint([
            FingerprintRecord {
                topic: "u",
                kind: DATA,
                content_hash: &a,
            },
            FingerprintRecord {
                topic: "t",
                kind: DATA,
                content_hash: &b,
            },
        ]);
        assert_ne!(base, swapped);
        assert_ne!(base, other_kind);
        assert_ne!(base, other_topic);
    }

    #[test]
    fn a_topic_cannot_borrow_bytes_from_its_neighbour() {
        let ab = fingerprint([
            FingerprintRecord {
                topic: "ab",
                kind: DATA,
                content_hash: &[],
            },
            FingerprintRecord {
                topic: "c",
                kind: DATA,
                content_hash: &[],
            },
        ]);
        let a_bc = fingerprint([
            FingerprintRecord {
                topic: "a",
                kind: DATA,
                content_hash: &[],
            },
            FingerprintRecord {
                topic: "bc",
                kind: DATA,
                content_hash: &[],
            },
        ]);
        assert_ne!(ab, a_bc);
    }

    #[test]
    fn the_stored_hash_is_lower_case_hex() {
        assert_eq!(content_hash_text(&[0x0a, 0xff]), "0aff");
    }
}
