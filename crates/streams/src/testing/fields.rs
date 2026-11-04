//! The fields of the `:meta` hash. The field names hold encoded ids only, so a `|` in a field name
//! is always a separator.

use crate::ids::encode_part;

/// The format version of the Stream.
pub const VERSION: &str = "v";

/// The field of a Producer. Its value is 129 bytes, with big-endian numbers:
///
/// - 1, the flags. Bit 0 is set when the held epoch is present, and the other bits are zero.
/// - 40, the fence, the Producer's newest epoch key. It is also the epoch of the last batch.
/// - 40, the held epoch key, the newest one that still has Records, or zeros.
/// - 8, the last batch's sequence.
/// - 12 and 12, the last batch's first and last entry ids, each the ms in 8 bytes and the seq in
///   4. They are zero when the batch wrote no entry.
/// - 16, the last batch's digest, which a resend must match.
///
/// A Store refuses a value of another length or with another flag bit.
pub fn producer(kind: i32, producer_id: &str) -> String {
    format!("p|{kind}|{producer_id}")
}

/// Set when a Producer of the run wrote a Record to the Stream.
pub fn run_wrote(run_id: &str) -> String {
    format!("run|{}", encode_part(run_id))
}

/// Set when a Workflow retry withdrew the run.
pub fn withdrawn_run(run_id: &str) -> String {
    format!("wr|{}", encode_part(run_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fields_match_the_shared_vectors() {
        let case: serde_json::Value =
            serde_json::from_str(include_str!("../../testdata/fields.json")).unwrap();
        let text = |field: &str| case[field].as_str().unwrap();
        let kind = case["kind"].as_i64().unwrap() as i32;
        let (producer_id, run) = (text("producer_id"), text("run_id"));
        assert_eq!(producer(kind, producer_id), text("producer"));
        assert_eq!(run_wrote(run), text("run_wrote"));
        assert_eq!(withdrawn_run(run), text("withdrawn_run"));
    }
}
