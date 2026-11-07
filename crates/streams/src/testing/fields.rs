//! The fields of the `:meta` hash. The field names hold encoded ids only, so a `|` in a field name
//! is always a separator.

use crate::ids::encode_part;

/// The format version of the Stream.
pub const VERSION: &str = "v";

/// The newest epoch key of a Producer, the fence.
pub fn fence(kind: i32, producer_id: &str) -> String {
    format!("e|{kind}|{producer_id}")
}

/// The last batch of a Producer's newest epoch, as
/// `<epoch key>|<sequence>|<first>|<last>|<digest>`. These are the dedupe key, the first and last
/// entry ids of the batch, and the content digest that a resend must match.
pub fn dedupe(kind: i32, producer_id: &str) -> String {
    format!("d|{kind}|{producer_id}")
}

/// The newest epoch key of a Producer that still has Records in the Stream.
pub fn holds(kind: i32, producer_id: &str) -> String {
    format!("h|{kind}|{producer_id}")
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
        let (producer, run) = (text("producer_id"), text("run_id"));
        assert_eq!(fence(kind, producer), text("fence"));
        assert_eq!(dedupe(kind, producer), text("dedupe"));
        assert_eq!(holds(kind, producer), text("holds"));
        assert_eq!(run_wrote(run), text("run_wrote"));
        assert_eq!(withdrawn_run(run), text("withdrawn_run"));
    }
}
