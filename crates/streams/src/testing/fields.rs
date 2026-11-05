//! The fields of the `:meta` hash. The field names hold encoded ids, so a `|` in a field name is a
//! separator. Only an `ow|` name ends in a binary task key, after its last separator.

use crate::{epoch::TASK_KEY_LEN, ids::encode_part};

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

/// The task keys of the run's Local Activity writes that no task facts showed completed yet, back
/// to back.
pub fn open_tasks(run_id: &str) -> String {
    format!("oa|{}", encode_part(run_id))
}

/// Set when a `Superseded` item withdrew the run's Workflow Task with the key `task_key`.
pub fn withdrawn_task(run_id: &str, task_key: &[u8; TASK_KEY_LEN]) -> Vec<u8> {
    [format!("ow|{}|", encode_part(run_id)).as_bytes(), task_key].concat()
}

/// The task facts of the run's highest task, as task keys back to back. These are its own key, the
/// completed task's key or the zero key before the first, and the key of each failed task's
/// started event id.
pub fn task_facts(run_id: &str) -> String {
    format!("tf|{}", encode_part(run_id))
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
        assert_eq!(open_tasks(run), text("open_tasks"));
        let task_key = crate::testing::support::hex(text("task_key"));
        assert_eq!(
            withdrawn_task(run, &task_key.as_slice().try_into().unwrap()),
            [text("withdrawn_task_prefix").as_bytes(), &task_key].concat()
        );
        assert_eq!(task_facts(run), text("task_facts"));
    }
}
