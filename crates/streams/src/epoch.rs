//! Producer epochs, their order, and the withdrawal rule of a `Superseded` item.
//!
//! The Lua scripts compare epochs as strings. So an epoch is stored in Redis as a key of
//! fixed-width hex fields, in the order of comparison. A string comparison of two keys then orders
//! the epochs field by field. `testdata/epochs.json` has the shared vectors of the keys.

use crate::proto::{
    Producer, ProducerEpoch, StreamProducerKind, StreamRecord, Supersession, WorkflowTaskEpoch,
    stream_record::Item,
};
use prost_types::Timestamp;
use std::cmp::Ordering;

fn hex64(value: i64) -> String {
    format!("{:016x}", value.max(0))
}

fn hex32(value: i32) -> String {
    format!("{:08x}", value.max(0))
}

fn time_fields(time: Option<&Timestamp>) -> [String; 2] {
    let (seconds, nanos) = time.map_or((0, 0), |time| (time.seconds, time.nanos));
    [hex64(seconds), hex32(nanos)]
}

/// The key of a Workflow Task attempt, from its started event id, attempt and started time. The
/// epoch keys of the run and of the task's Local Activities start with it.
pub fn task_key(task: &WorkflowTaskEpoch) -> String {
    let [seconds, nanos] = time_fields(task.started_time.as_ref());
    [
        hex64(task.started_event_id),
        hex32(task.attempt),
        seconds,
        nanos,
    ]
    .join(".")
}

/// The key of an epoch. Fields that a kind doesn't use compare as zero.
pub fn epoch_key(epoch: &ProducerEpoch) -> String {
    let [scheduled_seconds, scheduled_nanos] = time_fields(epoch.attempt_scheduled_time.as_ref());
    [
        task_key(&task_of(epoch)),
        scheduled_seconds,
        scheduled_nanos,
        hex32(epoch.attempt),
    ]
    .join(".")
}

/// The Workflow Task fields of an epoch.
pub fn task_of(epoch: &ProducerEpoch) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id: epoch.task_started_event_id,
        attempt: epoch.task_attempt,
        started_time: epoch.task_started_time,
    }
}

/// Orders two epochs field by field.
pub fn compare_epochs(a: &ProducerEpoch, b: &ProducerEpoch) -> Ordering {
    epoch_key(a).cmp(&epoch_key(b))
}

fn compare_tasks(a: &WorkflowTaskEpoch, b: &WorkflowTaskEpoch) -> Ordering {
    task_key(a).cmp(&task_key(b))
}

/// Whether a `Superseded` item withdraws an earlier Record.
///
/// For an Activity or a Local Activity, it withdraws that Producer's Records with a lower epoch.
/// For a Workflow task t with the previous completed task c, it withdraws the Records of the run
/// and of its Local Activities whose task is in `failed_tasks` or strictly between c and t, and
/// those at c or at t with a lower epoch. The item of a retry run withdraws every Record of the
/// previous run. A client's newer epoch fences the earlier ones but withdraws nothing, since a
/// restarted client's earlier Records are real messages, not a failed attempt's.
pub fn withdraws(supersession: &Supersession, record: &StreamRecord) -> bool {
    let Some(Item::Data(data)) = &record.item else {
        return false;
    };
    let Some(producer) = data.producer.as_ref() else {
        return false;
    };
    if !supersession.withdraw_run.is_empty() && producer.run_id == supersession.withdraw_run {
        return true;
    }
    let kind = StreamProducerKind::try_from(supersession.producer_kind).unwrap_or_default();
    if kind == StreamProducerKind::Client {
        return false;
    }
    let (Some(epoch), Some(old)) = (supersession.epoch.as_ref(), producer.epoch.as_ref()) else {
        return false;
    };
    if kind != StreamProducerKind::Workflow {
        return producer.kind == supersession.producer_kind
            && producer.id == supersession.producer_id
            && compare_epochs(old, epoch) == Ordering::Less;
    }
    if !of_run(producer, supersession) {
        return false;
    }
    let (task, old) = (task_of(epoch), task_of(old));
    let started = old.started_event_id;
    if supersession.failed_tasks.contains(&started) {
        return true;
    }
    let completed = supersession.previous_completed_task.as_ref();
    let after = completed.map_or(0, |c| c.started_event_id);
    if after < started && started < task.started_event_id {
        return true;
    }
    let lower_at = |at: &WorkflowTaskEpoch| {
        started == at.started_event_id && compare_tasks(&old, at) == Ordering::Less
    };
    lower_at(&task) || completed.is_some_and(lower_at)
}

/// Whether a Producer is the Workflow run of a `Superseded` item, or one of its Local
/// Activities.
fn of_run(producer: &Producer, supersession: &Supersession) -> bool {
    let kind = StreamProducerKind::try_from(producer.kind).unwrap_or_default();
    match kind {
        StreamProducerKind::Workflow => producer.id == supersession.producer_id,
        StreamProducerKind::LocalActivity => producer.run_id == supersession.run_id,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Without the clamp, the hex of a negative field would outrank every real epoch.
    #[test]
    fn a_negative_field_compares_as_zero() {
        let negative = ProducerEpoch {
            attempt: -1,
            ..Default::default()
        };
        assert_eq!(
            compare_epochs(&negative, &ProducerEpoch::default()),
            Ordering::Equal
        );
    }
}
