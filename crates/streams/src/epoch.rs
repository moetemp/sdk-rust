//! Producer epochs, their order, and the withdrawal rule of a `Superseded` item.
//!
//! A Store keeps an epoch as a key of fixed-width big-endian fields, in the order of comparison.
//! A byte comparison of two keys then orders the epochs field by field. `testdata/epochs.json` has
//! the shared vectors of the keys, in hex.

use crate::{
    StreamError, StreamResult,
    proto::{
        Producer, ProducerEpoch, StreamRecord, StreamSuperseded, WorkflowTaskEpoch,
        producer_epoch::Kind, stream_record::Item,
    },
};
use prost_types::Timestamp;
use std::cmp::Ordering;

/// The kind of a Producer, which is the case of its epoch. The stored field names and keys hold
/// its number, so each kind keeps its number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProducerKind {
    /// A Workflow run.
    Workflow = 1,
    /// A Local Activity.
    LocalActivity = 2,
    /// An Activity of a Workflow.
    Activity = 3,
    /// A client.
    Client = 4,
    /// A Standalone Activity.
    StandaloneActivity = 5,
}

impl From<&Kind> for ProducerKind {
    fn from(kind: &Kind) -> Self {
        match kind {
            Kind::Workflow(_) => Self::Workflow,
            Kind::LocalActivity(_) => Self::LocalActivity,
            Kind::Activity(_) => Self::Activity,
            Kind::StandaloneActivity(_) => Self::StandaloneActivity,
            Kind::Client(_) => Self::Client,
        }
    }
}

/// Refuses a Producer with no epoch, or with an epoch that has no case.
impl TryFrom<&Producer> for ProducerKind {
    type Error = StreamError;

    fn try_from(producer: &Producer) -> StreamResult<Self> {
        let epoch = producer
            .epoch
            .as_ref()
            .ok_or_else(|| StreamError::invalid("The Producer has no epoch"))?;
        kind(epoch).ok_or_else(|| StreamError::invalid("The Producer's epoch has no kind"))
    }
}

/// The kind of an epoch's case, `None` when the epoch has no case.
fn kind(epoch: &ProducerEpoch) -> Option<ProducerKind> {
    epoch.kind.as_ref().map(ProducerKind::from)
}

/// The length of a [task_key].
pub const TASK_KEY_LEN: usize = 24;
/// The length of an [epoch_key].
pub const EPOCH_KEY_LEN: usize = 40;

// A negative field is clamped at zero, since its bytes would outrank every real epoch.
fn be64(value: i64) -> [u8; 8] {
    value.max(0).to_be_bytes()
}

fn be32(value: i32) -> [u8; 4] {
    value.max(0).to_be_bytes()
}

fn time_fields(time: Option<&Timestamp>) -> (i64, i32) {
    time.map_or((0, 0), |time| (time.seconds, time.nanos))
}

/// The key of a Workflow Task attempt. It is the started event id, the attempt, and the started
/// seconds and nanos, in 8, 4, 8 and 4 bytes. The epoch keys of the run and of the task's Local
/// Activities start with it.
pub fn task_key(task: &WorkflowTaskEpoch) -> [u8; TASK_KEY_LEN] {
    let (seconds, nanos) = time_fields(task.started_time.as_ref());
    let mut key = [0; TASK_KEY_LEN];
    key[..8].copy_from_slice(&be64(task.started_event_id));
    key[8..12].copy_from_slice(&be32(task.attempt));
    key[12..20].copy_from_slice(&be64(seconds));
    key[20..].copy_from_slice(&be32(nanos));
    key
}

/// The key of an epoch. It is the [task_of] key, then the attempt's scheduled seconds and nanos
/// and the attempt, in 8, 4 and 4 bytes. A field that the case doesn't have is zero, and an epoch
/// with no case is all zeros.
pub fn epoch_key(epoch: &ProducerEpoch) -> [u8; EPOCH_KEY_LEN] {
    let (scheduled, attempt) = match &epoch.kind {
        Some(Kind::LocalActivity(local)) => (None, local.attempt),
        Some(Kind::Activity(activity) | Kind::StandaloneActivity(activity)) => {
            (activity.attempt_scheduled_time.as_ref(), activity.attempt)
        }
        Some(Kind::Client(client)) => (None, client.attempt),
        Some(Kind::Workflow(_)) | None => (None, 0),
    };
    let (seconds, nanos) = time_fields(scheduled);
    let mut key = [0; EPOCH_KEY_LEN];
    key[..TASK_KEY_LEN].copy_from_slice(&task_key(&task_of(epoch)));
    key[24..32].copy_from_slice(&be64(seconds));
    key[32..36].copy_from_slice(&be32(nanos));
    key[36..].copy_from_slice(&be32(attempt));
    key
}

/// The Workflow Task of an epoch. It is a Workflow's own task, or the task that dispatched a Local
/// Activity, and zero for the other kinds.
pub fn task_of(epoch: &ProducerEpoch) -> WorkflowTaskEpoch {
    match &epoch.kind {
        Some(Kind::Workflow(task)) => *task,
        Some(Kind::LocalActivity(local)) => local.task.unwrap_or_default(),
        _ => WorkflowTaskEpoch::default(),
    }
}

/// Orders two epochs field by field.
pub fn compare_epochs(a: &ProducerEpoch, b: &ProducerEpoch) -> Ordering {
    epoch_key(a).cmp(&epoch_key(b))
}

fn compare_tasks(a: &WorkflowTaskEpoch, b: &WorkflowTaskEpoch) -> Ordering {
    task_key(a).cmp(&task_key(b))
}

/// Whether a `Superseded` item withdraws an earlier Record. The case of the item's epoch picks the
/// rule.
///
/// For an Activity or a Local Activity, it withdraws that Producer's Records with a lower epoch.
/// For a Workflow task t with the previous completed task c, it withdraws the Records of the run
/// and of its Local Activities whose task is in `failed_tasks` or strictly between c and t, and
/// those at c or at t with a lower epoch. The item of a retry run withdraws every Record of the
/// previous run. A client's newer epoch fences the earlier ones but withdraws nothing, since a
/// restarted client's earlier Records are real messages, not a failed attempt's.
pub fn withdraws(superseded: &StreamSuperseded, record: &StreamRecord) -> bool {
    let Some(Item::Data(data)) = &record.item else {
        return false;
    };
    let Some(producer) = data.producer.as_ref() else {
        return false;
    };
    if !superseded.withdraw_run.is_empty() && producer.run_id == superseded.withdraw_run {
        return true;
    }
    let (Some(epoch), Some(old)) = (superseded.epoch.as_ref(), producer.epoch.as_ref()) else {
        return false;
    };
    match kind(epoch) {
        None | Some(ProducerKind::Client) => return false,
        Some(ProducerKind::Workflow) => {}
        same => {
            return kind(old) == same
                && producer.id == superseded.producer_id
                && compare_epochs(old, epoch) == Ordering::Less;
        }
    }
    if !of_run(producer, superseded) {
        return false;
    }
    let (task, old) = (task_of(epoch), task_of(old));
    let started = old.started_event_id;
    if superseded.failed_tasks.contains(&started) {
        return true;
    }
    let completed = superseded.previous_completed_task.as_ref();
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
fn of_run(producer: &Producer, superseded: &StreamSuperseded) -> bool {
    match producer.epoch.as_ref().and_then(kind) {
        Some(ProducerKind::Workflow) => producer.id == superseded.producer_id,
        Some(ProducerKind::LocalActivity) => producer.run_id == superseded.run_id,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ClientEpoch;

    // Without the clamp, the bytes of a negative field would outrank every real epoch.
    #[test]
    fn a_negative_field_compares_as_zero() {
        let negative = ProducerEpoch {
            kind: Some(Kind::Client(ClientEpoch { attempt: -1 })),
        };
        assert_eq!(
            compare_epochs(&negative, &ProducerEpoch::default()),
            Ordering::Equal
        );
    }
}
