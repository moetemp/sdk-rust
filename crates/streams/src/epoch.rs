//! Producer epochs, their order, and the withdrawal rule of a `Superseded` item.
//!
//! A Store keeps an epoch as a key of fixed-width big-endian fields, in the order of comparison.
//! A byte comparison of two keys then orders the epochs field by field. `testdata/epochs.json` has
//! the shared vectors of the keys, in hex.

use crate::{
    StreamError, StreamResult,
    proto::{
        Producer, StreamRecord, StreamSuperseded, StreamWriter, WorkflowTaskEpoch, producer::Kind,
        stream_record::Item,
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

/// Refuses a Producer with no kind case.
impl TryFrom<&Producer> for ProducerKind {
    type Error = StreamError;

    fn try_from(producer: &Producer) -> StreamResult<Self> {
        kind_of(producer).ok_or_else(|| StreamError::invalid("The Producer has no kind"))
    }
}

/// The kind case of a Producer, `None` when it has none.
pub fn kind_of(producer: &Producer) -> Option<ProducerKind> {
    producer.kind.as_ref().map(ProducerKind::from)
}

/// The run of a Producer. Empty for a client, which has no run.
pub fn producer_run_id(producer: &Producer) -> &str {
    match &producer.kind {
        Some(Kind::Workflow(w)) => &w.run_id,
        Some(Kind::LocalActivity(l)) => &l.run_id,
        Some(Kind::Activity(a) | Kind::StandaloneActivity(a)) => &a.run_id,
        Some(Kind::Client(_)) | None => "",
    }
}

/// The run that a writer withdraws, empty for none.
pub fn writer_withdraw_run(writer: &StreamWriter) -> &str {
    writer
        .withdrawal
        .as_ref()
        .map_or("", |w| w.withdraw_run.as_str())
}

/// The started event ids of the run's failed Workflow Tasks that a writer reports.
pub fn writer_failed_tasks(writer: &StreamWriter) -> &[i64] {
    writer
        .withdrawal
        .as_ref()
        .map_or(&[], |w| w.failed_tasks.as_slice())
}

/// The run's last completed Workflow Task that a writer reports, `None` for none.
pub fn writer_previous_completed_task(writer: &StreamWriter) -> Option<&WorkflowTaskEpoch> {
    writer
        .withdrawal
        .as_ref()
        .and_then(|w| w.previous_completed_task.as_ref())
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
/// and the attempt, in 8, 4 and 4 bytes. A field that the case doesn't have is zero, and a Producer
/// with no case is all zeros.
pub fn epoch_key(kind: Option<&Kind>) -> [u8; EPOCH_KEY_LEN] {
    let (scheduled, attempt) = match kind {
        Some(Kind::LocalActivity(local)) => (None, local.epoch.map_or(0, |e| e.attempt)),
        Some(Kind::Activity(activity) | Kind::StandaloneActivity(activity)) => {
            let epoch = activity.epoch;
            (
                epoch.and_then(|e| e.attempt_scheduled_time),
                epoch.map_or(0, |e| e.attempt),
            )
        }
        Some(Kind::Client(client)) => (None, client.epoch.map_or(0, |e| e.attempt)),
        Some(Kind::Workflow(_)) | None => (None, 0),
    };
    let (seconds, nanos) = time_fields(scheduled.as_ref());
    let mut key = [0; EPOCH_KEY_LEN];
    key[..TASK_KEY_LEN].copy_from_slice(&task_key(&task_of(kind)));
    key[24..32].copy_from_slice(&be64(seconds));
    key[32..36].copy_from_slice(&be32(nanos));
    key[36..].copy_from_slice(&be32(attempt));
    key
}

/// The Workflow Task of an epoch. It is a Workflow's own task, or the task that dispatched a Local
/// Activity, and zero for the other kinds.
pub fn task_of(kind: Option<&Kind>) -> WorkflowTaskEpoch {
    match kind {
        Some(Kind::Workflow(w)) => w.epoch.unwrap_or_default(),
        Some(Kind::LocalActivity(local)) => local.epoch.and_then(|e| e.task).unwrap_or_default(),
        _ => WorkflowTaskEpoch::default(),
    }
}

/// Orders two epochs field by field.
pub fn compare_epochs(a: Option<&Kind>, b: Option<&Kind>) -> Ordering {
    epoch_key(a).cmp(&epoch_key(b))
}

fn compare_tasks(a: &WorkflowTaskEpoch, b: &WorkflowTaskEpoch) -> Ordering {
    task_key(a).cmp(&task_key(b))
}

/// Whether a `Superseded` item withdraws an earlier Record. The case of the epoch of the item's
/// writer picks the rule.
///
/// For an Activity or a Local Activity, it withdraws that Producer's Records with a lower epoch.
/// For a Workflow task t with the previous completed task c, it withdraws the Records of the run
/// and of its Local Activities whose task is in `failed_tasks` or strictly between c and t, and
/// those at c or at t with a lower epoch. The item of a retry run withdraws every Record of the
/// previous run. A client's newer epoch fences the earlier ones but withdraws nothing, since a
/// restarted client's earlier Records are real messages, not a failed attempt's.
pub fn withdraws(superseded: &StreamSuperseded, record: &StreamRecord) -> bool {
    let Some(writer) = superseded.writer.as_ref() else {
        return false;
    };
    let Some(Item::Data(data)) = &record.item else {
        return false;
    };
    let Some(producer) = data.producer.as_ref() else {
        return false;
    };
    let withdraw_run = writer_withdraw_run(writer);
    if !withdraw_run.is_empty() && producer_run_id(producer) == withdraw_run {
        return true;
    }
    let Some(withdrawing) = writer.producer.as_ref() else {
        return false;
    };
    let (new, old) = (withdrawing.kind.as_ref(), producer.kind.as_ref());
    let (Some(new_kind), Some(old_kind)) = (new, old) else {
        return false;
    };
    match ProducerKind::from(new_kind) {
        ProducerKind::Client => return false,
        ProducerKind::Workflow => {}
        same => {
            return ProducerKind::from(old_kind) == same
                && producer.id == withdrawing.id
                && compare_epochs(old, new) == Ordering::Less;
        }
    }
    if !of_run(producer, withdrawing) {
        return false;
    }
    let (task, old_task) = (task_of(new), task_of(old));
    let started = old_task.started_event_id;
    if writer_failed_tasks(writer).contains(&started) {
        return true;
    }
    let completed = writer_previous_completed_task(writer);
    let after = completed.map_or(0, |c| c.started_event_id);
    if after < started && started < task.started_event_id {
        return true;
    }
    let lower_at = |at: &WorkflowTaskEpoch| {
        started == at.started_event_id && compare_tasks(&old_task, at) == Ordering::Less
    };
    lower_at(&task) || completed.is_some_and(lower_at)
}

/// Whether a Producer is the Workflow run that withdraws, or one of its Local Activities.
fn of_run(producer: &Producer, withdrawing: &Producer) -> bool {
    match kind_of(producer) {
        Some(ProducerKind::Workflow) => producer.id == withdrawing.id,
        Some(ProducerKind::LocalActivity) => {
            producer_run_id(producer) == producer_run_id(withdrawing)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ClientEpoch, ClientProducer, producer::Kind};

    // Without the clamp, the bytes of a negative field would outrank every real epoch.
    #[test]
    fn a_negative_field_compares_as_zero() {
        let negative = Kind::Client(ClientProducer {
            epoch: Some(ClientEpoch { attempt: -1 }),
        });
        assert_eq!(compare_epochs(Some(&negative), None), Ordering::Equal);
    }
}
