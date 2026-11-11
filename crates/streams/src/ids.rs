//! Stream ids, Producer ids and the Redis key layout. Every SDK must build them the same way, and
//! `testdata/ids.json` has the shared vectors that check it.
//!
//! ```text
//! <prefix>:{<stream id>}:meta     the create key, the fence, dedupe and close state
//! <prefix>:{<stream id>}:log      the log, one entry per Record
//! <prefix>:{<stream id>}:closed   the entry that a close adds, which wakes a waiting read
//! ```
//!
//! A key has no Temporal namespace, so a Stream id is unique in one Store and key prefix. The part
//! in braces is the Redis Cluster hash tag, so that the keys are in one slot for one script. Each
//! part is percent-encoded, so a `:` or a brace in an id can't make two Streams share a key.

use crate::{
    StreamError, StreamResult,
    epoch::{ProducerKind, producer_run_id},
    proto::{Producer, WorkflowTaskEpoch, producer::Kind},
};

/// The longest Stream id, in bytes.
pub const MAX_STREAM_ID_BYTES: usize = 4096;

/// The key prefix when the backend sets none.
pub const DEFAULT_KEY_PREFIX: &str = "temporal-streams";

/// Percent-encodes every byte but the unreserved ones, as Python's `quote(text, safe="")` does.
pub fn encode_part(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Decodes the text of [encode_part]. Returns `None` for text that [encode_part] can't make.
pub fn decode_part(text: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut rest = text.as_bytes();
    while let Some((&byte, tail)) = rest.split_first() {
        if byte == b'%' {
            let hex = std::str::from_utf8(tail.get(..2)?).ok()?;
            bytes.push(u8::from_str_radix(hex, 16).ok()?);
            rest = &tail[2..];
        } else {
            bytes.push(byte);
            rest = tail;
        }
    }
    String::from_utf8(bytes).ok()
}

fn join(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|part| encode_part(part))
        .collect::<Vec<_>>()
        .join(":")
}

/// The id of a Stream that a Workflow creates with a name, `<workflow id>:<first run id>:<name>`.
pub fn scoped_stream_id(workflow_id: &str, first_run_id: &str, name: &str) -> String {
    join(&[workflow_id, first_run_id, name])
}

/// Refuses an empty or too long Stream id.
pub fn check_stream_id(id: &str) -> StreamResult<()> {
    if id.is_empty() {
        return Err(StreamError::invalid("The Stream id is empty"));
    }
    if id.len() > MAX_STREAM_ID_BYTES {
        return Err(StreamError::invalid(format!(
            "The Stream id has {} bytes, more than {MAX_STREAM_ID_BYTES}",
            id.len()
        )));
    }
    Ok(())
}

/// The Producer id of a Workflow run, `wf:<run id>`.
pub fn workflow_producer_id(run_id: &str) -> String {
    format!("wf:{}", join(&[run_id]))
}

/// The Producer id of a Local Activity call, `la:<run id>:<call key>`.
pub fn local_activity_producer_id(run_id: &str, call_key: &str) -> String {
    format!("la:{}", join(&[run_id, call_key]))
}

/// The Producer id of an Activity, `act:<run id>:<activity id>:<schedule key>`.
pub fn activity_producer_id(run_id: &str, activity_id: &str, schedule_key: &str) -> String {
    format!("act:{}", join(&[run_id, activity_id, schedule_key]))
}

/// The Producer id of a Standalone Activity, `sa:<activity id>:<activity run id>`.
pub fn standalone_activity_producer_id(activity_id: &str, activity_run_id: &str) -> String {
    format!("sa:{}", join(&[activity_id, activity_run_id]))
}

/// The Producer id of a client process, `client:<identity>:<process key>`.
pub fn client_producer_id(identity: &str, process_key: &str) -> String {
    format!("client:{}", join(&[identity, process_key]))
}

/// The Producer id that a client names itself, `client:<name>`. The caller keeps the name across
/// restarts and raises the epoch on each one, so that a new incarnation fences the earlier one.
pub fn named_client_producer_id(name: &str) -> String {
    format!("client:{}", join(&[name]))
}

/// The id prefix of a Producer kind.
pub(crate) fn kind_prefix(kind: ProducerKind) -> &'static str {
    match kind {
        ProducerKind::Workflow => "wf:",
        ProducerKind::LocalActivity => "la:",
        ProducerKind::Activity => "act:",
        ProducerKind::StandaloneActivity => "sa:",
        ProducerKind::Client => "client:",
    }
}

/// Checks that a Producer's epoch has a case, that its id has the prefix of that kind, and that the
/// run in the id is its `run_id`. The kind prefix stops a Producer from using the id of another
/// kind.
pub fn check_producer(producer: &Producer) -> StreamResult<()> {
    let kind = ProducerKind::try_from(producer)?;
    let prefix = kind_prefix(kind);
    let Some(rest) = producer.id.strip_prefix(prefix) else {
        return Err(StreamError::invalid(format!(
            "The Producer id {:?} doesn't start with {prefix:?}, as its kind requires",
            producer.id
        )));
    };
    let parts: Vec<_> = rest.split(':').collect();
    let expected = match kind {
        ProducerKind::Workflow => 1,
        ProducerKind::LocalActivity | ProducerKind::StandaloneActivity => 2,
        // A named client id has one part. A process client id has two.
        ProducerKind::Client if parts.len() == 1 => 1,
        ProducerKind::Client => 2,
        ProducerKind::Activity => 3,
    };
    // The meta field names use `|` as a separator, so a part must be in the form of `encode_part`.
    let encoded = |part: &&str| decode_part(part).is_some_and(|text| encode_part(&text) == *part);
    if parts.len() != expected || !parts.iter().all(encoded) {
        return Err(StreamError::invalid(format!(
            "The Producer id {:?} must have {expected} encoded parts after {prefix:?}",
            producer.id
        )));
    }
    let runs = matches!(
        kind,
        ProducerKind::Workflow | ProducerKind::LocalActivity | ProducerKind::Activity
    );
    if runs && decode_part(parts[0]).as_deref() != Some(producer_run_id(producer)) {
        return Err(StreamError::invalid(format!(
            "The Producer id {:?} names another run than its run id {:?}",
            producer.id,
            producer_run_id(producer)
        )));
    }
    if kind == ProducerKind::Client && !producer_run_id(producer).is_empty() {
        return Err(StreamError::invalid("A client Producer has no run id"));
    }
    Ok(())
}

fn missing(what: &str) -> StreamResult<()> {
    Err(StreamError::invalid(format!("The Producer has no {what}")))
}

fn at_least_one(attempt: i32, of: &str) -> StreamResult<()> {
    if attempt < 1 {
        return Err(StreamError::invalid(format!(
            "The {of} attempt is {attempt}, and it must be 1 or more"
        )));
    }
    Ok(())
}

/// Refuses a Workflow Task epoch with no started event id, or with an attempt below 1.
fn check_task(task: &WorkflowTaskEpoch) -> StreamResult<()> {
    // Without its started event, a later task would have the epoch of an earlier one.
    if task.started_event_id <= 0 {
        return Err(StreamError::invalid(
            "The Workflow Task of the epoch has no started event id",
        ));
    }
    at_least_one(task.attempt, "Workflow Task")
}

/// Refuses a Producer that lacks a field the API requires. A Workflow Producer only closes a
/// Stream, so it may send no epoch, but an epoch that it sends must order. A Workflow Task's
/// started time stays optional, since Core leaves it empty when the server gave the task none.
pub fn check_required(producer: &Producer) -> StreamResult<()> {
    match producer.kind.as_ref() {
        Some(Kind::Workflow(workflow)) => {
            if workflow.run_id.is_empty() {
                return missing("run id");
            }
            workflow.epoch.as_ref().map_or(Ok(()), check_task)
        }
        Some(Kind::LocalActivity(local)) => {
            if local.run_id.is_empty() {
                return missing("run id");
            }
            let Some(epoch) = local.epoch.as_ref() else {
                return missing("epoch");
            };
            let Some(task) = epoch.task.as_ref() else {
                return missing("Workflow Task that dispatched it");
            };
            check_task(task)?;
            at_least_one(epoch.attempt, "Local Activity")
        }
        Some(Kind::WorkflowActivity(activity) | Kind::StandaloneActivity(activity)) => {
            let Some(epoch) = activity.epoch.as_ref() else {
                return missing("epoch");
            };
            if epoch.attempt_scheduled_time.is_none() {
                return missing("attempt scheduled time");
            }
            at_least_one(epoch.attempt, "Activity")
        }
        Some(Kind::Client(client)) => {
            let Some(epoch) = client.epoch.as_ref() else {
                return missing("epoch");
            };
            at_least_one(epoch.attempt, "client")
        }
        None => missing("kind"),
    }
}

/// The keys of one Stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamKeys {
    base: String,
}

impl StreamKeys {
    /// The keys of `stream_id` under `prefix`.
    pub fn new(prefix: &str, stream_id: &str) -> Self {
        Self {
            base: format!("{}:{{{}}}", encode_part(prefix), encode_part(stream_id)),
        }
    }

    /// The hash with the format version, the fence, dedupe and close state.
    pub fn meta(&self) -> String {
        format!("{}:meta", self.base)
    }

    /// The log.
    pub fn log(&self) -> String {
        format!("{}:log", self.base)
    }

    /// The key that a close adds one entry to. A close writes no log entry, so a waiting read also
    /// waits on this key, and the close wakes it.
    pub fn closed(&self) -> String {
        format!("{}:closed", self.base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        ActivityEpoch, ActivityProducer, ClientEpoch, ClientProducer, LocalActivityEpoch,
        LocalActivityProducer, WorkflowProducer, WorkflowTaskEpoch,
    };
    use prost_types::Timestamp;

    fn producer(kind: ProducerKind, id: &str, run_id: &str) -> Producer {
        let run_id = run_id.to_string();
        let kind = match kind {
            ProducerKind::Workflow => Kind::Workflow(WorkflowProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::LocalActivity => Kind::LocalActivity(LocalActivityProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::Activity => Kind::WorkflowActivity(ActivityProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::StandaloneActivity => Kind::StandaloneActivity(ActivityProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::Client => Kind::Client(ClientProducer::default()),
        };
        Producer {
            id: id.to_string(),
            kind: Some(kind),
        }
    }

    #[test]
    fn a_part_round_trips_and_bad_text_does_not_decode() {
        for text in ["plain", "a:b", "é {x}%", ""] {
            assert_eq!(decode_part(&encode_part(text)).as_deref(), Some(text));
        }
        assert_eq!(decode_part("%zz"), None);
        assert_eq!(decode_part("%4"), None);
    }

    #[test]
    fn the_kind_prefix_and_the_run_must_match_the_producer() {
        let run = "run:1";
        let ok = [
            producer(ProducerKind::Workflow, &workflow_producer_id(run), run),
            producer(
                ProducerKind::LocalActivity,
                &local_activity_producer_id(run, "k"),
                run,
            ),
            producer(
                ProducerKind::Activity,
                &activity_producer_id(run, "a:1", "s"),
                run,
            ),
            producer(
                ProducerKind::StandaloneActivity,
                &standalone_activity_producer_id("a", "r"),
                "",
            ),
            producer(
                ProducerKind::Client,
                &client_producer_id("me@host", "p"),
                "",
            ),
        ];
        for producer in &ok {
            check_producer(producer).unwrap();
        }
        let bad = [
            producer(ProducerKind::Client, &workflow_producer_id(run), run),
            producer(ProducerKind::Workflow, &workflow_producer_id(run), "other"),
            producer(ProducerKind::Activity, "act:run%3A1:a", run),
            Producer {
                kind: None,
                ..producer(ProducerKind::Workflow, "wf:x", "x")
            },
            producer(ProducerKind::Client, "client:a|b", ""),
            producer(ProducerKind::Client, "client:run%3a1", ""),
        ];
        for producer in &bad {
            assert_eq!(
                check_producer(producer).unwrap_err().reason,
                crate::Reason::Invalid,
                "{producer:?}"
            );
        }
    }

    #[test]
    fn a_stream_id_is_bounded() {
        check_stream_id("a").unwrap();
        check_stream_id(&"a".repeat(MAX_STREAM_ID_BYTES)).unwrap();
        assert!(check_stream_id("").is_err());
        assert!(check_stream_id(&"a".repeat(MAX_STREAM_ID_BYTES + 1)).is_err());
    }

    /// A Local Activity Producer with every field the API requires.
    fn local_activity() -> Producer {
        Producer {
            id: local_activity_producer_id("r", "k"),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: "r".to_string(),
                epoch: Some(LocalActivityEpoch {
                    task: Some(WorkflowTaskEpoch {
                        started_event_id: 3,
                        attempt: 1,
                        started_time: None,
                    }),
                    attempt: 1,
                }),
            })),
        }
    }

    /// An Activity Producer with every field the API requires.
    fn activity() -> Producer {
        Producer {
            id: activity_producer_id("r", "a", "s"),
            kind: Some(Kind::WorkflowActivity(ActivityProducer {
                run_id: "r".to_string(),
                epoch: Some(ActivityEpoch {
                    attempt_scheduled_time: Some(Timestamp::default()),
                    attempt: 1,
                }),
            })),
        }
    }

    /// A client Producer with every field the API requires.
    fn client() -> Producer {
        Producer {
            id: named_client_producer_id("n"),
            kind: Some(Kind::Client(ClientProducer {
                epoch: Some(ClientEpoch { attempt: 1 }),
            })),
        }
    }

    fn local_epoch(producer: &mut Producer) -> &mut LocalActivityEpoch {
        let Some(Kind::LocalActivity(local)) = producer.kind.as_mut() else {
            unreachable!()
        };
        local.epoch.as_mut().unwrap()
    }

    fn activity_epoch(producer: &mut Producer) -> &mut ActivityEpoch {
        let Some(Kind::WorkflowActivity(activity)) = producer.kind.as_mut() else {
            unreachable!()
        };
        activity.epoch.as_mut().unwrap()
    }

    fn refused(producer: &Producer) -> String {
        let error = check_required(producer).unwrap_err();
        assert_eq!(error.reason, crate::Reason::Invalid);
        error.message
    }

    #[test]
    fn a_producer_with_its_required_fields_passes() {
        // A Workflow Producer only closes, with no epoch, and a standalone Activity has no run.
        let workflow = producer(ProducerKind::Workflow, &workflow_producer_id("r"), "r");
        let mut standalone = activity();
        let Some(Kind::WorkflowActivity(kind)) = standalone.kind.take() else {
            unreachable!()
        };
        standalone.kind = Some(Kind::StandaloneActivity(ActivityProducer {
            run_id: String::new(),
            ..kind
        }));
        for producer in [workflow, local_activity(), activity(), standalone, client()] {
            check_required(&producer).unwrap();
        }
    }

    #[test]
    fn a_workflow_producer_needs_its_run_id() {
        let workflow = producer(ProducerKind::Workflow, "wf:", "");
        assert!(refused(&workflow).contains("run id"));
    }

    /// A Workflow Producer with `epoch`.
    fn workflow_with(epoch: WorkflowTaskEpoch) -> Producer {
        Producer {
            id: workflow_producer_id("r"),
            kind: Some(Kind::Workflow(WorkflowProducer {
                run_id: "r".to_string(),
                epoch: Some(epoch),
            })),
        }
    }

    #[test]
    fn a_workflow_epoch_passes_with_its_required_fields() {
        let epoch = WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            started_time: None,
        };
        check_required(&workflow_with(epoch)).unwrap();
    }

    #[test]
    fn a_workflow_epoch_needs_its_started_event_id() {
        let epoch = WorkflowTaskEpoch {
            attempt: 1,
            ..Default::default()
        };
        assert!(refused(&workflow_with(epoch)).contains("started event id"));
    }

    #[test]
    fn a_workflow_epoch_attempt_is_1_or_more() {
        let epoch = WorkflowTaskEpoch {
            started_event_id: 3,
            ..Default::default()
        };
        assert!(refused(&workflow_with(epoch)).contains("Workflow Task attempt"));
    }

    #[test]
    fn a_local_activity_producer_needs_its_run_id() {
        let mut local = local_activity();
        let Some(Kind::LocalActivity(kind)) = local.kind.as_mut() else {
            unreachable!()
        };
        kind.run_id.clear();
        assert!(refused(&local).contains("run id"));
    }

    #[test]
    fn a_local_activity_producer_needs_its_epoch() {
        let mut local = local_activity();
        let Some(Kind::LocalActivity(kind)) = local.kind.as_mut() else {
            unreachable!()
        };
        kind.epoch = None;
        assert!(refused(&local).contains("epoch"));
    }

    #[test]
    fn a_local_activity_epoch_needs_its_workflow_task() {
        let mut local = local_activity();
        local_epoch(&mut local).task = None;
        assert!(refused(&local).contains("Workflow Task"));
    }

    #[test]
    fn a_local_activity_task_needs_its_started_event_id() {
        let mut local = local_activity();
        local_epoch(&mut local)
            .task
            .as_mut()
            .unwrap()
            .started_event_id = 0;
        assert!(refused(&local).contains("Workflow Task"));
    }

    #[test]
    fn a_local_activity_task_attempt_is_1_or_more() {
        let mut local = local_activity();
        local_epoch(&mut local).task.as_mut().unwrap().attempt = 0;
        assert!(refused(&local).contains("Workflow Task attempt"));
    }

    #[test]
    fn a_local_activity_attempt_is_1_or_more() {
        let mut local = local_activity();
        local_epoch(&mut local).attempt = 0;
        assert!(refused(&local).contains("Local Activity attempt"));
    }

    #[test]
    fn an_activity_producer_needs_its_epoch() {
        let mut act = activity();
        let Some(Kind::WorkflowActivity(kind)) = act.kind.as_mut() else {
            unreachable!()
        };
        kind.epoch = None;
        assert!(refused(&act).contains("epoch"));
    }

    #[test]
    fn an_activity_epoch_needs_its_scheduled_time() {
        let mut act = activity();
        activity_epoch(&mut act).attempt_scheduled_time = None;
        assert!(refused(&act).contains("scheduled time"));
    }

    #[test]
    fn an_activity_attempt_is_1_or_more() {
        let mut act = activity();
        activity_epoch(&mut act).attempt = 0;
        assert!(refused(&act).contains("Activity attempt"));
    }

    #[test]
    fn a_client_producer_needs_its_epoch() {
        let client = Producer {
            kind: Some(Kind::Client(ClientProducer::default())),
            ..client()
        };
        assert!(refused(&client).contains("epoch"));
    }

    #[test]
    fn a_client_attempt_is_1_or_more() {
        let client = Producer {
            kind: Some(Kind::Client(ClientProducer {
                epoch: Some(ClientEpoch { attempt: 0 }),
            })),
            ..client()
        };
        assert!(refused(&client).contains("client attempt"));
    }
}
