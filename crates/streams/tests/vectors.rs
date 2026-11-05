//! The shared test vectors, which `testdata/gen_vectors.py` writes from a second implementation.
//! The crate's README says which other side reads each file.

use prost_types::Timestamp;
use serde_json::Value;
use temporalio_common::protos::temporal::api::common::v1::{
    Payload, payload::ExternalPayloadDetails,
};
use temporalio_streams::{
    digest::batch_digest,
    epoch::{epoch_key, task_key, task_of, withdraws},
    ids::{self, StreamKeys},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{
        ActivityEpoch, AppendRecord, ClientEpoch, LocalActivityEpoch, Producer, ProducerEpoch,
        StreamData, StreamRecord, StreamSuperseded, StreamWriter, WorkflowTaskEpoch,
        producer_epoch::Kind, stream_record::Item,
    },
};

fn load(name: &str) -> Value {
    let path = format!("{}/testdata/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().unwrap()
}

fn time(value: &Value) -> Option<Timestamp> {
    let pair = value.as_array()?;
    Some(Timestamp {
        seconds: pair[0].as_i64().unwrap(),
        nanos: pair[1].as_i64().unwrap() as i32,
    })
}

fn number(value: &Value, field: &str) -> i64 {
    value[field].as_i64().unwrap_or(0)
}

/// The epoch of a kind, from the flat fields of the vectors. The case takes the fields of its kind.
fn epoch(kind: &str, value: &Value) -> ProducerEpoch {
    let task = WorkflowTaskEpoch {
        started_event_id: number(value, "task_started_event_id"),
        attempt: number(value, "task_attempt") as i32,
        started_time: time(&value["task_started_time"]),
    };
    let attempt = number(value, "attempt") as i32;
    let activity = ActivityEpoch {
        attempt_scheduled_time: time(&value["attempt_scheduled_time"]),
        attempt,
    };
    let kind = match kind {
        "workflow" => Some(Kind::Workflow(task)),
        "local_activity" => Some(Kind::LocalActivity(LocalActivityEpoch {
            task: Some(task),
            attempt,
        })),
        "activity" => Some(Kind::Activity(activity)),
        "standalone_activity" => Some(Kind::StandaloneActivity(activity)),
        "client" => Some(Kind::Client(ClientEpoch { attempt })),
        "empty" => None,
        other => panic!("unknown kind {other}"),
    };
    ProducerEpoch { kind }
}

#[test]
fn stream_ids_and_keys_match() {
    let vectors = load("ids.json");
    for case in vectors["streams"].as_array().unwrap() {
        let id = ids::scoped_stream_id(
            text(case, "workflow_id"),
            text(case, "first_run_id"),
            text(case, "name"),
        );
        assert_eq!(id, text(case, "stream_id"));
        let keys = StreamKeys::new(text(case, "prefix"), text(case, "namespace"), &id);
        assert_eq!(keys.meta(), text(case, "meta"));
        assert_eq!(keys.log(), text(case, "log"));
        assert_eq!(keys.producers(), text(case, "producers"));
        assert_eq!(keys.closed(), text(case, "closed"));
    }
}

#[test]
fn producer_ids_match() {
    let producers = &load("ids.json")["producers"];
    let id = |name: &str| text(&producers[name], "id");
    let field = |name: &str, field: &str| text(&producers[name], field);
    assert_eq!(
        ids::workflow_producer_id(field("workflow", "run_id")),
        id("workflow")
    );
    assert_eq!(
        ids::local_activity_producer_id(
            field("local_activity", "run_id"),
            field("local_activity", "call_key")
        ),
        id("local_activity")
    );
    assert_eq!(
        ids::activity_producer_id(
            field("activity", "run_id"),
            field("activity", "activity_id"),
            field("activity", "schedule_key")
        ),
        id("activity")
    );
    assert_eq!(
        ids::standalone_activity_producer_id(
            field("standalone_activity", "activity_id"),
            field("standalone_activity", "activity_run_id")
        ),
        id("standalone_activity")
    );
    assert_eq!(
        ids::client_producer_id(field("client", "identity"), field("client", "process_key")),
        id("client")
    );
    assert_eq!(
        ids::named_client_producer_id(field("named_client", "name")),
        id("named_client")
    );
}

#[test]
fn offsets_match() {
    for case in load("offsets.json").as_array().unwrap() {
        let offset = number(case, "offset");
        assert_eq!(offset_of_entry_id(text(case, "entry_id")).unwrap(), offset);
        assert_eq!(entry_id_of_offset(offset), text(case, "entry_id"));
    }
}

#[test]
fn epoch_keys_match() {
    for case in load("epochs.json").as_array().unwrap() {
        let epoch = epoch(text(case, "kind"), &case["epoch"]);
        assert_eq!(epoch_key(&epoch).to_vec(), hex(text(case, "key")));
        assert_eq!(
            task_key(&task_of(&epoch)).to_vec(),
            hex(text(case, "task_key"))
        );
    }
}

#[test]
fn the_appendix_e_withdrawals_match() {
    let cases = load("withdrawals.json");
    assert!(cases.as_array().unwrap().len() >= 35);
    for case in cases.as_array().unwrap() {
        let s = &case["superseded"];
        let withdrawing = Producer {
            id: text(s, "producer_id").to_string(),
            // A create's item has no epoch, and a Reader must not read it as an empty one.
            epoch: s
                .get("epoch")
                .map(|value| epoch(text(s, "producer_kind"), value)),
            run_id: s["run_id"].as_str().unwrap_or_default().to_string(),
        };
        let superseded = StreamSuperseded {
            writer: Some(StreamWriter {
                producer: Some(withdrawing),
                previous_completed_task: s.get("previous_completed_task").map(|c| {
                    WorkflowTaskEpoch {
                        started_event_id: number(c, "started_event_id"),
                        attempt: number(c, "attempt") as i32,
                        started_time: time(&c["started_time"]),
                    }
                }),
                failed_tasks: s["failed_tasks"]
                    .as_array()
                    .map(|list| list.iter().map(|v| v.as_i64().unwrap()).collect())
                    .unwrap_or_default(),
                withdraw_run: s["withdraw_run"].as_str().unwrap_or_default().to_string(),
            }),
        };
        let p = &case["record"]["producer"];
        let record = StreamRecord {
            item: Some(Item::Data(StreamData {
                producer: Some(Producer {
                    id: text(p, "id").to_string(),
                    epoch: p.get("epoch").map(|value| epoch(text(p, "kind"), value)),
                    run_id: p["run_id"].as_str().unwrap_or_default().to_string(),
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            withdraws(&superseded, &record),
            case["withdraws"].as_bool().unwrap(),
            "{}",
            text(case, "name")
        );
    }
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn batch_digests_match() {
    for case in load("digests.json").as_array().unwrap() {
        let records: Vec<_> = case["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| AppendRecord {
                // A record with none of these keys has no body.
                body: ["metadata", "data", "external_payloads"]
                    .iter()
                    .any(|key| r.get(key).is_some())
                    .then(|| Payload {
                        metadata: r["metadata"]
                            .as_object()
                            .into_iter()
                            .flatten()
                            .map(|(key, value)| (key.clone(), hex(value.as_str().unwrap())))
                            .collect(),
                        data: hex(r["data"].as_str().unwrap_or_default()),
                        external_payloads: r["external_payloads"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .map(|size| ExternalPayloadDetails {
                                size_bytes: size.as_i64().unwrap(),
                            })
                            .collect(),
                    }),
            })
            .collect();
        assert_eq!(
            batch_digest(&records).to_vec(),
            hex(text(case, "digest")),
            "{}",
            text(case, "name")
        );
    }
}
