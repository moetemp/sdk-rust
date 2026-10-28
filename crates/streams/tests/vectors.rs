//! The shared test vectors, which `testdata/gen_vectors.py` writes from a second implementation.
//! Every SDK that builds ids or reads Records runs the same files.

use prost::Message;
use prost_types::Timestamp;
use serde_json::Value;
use temporalio_common::protos::temporal::api::common::v1::{
    Payload, payload::ExternalPayloadDetails,
};
use temporalio_streams::{
    call_static, derive_producer,
    digest::batch_digest,
    epoch::{epoch_key, task_key, withdraws},
    ids::{self, StreamKeys, fields},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{
        ActivityContext, AppendRecord, DeriveProducerRequest, Producer, ProducerEpoch,
        ProducerPurpose, StreamData, StreamProducerKind, StreamRecord, Supersession,
        WithdrawsRequest, WithdrawsResponse, WorkflowTaskAttempt, WorkflowTaskEpoch,
        stream_record::Item,
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

fn epoch(value: &Value) -> ProducerEpoch {
    ProducerEpoch {
        task_started_event_id: number(value, "task_started_event_id"),
        task_attempt: number(value, "task_attempt") as i32,
        task_started_time: time(&value["task_started_time"]),
        attempt_scheduled_time: time(&value["attempt_scheduled_time"]),
        attempt: number(value, "attempt") as i32,
    }
}

fn kind(name: &str) -> StreamProducerKind {
    match name {
        "workflow" => StreamProducerKind::Workflow,
        "local_activity" => StreamProducerKind::LocalActivity,
        "activity" => StreamProducerKind::Activity,
        "standalone_activity" => StreamProducerKind::StandaloneActivity,
        "client" => StreamProducerKind::Client,
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn stream_ids_and_keys_match() {
    let vectors = load("ids.json");
    for case in vectors["streams"].as_array().unwrap() {
        let (workflow_id, run) = (text(case, "workflow_id"), text(case, "first_run_id"));
        let id = match case["name"].as_str() {
            Some(name) => ids::scoped_stream_id(workflow_id, run, name),
            None => ids::default_stream_id(workflow_id, run),
        };
        assert_eq!(id, text(case, "stream_id"));
        let keys = StreamKeys::new(text(case, "prefix"), text(case, "namespace"), &id);
        assert_eq!(keys.meta(), text(case, "meta"));
        assert_eq!(keys.log(), text(case, "log"));
        assert_eq!(keys.producers(), text(case, "producers"));
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

fn task_epoch(value: &Value) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id: number(value, "started_event_id"),
        attempt: number(value, "attempt") as i32,
        started_time: time(&value["started_time"]),
    }
}

fn numbers(value: &Value) -> Vec<i64> {
    value
        .as_array()
        .map(|list| list.iter().map(|v| v.as_i64().unwrap()).collect())
        .unwrap_or_default()
}

fn activity_context(value: &Value) -> ActivityContext {
    let field = |name: &str| value[name].as_str().unwrap_or_default().to_string();
    ActivityContext {
        is_local: value["is_local"].as_bool().unwrap_or_default(),
        in_workflow: value["in_workflow"].as_bool().unwrap_or_default(),
        workflow_id: field("workflow_id"),
        workflow_run_id: field("workflow_run_id"),
        activity_id: field("activity_id"),
        activity_run_id: field("activity_run_id"),
        attempt: number(value, "attempt") as i32,
        current_attempt_scheduled_time: time(&value["current_attempt_scheduled_time"]),
        call_key: field("call_key"),
        workflow_task: value.get("workflow_task").map(|task| WorkflowTaskAttempt {
            started_event_id: number(task, "started_event_id"),
            attempt: number(task, "attempt") as u32,
            started_time: time(&task["started_time"]),
            previous_started_event_id: number(task, "previous_started_event_id"),
            failed_tasks: numbers(&task["failed_tasks"]),
            previous_attempt: number(task, "previous_attempt") as u32,
            previous_started_time: time(&task["previous_started_time"]),
        }),
    }
}

#[test]
fn the_producer_of_each_activity_kind_matches() {
    for case in load("producers.json").as_array().unwrap() {
        let name = text(case, "name");
        let purpose = match text(case, "purpose") {
            "workflow_write" => ProducerPurpose::WorkflowWrite,
            _ => ProducerPurpose::ActivityWriter,
        };
        let derived = derive_producer(DeriveProducerRequest {
            context: Some(activity_context(&case["context"])),
            purpose: purpose as i32,
        });
        if let Some(error) = case["error"].as_str() {
            assert_eq!(derived.unwrap_err().reason.token(), error, "{name}");
            continue;
        }
        let derived = derived.unwrap();
        let p = &case["producer"];
        let expected = Producer {
            kind: kind(text(p, "kind")) as i32,
            id: text(p, "id").to_string(),
            epoch: Some(epoch(&p["epoch"])),
            run_id: text(p, "run_id").to_string(),
        };
        assert_eq!(derived.producer, Some(expected), "{name}");
        let completed = case["previous_completed_task"]
            .as_object()
            .map(|_| task_epoch(&case["previous_completed_task"]));
        assert_eq!(derived.previous_completed_task, completed, "{name}");
        assert_eq!(
            derived.failed_tasks,
            numbers(&case["failed_tasks"]),
            "{name}"
        );
    }
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
        let epoch = epoch(&case["epoch"]);
        assert_eq!(epoch_key(&epoch), text(case, "key"));
        let task = WorkflowTaskEpoch {
            started_event_id: epoch.task_started_event_id,
            attempt: epoch.task_attempt,
            started_time: epoch.task_started_time,
        };
        assert_eq!(task_key(&task), text(case, "task_key"));
    }
}

#[test]
fn meta_fields_match() {
    let case = load("fields.json");
    let (kind, producer, run) = (
        number(&case, "kind") as i32,
        text(&case, "producer_id"),
        text(&case, "run_id"),
    );
    assert_eq!(fields::fence(kind, producer), text(&case, "fence"));
    assert_eq!(fields::dedupe(kind, producer), text(&case, "dedupe"));
    assert_eq!(fields::holds(kind, producer), text(&case, "holds"));
    assert_eq!(fields::run_wrote(run), text(&case, "run_wrote"));
    assert_eq!(fields::withdrawn_run(run), text(&case, "withdrawn_run"));
}

#[test]
fn the_appendix_e_withdrawals_match() {
    let cases = load("withdrawals.json");
    assert!(cases.as_array().unwrap().len() >= 35);
    for case in cases.as_array().unwrap() {
        let s = &case["supersession"];
        let supersession = Supersession {
            producer_kind: kind(text(s, "producer_kind")) as i32,
            producer_id: text(s, "producer_id").to_string(),
            // A create's item has no epoch, and a Reader must not read it as an empty one.
            epoch: s.get("epoch").map(epoch),
            previous_completed_task: s.get("previous_completed_task").map(|c| WorkflowTaskEpoch {
                started_event_id: number(c, "started_event_id"),
                attempt: number(c, "attempt") as i32,
                started_time: time(&c["started_time"]),
            }),
            failed_tasks: s["failed_tasks"]
                .as_array()
                .map(|list| list.iter().map(|v| v.as_i64().unwrap()).collect())
                .unwrap_or_default(),
            withdraw_run: s["withdraw_run"].as_str().unwrap_or_default().to_string(),
            run_id: s["run_id"].as_str().unwrap_or_default().to_string(),
        };
        let p = &case["record"]["producer"];
        let producer = Producer {
            kind: kind(text(p, "kind")) as i32,
            id: text(p, "id").to_string(),
            epoch: p.get("epoch").map(epoch),
            run_id: p["run_id"].as_str().unwrap_or_default().to_string(),
        };
        let record = StreamRecord {
            item: Some(Item::Data(StreamData {
                producer: Some(producer.clone()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let expected = case["withdraws"].as_bool().unwrap();
        assert_eq!(
            withdraws(&supersession, &record),
            expected,
            "{}",
            text(case, "name")
        );
        // The static method that lang calls for Records of an earlier read applies the same rule.
        let request = WithdrawsRequest {
            supersession: Some(supersession),
            producer: Some(producer),
        };
        let output = call_static("Withdraws", &request.encode_to_vec()).unwrap();
        let answer = WithdrawsResponse::decode(output.as_slice()).unwrap();
        assert_eq!(answer.withdraws, expected, "{}", text(case, "name"));
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
            batch_digest(&records),
            text(case, "digest"),
            "{}",
            text(case, "name")
        );
    }
}
