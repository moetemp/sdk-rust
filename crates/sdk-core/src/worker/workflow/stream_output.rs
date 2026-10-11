//! The stream output a Workflow Task publishes: the records lang commits with a completion, and
//! the manifest Core builds from them and records in a `core_external_stream` marker.

use super::WFMachinesError;
use std::collections::HashMap;
use temporalio_common::{
    protos::{
        coresdk::{
            external_data::{
                ExternalOutputSegmentManifest, ExternalOutputStreamManifest,
                ExternalOutputTopicManifest,
            },
            workflow_commands::{OutputClose, OutputRecord},
        },
        temporal::sdk::streams::v1::StreamRecordKind,
    },
    streams::{FINGERPRINT_VERSION, FingerprintRecord, check_topic, fingerprint},
};

/// The marker version Core writes. Readers must accept every version up to this one.
pub(super) const MARKER_SCHEMA_VERSION: u32 = 1;
const MANIFEST_SCHEMA_VERSION: u32 = 1;
/// The layout a store keeps records in. A store that changes it must refuse older stages.
const STORE_FORMAT_VERSION: u32 = 1;
/// Keeps one output proof well inside the server's per-event payload limits.
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const CONTENT_HASH_BYTES: usize = 32;
/// The largest record lang may commit, in logical bytes. Lang checks it at the publish call, and
/// Core checks again, since a store call that fails on size fails every retry of the task too.
const MAX_RECORD_BYTES: u64 = 3 * 512 * 1024;
/// The most logical bytes one activation may commit, checked the same way as a record.
const MAX_COMMIT_BYTES: u64 = 16 * 1024 * 1024;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// Builds the manifest for the records one completion committed.
///
/// `replaying` allows records without bodies, since lang sends no body when nothing is stored
/// again. The run id and stage token are recorded but replay never compares them. Fails the
/// Workflow Task when a record is one no writer stores, when Core can't name the task's history
/// floor, or when the manifest is over its budget. Each failure names what to fix, because the
/// task fails the same way on every retry.
pub(super) fn build_output_manifest(
    records: &[OutputRecord],
    history_floor_event_id: Option<i64>,
    run_id: &str,
    stage_token: String,
    replaying: bool,
) -> Result<ExternalOutputStreamManifest> {
    let Some(history_floor_event_id) = history_floor_event_id.filter(|id| *id > 0) else {
        return Err(refused(
            "Core could not identify the exact event preceding this Workflow Task's scheduled \
             event"
                .to_string(),
        ));
    };
    if records.is_empty() {
        return Err(refused("the commit carried no records".to_string()));
    }
    for (index, record) in records.iter().enumerate() {
        check_record(record, replaying)
            .map_err(|reason| refused(format!("record {index} {reason}")))?;
    }
    // Replay takes what History holds.
    if !replaying {
        let committed: u64 = records.iter().map(|r| r.logical_size).sum();
        if committed > MAX_COMMIT_BYTES {
            return Err(refused(format!(
                "the commit holds {committed} logical bytes, over the {} MiB budget of one \
                 activation; publish across activations",
                MAX_COMMIT_BYTES / 1024 / 1024
            )));
        }
    }
    let mut topics: Vec<&str> = vec![];
    let mut by_topic: HashMap<&str, Vec<&OutputRecord>> = HashMap::new();
    for record in records {
        let topic = record.topic.as_str();
        by_topic
            .entry(topic)
            .or_insert_with(|| {
                topics.push(topic);
                vec![]
            })
            .push(record);
    }
    let topic_manifests: Vec<ExternalOutputTopicManifest> = topics
        .iter()
        .map(|topic| {
            let topic_records = &by_topic[topic];
            ExternalOutputTopicManifest {
                topic: topic.to_string(),
                record_count: topic_records.len() as u32,
                logical_byte_count: topic_records.iter().map(|r| r.logical_size).sum(),
                logical_fingerprint: fingerprint(topic_records.iter().map(|r| FingerprintRecord {
                    topic,
                    kind: r.kind,
                    content_hash: &r.content_hash,
                }))
                .to_vec(),
                finished: topic_records
                    .iter()
                    .any(|r| r.kind == StreamRecordKind::Finish as i32),
            }
        })
        .collect();
    let manifest = ExternalOutputStreamManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        fingerprint_version: FINGERPRINT_VERSION,
        stage_token,
        history_floor_event_id,
        run_id: run_id.to_string(),
        segments: vec![ExternalOutputSegmentManifest {
            record_counts_by_topic: topic_manifests.iter().map(|t| t.record_count).collect(),
        }],
        topics: topic_manifests,
        provider_id: String::new(),
        provider_format_version: STORE_FORMAT_VERSION,
    };
    let encoded_len = prost::Message::encoded_len(&manifest);
    if encoded_len > MAX_MANIFEST_BYTES {
        return Err(refused(format!(
            "the manifest of {} topic(s) encodes to {encoded_len} bytes, over the {} KiB marker \
             budget; spread the topics across Workflow Tasks",
            manifest.topics.len(),
            MAX_MANIFEST_BYTES / 1024
        )));
    }
    Ok(manifest)
}

/// Compares the manifest Core built from the records lang committed while replaying with the one
/// History recorded.
///
/// The stage token is left out because nothing is staged on replay, so there is no token. The
/// run id is left out because a reset forks the base run's History, so the new run replays
/// markers that name the base run. Everything else derives from the Workflow's own output.
pub(super) fn check_replayed_output_manifest(
    mut replayed: ExternalOutputStreamManifest,
    recorded: Option<ExternalOutputStreamManifest>,
) -> Result<()> {
    let Some(mut recorded) = recorded else {
        return Err(WFMachinesError::Nondeterminism(
            "Lang committed external output while replaying a Workflow Task that recorded none"
                .to_string(),
        ));
    };
    // Only the logical output must replay the same. A reset run replays markers that name its
    // base run, a stage token is new on every attempt, and a provider can be renamed or swapped
    // for a Replayer without changing what the Workflow published.
    for manifest in [&mut replayed, &mut recorded] {
        manifest.stage_token.clear();
        manifest.run_id.clear();
        manifest.provider_id.clear();
    }
    if replayed != recorded {
        return Err(WFMachinesError::Nondeterminism(format!(
            "External output committed while replaying differs from the manifest recorded in \
             History: committed {replayed:?}, recorded {recorded:?}"
        )));
    }
    Ok(())
}

/// Refuses closes the marker could not prove. Each close names a topic once, carries its result,
/// and has its topic's FINISH record among the committed records.
pub(super) fn check_output_closes(records: &[OutputRecord], closes: &[OutputClose]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for close in closes {
        let reason = if close.topic.is_empty() {
            Some("a close names no topic".to_string())
        } else if !seen.insert(close.topic.as_str()) {
            Some(format!("topic {:?} is closed twice", close.topic))
        } else if close.result.is_none() {
            Some(format!(
                "the close of topic {:?} has no result",
                close.topic
            ))
        } else if !records
            .iter()
            .any(|r| r.topic == close.topic && r.kind == StreamRecordKind::Finish as i32)
        {
            Some(format!(
                "the close of topic {:?} has no FINISH record in the same commit",
                close.topic
            ))
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(refused(reason));
        }
    }
    Ok(())
}

fn check_record(record: &OutputRecord, replaying: bool) -> std::result::Result<(), String> {
    if record.topic.is_empty() {
        return Err("has no topic".to_string());
    }
    check_topic(&record.topic)?;
    if !replaying && record.logical_size > MAX_RECORD_BYTES {
        return Err(format!(
            "holds {} logical bytes, over the 1.5 MiB record budget",
            record.logical_size
        ));
    }
    match StreamRecordKind::try_from(record.kind) {
        Ok(StreamRecordKind::Data) => {
            if record.body.is_none() && !replaying {
                return Err("is DATA without a body".to_string());
            }
            if record.content_hash.len() != CONTENT_HASH_BYTES {
                return Err(format!(
                    "is DATA with a {}-byte content hash, not the {CONTENT_HASH_BYTES}-byte \
                     SHA-256 of its plaintext body",
                    record.content_hash.len()
                ));
            }
        }
        Ok(StreamRecordKind::Finish) => {
            if record.body.is_some() || !record.content_hash.is_empty() || record.logical_size != 0
            {
                return Err("is FINISH with a body".to_string());
            }
        }
        _ => return Err(format!("has kind {}, not DATA or FINISH", record.kind)),
    }
    Ok(())
}

fn refused(reason: String) -> WFMachinesError {
    WFMachinesError::Fatal(format!("Refusing a stream output commit, since {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    const RUN_ID: &str = "run-id";
    const FLOOR: i64 = 4;

    fn data(topic: &str, hash: u8, size: u64) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: b"ciphertext".to_vec(),
                ..Default::default()
            }),
            content_hash: vec![hash; CONTENT_HASH_BYTES],
            logical_size: size,
        }
    }

    fn finish(topic: &str) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Finish as i32,
            ..Default::default()
        }
    }

    fn build(records: &[OutputRecord]) -> Result<ExternalOutputStreamManifest> {
        build_output_manifest(records, Some(FLOOR), RUN_ID, "token".to_string(), false)
    }

    fn rejection(records: &[OutputRecord]) -> String {
        build(records)
            .expect_err("commit must be refused")
            .to_string()
    }

    #[test]
    fn the_manifest_groups_topics_in_order_of_first_publish() {
        let records = [
            data("b", 1, 3),
            data("a", 2, 4),
            data("b", 3, 5),
            finish("b"),
        ];
        let manifest = build(&records).unwrap();
        assert_eq!(manifest.schema_version, 1);
        assert_eq!(manifest.fingerprint_version, 2);
        assert_eq!(manifest.stage_token, "token");
        assert_eq!(manifest.history_floor_event_id, FLOOR);
        assert_eq!(manifest.run_id, RUN_ID);
        assert_eq!(manifest.provider_format_version, 1);
        let topics: Vec<_> = manifest
            .topics
            .iter()
            .map(|t| {
                (
                    t.topic.as_str(),
                    t.record_count,
                    t.logical_byte_count,
                    t.finished,
                )
            })
            .collect();
        assert_eq!(topics, [("b", 3, 8, true), ("a", 1, 4, false)]);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].record_counts_by_topic, [3, 1]);
        let b = [&records[0], &records[2], &records[3]];
        assert_eq!(
            manifest.topics[0].logical_fingerprint,
            fingerprint(b.iter().map(|r| FingerprintRecord {
                topic: "b",
                kind: r.kind,
                content_hash: &r.content_hash,
            }))
        );
    }

    #[test]
    fn the_manifest_never_depends_on_the_encoded_body() {
        let mut reencrypted = data("t", 1, 3);
        reencrypted.body.as_mut().unwrap().data = b"other ciphertext".to_vec();
        let mut replayed = data("t", 1, 3);
        replayed.body = None;
        let live = build(&[data("t", 1, 3)]).unwrap();
        assert_eq!(build(&[reencrypted]).unwrap(), live);
        let replay =
            build_output_manifest(&[replayed], Some(FLOOR), RUN_ID, String::new(), true).unwrap();
        assert_eq!(replay.topics, live.topics);
    }

    #[test]
    fn an_unknown_floor_is_refused_rather_than_guessed() {
        for floor in [None, Some(0)] {
            let error = build_output_manifest(
                &[data("t", 1, 1)],
                floor,
                RUN_ID,
                "token".to_string(),
                false,
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains("exact event"), "{error}");
        }
    }

    #[test]
    fn each_record_no_writer_stores_is_refused_with_its_own_message() {
        let mut no_body = data("t", 1, 1);
        no_body.body = None;
        let mut short_hash = data("t", 1, 1);
        short_hash.content_hash.truncate(3);
        let mut finish_with_body = finish("t");
        finish_with_body.body = Some(Payload::default());
        let unspecified = OutputRecord {
            kind: StreamRecordKind::Unspecified as i32,
            ..data("t", 1, 1)
        };
        for (record, expected) in [
            (data("", 1, 1), "record 1 has no topic"),
            (no_body, "record 1 is DATA without a body"),
            (short_hash, "record 1 is DATA with a 3-byte content hash"),
            (finish_with_body, "record 1 is FINISH with a body"),
            (unspecified, "record 1 has kind 0"),
        ] {
            let message = rejection(&[data("t", 9, 1), record]);
            assert!(message.contains(expected), "{expected}: {message}");
        }
        assert!(rejection(&[]).contains("no records"));
    }

    fn manifest() -> ExternalOutputStreamManifest {
        build(&[data("a", 1, 3), finish("b")]).unwrap()
    }

    #[test]
    fn a_replayed_commit_matches_the_recorded_manifest_whatever_its_token() {
        let mut replayed = manifest();
        replayed.stage_token.clear();
        check_replayed_output_manifest(replayed, Some(manifest())).unwrap();
    }

    #[test]
    fn a_replayed_commit_matches_a_manifest_recorded_by_another_run() {
        let mut recorded = manifest();
        recorded.run_id = "reset-base-run".to_string();
        check_replayed_output_manifest(manifest(), Some(recorded)).unwrap();
    }

    #[test]
    fn a_replayed_commit_matches_a_manifest_recorded_under_another_store_name() {
        let mut recorded = manifest();
        recorded.provider_id = "renamed-store".to_string();
        check_replayed_output_manifest(manifest(), Some(recorded)).unwrap();
    }

    #[test]
    fn a_replayed_commit_in_another_store_format_is_nondeterministic() {
        let mut recorded = manifest();
        recorded.provider_format_version = 2;
        assert!(matches!(
            check_replayed_output_manifest(manifest(), Some(recorded)),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("differs")
        ));
    }

    #[test]
    fn a_replayed_commit_that_differs_from_history_is_nondeterministic() {
        let replayed = build(&[data("a", 2, 3), finish("b")]).unwrap();
        assert!(matches!(
            check_replayed_output_manifest(replayed, Some(manifest())),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("differs")
        ));
    }

    #[test]
    fn a_replayed_commit_with_nothing_recorded_is_nondeterministic() {
        assert!(matches!(
            check_replayed_output_manifest(manifest(), None),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("recorded none")
        ));
    }

    #[test]
    fn a_topic_no_store_keys_reliably_is_refused() {
        // A store joins a stage's topics with a control character.
        let long = "t".repeat(257);
        for (topic, expected) in [
            ("a\u{1f}b", "holds a control character"),
            (long.as_str(), "at most 256 UTF-8 bytes"),
        ] {
            let message = rejection(&[data(topic, 1, 1)]);
            assert!(message.contains(expected), "{expected}: {message}");
        }
        build(&[data(&"t".repeat(256), 1, 1)]).unwrap();
    }

    #[test]
    fn a_record_or_commit_over_its_byte_budget_is_refused() {
        let at_limit = data("t", 1, 3 * 512 * 1024);
        build(std::slice::from_ref(&at_limit)).unwrap();
        let message = rejection(&[data("t", 1, 3 * 512 * 1024 + 1)]);
        assert!(
            message.contains("over the 1.5 MiB record budget"),
            "{message}"
        );

        let commit: Vec<_> = (0..11).map(|i| data("t", i, 3 * 512 * 1024)).collect();
        let message = rejection(&commit);
        assert!(message.contains("over the 16 MiB budget"), "{message}");
    }

    fn close(topic: &str) -> OutputClose {
        OutputClose {
            topic: topic.to_string(),
            result: Some(Payload::default()),
        }
    }

    #[test]
    fn a_close_needs_its_topics_finish_in_the_same_commit() {
        let records = [data("a", 1, 1), finish("a"), data("b", 2, 1)];
        check_output_closes(&records, &[close("a")]).unwrap();
        let mut no_result = close("a");
        no_result.result = None;
        for (closes, expected) in [
            (vec![close("b")], "topic \"b\" has no FINISH"),
            (vec![close("a"), close("a")], "closed twice"),
            (vec![close("")], "names no topic"),
            (vec![no_result], "has no result"),
        ] {
            let message = check_output_closes(&records, &closes)
                .unwrap_err()
                .to_string();
            assert!(message.contains(expected), "{expected}: {message}");
        }
    }

    #[test]
    fn a_manifest_over_its_budget_is_refused() {
        let records: Vec<_> = (0..1200)
            .map(|i| data(&format!("topic-{i:04}-{}", "x".repeat(16)), 1, 1))
            .collect();
        let message = rejection(&records);
        assert!(
            message.contains("over the 64 KiB marker budget"),
            "{message}"
        );
    }
}
