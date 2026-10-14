//! The stream service's Nexus operations as a caller sees them: what lands, what a read answers,
//! the limits and the failure classes.
//!
//! These are the store cases the Python handler's tests held before Core served the calls. They
//! run on memory, and on Redis when `STREAMS_REDIS_URL` is set.

mod nexus_support;

use nexus_support::*;
use prost::Message;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload, enums::v1::WorkflowExecutionStatus,
};
use temporalio_streams::{
    MemoryStore, OwnerClient, OwnerDescription, OwnerError, StreamStore, Streams, StreamsOptions,
    nexus::{NexusCalls, NexusOptions},
    proto::{ChainId, NexusFailure, StreamRecord, StreamRecordKind},
};

static IDS: AtomicUsize = AtomicUsize::new(0);

fn workflow() -> String {
    format!(
        "service-{}-{}",
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

const TOPIC: &str = "out";

fn append_input(wf: &str, producer: &str, attempt: i64, sequence: i64, texts: &[&str]) -> Value {
    json!({
        "stream": stream(wf, TOPIC),
        "producer_id": producer,
        "attempt": attempt,
        "sequence": sequence,
        "payloads": texts.iter().map(|text| body(text)).collect::<Vec<_>>(),
    })
}

/// Appends `texts` as producer `p`, attempt one, and answers with the cursor.
async fn write(calls: &NexusCalls, wf: &str, sequence: i64, texts: &[&str]) -> String {
    let answer = append(calls, append_input(wf, "p", 1, sequence, texts))
        .await
        .unwrap();
    answer["cursor"].as_str().unwrap().to_string()
}

fn read_input(wf: &str, after: &str) -> Value {
    json!({"stream": stream(wf, TOPIC), "after_token": after})
}

async fn read_after(calls: &NexusCalls, wf: &str, after: &str) -> Value {
    read(calls, read_input(wf, after)).await.unwrap()
}

/// The stored records of a read answer, without its supersessions and errors.
fn stored(answer: &Value) -> Vec<StreamRecord> {
    use base64::Engine;
    answer["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|wire| wire.get("superseded").is_none() && wire.get("error").is_none())
        .map(|wire| {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(wire["record"].as_str().unwrap())
                .unwrap();
            StreamRecord::decode(bytes.as_slice()).unwrap()
        })
        .collect()
}

fn texts(answer: &Value) -> Vec<String> {
    stored(answer)
        .into_iter()
        .map(|record| serde_json::from_slice(&record.body.unwrap().data).unwrap())
        .collect()
}

fn records(answer: &Value) -> &Vec<Value> {
    answer["records"].as_array().unwrap()
}

/// The Nexus error type, whether a retry may help, and the class the caller raises.
fn kind_of(failure: &NexusFailure) -> (&str, bool, &str) {
    let class = failure.message.split(':').next().unwrap();
    (
        failure.handler_error_type.as_str(),
        failure.retryable,
        class,
    )
}

fn token(answer: &Value) -> &str {
    answer["next_token"].as_str().unwrap()
}

/// Temporal where every Workflow's first run `run-1` has completed.
struct Ended;

#[async_trait::async_trait]
impl OwnerClient for Ended {
    async fn describe(&self, _: &str, _: &str, _: &str) -> Result<OwnerDescription, OwnerError> {
        Ok(OwnerDescription {
            run_id: "run-1".to_string(),
            first_run_id: "run-1".to_string(),
            status: WorkflowExecutionStatus::Completed,
        })
    }
}

/// Temporal where run `run-1` continued as new into `run-2`, which still runs.
struct Continued;

#[async_trait::async_trait]
impl OwnerClient for Continued {
    async fn describe(
        &self,
        _: &str,
        _: &str,
        run_id: &str,
    ) -> Result<OwnerDescription, OwnerError> {
        Ok(if run_id == "run-1" {
            OwnerDescription {
                run_id: "run-1".to_string(),
                first_run_id: "run-1".to_string(),
                status: WorkflowExecutionStatus::ContinuedAsNew,
            }
        } else {
            OwnerDescription {
                run_id: "run-2".to_string(),
                first_run_id: "run-1".to_string(),
                status: WorkflowExecutionStatus::Running,
            }
        })
    }
}

mod cases {
    use super::*;

    pub(crate) async fn an_append_reads_back_as_the_stored_record(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let landed = write(&calls, &wf, 1, &["one", "two"]).await;
        let answer = read_after(&calls, &wf, "").await;
        let found = stored(&answer);
        assert_eq!(
            found
                .iter()
                .map(|r| (
                    r.topic.as_str(),
                    r.kind,
                    r.producer_id.as_str(),
                    r.attempt,
                    r.sequence
                ))
                .collect::<Vec<_>>(),
            [
                (TOPIC, StreamRecordKind::Data as i32, "p", 1, 1),
                (TOPIC, StreamRecordKind::Data as i32, "p", 1, 2),
            ]
        );
        // The handler's codec decodes the body back to what the writer sent.
        let sent = {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(body("one"))
                .unwrap();
            Payload::decode(bytes.as_slice()).unwrap()
        };
        assert_eq!(found[0].body.as_ref(), Some(&sent));
        assert_eq!(records(&answer).last().unwrap()["token"], landed);
        assert_eq!(answer["next_token"], landed);
        assert_eq!(answer["done"], false);
    }

    pub(crate) async fn a_batch_lands_in_order_and_a_cursor_resumes_strictly_after(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let first = write(&calls, &wf, 1, &["one"]).await;
        write(&calls, &wf, 2, &["two", "three"]).await;
        assert_eq!(
            texts(&read_after(&calls, &wf, &first).await),
            ["two", "three"]
        );
        let answer = read_after(&calls, &wf, "").await;
        // A cursor a reader saw resumes the same way as one an append answered with.
        let seen = records(&answer)[1]["token"].as_str().unwrap().to_string();
        assert_eq!(texts(&read_after(&calls, &wf, &seen).await), ["three"]);
        assert_eq!(
            stored(&answer)
                .iter()
                .map(|r| r.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    pub(crate) async fn max_records_caps_an_answer_and_the_next_call_continues(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &["a", "b", "c", "d", "e"]).await;
        let mut after = String::new();
        let mut pages = vec![];
        for _ in 0..3 {
            let mut input = read_input(&wf, &after);
            input["max_records"] = json!(2);
            let answer = read(&calls, input).await.unwrap();
            pages.push(texts(&answer));
            after = token(&answer).to_string();
        }
        assert_eq!(pages, [vec!["a", "b"], vec!["c", "d"], vec!["e"]]);
    }

    pub(crate) async fn a_read_that_collects_nothing_answers_with_the_callers_cursor(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let landed = write(&calls, &wf, 1, &["one"]).await;
        let mut input = read_input(&wf, &landed);
        input["wait_ms"] = json!(200);
        let answer = read(&calls, input).await.unwrap();
        assert!(records(&answer).is_empty());
        assert_eq!(answer["next_token"], landed);
        assert_eq!(answer["done"], false);
    }

    pub(crate) async fn a_parked_read_answers_when_a_record_arrives(store: Arc<dyn StreamStore>) {
        let calls = Arc::new(service(store, NexusOptions::default()));
        let wf = workflow();
        let landed = write(&calls, &wf, 1, &["one"]).await;
        let mut input = read_input(&wf, &landed);
        input["wait_ms"] = json!(10_000);
        let parked = tokio::spawn({
            let calls = calls.clone();
            async move { read(&calls, input).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!parked.is_finished());
        write(&calls, &wf, 2, &["two"]).await;
        let answer = tokio::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("the parked read never answered")
            .unwrap();
        assert_eq!(texts(&answer), ["two"]);
    }

    pub(crate) async fn latest_only_positions_a_reader_at_the_end(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let latest = json!({"stream": stream(&wf, TOPIC), "latest_only": true});
        let empty = read(&calls, latest.clone()).await.unwrap();
        assert!(records(&empty).is_empty());
        assert_eq!(empty["next_token"], "");
        let landed = write(&calls, &wf, 1, &["one"]).await;
        let at_end = read(&calls, latest.clone()).await.unwrap();
        assert_eq!(at_end["next_token"], landed);
        write(&calls, &wf, 2, &["two"]).await;
        assert_eq!(
            texts(&read_after(&calls, &wf, token(&at_end)).await),
            ["two"]
        );
        let mut with_cursor = latest;
        with_cursor["after_token"] = json!(landed);
        let refused = read(&calls, with_cursor).await.unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    }

    pub(crate) async fn a_retried_append_is_written_once(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let input = append_input(&wf, "p", 1, 1, &["one", "two"]);
        let first = append(&calls, input.clone()).await.unwrap();
        // The caller lost the answer and sends the same call again.
        assert_eq!(append(&calls, input).await.unwrap(), first);
        assert_eq!(texts(&read_after(&calls, &wf, "").await), ["one", "two"]);
        let mut after = read_input(&wf, first["cursor"].as_str().unwrap());
        after["wait_ms"] = json!(200);
        assert!(texts(&read(&calls, after).await.unwrap()).is_empty());
    }

    pub(crate) async fn a_divergent_or_stale_retry_is_refused(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &["one"]).await;
        let divergent = append(&calls, append_input(&wf, "p", 1, 1, &["different"]))
            .await
            .unwrap_err();
        assert_eq!(
            kind_of(&divergent),
            ("BAD_REQUEST", false, "StreamProducerError")
        );
        write(&calls, &wf, 2, &["two"]).await;
        let stale = append(&calls, append_input(&wf, "p", 1, 1, &["one"]))
            .await
            .unwrap_err();
        assert_eq!(kind_of(&stale).2, "StreamProducerError");
        assert_eq!(texts(&read_after(&calls, &wf, "").await), ["one", "two"]);
    }

    pub(crate) async fn a_new_attempt_crosses_as_a_supersession_at_its_cursor(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let old = append(&calls, append_input(&wf, "model", 1, 1, &["old"]))
            .await
            .unwrap();
        append(&calls, append_input(&wf, "model", 2, 1, &["new"]))
            .await
            .unwrap();
        let answer = read_after(&calls, &wf, "").await;
        assert_eq!(
            values(&answer),
            ["model@1 old", "SUPERSEDED model 1->2", "model@2 new"]
        );
        // A read that resumes after the old attempt's record has seen no attempt of its own, so
        // only the store's reader knows the new one supersedes it.
        let old = old["cursor"].as_str().unwrap();
        let resumed = read_after(&calls, &wf, old).await;
        assert_eq!(values(&resumed), ["SUPERSEDED model 1->2", "model@2 new"]);
        assert_eq!(records(&resumed)[0]["token"], old);
    }

    pub(crate) async fn an_append_counts_the_producer_id_toward_the_record_limit(
        store: Arc<dyn StreamStore>,
    ) {
        // The stored record holds the topic and the producer id beside the body, and a record no
        // read can carry would be lost to every reader.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let big = "x".repeat(2600);
        let limited = |producer: &str| {
            call_with(
                append_input(&wf, producer, 1, 1, &[&big]),
                0,
                4096,
                Duration::from_secs(60),
            )
        };
        let refused = drive(
            &calls,
            calls.append(limited(&"p".repeat(600))).await,
            Codec::Sealing,
        )
        .await
        .unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
        assert!(refused.message.contains("limit"), "{}", refused.message);
        drive(&calls, calls.append(limited("p")).await, Codec::Sealing)
            .await
            .unwrap();
    }

    pub(crate) async fn finish_is_a_call_of_its_own(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &["one"]).await;
        let finished = append(
            &calls,
            json!({
                "stream": stream(&wf, TOPIC),
                "producer_id": "p",
                "attempt": 1,
                "sequence": 2,
                "finish": true,
            }),
        )
        .await
        .unwrap();
        let found = stored(&read_after(&calls, &wf, "").await);
        assert_eq!(
            found
                .iter()
                .map(|r| (r.kind, r.sequence))
                .collect::<Vec<_>>(),
            [
                (StreamRecordKind::Data as i32, 1),
                (StreamRecordKind::Finish as i32, 2),
            ]
        );
        assert!(found[1].body.is_none());
        for (payloads, finish) in [(vec![body("x")], true), (vec![], false)] {
            let refused = append(
                &calls,
                json!({
                    "stream": stream(&wf, TOPIC),
                    "producer_id": "p",
                    "attempt": 1,
                    "sequence": 3,
                    "payloads": payloads,
                    "finish": finish,
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
        }
        assert_eq!(
            finished["cursor"],
            read_after(&calls, &wf, "").await["next_token"]
        );
    }

    pub(crate) async fn a_foreign_cursor_is_refused(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let other = workflow();
        write(&calls, &wf, 1, &["one"]).await;
        let theirs = write(&calls, &other, 1, &["one"]).await;
        for cursor in [theirs.as_str(), "elsewhere:0000abcd:1", "not a cursor"] {
            let refused = read(&calls, read_input(&wf, cursor)).await.unwrap_err();
            assert_eq!(
                kind_of(&refused),
                ("BAD_REQUEST", false, "StreamCursorError"),
                "{cursor}"
            );
        }
        // Another topic of the same owner is another stream too.
        let mine = write(&calls, &wf, 2, &["two"]).await;
        let refused = read(
            &calls,
            json!({"stream": stream(&wf, "other"), "after_token": mine}),
        )
        .await
        .unwrap_err();
        assert_eq!(kind_of(&refused).2, "StreamCursorError");
    }

    pub(crate) async fn argument_mistakes_are_bad_requests(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let no_producer = append(&calls, append_input(&wf, "", 1, 1, &["one"]))
            .await
            .unwrap_err();
        assert_eq!(kind_of(&no_producer), ("BAD_REQUEST", false, "ValueError"));
        let mut not_a_payload = append_input(&wf, "p", 1, 1, &[]);
        not_a_payload["payloads"] = json!(["/w=="]);
        let refused = append(&calls, not_a_payload).await.unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
    }

    pub(crate) async fn a_reference_this_release_cannot_open_is_refused(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = service(store, NexusOptions::default());
        let refused = read(
            &calls,
            json!({"stream": {"kind": "activity", "workflow_id": "wf", "topic": "t"}}),
        )
        .await
        .unwrap_err();
        // The reference decodes, and the call refuses it where it opens the stream, as not
        // implemented in this release.
        assert_eq!(
            kind_of(&refused),
            ("NOT_IMPLEMENTED", false, "StreamUnsupportedError")
        );
        assert!(refused.message.contains("activity"), "{}", refused.message);
    }

    pub(crate) async fn a_read_says_done_once_the_owner_ends(store: Arc<dyn StreamStore>) {
        let wf = workflow();
        write(
            &service(store.clone(), NexusOptions::default()),
            &wf,
            1,
            &["one"],
        )
        .await;
        let ended = NexusCalls::new(
            Arc::new(Streams::new(
                store,
                Arc::new(Ended),
                StreamsOptions::default(),
            )),
            NexusOptions::default(),
        );
        let mut collected = vec![];
        let mut after = String::new();
        for _ in 0..10 {
            let mut input = read_input(&wf, &after);
            input["wait_ms"] = json!(5000);
            let answer = read(&ended, input).await.unwrap();
            collected.extend(texts(&answer));
            if answer["done"] == true {
                assert_eq!(collected, ["one"]);
                return;
            }
            after = token(&answer).to_string();
        }
        panic!("the read never said done, after {collected:?}");
    }

    pub(crate) async fn a_refs_run_picks_the_chain_and_never_ends_a_read_there(
        store: Arc<dyn StreamStore>,
    ) {
        let wf = workflow();
        let calls = NexusCalls::new(
            Arc::new(Streams::new(
                store,
                Arc::new(Continued),
                StreamsOptions {
                    owner_check_min: std::time::Duration::from_millis(10),
                    ..StreamsOptions::default()
                },
            )),
            NexusOptions::default(),
        );
        write(&calls, &wf, 1, &["one"]).await;
        let mut input = read_input(&wf, "");
        input["stream"]["chain_run_id"] = json!("run-1");
        input["wait_ms"] = json!(300);
        let answer = read(&calls, input.clone()).await.unwrap();
        assert_eq!(texts(&answer), ["one"]);
        // `run-1` continued as new, so a read pinned to that run would end here.
        input["after_token"] = answer["next_token"].clone();
        input["state"] = answer["state"].clone();
        let answer = read(&calls, input).await.unwrap();
        assert_eq!(answer["done"], false, "{answer}");
    }

    pub(crate) async fn two_readers_on_one_stream_keep_their_own_places(
        store: Arc<dyn StreamStore>,
    ) {
        let calls = Arc::new(service(store, NexusOptions::default()));
        let wf = workflow();
        write(&calls, &wf, 1, &["a", "b", "c", "d"]).await;
        let page = |after: String, max: i64| {
            let mut input = read_input(&wf, &after);
            input["max_records"] = json!(max);
            input
        };
        // The readers interleave their calls, and each resumes where it left off, so neither
        // takes over the other's read.
        let one = read(&calls, page(String::new(), 1)).await.unwrap();
        let two = read(&calls, page(String::new(), 3)).await.unwrap();
        let one = read(&calls, page(token(&one).to_string(), 1))
            .await
            .unwrap();
        let two = read(&calls, page(token(&two).to_string(), 3))
            .await
            .unwrap();
        assert_eq!(texts(&one), ["b"]);
        assert_eq!(texts(&two), ["d"]);
        // Both park on the tail, at the same place, and both are answered.
        let parked: Vec<_> = (0..2)
            .map(|_| {
                let calls = calls.clone();
                let mut input = read_input(&wf, token(&two));
                input["wait_ms"] = json!(10_000);
                tokio::spawn(async move { read(&calls, input).await.unwrap() })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(300)).await;
        write(&calls, &wf, 5, &["e"]).await;
        for answer in parked {
            let answer = tokio::time::timeout(Duration::from_secs(5), answer)
                .await
                .expect("a parked read never answered")
                .unwrap();
            assert_eq!(texts(&answer), ["e"]);
        }
    }

    pub(crate) async fn a_read_that_does_not_wait_answers_with_what_the_store_holds(
        store: Arc<dyn StreamStore>,
    ) {
        // A zero wait means "do not wait for new records", not "skip a fetch that needs I/O".
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        for start in (0..250).step_by(50) {
            let texts: Vec<String> = (start..start + 50).map(|n| format!("r{n}")).collect();
            let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
            write(&calls, &wf, start + 1, &texts).await;
        }
        let mut sizes = vec![];
        let mut after = String::new();
        loop {
            let mut input = read_input(&wf, &after);
            input["wait_ms"] = json!(0);
            input["max_records"] = json!(100);
            let answer = read(&calls, input).await.unwrap();
            if records(&answer).is_empty() {
                break;
            }
            sizes.push(records(&answer).len());
            after = token(&answer).to_string();
        }
        assert_eq!(sizes, [100, 100, 50]);
    }

    pub(crate) async fn a_read_answer_stays_within_its_byte_budget(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let big = "x".repeat(300 * 1024);
        for n in 0..5 {
            write(&calls, &wf, n + 1, &[&format!("{n}{big}")]).await;
        }
        let mut sizes = vec![];
        let mut after = String::new();
        loop {
            let answer = read_after(&calls, &wf, &after).await;
            if records(&answer).is_empty() {
                break;
            }
            // The budget counts what crosses, the base64 of each record.
            let crossing: usize = records(&answer)
                .iter()
                .map(|wire| wire["record"].as_str().unwrap().len())
                .sum();
            assert!(crossing <= 1 << 20, "{crossing}");
            sizes.push(records(&answer).len());
            after = token(&answer).to_string();
        }
        assert_eq!(sizes, [2, 2, 1]);
    }

    pub(crate) async fn a_record_above_the_budget_still_crosses_alone(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let huge = format!("{}z", "y".repeat(1 << 20));
        write(&calls, &wf, 1, &[&huge, "small"]).await;
        let first = read_after(&calls, &wf, "").await;
        assert_eq!(records(&first).len(), 1);
        assert_eq!(texts(&first)[0].len(), huge.len());
        assert_eq!(
            texts(&read_after(&calls, &wf, token(&first)).await),
            ["small"]
        );
    }

    pub(crate) async fn an_append_above_the_record_limit_is_refused(store: Arc<dyn StreamStore>) {
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        let big = "x".repeat(5000);
        let limited = call_with(
            append_input(&wf, "p", 1, 1, &[&big]),
            0,
            4096,
            Duration::from_secs(60),
        );
        let refused = drive(&calls, calls.append(limited).await, Codec::Sealing)
            .await
            .unwrap_err();
        assert_eq!(kind_of(&refused), ("BAD_REQUEST", false, "ValueError"));
        assert!(refused.message.contains("limit"), "{}", refused.message);
        assert!(records(&read_after(&calls, &wf, "").await).is_empty());
    }

    pub(crate) async fn a_stored_record_above_the_limit_crosses_as_an_error(
        store: Arc<dyn StreamStore>,
    ) {
        // Written under the default limit, as another handler or an Activity may, then read by a
        // handler with a smaller one.
        let calls = service(store, NexusOptions::default());
        let wf = workflow();
        write(&calls, &wf, 1, &[&"x".repeat(5000), "small"]).await;
        let limited = call_with(read_input(&wf, ""), 0, 4096, Duration::from_secs(60));
        let answer = drive(&calls, calls.read(limited).await, Codec::Sealing)
            .await
            .unwrap();
        let [large, small] = records(&answer).as_slice() else {
            panic!("expected two records, got {answer}");
        };
        // The reader is told why, at the cursor it goes on past.
        assert!(
            large["error"].as_str().unwrap().contains("limit"),
            "{large}"
        );
        assert_eq!(large["record"], "");
        assert!(small.get("error").is_none());
        assert_eq!(texts(&answer), ["small"]);
    }
}

macro_rules! lanes {
    ($store:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(store) = $store.await else { return };
                super::cases::$name(store).await;
            }
        )*
    };
}

macro_rules! all_cases {
    ($store:expr) => {
        lanes!($store;
            an_append_reads_back_as_the_stored_record,
            a_batch_lands_in_order_and_a_cursor_resumes_strictly_after,
            max_records_caps_an_answer_and_the_next_call_continues,
            a_read_that_collects_nothing_answers_with_the_callers_cursor,
            a_parked_read_answers_when_a_record_arrives,
            latest_only_positions_a_reader_at_the_end,
            a_retried_append_is_written_once,
            a_divergent_or_stale_retry_is_refused,
            a_new_attempt_crosses_as_a_supersession_at_its_cursor,
            an_append_counts_the_producer_id_toward_the_record_limit,
            finish_is_a_call_of_its_own,
            a_foreign_cursor_is_refused,
            argument_mistakes_are_bad_requests,
            a_reference_this_release_cannot_open_is_refused,
            a_read_says_done_once_the_owner_ends,
            a_refs_run_picks_the_chain_and_never_ends_a_read_there,
            two_readers_on_one_stream_keep_their_own_places,
            a_read_that_does_not_wait_answers_with_what_the_store_holds,
            a_read_answer_stays_within_its_byte_budget,
            a_record_above_the_budget_still_crosses_alone,
            an_append_above_the_record_limit_is_refused,
            a_stored_record_above_the_limit_crosses_as_an_error,
        );
    };
}

async fn on_memory() -> Option<Arc<dyn StreamStore>> {
    Some(memory())
}

mod memory_lane {
    all_cases!(super::on_memory());
}

#[cfg(feature = "redis")]
async fn on_redis() -> Option<Arc<dyn StreamStore>> {
    let Ok(url) = std::env::var("STREAMS_REDIS_URL") else {
        eprintln!("set STREAMS_REDIS_URL to run the Redis cases");
        return None;
    };
    let mut options = temporalio_streams::RedisStoreOptions::new(url);
    options.key_prefix = workflow();
    Some(Arc::new(
        temporalio_streams::RedisStore::connect(options)
            .await
            .unwrap(),
    ))
}

#[cfg(feature = "redis")]
mod redis_lane {
    all_cases!(super::on_redis());
}

/// Each store's own tests cover when a cursor expires. What this pins is how the answer crosses,
/// so it runs on memory, where a test can drop records.
#[tokio::test]
async fn an_expired_cursor_is_told_apart() {
    let memory = Arc::new(MemoryStore::new());
    let calls = service(memory.clone(), NexusOptions::default());
    let wf = workflow();
    let old = write(&calls, &wf, 1, &["one"]).await;
    write(&calls, &wf, 2, &["two"]).await;
    let chain = ChainId {
        namespace: NAMESPACE.to_string(),
        workflow_id: wf.clone(),
        first_run_id: "run-1".to_string(),
    };
    memory.truncate(&chain, TOPIC, 0);
    let refused = read(&calls, read_input(&wf, &old)).await.unwrap_err();
    assert_eq!(
        kind_of(&refused),
        ("BAD_REQUEST", false, "StreamExpiredError")
    );
}
