//! The Store suite. Every Store runs it, so that each one keeps the same rules.
//!
//! A case says its guarantee and what it needs from a backend in its comment.

use super::{
    case, fields,
    support::{Needs, Run, Target, digest, record, records_of, stores_for, supersession},
};
use crate::{
    Reason,
    ids::{
        activity_producer_id, client_producer_id, named_client_producer_id, workflow_producer_id,
    },
    offset::offset_of_entry_id,
    proto::{Producer, ProducerEpoch, StreamProducerKind, StreamRef},
    store::{Batch, CreateOutcome, Owner, Withdrawal},
};
use futures_util::future::join_all;
use prost_types::Timestamp;
use std::time::Duration;

fn activity(run: &str, attempt: i32, scheduled: i64) -> Producer {
    Producer {
        kind: StreamProducerKind::Activity as i32,
        id: activity_producer_id(run, "a", "s"),
        epoch: Some(ProducerEpoch {
            attempt_scheduled_time: Some(Timestamp {
                seconds: scheduled,
                nanos: 0,
            }),
            attempt,
            ..Default::default()
        }),
        run_id: run.to_string(),
    }
}

fn client(attempt: i32) -> Producer {
    Producer {
        kind: StreamProducerKind::Client as i32,
        id: client_producer_id("me", "p1"),
        epoch: Some(ProducerEpoch {
            attempt,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

/// A second client Producer, which every backend accepts as sent.
fn other_client() -> Producer {
    Producer {
        id: named_client_producer_id("other"),
        ..client(1)
    }
}

async fn append(
    target: &Target,
    producer: &Producer,
    sequence: i64,
    texts: &[&str],
) -> Result<crate::store::Appended, crate::StreamError> {
    let records: Vec<_> = texts.iter().map(|text| record(producer, text)).collect();
    let mut withdrawal = Withdrawal::default();
    let bytes = supersession(producer, &withdrawal);
    withdrawal.supersession = &bytes;
    target
        .store
        .append(
            &target.namespace,
            &target.stream_ref(),
            &Batch {
                producer,
                sequence,
                records: &records,
                digest: &digest(texts),
                withdrawal,
            },
        )
        .await
}

/// A create writes the key once. A create with another Retention finds the Stream and keeps the
/// stored Retention.
async fn a_create_writes_the_key_once_and_keeps_its_retention() {
    for target in stores_for(Needs::STORE_ANSWERS).await {
        let none = Withdrawal::default();
        assert_eq!(
            append(&target, &client(1), 1, &["x"])
                .await
                .unwrap_err()
                .reason,
            Reason::NotFound
        );
        let day = Some(Duration::from_secs(86_400));
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                day,
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(created.outcome, CreateOutcome::Created);
        let again = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                day,
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(again.outcome, CreateOutcome::Exists);
        let other = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Owner::Client,
                &none,
            )
            .await
            .unwrap();
        assert_eq!(other.outcome, CreateOutcome::Exists);
        assert_eq!(other.retention, Duration::from_secs(86_400));
    }
}

/// A resend with the last sequence gets the offsets of its first result, even after another
/// Producer wrote, and a lower sequence is refused.
async fn a_resend_gets_its_first_positions_and_a_lower_sequence_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        let first = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        // Another Producer writes between the batch and its resend.
        append(&target, &other_client(), 1, &["q1"]).await.unwrap();
        let resend = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        if target.gives.gives(Needs::STORE_ANSWERS) {
            assert!(!first.resend && resend.resend);
        }
        append(&target, &p, 2, &["p3"]).await.unwrap();
        assert_eq!(
            append(&target, &p, 1, &["p1"]).await.unwrap_err().reason,
            Reason::Sequence
        );
        let texts = target.texts().await;
        assert_eq!(texts, ["p1", "p2", "q1", "p3"]);
    }
}

/// A resend with the last sequence and other content fails as `seq_divergent` and writes nothing.
async fn a_resend_with_other_content_fails_as_divergent() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        append(&target, &p, 1, &["p1"]).await.unwrap();
        let divergent = append(&target, &p, 1, &["other"]).await.unwrap_err();
        assert_eq!(divergent.reason, Reason::SeqDivergent);
        assert_eq!(target.texts().await, ["p1"]);
    }
}

/// A lower epoch is fenced for each Producer kind.
async fn a_lower_epoch_is_fenced_for_each_producer_kind() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create(Owner::Workflow).await;
        let run = Run::new("r1");
        let la = run.local_activity("k", 8, 1, 2);
        target.batch(&la, 1, &["la2"]).await.unwrap();
        let stale = target
            .batch(&run.local_activity("k", 8, 1, 1), 1, &["la1"])
            .await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        append(&target, &activity("r1", 2, 20), 1, &["a2"])
            .await
            .unwrap();
        let stale = append(&target, &activity("r1", 3, 15), 1, &["a3"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        append(&target, &client(2), 1, &["c2"]).await.unwrap();
        let stale = append(&target, &client(1), 1, &["c1"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        assert_eq!(target.texts().await, ["la2", "a2", "c2"]);
    }
}

/// A close refuses later appends but not a resend of an earlier batch, and a create of the Stream
/// fails as closed. With `STORE_ANSWERS`, a second close returns `already` with the same offset.
async fn a_close_stops_appends_but_not_a_resend() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        let first = append(&target, &p, 1, &["a"]).await.unwrap();
        let closed = target.close(&p).await.unwrap();
        assert_eq!(
            append(&target, &p, 2, &["b"]).await.unwrap_err().reason,
            Reason::Closed
        );
        assert_eq!(
            append(&target, &p, 1, &["a"]).await.unwrap().offsets,
            first.offsets
        );
        let again = target.close(&p).await.unwrap();
        if target.gives.gives(Needs::STORE_ANSWERS) {
            assert!(!closed.already && again.already);
            assert_eq!(again.offset, closed.offset);
        }
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await;
        assert_eq!(created.unwrap_err().reason, Reason::Closed);
    }
}

/// A Stream with a newer format version refuses every write.
async fn a_newer_format_refuses_every_write() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Client).await;
        target.set_meta("v", "2").await;
        let error = append(&target, &client(1), 1, &["a"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::FormatTooNew);
        assert_eq!(
            target.close(&client(1)).await.unwrap_err().reason,
            Reason::FormatTooNew
        );
        let create = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await;
        assert_eq!(create.unwrap_err().reason, Reason::FormatTooNew);
    }
}

/// An expired Stream refuses writes and its create.
async fn an_expired_stream_refuses_writes_and_its_create() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Client).await;
        target.set_meta("exp", "1").await;
        let error = append(&target, &client(1), 1, &["a"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Expired);
        let created = target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await;
        assert_eq!(created.unwrap_err().reason, Reason::Expired);
    }
}

/// An Activity retry flags only its first batch, which withdraws the earlier attempt.
async fn an_activity_retry_flags_its_first_batch_only() {
    for target in stores_for(Needs::UNCHECKED.and(Needs::STORE_ANSWERS)).await {
        target.create(Owner::Workflow).await;
        let (a1, a2) = (activity("r1", 1, 10), activity("r1", 2, 20));
        let other = Producer {
            id: activity_producer_id("r1", "b", "s"),
            ..activity("r1", 1, 10)
        };
        append(&target, &a1, 1, &["x1"]).await.unwrap();
        append(&target, &other, 1, &["y1"]).await.unwrap();
        assert!(append(&target, &a2, 1, &["x2"]).await.unwrap().flagged);
        assert!(!append(&target, &a2, 2, &["x3"]).await.unwrap().flagged);
        assert_eq!(target.visible().await, ["y1", "x2", "x3"]);
    }
}

/// An empty batch of an Activity retry withdraws the earlier attempt's Records.
async fn an_empty_batch_of_a_retry_withdraws_the_earlier_attempt() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create(Owner::Workflow).await;
        append(&target, &activity("r1", 1, 10), 1, &["x1"])
            .await
            .unwrap();
        let empty = append(&target, &activity("r1", 2, 20), 1, &[])
            .await
            .unwrap();
        assert!(empty.flagged);
        assert!(empty.offsets.is_some());
        assert!(target.visible().await.is_empty());
        // The withdrawal is done, so another empty batch writes nothing.
        let again = append(&target, &activity("r1", 3, 30), 1, &[])
            .await
            .unwrap();
        assert!(!again.flagged);
        assert_eq!(again.offsets, None);
        // An empty batch still raises the fence.
        let stale = append(&target, &activity("r1", 2, 20), 2, &["late"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
    }
}

/// An open Workflow Stream's meta keeps the safety TTL, and its log keeps the Retention, after
/// which a read hides every Record. Its close gives the meta the Retention too.
async fn the_ttl_follows_the_owner_and_the_close() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Workflow).await;
        let run = Run::new("r1");
        target
            .batch(&run.local_activity("k", 3, 1, 1), 1, &["a"])
            .await
            .unwrap();
        let week = Duration::from_secs(7 * 86_400).as_millis() as i64;
        let (ttl, meta) = target.ttls().await;
        assert!(0 < ttl && ttl <= week, "{ttl}");
        assert!(
            meta > Duration::from_secs(80 * 86_400).as_millis() as i64,
            "{meta}"
        );
        target.close(&workflow("r1")).await.unwrap();
        let (ttl, meta) = target.ttls().await;
        assert!(0 < ttl && ttl <= week, "{ttl}");
        assert!(
            meta > Duration::from_secs(36 * 86_400).as_millis() as i64,
            "{meta}"
        );
    }
}

/// A read hides Records past the Retention, and the next append trims them and keeps the
/// newest trimmed offset.
async fn an_append_trims_by_retention_and_keeps_the_watermark() {
    for target in stores_for(Needs::RAW_KEYS.and(Needs::SHORT_TIMES)).await {
        let none = Withdrawal::default();
        target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                Some(Duration::from_millis(600)),
                Owner::Workflow,
                &none,
            )
            .await
            .unwrap();
        let old = append(&target, &client(1), 1, &["old"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        // This append keeps the log past the old Record's Retention.
        append(&target, &client(1), 2, &["mid"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        // The read hides the old Record before any append trims it.
        let (state, entries) = target
            .store
            .read(&target.namespace, &target.stream, 0, 10)
            .await
            .unwrap();
        assert!(state.retained_from() > old.offsets.unwrap().1);
        assert_eq!(entries.len(), 2);
        append(&target, &client(1), 3, &["new"]).await.unwrap();
        let (state, entries) = target
            .store
            .read(&target.namespace, &target.stream, 0, 10)
            .await
            .unwrap();
        assert_eq!(state.trimmed, old.offsets.unwrap().1);
        assert_eq!(records_of(&entries), ["mid", "new"]);
    }
}

/// The log expires one Retention after the last write. The next write raises the trimmed offset
/// to the meta's newest one, and its offsets go above it, also with the clock behind it.
async fn offsets_go_on_after_the_log_expires() {
    for target in stores_for(Needs::RAW_KEYS.and(Needs::SHORT_TIMES)).await {
        target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                Some(Duration::from_millis(200)),
                Owner::Workflow,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        append(&target, &client(1), 1, &["old"]).await.unwrap();
        // A newest id one minute ahead, as after the clock steps back.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let ahead = format!("{}-3", now + 60_000);
        target.set_meta("newest", &ahead).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(target.ttls().await.0, -2);
        let new = append(&target, &client(1), 2, &["new"]).await.unwrap();
        let ahead = offset_of_entry_id(&ahead).unwrap();
        assert!(new.offsets.unwrap().0 > ahead);
        let (state, _) = target
            .store
            .read(&target.namespace, &target.stream, 0, 0)
            .await
            .unwrap();
        assert_eq!(state.trimmed, ahead);
        assert_eq!(target.texts().await, ["new"]);
    }
}

/// A wait for the next entry wakes on an append and ends at its deadline.
async fn a_blocked_read_wakes_on_an_append_and_ends_at_its_deadline() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Client).await;
        let first = append(&target, &client(1), 1, &["a"]).await.unwrap();
        let newest = first.offsets.unwrap().1;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let woke = target
            .store
            .wait_after(&target.namespace, &target.stream, newest, deadline)
            .await
            .unwrap();
        assert!(!woke);
        let store = target.store.clone();
        let (namespace, stream) = (target.namespace.clone(), target.stream.clone());
        let waiter = tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            store
                .wait_after(&namespace, &stream, newest, deadline)
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        append(&target, &client(1), 2, &["b"]).await.unwrap();
        assert!(waiter.await.unwrap().unwrap());
    }
}

/// The Records of one batch get consecutive offsets that end at the batch's last offset, so a
/// caller knows the offset of each one.
async fn a_batch_gets_consecutive_offsets_that_end_at_its_last_offset() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Client).await;
        append(&target, &client(1), 1, &["before"]).await.unwrap();
        // Enough Records that `XADD *` on Redis 7.0 would cross a millisecond.
        let texts: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
        let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
        let appended = append(&target, &client(1), 2, &texts).await.unwrap();
        let offsets: Vec<i64> = target.entries().await[1..]
            .iter()
            .map(|entry| entry.offset)
            .collect();
        assert_eq!(offsets.len(), texts.len());
        assert_eq!(
            appended.offsets,
            Some((offsets[0], offsets[offsets.len() - 1]))
        );
        assert!(
            offsets.windows(2).all(|pair| pair[1] == pair[0] + 1),
            "{offsets:?}"
        );
    }
}

/// A negative sequence is refused.
async fn a_negative_sequence_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let error = append(&target, &client(1), -1, &["x"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        assert!(target.texts().await.is_empty());
    }
}

/// A batch with no sequence takes the next one of its Producer's epoch, and a newer epoch starts
/// at one. Such a batch has no dedupe, so its retry writes it again. A batch that sends the taken
/// number is a resend.
async fn a_batch_with_no_sequence_takes_the_next_one_of_its_epoch() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        append(&target, &p, 0, &["a"]).await.unwrap();
        let retry = append(&target, &p, 0, &["a"]).await.unwrap();
        let resend = append(&target, &p, 2, &["a"]).await.unwrap();
        assert_eq!(resend.offsets, retry.offsets);
        let lower = append(&target, &p, 1, &["a"]).await.unwrap_err();
        assert_eq!(lower.reason, Reason::Sequence);
        append(&target, &p, 7, &["b"]).await.unwrap();
        let next = append(&target, &p, 0, &["c"]).await.unwrap();
        let resend = append(&target, &p, 8, &["c"]).await.unwrap();
        assert_eq!(resend.offsets, next.offsets);
        let newer = client(2);
        let first = append(&target, &newer, 0, &["d"]).await.unwrap();
        let resend = append(&target, &newer, 1, &["d"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        assert_eq!(target.texts().await, ["a", "a", "b", "c", "d"]);
    }
}

/// Batches with no sequence that one Producer epoch sends at once each take their own number, so
/// the Store writes all of them.
async fn batches_with_no_sequence_at_once_each_take_their_own_number() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        let texts = ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"];
        let appends = texts
            .iter()
            .map(|text| append(&target, &p, 0, std::slice::from_ref(text)));
        for appended in join_all(appends).await {
            appended.unwrap();
        }
        let mut written = target.texts().await;
        written.sort();
        assert_eq!(written, texts);
        // The batches took 1 to 8, so 8 holds one of them.
        let taken = append(&target, &p, 8, &["other"]).await.unwrap_err();
        assert_eq!(taken.reason, Reason::SeqDivergent);
    }
}

/// A large sequence keeps its dedupe entry, so a resend gets its first answer and a lower or
/// divergent batch is refused. A sequence past 2^53 - 1 is refused.
async fn a_large_sequence_dedupes_and_one_past_2_53_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        let p = client(1);
        let large = 100_000_000_000_000;
        let first = append(&target, &p, large, &["x"]).await.unwrap();
        let resend = append(&target, &p, large, &["x"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        let divergent = append(&target, &p, large, &["other"]).await;
        assert_eq!(divergent.unwrap_err().reason, Reason::SeqDivergent);
        let lower = append(&target, &p, large - 1, &["lower"]).await;
        assert_eq!(lower.unwrap_err().reason, Reason::Sequence);
        assert_eq!(target.texts().await, ["x"]);
        let max = (1 << 53) - 1;
        append(&target, &p, max, &["max"]).await.unwrap();
        let past = append(&target, &p, max + 1, &["past"]).await;
        assert_eq!(past.unwrap_err().reason, Reason::Invalid);
    }
}

/// A create that names another owner than the Stream has fails and changes nothing. So a Workflow
/// doesn't take a client's Stream, and another run chain of a Workflow id is another owner.
async fn a_create_with_another_owner_is_refused() {
    for target in stores_for(Needs::RAW_KEYS.and(Needs::STORE_ANSWERS)).await {
        let target = &target;
        let create = |stream: StreamRef, owner: Owner| async move {
            target
                .store
                .create(
                    &target.namespace,
                    &stream,
                    None,
                    owner,
                    &Withdrawal::default(),
                )
                .await
        };
        let owned = |workflow_id: &str, first_run_id: &str| StreamRef {
            owner: super::support::workflow_execution(workflow_id, first_run_id),
            ..target.stream_ref()
        };
        target.create(Owner::Client).await;
        let refused = create(owned("agent", "first-run"), Owner::Workflow).await;
        assert_eq!(refused.unwrap_err().reason, Reason::NotOwner);
        assert_eq!(target.meta("own").await.as_deref(), Some("client"));
        // The meta keeps the TTL of a client's Stream, below the safety TTL of 90 days.
        let (_, ttl) = target.ttls().await;
        assert!(
            ttl < Duration::from_secs(90 * 86_400).as_millis() as i64,
            "{ttl}"
        );

        let owned = |workflow_id: &str, first_run_id: &str| StreamRef {
            id: format!("{}-owned", target.stream),
            ..owned(workflow_id, first_run_id)
        };
        let created = create(owned("agent", "first-run"), Owner::Workflow).await;
        assert_eq!(created.unwrap().outcome, CreateOutcome::Created);
        for (stream, owner) in [
            (owned("agent", "other-run"), Owner::Workflow),
            (owned("another-agent", "first-run"), Owner::Workflow),
            (owned("agent", "first-run"), Owner::Client),
        ] {
            let refused = create(stream, owner).await;
            assert_eq!(refused.unwrap_err().reason, Reason::NotOwner);
        }
        let again = create(owned("agent", "first-run"), Owner::Workflow).await;
        assert_eq!(again.unwrap().outcome, CreateOutcome::Exists);
    }
}

/// An append removes the fence and dedupe fields of a Producer whose last append is older than
/// the Retention, since its Records are gone too. A lower epoch of that Producer then writes.
async fn an_append_forgets_the_producers_that_the_retention_outlived() {
    for target in stores_for(Needs::RAW_KEYS.and(Needs::SHORT_TIMES)).await {
        target
            .store
            .create(
                &target.namespace,
                &target.stream_ref(),
                Some(Duration::from_millis(300)),
                Owner::Workflow,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        let (old, new) = (client(2), other_client());
        append(&target, &old, 1, &["old"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        append(&target, &new, 1, &["new"]).await.unwrap();
        let kind = StreamProducerKind::Client as i32;
        for field in [fields::fence(kind, &old.id), fields::dedupe(kind, &old.id)] {
            assert_eq!(target.meta(&field).await, None, "{field}");
        }
        assert!(target.meta(&fields::fence(kind, &new.id)).await.is_some());
        append(&target, &client(1), 1, &["late"]).await.unwrap();
        assert_eq!(target.texts().await, ["new", "late"]);
    }
}

/// A Producer with a lower epoch can't close the Stream.
async fn a_stale_producer_cant_close_the_stream() {
    for target in stores_for(Needs::CALLS).await {
        target.create(Owner::Client).await;
        append(&target, &client(2), 1, &["a"]).await.unwrap();
        assert_eq!(
            target.close(&client(1)).await.unwrap_err().reason,
            Reason::Fenced
        );
        target.close(&client(2)).await.unwrap();
    }
}

/// The close Activity's Producer of run `run`.
fn workflow(run: &str) -> Producer {
    Producer {
        kind: StreamProducerKind::Workflow as i32,
        id: workflow_producer_id(run),
        run_id: run.to_string(),
        epoch: None,
    }
}

/// Only the owner closes a Stream. That's the close Activity of its run chain for a Workflow-owned
/// Stream, and a client for a client's Stream. A Local Activity of the chain can't close it, and a
/// ref with no chain can't either.
async fn only_the_owner_closes_a_stream() {
    for target in stores_for(Needs::UNCHECKED.and(Needs::STORE_ANSWERS)).await {
        let run = Run::new("r1");
        let la = run.local_activity("k", 3, 1, 1);
        target.create(Owner::Client).await;
        for producer in [&la, &activity("r1", 1, 10)] {
            let refused = target.close(producer).await.unwrap_err();
            assert_eq!(refused.reason, Reason::NotOwner);
        }
        target.close(&client(1)).await.unwrap();

        let owned = |workflow_id: &str| StreamRef {
            id: format!("{}-owned", target.stream),
            owner: super::support::workflow_execution(workflow_id, "first-run")
                .filter(|_| !workflow_id.is_empty()),
            ..Default::default()
        };
        target
            .store
            .create(
                &target.namespace,
                &owned("agent"),
                None,
                Owner::Workflow,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        for (stream, producer) in [
            (owned("agent"), client(1)),
            (owned("agent"), activity("r1", 1, 10)),
            (owned("agent"), la.clone()),
            (owned("another-agent"), workflow("r1")),
            (owned(""), workflow("r1")),
        ] {
            let refused = target.close_at(&stream, &producer).await.unwrap_err();
            assert_eq!(refused.reason, Reason::NotOwner);
        }
        let closed = target.close_at(&owned("agent"), &workflow("r1")).await;
        assert!(!closed.unwrap().already);
        // The owner check comes first, even on a closed Stream.
        let refused = target.close_at(&owned("agent"), &la).await.unwrap_err();
        assert_eq!(refused.reason, Reason::NotOwner);
    }
}

/// An append whose entry ids would not fit an offset is refused before it writes.
async fn an_append_whose_ids_would_not_fit_an_offset_is_refused_before_it_writes() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Client).await;
        // An entry a minute ahead of the clock, with nearly the last 20-bit sequence.
        let ahead = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 60_000;
        target.add_raw_entry(&format!("{ahead}-1048570")).await;
        let error = append(&target, &client(1), 1, &["a"; 10])
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Storage);
        assert_eq!(target.log_len().await, 1);
    }
}

/// Every Store keeps the meta fields that the shared vectors name.
async fn every_store_keeps_the_meta_fields_of_the_shared_vectors() {
    let vectors: serde_json::Value =
        serde_json::from_str(include_str!("../../testdata/fields.json")).unwrap();
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create(Owner::Workflow).await;
        let producer = activity("run:1", 1, 10);
        append(&target, &producer, 1, &["a"]).await.unwrap();
        let kind = StreamProducerKind::Activity as i32;
        // The vector's Producer is this one, so its field names are the ones the Store keeps.
        assert_eq!(producer.id, vectors["producer_id"].as_str().unwrap());
        let fence = fields::fence(kind, &producer.id);
        assert_eq!(fence, vectors["fence"].as_str().unwrap());
        for field in [
            fence,
            fields::dedupe(kind, &producer.id),
            fields::holds(kind, &producer.id),
            fields::VERSION.to_string(),
        ] {
            assert!(target.meta(&field).await.is_some(), "{field}");
        }
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(a_create_writes_the_key_once_and_keeps_its_retention),
    case!(a_resend_gets_its_first_positions_and_a_lower_sequence_is_refused),
    case!(a_resend_with_other_content_fails_as_divergent),
    case!(a_lower_epoch_is_fenced_for_each_producer_kind),
    case!(a_close_stops_appends_but_not_a_resend),
    case!(a_newer_format_refuses_every_write),
    case!(an_expired_stream_refuses_writes_and_its_create),
    case!(an_activity_retry_flags_its_first_batch_only),
    case!(an_empty_batch_of_a_retry_withdraws_the_earlier_attempt),
    case!(the_ttl_follows_the_owner_and_the_close),
    case!(an_append_trims_by_retention_and_keeps_the_watermark),
    case!(offsets_go_on_after_the_log_expires),
    case!(a_blocked_read_wakes_on_an_append_and_ends_at_its_deadline),
    case!(a_batch_gets_consecutive_offsets_that_end_at_its_last_offset),
    case!(a_negative_sequence_is_refused),
    case!(a_batch_with_no_sequence_takes_the_next_one_of_its_epoch),
    case!(batches_with_no_sequence_at_once_each_take_their_own_number),
    case!(a_large_sequence_dedupes_and_one_past_2_53_is_refused),
    case!(a_create_with_another_owner_is_refused),
    case!(an_append_forgets_the_producers_that_the_retention_outlived),
    case!(a_stale_producer_cant_close_the_stream),
    case!(only_the_owner_closes_a_stream),
    case!(an_append_whose_ids_would_not_fit_an_offset_is_refused_before_it_writes),
    case!(every_store_keeps_the_meta_fields_of_the_shared_vectors),
];
