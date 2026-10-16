//! What the PostgreSQL store owes beyond the store contract: migrations, positions under
//! concurrent writers, the chain close, the janitor and its failures. Each test runs in a schema
//! of its own on `STREAMS_POSTGRES_URL`.

#![cfg(feature = "postgres")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_streams::{
    PostgresStore, PostgresStoreOptions, StreamStore,
    proto::{
        ChainId, DeleteOwnerRequest, StoreAppendRequest, StoreReadRequest, StreamFailureKind,
        StreamOwnerKind,
    },
};
use tokio::time::Instant;

static SCHEMAS: AtomicUsize = AtomicUsize::new(0);

fn url() -> Option<String> {
    let url = std::env::var("STREAMS_POSTGRES_URL").ok();
    if url.is_none() {
        eprintln!("set STREAMS_POSTGRES_URL to run the PostgreSQL store tests");
    }
    url
}

fn options(url: &str) -> PostgresStoreOptions {
    let mut options = PostgresStoreOptions::new(url);
    options.schema = format!(
        "store_{}_{}",
        std::process::id(),
        SCHEMAS.fetch_add(1, Ordering::Relaxed)
    );
    options.max_connections = 4;
    options
}

async fn raw(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

fn chain(workflow_id: &str) -> ChainId {
    ChainId {
        namespace: "ns".to_string(),
        workflow_id: workflow_id.to_string(),
        first_run_id: "run-1".to_string(),
        ..Default::default()
    }
}

fn append(
    chain: &ChainId,
    topic: &str,
    producer: &str,
    sequence: i64,
    n: usize,
) -> StoreAppendRequest {
    StoreAppendRequest {
        chain: Some(chain.clone()),
        topic: topic.to_string(),
        producer_id: producer.to_string(),
        attempt: 1,
        sequence,
        digest: format!("{producer}/{sequence}").into_bytes(),
        records: (0..n)
            .map(|i| format!("r{sequence}.{i}").into_bytes())
            .collect(),
    }
}

fn read(chain: &ChainId, topic: &str, after: &str, wait: Duration) -> StoreReadRequest {
    StoreReadRequest {
        chain: Some(chain.clone()),
        topic: topic.to_string(),
        after_position: after.to_string(),
        wait: Some(wait.try_into().unwrap()),
        max_records: 1000,
    }
}

/// The positions and write times of a topic's records, in position order.
async fn rows(raw: &tokio_postgres::Client, schema: &str) -> Vec<(i64, std::time::SystemTime)> {
    raw.query(
        &format!("SELECT position, written_at FROM {schema}.records ORDER BY topic_id, position"),
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ten_connects_at_once_apply_each_migration_once() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let connects: Vec<_> = (0..10)
        .map(|_| {
            let options = options.clone();
            tokio::spawn(async move { PostgresStore::connect(options).await.map(drop) })
        })
        .collect();
    for connect in connects {
        connect.await.unwrap().unwrap();
    }
    let raw = raw(&url).await;
    let row = raw
        .query_one(
            &format!(
                "SELECT count(*), count(DISTINCT version) FROM {}.schema_migrations",
                options.schema
            ),
            &[],
        )
        .await
        .unwrap();
    let (all, distinct): (i64, i64) = (row.get(0), row.get(1));
    assert!(all >= 1);
    assert_eq!(all, distinct);
}

#[tokio::test]
async fn a_changed_migration_is_refused() {
    let Some(url) = url() else { return };
    let options = options(&url);
    drop(PostgresStore::connect(options.clone()).await.unwrap());
    raw(&url)
        .await
        .execute(
            &format!(
                "UPDATE {}.schema_migrations SET checksum = '\\x00' WHERE version = 1",
                options.schema
            ),
            &[],
        )
        .await
        .unwrap();
    let error = PostgresStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused);
    assert!(error.message.contains("other content"), "{error}");
}

#[tokio::test]
async fn a_schema_with_no_migrations_is_refused_when_they_are_skipped() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.skip_migrations = true;
    let error = PostgresStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Unsupported);
    assert!(error.message.contains("lacks migration 1"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writers_on_one_topic_get_dense_positions_in_time_order() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.max_connections = 16;
    let schema = options.schema.clone();
    let store = Arc::new(PostgresStore::connect(options).await.unwrap());
    let chain = chain("dense");
    let writers: Vec<_> = (0..16)
        .map(|writer| {
            let store = store.clone();
            let chain = chain.clone();
            tokio::spawn(async move {
                for batch in 0..20 {
                    store
                        .append(append(
                            &chain,
                            "out",
                            &format!("p{writer}"),
                            1 + batch * 3,
                            3,
                        ))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    let rows = rows(&raw(&url).await, &schema).await;
    assert_eq!(rows.len(), 16 * 20 * 3);
    for pair in rows.windows(2) {
        assert_eq!(
            pair[1].0,
            pair[0].0 + 1,
            "a gap or a repeat at {}",
            pair[0].0
        );
        assert!(pair[1].1 >= pair[0].1, "time went back at {}", pair[1].0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn appends_against_a_chain_close_land_before_it_returns() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.max_connections = 17;
    let schema = options.schema.clone();
    let store = Arc::new(PostgresStore::connect(options).await.unwrap());
    let chain = chain("closing");
    let landed = Arc::new(AtomicUsize::new(0));
    let writers: Vec<_> = (0..16)
        .map(|writer| {
            let (store, chain, landed) = (store.clone(), chain.clone(), landed.clone());
            tokio::spawn(async move {
                // Writers share 40 topics, and some take their first record near the close.
                for batch in 0.. {
                    let topic = format!("t{}", (writer * 7 + batch) % 40);
                    match store
                        .append(append(&chain, &topic, &format!("p{writer}.{batch}"), 1, 2))
                        .await
                    {
                        Ok(_) => landed.fetch_add(2, Ordering::Relaxed),
                        Err(error) => {
                            assert_eq!(error.kind, StreamFailureKind::Closed, "{error}");
                            return;
                        }
                    };
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    store.close_chain(&chain).await.unwrap();
    let raw = raw(&url).await;
    let closed: std::time::SystemTime = raw
        .query_one("SELECT clock_timestamp()", &[])
        .await
        .unwrap()
        .get(0);
    for writer in writers {
        writer.await.unwrap();
    }
    let rows = rows(&raw, &schema).await;
    assert!(!rows.is_empty());
    assert_eq!(rows.len(), landed.load(Ordering::Relaxed));
    assert!(rows.iter().all(|(_, at)| *at < closed));
}

/// Moves a schema's clock back by `days` for every row the janitor looks at.
async fn age(raw: &tokio_postgres::Client, schema: &str, days: i32) {
    raw.batch_execute(&format!(
        "UPDATE {schema}.records SET written_at = written_at - interval '{days} days'; \
         UPDATE {schema}.producers SET written_at = written_at - interval '{days} days'; \
         UPDATE {schema}.topics SET written_at = written_at - interval '{days} days', \
           trim_at = now(); \
         UPDATE {schema}.chains SET touched_at = touched_at - interval '{days} days'"
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn the_janitor_keeps_holds_and_deletes_ended_holds_and_tombstones() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let raw = raw(&url).await;
    let chain = chain("janitor");
    let landed = store
        .append(append(&chain, "out", "p", 1, 5))
        .await
        .unwrap();
    let first: i64 = landed.first_position.parse().unwrap();
    let last: i64 = landed.last_position.parse().unwrap();
    store
        .hold_floor(&chain, "out", "a/1", &(first + 2).to_string())
        .await
        .unwrap();
    age(&raw, &schema, 8).await;
    assert_eq!(store.run_janitor().await.unwrap(), 2);
    let kept: Vec<i64> = rows(&raw, &schema).await.iter().map(|row| row.0).collect();
    assert_eq!(kept, (first + 2..=last).collect::<Vec<_>>());
    assert_eq!(
        store.dropped_after(&chain, "out", "").await.unwrap(),
        Some((first + 1).to_string())
    );
    // An ended hold goes, and so do the records it held.
    raw.batch_execute(&format!(
        "UPDATE {schema}.floors SET until = now() - interval '1 second'; \
         UPDATE {schema}.topics SET trim_at = now()"
    ))
    .await
    .unwrap();
    assert_eq!(store.run_janitor().await.unwrap(), 3);
    let floors: i64 = raw
        .query_one(&format!("SELECT count(*) FROM {schema}.floors"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(floors, 0);
    // The tombstone still answers what the topic held.
    assert_eq!(
        store.newest_ever(&chain, "out").await.unwrap(),
        last.to_string()
    );
    let expired = store
        .read(read(&chain, "out", &first.to_string(), Duration::ZERO))
        .await
        .unwrap_err();
    assert_eq!(expired.kind, StreamFailureKind::Expired);
    // After the retention plus 30 days, the tombstone and the idle chain go.
    age(&raw, &schema, 31).await;
    store.run_janitor().await.unwrap();
    assert_eq!(store.newest_ever(&chain, "out").await.unwrap(), "");
    let chains: i64 = raw
        .query_one(&format!("SELECT count(*) FROM {schema}.chains"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(chains, 0);
    // A topic made again starts above every position it had before.
    let again = store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    assert!(again.first_position.parse::<i64>().unwrap() > last);
}

#[tokio::test]
async fn an_owner_delete_purges_every_row() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let chain = chain("deleted");
    let scheduled = ChainId {
        owner_kind: StreamOwnerKind::WorkflowActivity as i32,
        activity_id: "1".to_string(),
        ..chain.clone()
    };
    store
        .append(append(&chain, "out", "p", 1, 3))
        .await
        .unwrap();
    store
        .append(append(&scheduled, "out", "p", 1, 2))
        .await
        .unwrap();
    store.hold_floor(&chain, "out", "a/1", "").await.unwrap();
    let deleted = store
        .delete_owner(DeleteOwnerRequest {
            namespace: "ns".to_string(),
            owner_kind: StreamOwnerKind::Workflow as i32,
            workflow_id: "deleted".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(deleted.deleted, 4);
    let raw = raw(&url).await;
    for table in [
        "chains",
        "topics",
        "records",
        "producers",
        "floors",
        "purge",
    ] {
        let count: i64 = raw
            .query_one(&format!("SELECT count(*) FROM {schema}.{table}"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0, "{table}");
    }
}

/// Holds the row lock of a topic in an open transaction of its own, so an append waits.
async fn lock_topic(url: &str, schema: &str) -> tokio_postgres::Client {
    let holder = raw(url).await;
    holder
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM {schema}.topics WHERE topic = 'out' FOR UPDATE"
        ))
        .await
        .unwrap();
    holder
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_killed_during_an_append_leaves_the_outcome_unknown() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let schema = options.schema.clone();
    let store = Arc::new(PostgresStore::connect(options).await.unwrap());
    let chain = chain("killed");
    store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let holder = lock_topic(&url, &schema).await;
    let appending = {
        let (store, chain) = (store.clone(), chain.clone());
        tokio::spawn(async move { store.append(append(&chain, "out", "p", 2, 1)).await })
    };
    let admin = raw(&url).await;
    let started = Instant::now();
    loop {
        let killed: i64 = admin
            .query_one(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
                 WHERE application_name = 'temporal-streams' AND wait_event_type = 'Lock' \
                 AND query LIKE $1",
                &[&format!("%{schema}.append_v1%")],
            )
            .await
            .unwrap()
            .get(0);
        if killed > 0 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "no append waited"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let error = appending.await.unwrap().unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::OutcomeUnknown, "{error}");
    holder.batch_execute("ROLLBACK").await.unwrap();
    // Lang retries with the same sequence. The first retry lands, and a second one repeats it.
    let retry = store
        .append(append(&chain, "out", "p", 2, 1))
        .await
        .unwrap();
    let repeat = store
        .append(append(&chain, "out", "p", 2, 1))
        .await
        .unwrap();
    assert_eq!(retry, repeat);
}

#[tokio::test]
async fn an_append_past_the_statement_timeout_leaves_the_outcome_unknown() {
    let Some(url) = url() else { return };
    let mut options = options(&url);
    options.response_timeout = Duration::from_millis(300);
    let schema = options.schema.clone();
    let store = PostgresStore::connect(options).await.unwrap();
    let chain = chain("slow");
    store
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let holder = lock_topic(&url, &schema).await;
    let error = store
        .append(append(&chain, "out", "p", 2, 1))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::OutcomeUnknown, "{error}");
    assert!(error.message.contains("57014"), "{error}");
    holder.batch_execute("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_read_wakes_on_another_process_append() {
    let Some(url) = url() else { return };
    let options = options(&url);
    let reader = Arc::new(PostgresStore::connect(options.clone()).await.unwrap());
    let writer = PostgresStore::connect(options).await.unwrap();
    let chain = chain("poll");
    let waiting = {
        let (reader, chain) = (reader.clone(), chain.clone());
        tokio::spawn(async move {
            let started = Instant::now();
            let page = reader
                .read(read(&chain, "out", "", Duration::from_secs(10)))
                .await
                .unwrap();
            (page, started.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    writer
        .append(append(&chain, "out", "p", 1, 1))
        .await
        .unwrap();
    let (page, waited) = waiting.await.unwrap();
    assert_eq!(page.records.len(), 1);
    // The append came at 300 ms, and the poll runs every 100 ms.
    assert!(waited < Duration::from_secs(2), "{waited:?}");
}

#[tokio::test]
async fn tls_with_require_reaches_a_server_with_tls() {
    let Ok(url) = std::env::var("STREAMS_POSTGRES_TLS_URL") else {
        eprintln!("set STREAMS_POSTGRES_TLS_URL to run the PostgreSQL TLS test");
        return;
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut options = options(&url);
    options.url = format!("{url}?sslmode=require");
    let store = PostgresStore::connect(options.clone()).await.unwrap();
    let chain = chain("tls");
    store
        .append(append(&chain, "out", "p", 1, 2))
        .await
        .unwrap();
    let page = store
        .read(read(&chain, "out", "", Duration::ZERO))
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    let raw = raw(&url).await;
    let encrypted: bool = raw
        .query_one(
            "SELECT count(*) FILTER (WHERE s.ssl) > 0 FROM pg_stat_ssl s \
             JOIN pg_stat_activity a USING (pid) WHERE a.application_name = 'temporal-streams'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(encrypted);
    // The server's certificate is self-signed, so a check of its chain fails.
    options.url = format!("{url}?sslmode=verify-full");
    let error = PostgresStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Storage, "{error}");
}
