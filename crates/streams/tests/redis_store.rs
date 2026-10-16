//! What the Redis store owes beyond the store contract: the stored format other SDKs read, and
//! how Redis failures reach the caller.
//!
//! Set `STREAMS_REDIS_URL` to run these.

#![cfg(feature = "redis")]

use prost::Message;
use redis::AsyncCommands;
use serde_json::Value as Json;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_streams::{
    RedisStore, RedisStoreOptions, StreamResult, StreamStore,
    proto::{
        ChainId, StoreAppendRequest, StoreAppendResponse, StoreLatestRequest, StoreReadRequest,
        StreamFailureKind, StreamRecord,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

static IDS: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

fn url() -> Option<String> {
    let url = std::env::var("STREAMS_REDIS_URL").ok();
    if url.is_none() {
        eprintln!("set STREAMS_REDIS_URL to run the Redis store tests");
    }
    url
}

struct Setup {
    store: RedisStore,
    raw: redis::aio::MultiplexedConnection,
    prefix: String,
}

async fn setup_at(url: &str, raw_url: &str) -> Setup {
    setup_with(url, raw_url, &unique("redis-store"), |_| {}).await
}

async fn setup_with(
    url: &str,
    raw_url: &str,
    prefix: &str,
    configure: impl FnOnce(&mut RedisStoreOptions),
) -> Setup {
    let prefix = prefix.to_string();
    let mut options = RedisStoreOptions::new(url);
    options.key_prefix = prefix.clone();
    configure(&mut options);
    Setup {
        store: RedisStore::connect(options).await.unwrap(),
        raw: redis::Client::open(raw_url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap(),
        prefix,
    }
}

async fn setup() -> Option<Setup> {
    let url = url()?;
    Some(setup_at(&url, &url).await)
}

fn chain() -> ChainId {
    ChainId {
        namespace: "ns".to_string(),
        workflow_id: unique("wf"),
        first_run_id: "run-1".to_string(),
    }
}

fn record(topic: &str, producer: &str, sequence: i64) -> Vec<u8> {
    StreamRecord {
        topic: topic.to_string(),
        producer_id: producer.to_string(),
        attempt: 1,
        sequence,
        ..Default::default()
    }
    .encode_to_vec()
}

fn append(
    chain: &ChainId,
    producer: &str,
    sequence: i64,
    count: i64,
    digest: &[u8],
) -> StoreAppendRequest {
    StoreAppendRequest {
        chain: Some(chain.clone()),
        topic: "events".to_string(),
        producer_id: producer.to_string(),
        attempt: 1,
        sequence,
        digest: digest.to_vec(),
        records: (0..count)
            .map(|index| record("events", producer, sequence + index))
            .collect(),
    }
}

fn part(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn base(prefix: &str, chain: &ChainId) -> String {
    format!(
        "{}:{{{}:{}:{}}}",
        part(prefix),
        part(&chain.namespace),
        part(&chain.workflow_id),
        part(&chain.first_run_id)
    )
}

fn log_key(prefix: &str, chain: &ChainId, topic: &str) -> String {
    format!("{}:t:{}", base(prefix, chain), part(topic))
}

async fn held(raw: &mut redis::aio::MultiplexedConnection, meta: &str) -> HashMap<String, String> {
    let fields: HashMap<String, String> = raw.hgetall(meta).await.unwrap();
    fields
        .into_iter()
        .filter(|(field, _)| field.starts_with("hw:"))
        .collect()
}

#[tokio::test]
async fn a_batch_is_one_script_and_keeps_one_high_water_field() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    for batch in 0..5 {
        setup
            .store
            .append(append(&chain, "p", 1 + 2 * batch, 2, &[batch as u8 + 1]))
            .await
            .unwrap();
    }
    let log = log_key(&setup.prefix, &chain, "events");
    let entries: Vec<(String, HashMap<String, Vec<u8>>)> =
        setup.raw.xrange_all(&log).await.unwrap();
    let sequences: Vec<_> = entries
        .iter()
        .map(|(_, fields)| StreamRecord::decode(&*fields["r"]).unwrap().sequence)
        .collect();
    assert_eq!(sequences, (1..=10).collect::<Vec<_>>());
    // Bounded state: one field per producer attempt, however long it writes.
    let held = held(&mut setup.raw, &format!("{log}:meta")).await;
    assert_eq!(held.len(), 1);
    let value = &held["hw:1:p:1"];
    // The newest batch's first sequence, its record count, its ids and the digest in hex.
    let parts: Vec<_> = value.split('|').collect();
    assert_eq!(parts[..2], ["9", "2"]);
    assert_eq!(parts[2], entries[8].0);
    assert_eq!(parts[3], entries[9].0);
    assert_eq!(parts[4], "05");
}

#[tokio::test]
async fn appends_answer_as_the_python_sdk_recorded() {
    // The outcomes the Python provider's script gave, run again through the Core store.
    let Some(mut setup) = setup().await else {
        return;
    };
    let vectors: Json =
        serde_json::from_str(include_str!("../testdata/redis/scripts.json")).unwrap();
    let chain = chain();
    let mut landed: HashMap<String, StoreAppendResponse> = HashMap::new();
    for step in vectors["append"].as_array().unwrap() {
        let label = step["label"].as_str().unwrap();
        if label == "new batch after close" {
            setup.store.close_chain(&chain).await.unwrap();
        }
        let digest = hex(step["digest"].as_str().unwrap());
        let result = setup
            .store
            .append(append(
                &chain,
                "p",
                step["sequence"].as_i64().unwrap(),
                step["records"].as_i64().unwrap(),
                &digest,
            ))
            .await;
        match step["outcome"].as_str().unwrap() {
            "ok" => {
                let response = result.unwrap();
                if let Some(same) = step.get("same_ids_as").and_then(Json::as_str) {
                    assert_eq!(response, landed[same], "{label}");
                }
                landed.insert(label.to_string(), response);
            }
            _ => {
                let kind = match step["error_prefix"].as_str().unwrap() {
                    "STREAMS_DIVERGENT" => StreamFailureKind::ProducerDivergent,
                    "STREAMS_STALE" => StreamFailureKind::ProducerStale,
                    "STREAMS_CLOSED" => StreamFailureKind::Closed,
                    other => panic!("no class for {other}"),
                };
                assert_eq!(result.unwrap_err().kind, kind, "{label}");
            }
        }
    }
    assert_eq!(
        landed["retry of newest batch after close"],
        landed["gap after the newest batch"]
    );
    let gap = &landed["gap after the newest batch"];
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let expected = format!("10|1|{}|{}|ee", gap.first_position, gap.last_position);
    let shape: Vec<_> = vectors["meta_value_after_next_and_gap"]
        .as_str()
        .unwrap()
        .split('|')
        .collect();
    assert_eq!(expected.split('|').count(), shape.len());
    assert_eq!(held(&mut setup.raw, &meta).await["hw:1:p:1"], expected);
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[tokio::test]
async fn the_session_field_counts_the_producer_id_in_utf8_bytes() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    let mut request = append(&chain, "日本", 1, 1, b"\x01");
    request.attempt = 7;
    setup.store.append(request).await.unwrap();
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let held = held(&mut setup.raw, &meta).await;
    assert_eq!(held.keys().collect::<Vec<_>>(), ["hw:6:日本:7"]);
}

#[tokio::test]
async fn ids_with_separators_and_braces_keep_their_own_keys() {
    let Some(url) = url() else {
        return;
    };
    let mut setup = setup_at(&url, &url).await;
    let one = ChainId {
        namespace: "ns".to_string(),
        workflow_id: format!("{}:{{b}}", unique("a")),
        first_run_id: "c".to_string(),
    };
    let (head, tail) = one.workflow_id.split_once(':').unwrap();
    let other = ChainId {
        workflow_id: head.to_string(),
        first_run_id: format!("{tail}:c"),
        ..one.clone()
    };
    setup
        .store
        .append(append(&one, "p", 1, 1, b"\x01"))
        .await
        .unwrap();
    setup
        .store
        .append(append(&other, "p", 1, 2, b"\x02"))
        .await
        .unwrap();
    for (chain, count) in [(&one, 1), (&other, 2)] {
        let log = log_key(&setup.prefix, chain, "events");
        assert!(log.contains("%7Bb%7D") || log.contains("%3A"), "{log}");
        let length: usize = setup.raw.xlen(&log).await.unwrap();
        assert_eq!(length, count, "{log}");
    }
}

#[tokio::test]
async fn a_reply_that_refuses_a_write_arrives_as_refused() {
    let Some(mut setup) = setup().await else {
        return;
    };
    let chain = chain();
    let meta = format!("{}:meta", log_key(&setup.prefix, &chain, "events"));
    let _: () = setup.raw.set(&meta, "not a hash").await.unwrap();
    let error = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x01"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused);
    assert!(error.message.contains("WRONGTYPE"), "{error}");
}

#[tokio::test]
async fn a_retention_past_the_epoch_is_refused_at_connect() {
    // The check runs before any connection, so no server is needed.
    let mut options = RedisStoreOptions::new("redis://127.0.0.1:1");
    options.retention = Duration::from_secs(100 * 365 * 24 * 60 * 60);
    let error = RedisStore::connect(options).await.unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Refused);
    assert!(error.message.contains("retention"), "{error}");
}

/// Passes bytes between the store and Redis, and fails one connection on purpose when asked.
struct Faults {
    port: u16,
    mode: Arc<AtomicU8>,
    kills: tokio::sync::watch::Sender<u64>,
}

const PASS: u8 = 0;
/// Forward the next script call, then drop the connection before Redis answers.
const LOSE_REPLY: u8 = 1;
/// Drop the connection instead of forwarding the next request.
const LOSE_REQUEST: u8 = 2;
/// Forward the next blocking read, then never answer on that connection, and keep it open, as a
/// server that died without a reset does.
const HOLD_BLOCKED: u8 = 3;
/// Hold the next script call back a while before forwarding it.
const DELAY_SCRIPT: u8 = 4;

impl Faults {
    async fn start(url: &str) -> Self {
        let upstream = url
            .trim_start_matches("redis://")
            .split('/')
            .next()
            .unwrap()
            .to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mode = Arc::new(AtomicU8::new(PASS));
        let (kills, _) = tokio::sync::watch::channel(0);
        let shared = mode.clone();
        let killer = kills.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let server = TcpStream::connect(&upstream).await.unwrap();
                tokio::spawn(relay(client, server, shared.clone(), killer.subscribe()));
            }
        });
        Self { port, mode, kills }
    }

    fn url(&self) -> String {
        format!("redis://127.0.0.1:{}/0", self.port)
    }

    fn next(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    /// Drops every connection that has carried a blocking read, as a Redis restart does to the
    /// idle ones in a pool.
    fn kill_blocking_connections(&self) {
        self.kills.send_modify(|generation| *generation += 1);
    }
}

async fn relay(
    client: TcpStream,
    server: TcpStream,
    mode: Arc<AtomicU8>,
    mut kills: tokio::sync::watch::Receiver<u64>,
) {
    let (mut client_in, mut client_out) = client.into_split();
    let (mut server_in, mut server_out) = server.into_split();
    let muted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mute = muted.clone();
    let replies = tokio::spawn(async move {
        let mut buffer = vec![0; 64 * 1024];
        while let Ok(read) = server_in.read(&mut buffer).await {
            if read == 0 {
                break;
            }
            if mute.load(Ordering::SeqCst) {
                continue;
            }
            if client_out.write_all(&buffer[..read]).await.is_err() {
                break;
            }
        }
    });
    let mut blocking = false;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = tokio::select! {
            read = client_in.read(&mut buffer) => read,
            _ = kills.changed(), if blocking => break,
        };
        let Ok(read) = read else { break };
        if read == 0 {
            break;
        }
        let chunk = &buffer[..read];
        let has = |needle: &[u8]| chunk.windows(needle.len()).any(|window| window == needle);
        let script = has(b"EVAL");
        blocking |= has(b"BLOCK");
        let now = match mode.load(Ordering::SeqCst) {
            LOSE_REPLY | DELAY_SCRIPT if !script => PASS,
            HOLD_BLOCKED if !has(b"BLOCK") => PASS,
            _ => mode.swap(PASS, Ordering::SeqCst),
        };
        match now {
            LOSE_REQUEST => break,
            LOSE_REPLY => {
                muted.store(true, Ordering::SeqCst);
                let _ = server_out.write_all(chunk).await;
                // Long enough for Redis to run the request, so only its answer is lost.
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            HOLD_BLOCKED => {
                muted.store(true, Ordering::SeqCst);
                let _ = server_out.write_all(chunk).await;
            }
            DELAY_SCRIPT => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                if server_out.write_all(chunk).await.is_err() {
                    break;
                }
            }
            _ => {
                if server_out.write_all(chunk).await.is_err() {
                    break;
                }
            }
        }
    }
    replies.abort();
}

/// Repeats a call the way a producer does after a lost connection, until Redis is reachable.
async fn until_reachable<T>(mut call: impl AsyncFnMut() -> StreamResult<T>) -> StreamResult<T> {
    for _ in 0..50 {
        match call().await {
            Err(error)
                if matches!(
                    error.kind,
                    StreamFailureKind::OutcomeUnknown | StreamFailureKind::Storage
                ) =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => return other,
        }
    }
    call().await
}

#[tokio::test]
async fn a_lost_connection_is_an_unknown_outcome_and_the_retry_dedupes() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let mut setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    // A first batch loads the script, so the lost request is the append itself.
    setup
        .store
        .append(append(&chain, "warm", 1, 1, b"\x01"))
        .await
        .unwrap();
    faults.next(LOSE_REPLY);
    let error = setup
        .store
        .append(append(&chain, "p", 1, 1, b"\x02"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::OutcomeUnknown, "{error}");
    let log = log_key(&setup.prefix, &chain, "events");
    let length: usize = setup.raw.xlen(&log).await.unwrap();
    assert_eq!(length, 2, "the lost append landed");
    // The producer kept its sequence, so the retry matches what landed.
    let retried =
        until_reachable(async || setup.store.append(append(&chain, "p", 1, 1, b"\x02")).await)
            .await
            .unwrap();
    let latest = setup
        .store
        .latest(StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: "events".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(retried.last_position, latest.position);
    let length: usize = setup.raw.xlen(&log).await.unwrap();
    assert_eq!(length, 2);
}

#[tokio::test]
async fn store_errors_on_reads_arrive_as_storage_failures() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    faults.next(LOSE_REQUEST);
    let error = setup
        .store
        .latest(StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: "events".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Storage, "{error}");
    // Reconnected, and lost again.
    until_reachable(async || {
        setup
            .store
            .latest(StoreLatestRequest {
                chain: Some(chain.clone()),
                topic: "events".to_string(),
            })
            .await
    })
    .await
    .unwrap();
    faults.next(LOSE_REQUEST);
    let error = setup
        .store
        .read(StoreReadRequest {
            chain: Some(chain.clone()),
            topic: "events".to_string(),
            max_records: 10,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, StreamFailureKind::Storage, "{error}");
}

fn waiting_read(chain: &ChainId, wait: Duration) -> StoreReadRequest {
    StoreReadRequest {
        chain: Some(chain.clone()),
        topic: "events".to_string(),
        max_records: 10,
        wait: Some(wait.try_into().unwrap()),
        ..Default::default()
    }
}

#[tokio::test]
async fn rule_8_2_a_blocked_read_on_a_silent_server_ends_by_its_deadline() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_with(&faults.url(), &url, &unique("silent"), |options| {
        options.blocking_reads_per_node = 1;
        options.response_timeout = Duration::from_millis(500);
    })
    .await;
    let chain = chain();
    faults.next(HOLD_BLOCKED);
    let started = tokio::time::Instant::now();
    let read = tokio::time::timeout(
        Duration::from_secs(10),
        setup
            .store
            .read(waiting_read(&chain, Duration::from_millis(300))),
    )
    .await
    .expect("the read must end by its wait plus the response timeout");
    assert_eq!(read.unwrap_err().kind, StreamFailureKind::Storage);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    // The connection was dropped, so its place in the pool is free for the next read.
    let next = setup
        .store
        .read(waiting_read(&chain, Duration::from_millis(100)))
        .await
        .unwrap();
    assert!(next.records.is_empty());
}

#[tokio::test]
async fn rule_8_3_a_dead_idle_connection_is_replaced_in_the_same_read() {
    let Some(url) = url() else {
        return;
    };
    let faults = Faults::start(&url).await;
    let setup = setup_at(&faults.url(), &url).await;
    let chain = chain();
    // The first waiting read leaves its connection idle in the pool.
    setup
        .store
        .read(waiting_read(&chain, Duration::from_millis(100)))
        .await
        .unwrap();
    faults.kill_blocking_connections();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let read = setup
        .store
        .read(waiting_read(&chain, Duration::from_millis(100)))
        .await;
    assert!(read.is_ok(), "{read:?}");
}
