//! The stream driver: reads a store's notification log and tells the server's stream notifiers.
//!
//! A driver blocks on each doorbell, drains each slot a doorbell names, and sweeps every slot
//! that holds entries now and then, since a ring can be lost. It folds a drain's entries to the
//! newest counter of each stream, sends them in one call, and acks them only once the call
//! landed. A failed call leaves them pending, so this driver or another one sends them again.
//! Delivery is at least once, and a notifier ignores a counter it has seen.

use crate::{
    NotificationEntry, NotificationLog, NotifierClient, NotifyError, StreamError,
    StreamNotification, StreamResult, StreamStore, chain_owner, mint_cursor, progress_counter,
    proto::ChainId, stream_hash,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinSet;

/// How a driver reads the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDriverOptions {
    /// The driver's name in the log's consumer group. Each driver needs its own.
    pub consumer: String,
    /// How often the driver looks at every slot that holds entries.
    pub sweep_interval: Duration,
    /// How long another driver's unacked entries wait before this one takes them.
    pub claim_idle: Duration,
    /// How long one doorbell read blocks.
    pub doorbell_wait: Duration,
    /// The most entries one take reads, and so the most streams one notify call names.
    pub batch: usize,
}

impl Default for StreamDriverOptions {
    fn default() -> Self {
        Self {
            consumer: "driver".to_string(),
            sweep_interval: Duration::from_secs(30),
            claim_idle: Duration::from_secs(60),
            doorbell_wait: Duration::from_secs(5),
            batch: 100,
        }
    }
}

/// Waits before a slot whose notify call failed is drained again.
const RETRY_WAIT: Duration = Duration::from_secs(1);

/// A driver over one store's notification log.
pub struct StreamDriver {
    store: Arc<dyn StreamStore>,
    log: Arc<dyn NotificationLog>,
    client: Arc<dyn NotifierClient>,
    options: StreamDriverOptions,
    /// The slots a drain runs on now, so the doorbells and the sweep don't drain one twice. Set
    /// when another task wanted to drain the slot meanwhile, so the drain looks once more.
    draining: Mutex<HashMap<u32, bool>>,
}

impl StreamDriver {
    /// A driver for `store`, which must keep a notification log, sending through `client`.
    pub fn new(
        store: Arc<dyn StreamStore>,
        client: Arc<dyn NotifierClient>,
        options: StreamDriverOptions,
    ) -> StreamResult<Self> {
        let log = store.notification_log().ok_or_else(|| {
            StreamError::refused(format!(
                "the {} store keeps no notification log, so it needs no driver",
                store.name()
            ))
        })?;
        if options.consumer.is_empty() {
            return Err(StreamError::invalid_argument(
                "a driver needs a consumer name",
            ));
        }
        Ok(Self {
            store,
            log,
            client,
            options,
            draining: Mutex::default(),
        })
    }

    /// Runs until the returned future is dropped. It only ends early when the server can't take
    /// notifications at all.
    pub async fn run(self: Arc<Self>) -> StreamResult<()> {
        let mut tasks = JoinSet::new();
        for doorbell in 0..self.log.doorbells() {
            let driver = self.clone();
            tasks.spawn(async move { driver.listen(doorbell).await });
        }
        let driver = self.clone();
        tasks.spawn(async move { driver.sweep().await });
        // Dropping the set aborts every task, so a caller stops the driver by dropping this.
        match tasks.join_next().await {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(StreamError::storage(format!(
                "a stream driver task stopped: {error}"
            ))),
            None => Ok(()),
        }
    }

    async fn listen(&self, doorbell: u32) -> StreamResult<()> {
        loop {
            let slots = match self
                .log
                .wait_doorbell(doorbell, &self.options.consumer, self.options.doorbell_wait)
                .await
            {
                Ok(slots) => slots,
                Err(error) => {
                    tracing::warn!(doorbell, %error, "A stream driver could not read a doorbell");
                    tokio::time::sleep(RETRY_WAIT).await;
                    continue;
                }
            };
            for slot in slots {
                self.drain(slot).await?;
            }
        }
    }

    async fn sweep(&self) -> StreamResult<()> {
        loop {
            match self.log.busy().await {
                Ok(slots) => {
                    for slot in slots {
                        self.drain(slot).await?;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "A stream driver could not sweep the notification log");
                }
            }
            tokio::time::sleep(self.options.sweep_interval).await;
        }
    }

    /// Sends everything `slot` holds, one batch at a time, unless another task drains it now.
    /// Fails only when the server can't take notifications at all.
    async fn drain(&self, slot: u32) -> StreamResult<()> {
        {
            let mut draining = self.draining.lock().unwrap();
            if let Some(again) = draining.get_mut(&slot) {
                *again = true;
                return Ok(());
            }
            draining.insert(slot, false);
        }
        loop {
            let drained = self.drain_slot(slot).await;
            let mut draining = self.draining.lock().unwrap();
            if drained.is_ok() && draining.get(&slot) == Some(&true) {
                draining.insert(slot, false);
                continue;
            }
            draining.remove(&slot);
            return drained;
        }
    }

    async fn drain_slot(&self, slot: u32) -> StreamResult<()> {
        loop {
            let entries = match self
                .log
                .take(
                    slot,
                    &self.options.consumer,
                    self.options.claim_idle,
                    self.options.batch,
                )
                .await
            {
                Ok(entries) if entries.is_empty() => return Ok(()),
                Ok(entries) => entries,
                Err(error) => {
                    tracing::warn!(slot, %error, "A stream driver could not read a slot");
                    return Ok(());
                }
            };
            let ids: Vec<String> = entries.iter().map(|entry| entry.id.clone()).collect();
            match self.client.notify_streams(self.fold(entries)).await {
                Ok(()) => {
                    if let Err(error) = self.log.ack(slot, &ids).await {
                        // The entries come again, and the notifier drops the repeat.
                        tracing::warn!(slot, %error, "A stream driver could not ack its entries");
                        return Ok(());
                    }
                }
                Err(NotifyError::Unsupported(message)) => {
                    return Err(StreamError::unsupported(format!(
                        "the server takes no stream notifications: {message}"
                    )));
                }
                Err(error) => {
                    // Left pending, so the next drain sends them again.
                    tracing::warn!(slot, %error, "A stream driver's notify call failed");
                    tokio::time::sleep(RETRY_WAIT).await;
                    return Ok(());
                }
            }
        }
    }

    /// One notification per stream, at the newest counter the entries hold for it.
    fn fold(&self, entries: Vec<NotificationEntry>) -> Vec<StreamNotification> {
        let mut newest: BTreeMap<(ChainKey, String), (i64, String)> = BTreeMap::new();
        for entry in entries {
            let Some(counter) = progress_counter(&entry.position) else {
                tracing::warn!(
                    workflow_id = %entry.chain.workflow_id,
                    activity_id = %entry.chain.activity_id,
                    topic = %entry.topic,
                    position = %entry.position,
                    "No notifier counter comes from this position, so it isn't notified"
                );
                continue;
            };
            let key = (ChainKey::of(&entry.chain), entry.topic);
            match newest.get(&key) {
                Some((held, _)) if *held >= counter => {}
                _ => {
                    newest.insert(key, (counter, entry.position));
                }
            }
        }
        newest
            .into_iter()
            .map(|((chain, topic), (counter, position))| {
                let chain = chain.into_chain();
                let (kind, id) = chain_owner(&chain);
                let hash = stream_hash(&chain.namespace, kind, id, &topic);
                StreamNotification {
                    position: mint_cursor(self.store.name(), &hash, &position),
                    chain,
                    topic,
                    counter,
                    close: None,
                }
            })
            .collect()
    }
}

/// A chain as a map key: its namespace, Workflow id, first run, owner kind and Activity id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ChainKey(String, String, String, i32, String);

impl ChainKey {
    fn of(chain: &ChainId) -> Self {
        Self(
            chain.namespace.clone(),
            chain.workflow_id.clone(),
            chain.first_run_id.clone(),
            chain.owner_kind,
            chain.activity_id.clone(),
        )
    }

    fn into_chain(self) -> ChainId {
        ChainId {
            namespace: self.0,
            workflow_id: self.1,
            first_run_id: self.2,
            owner_kind: self.3,
            activity_id: self.4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryStore, proto::StoreAppendRequest};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Records each notify call, and fails them while `fail` is set.
    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<Vec<StreamNotification>>>,
        fail: AtomicBool,
        unsupported: AtomicBool,
    }

    #[async_trait::async_trait]
    impl NotifierClient for Recorder {
        async fn notify(&self, _: StreamNotification) -> Result<(), NotifyError> {
            unreachable!("a driver sends batches")
        }

        async fn notify_streams(
            &self,
            notifications: Vec<StreamNotification>,
        ) -> Result<(), NotifyError> {
            if self.unsupported.load(Ordering::SeqCst) {
                return Err(NotifyError::Unsupported("no notifier".to_string()));
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(NotifyError::Failed("the server is down".to_string()));
            }
            self.calls.lock().unwrap().push(notifications);
            Ok(())
        }
    }

    impl Recorder {
        /// Each stream's notifications, as `(workflow id, topic, counter)`, in call order.
        fn sent(&self) -> Vec<(String, String, i64)> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .map(|n| (n.chain.workflow_id.clone(), n.topic.clone(), n.counter))
                .collect()
        }
    }

    fn chain(workflow_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: workflow_id.to_string(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        }
    }

    async fn append(store: &MemoryStore, workflow_id: &str, topic: &str, sequence: i64) {
        store
            .append(StoreAppendRequest {
                chain: Some(chain(workflow_id)),
                topic: topic.to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence,
                digest: vec![sequence as u8],
                records: vec![b"record".to_vec()],
            })
            .await
            .unwrap();
    }

    fn driver(store: &Arc<MemoryStore>, client: &Arc<Recorder>, consumer: &str) -> StreamDriver {
        driver_claiming(store, client, consumer, Duration::from_secs(60))
    }

    fn driver_claiming(
        store: &Arc<MemoryStore>,
        client: &Arc<Recorder>,
        consumer: &str,
        claim_idle: Duration,
    ) -> StreamDriver {
        StreamDriver::new(
            store.clone(),
            client.clone(),
            StreamDriverOptions {
                consumer: consumer.to_string(),
                claim_idle,
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_drain_sends_each_streams_newest_counter_once_then_acks() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        for sequence in 1..=3 {
            append(&store, "a", "out", sequence).await;
        }
        append(&store, "b", "out", 1).await;
        let driver = driver(&store, &client, "d1");
        driver.drain(0).await.unwrap();
        // Memory positions are offsets, and the counter is the offset plus one.
        assert_eq!(
            client.sent(),
            [
                ("a".to_string(), "out".to_string(), 3),
                ("b".to_string(), "out".to_string(), 1)
            ]
        );
        assert_eq!(client.calls.lock().unwrap().len(), 1, "one call per batch");
        let position = client.calls.lock().unwrap()[0][0].position.clone();
        assert!(position.starts_with("memory:"), "a cursor");
        assert!(
            store
                .notification_log()
                .unwrap()
                .busy()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_activity_chains_entries_notify_the_activitys_stream() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        let activity = ChainId {
            namespace: "ns".to_string(),
            first_run_id: "run-1".to_string(),
            owner_kind: crate::proto::StreamOwnerKind::Activity as i32,
            activity_id: "a".to_string(),
            ..Default::default()
        };
        // A Workflow with the same id and run writes another stream.
        append(&store, "a", "out", 1).await;
        let landed = store
            .append(StoreAppendRequest {
                chain: Some(activity.clone()),
                topic: "out".to_string(),
                producer_id: "p".to_string(),
                attempt: 1,
                sequence: 1,
                digest: vec![1],
                records: vec![b"record".to_vec(), b"record".to_vec()],
            })
            .await
            .unwrap();
        driver(&store, &client, "d1").drain(0).await.unwrap();
        let calls = client.calls.lock().unwrap().clone();
        assert_eq!(calls[0].len(), 2, "two streams, {calls:?}");
        let sent = calls[0].iter().find(|n| n.chain == activity).unwrap();
        assert_eq!(sent.counter, 2);
        assert_eq!(
            sent.position,
            mint_cursor(
                "memory",
                &stream_hash("ns", crate::ACTIVITY_OWNER_KIND, "a", "out"),
                &landed.last_position
            )
        );
    }

    #[tokio::test]
    async fn a_failed_call_leaves_the_entries_for_the_next_drain() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        append(&store, "a", "out", 1).await;
        let driver = driver(&store, &client, "d1");
        client.fail.store(true, Ordering::SeqCst);
        driver.drain(0).await.unwrap();
        assert_eq!(store.notification_log().unwrap().busy().await.unwrap(), [0]);
        client.fail.store(false, Ordering::SeqCst);
        driver.drain(0).await.unwrap();
        assert_eq!(client.sent(), [("a".to_string(), "out".to_string(), 1)]);
    }

    #[tokio::test]
    async fn a_stopped_drivers_entries_move_to_another_driver() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        append(&store, "a", "out", 1).await;
        client.fail.store(true, Ordering::SeqCst);
        driver(&store, &client, "stopped").drain(0).await.unwrap();
        client.fail.store(false, Ordering::SeqCst);
        driver(&store, &client, "other").drain(0).await.unwrap();
        assert!(
            client.sent().is_empty(),
            "the first driver's hold is still fresh"
        );
        driver_claiming(&store, &client, "other", Duration::from_millis(10))
            .drain(0)
            .await
            .unwrap();
        assert_eq!(client.sent(), [("a".to_string(), "out".to_string(), 1)]);
    }

    #[tokio::test]
    async fn a_running_driver_wakes_on_the_doorbell() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        let running = tokio::spawn(Arc::new(driver(&store, &client, "d1")).run());
        append(&store, "a", "out", 1).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.sent().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the doorbell woke the driver");
        running.abort();
    }

    #[tokio::test]
    async fn a_server_without_notifiers_stops_the_driver() {
        let store = Arc::new(MemoryStore::with_notification_log());
        let client = Arc::new(Recorder::default());
        client.unsupported.store(true, Ordering::SeqCst);
        append(&store, "a", "out", 1).await;
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            Arc::new(driver(&store, &client, "d1")).run(),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind, crate::proto::StreamFailureKind::Unsupported);
    }

    #[test]
    fn a_store_without_a_log_needs_no_driver() {
        let error = StreamDriver::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Recorder::default()),
            StreamDriverOptions::default(),
        )
        .err()
        .unwrap();
        assert_eq!(error.kind, crate::proto::StreamFailureKind::Refused);
    }
}
