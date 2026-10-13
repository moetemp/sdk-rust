//! Telling a stream's notifier on the server that the stream moved or closed.
//!
//! The notifier holds the callbacks of every caller a stream-returning Nexus operation attached. A
//! notification is a hint that there is something new to read. The records stay on the read path.

use crate::{
    StreamResult, StreamStore, WORKFLOW_OWNER_KIND, mint_cursor,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse,
    },
    stream_hash,
};
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use tokio::sync::watch;

/// How many streams a process keeps a notifier for when its configuration doesn't say.
pub const DEFAULT_MAX_NOTIFIERS: usize = 1000;

/// How long a stopping Worker waits for the notifications still out.
pub const FLUSH_LIMIT: Duration = Duration::from_secs(10);

// A stream that goes quiet after a failed notification leaves its reader parked until the next
// write, so the last one is tried again a few times. A newer one replaces the retry.
const NOTIFY_RETRIES: u32 = 2;
const NOTIFY_RETRY_FIRST: Duration = Duration::from_millis(250);

// Redis entry ids are `<ms>-<seq>`. Twenty bits hold the sequence, far more entries than one
// stream takes in a millisecond, and the milliseconds stay below 2^43 until the year 2248, so the
// packed value fits a positive i64.
const REDIS_SEQUENCE_BITS: u32 = 20;

/// One notification for one stream's notifier.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamNotification {
    /// The run chain whose stream moved. Its first run keys the notifier.
    pub chain: ChainId,
    /// The stream's topic.
    pub topic: String,
    /// The cursor of the newest record the notification covers.
    pub position: String,
    /// Grows with the stream. The notifier keeps the highest and a caller drops a lower one.
    pub counter: i64,
    /// Set on the close, which completes every attached operation with it.
    pub close_result: Option<Payload>,
}

/// Why a notifier didn't take a notification.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotifyError {
    /// The server will never take it, so trying again can't help.
    #[error("the server refused the notification: {0}")]
    Refused(String),
    /// It may take it later.
    #[error("the notification failed: {0}")]
    Failed(String),
    /// The server has no stream notifier.
    #[error("the server has no stream notifier: {0}")]
    Unsupported(String),
}

/// Sends notifications to the stream notifier on the server.
#[async_trait::async_trait]
pub trait NotifierClient: Send + Sync {
    /// Sends one notification.
    async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError>;
}

/// The notifier counter for a store position.
///
/// It must grow with the stream, and producers in different processes must agree on it. So it
/// comes from the store's own position, not from a clock. A memory position, an offset, gives
/// `offset + 1`. A Redis entry id `<ms>-<seq>` gives `ms << 20 | seq`, with a sequence beyond 20
/// bits held at the largest, which keeps the order non-decreasing. No position gives zero. Two
/// records past the 20-bit sequence in one millisecond get one counter, and the notifier drops
/// the second as a repeat. The next notification tells the reader again.
pub fn progress_counter(position: &str) -> Option<i64> {
    if position.is_empty() {
        return Some(0);
    }
    match position.split_once('-') {
        Some((milliseconds, sequence)) => {
            let milliseconds: i64 = milliseconds.parse().ok()?;
            let sequence: u64 = sequence.parse().ok()?;
            let largest = (1u64 << REDIS_SEQUENCE_BITS) - 1;
            let sequence = i64::try_from(sequence.min(largest)).ok()?;
            milliseconds
                .checked_mul(1 << REDIS_SEQUENCE_BITS)
                .map(|shifted| shifted | sequence)
                .filter(|counter| *counter >= 0)
        }
        None => position.parse::<i64>().ok()?.checked_add(1),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    namespace: String,
    workflow_id: String,
    first_run_id: String,
    topic: String,
}

impl StreamKey {
    fn new(chain: &ChainId, topic: &str) -> Self {
        Self {
            namespace: chain.namespace.clone(),
            workflow_id: chain.workflow_id.clone(),
            first_run_id: chain.first_run_id.clone(),
            topic: topic.to_string(),
        }
    }

    fn chain(&self) -> ChainId {
        ChainId {
            namespace: self.namespace.clone(),
            workflow_id: self.workflow_id.clone(),
            first_run_id: self.first_run_id.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct SlotState {
    pending: Option<(String, i64)>,
    sending: bool,
    /// Positions still being looked up, which the slot sends once found.
    resolving: usize,
    closed: bool,
}

/// One stream's notifications: folded, with one call in flight per slot.
#[derive(Debug)]
struct Slot {
    key: StreamKey,
    state: Mutex<SlotState>,
    idle: watch::Sender<bool>,
}

impl Slot {
    fn new(key: StreamKey) -> Arc<Self> {
        Arc::new(Self {
            key,
            state: Mutex::default(),
            idle: watch::Sender::new(true),
        })
    }

    /// Records the newest position and returns at once. While a call is out, later
    /// notifications fold into the one that goes next, so a burst costs at most two calls.
    fn notify(self: &Arc<Self>, client: &Arc<dyn NotifierClient>, position: String, counter: i64) {
        let mut state = self.state.lock().unwrap();
        self.hold(&mut state, client, position, counter);
    }

    /// Like [Slot::notify] with the position `lookup` finds, without waiting for it. The slot
    /// counts as busy until the lookup ends, so a close or a flush waits for it.
    fn notify_found<F>(self: &Arc<Self>, client: &Arc<dyn NotifierClient>, lookup: F)
    where
        F: Future<Output = Option<(String, i64)>> + Send + 'static,
    {
        {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return;
            }
            state.resolving += 1;
            self.idle.send_replace(false);
        }
        let (slot, client) = (self.clone(), client.clone());
        tokio::spawn(async move {
            let found = lookup.await;
            let mut state = slot.state.lock().unwrap();
            state.resolving -= 1;
            if let Some((position, counter)) = found {
                slot.hold(&mut state, &client, position, counter);
            }
            slot.mark_idle_if_done(&state);
        });
    }

    fn hold(
        self: &Arc<Self>,
        state: &mut SlotState,
        client: &Arc<dyn NotifierClient>,
        position: String,
        counter: i64,
    ) {
        if state.closed {
            return;
        }
        if state
            .pending
            .as_ref()
            .is_none_or(|(_, held)| counter >= *held)
        {
            state.pending = Some((position, counter));
        }
        if !state.sending {
            state.sending = true;
            self.idle.send_replace(false);
            tokio::spawn(self.clone().send_pending(client.clone()));
        }
    }

    fn mark_idle_if_done(&self, state: &SlotState) {
        if !state.sending && state.resolving == 0 {
            self.idle.send_replace(true);
        }
    }

    async fn send_pending(self: Arc<Self>, client: Arc<dyn NotifierClient>) {
        loop {
            let (position, counter) = {
                let mut state = self.state.lock().unwrap();
                match state.pending.take() {
                    Some(next) => next,
                    None => {
                        state.sending = false;
                        self.mark_idle_if_done(&state);
                        return;
                    }
                }
            };
            let notification = StreamNotification {
                chain: self.key.chain(),
                topic: self.key.topic.clone(),
                position,
                counter,
                close_result: None,
            };
            self.send(&client, notification).await;
        }
    }

    async fn send(&self, client: &Arc<dyn NotifierClient>, notification: StreamNotification) {
        let mut delay = NOTIFY_RETRY_FIRST;
        for tries_left in (0..=NOTIFY_RETRIES).rev() {
            // A notification is only a hint, and the reader reads from its own cursor, so a
            // notification that can't go never fails the write.
            match client.notify(notification.clone()).await {
                Ok(()) => return,
                Err(NotifyError::Failed(error)) if tries_left > 0 && !self.superseded() => {
                    tracing::debug!(
                        workflow_id = %self.key.workflow_id,
                        topic = %self.key.topic,
                        %error,
                        "A stream notification failed; trying again in {delay:?}"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        workflow_id = %self.key.workflow_id,
                        topic = %self.key.topic,
                        %error,
                        "A stream notification failed. The next write tells the reader again"
                    );
                    return;
                }
            }
            tokio::time::sleep(delay).await;
            delay *= 2;
            if self.superseded() {
                return;
            }
        }
    }

    /// Whether something newer will reach the notifier anyway: a later notification, or the
    /// close.
    fn superseded(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.pending.is_some() || state.closed
    }

    /// Stops new notifications and waits for the one in flight, so a close is the last thing the
    /// notifier hears from this process.
    async fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.wait_idle().await;
    }

    async fn wait_idle(&self) {
        let mut idle = self.idle.subscribe();
        // The sender lives as long as the slot, so the wait can't fail.
        let _ = idle.wait_for(|idle| *idle).await;
    }

    fn is_idle(&self) -> bool {
        *self.idle.borrow()
    }
}

/// Notifies the stream notifiers of the streams a process writes to.
///
/// Keeps one slot per stream, and drops a slot when its stream closes, when its chain ends, or
/// when it's the least recently used of more than the limit. A dropped slot still sends what it
/// holds, and [Notifier::flush] waits for it.
pub struct Notifier {
    client: Arc<dyn NotifierClient>,
    slots: Mutex<lru::LruCache<StreamKey, Arc<Slot>>>,
    retired: Mutex<Vec<Arc<Slot>>>,
}

impl std::fmt::Debug for Notifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notifier")
            .field("slots", &self.slots.lock().unwrap().len())
            .finish_non_exhaustive()
    }
}

impl Notifier {
    /// Notifies through `client`, keeping at most `max_notifiers` streams' slots.
    pub fn new(client: Arc<dyn NotifierClient>, max_notifiers: usize) -> Self {
        Self {
            client: Arc::new(UnlessUnsupported {
                client,
                unsupported: AtomicBool::new(false),
            }),
            slots: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(max_notifiers).unwrap_or(NonZeroUsize::MIN),
            )),
            retired: Mutex::default(),
        }
    }

    /// Tells the notifier of `chain`'s stream on `topic` that it moved to `position`. Never
    /// waits, and never fails: a notification that can't go is logged and dropped.
    pub fn notify(&self, chain: &ChainId, topic: &str, position: String, counter: i64) {
        self.slot(StreamKey::new(chain, topic))
            .notify(&self.client, position, counter);
    }

    /// Like [Notifier::notify] with the cursor and counter `lookup` finds, without waiting for
    /// it. A close of the stream and [Notifier::flush] wait for the lookup.
    pub fn notify_found<F>(&self, chain: &ChainId, topic: &str, lookup: F)
    where
        F: Future<Output = Option<(String, i64)>> + Send + 'static,
    {
        self.slot(StreamKey::new(chain, topic))
            .notify_found(&self.client, lookup);
    }

    /// Closes the stream on the server with `result`, after the notification in flight, and
    /// drops its slot. `counter` must rank above the stream's last notification.
    pub async fn close(
        &self,
        chain: &ChainId,
        topic: &str,
        position: String,
        counter: i64,
        result: Payload,
    ) -> Result<(), NotifyError> {
        let key = StreamKey::new(chain, topic);
        let held = self.slots.lock().unwrap().pop(&key);
        if let Some(slot) = &held {
            slot.close().await;
        }
        self.client
            .notify(StreamNotification {
                chain: chain.clone(),
                topic: topic.to_string(),
                position,
                counter,
                close_result: Some(result),
            })
            .await
    }

    /// Drops the slots of `chain`'s streams, once the chain ended.
    pub fn forget_chain(&self, chain: &ChainId) {
        let mut slots = self.slots.lock().unwrap();
        let ended: Vec<StreamKey> = slots
            .iter()
            .map(|(key, _)| key)
            .filter(|key| key.chain() == *chain)
            .cloned()
            .collect();
        let dropped: Vec<Arc<Slot>> = ended.iter().filter_map(|key| slots.pop(key)).collect();
        drop(slots);
        self.retire(dropped);
    }

    /// Drops the slot of one stream, once it closed.
    pub fn forget_stream(&self, chain: &ChainId, topic: &str) {
        let dropped = self
            .slots
            .lock()
            .unwrap()
            .pop(&StreamKey::new(chain, topic));
        self.retire(dropped);
    }

    /// Waits until no notification is out or waiting.
    pub async fn flush(&self) {
        let slots: Vec<Arc<Slot>> = self
            .slots
            .lock()
            .unwrap()
            .iter()
            .map(|(_, slot)| slot.clone())
            .chain(self.retired.lock().unwrap().iter().cloned())
            .collect();
        for slot in slots {
            slot.wait_idle().await;
        }
        self.retired.lock().unwrap().retain(|slot| !slot.is_idle());
    }

    fn slot(&self, key: StreamKey) -> Arc<Slot> {
        let mut slots = self.slots.lock().unwrap();
        if let Some(slot) = slots.get(&key) {
            return slot.clone();
        }
        let slot = Slot::new(key.clone());
        let evicted = slots.push(key, slot.clone()).map(|(_, slot)| slot);
        drop(slots);
        self.retire(evicted);
        slot
    }

    fn retire(&self, dropped: impl IntoIterator<Item = Arc<Slot>>) {
        let mut retired = self.retired.lock().unwrap();
        retired.retain(|slot| !slot.is_idle());
        retired.extend(dropped.into_iter().filter(|slot| !slot.is_idle()));
    }
}

/// Stops every call once the server answers that it has no stream notifier. The answer can't
/// change while the process runs, and each call would cost a round trip.
struct UnlessUnsupported {
    client: Arc<dyn NotifierClient>,
    unsupported: AtomicBool,
}

#[async_trait::async_trait]
impl NotifierClient for UnlessUnsupported {
    async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError> {
        if self.unsupported.load(Ordering::Relaxed) {
            return Ok(());
        }
        match self.client.notify(notification).await {
            // No caller can be attached to a notifier the server doesn't have.
            Err(NotifyError::Unsupported(error)) => {
                if !self.unsupported.swap(true, Ordering::Relaxed) {
                    tracing::debug!(
                        %error,
                        "The server has no stream notifier, so this process stops notifying"
                    );
                }
                Ok(())
            }
            other => other,
        }
    }
}

/// A store that notifies each stream's notifier after an append lands, a Workflow's output
/// included.
///
/// Wraps the store a process hands to its stream calls and its Workers, so every write path
/// notifies without knowing about notifiers. It forwards every trait method by name, since a
/// method left to its default would answer for the inner store.
pub struct NotifyingStore {
    inner: Arc<dyn StreamStore>,
    notifier: Arc<Notifier>,
}

impl NotifyingStore {
    /// Wraps `inner`, notifying through `notifier`.
    pub fn new(inner: Arc<dyn StreamStore>, notifier: Arc<Notifier>) -> Self {
        Self { inner, notifier }
    }

    /// The notifier this store notifies through.
    pub fn notifier(&self) -> &Arc<Notifier> {
        &self.notifier
    }

    fn notify_at(&self, chain: &ChainId, topic: &str, position: &str) {
        if let Some((cursor, counter)) =
            notification_at(self.inner.as_ref(), chain, topic, position)
        {
            self.notifier.notify(chain, topic, cursor, counter);
        }
    }
}

fn cursor_of(store: &dyn StreamStore, chain: &ChainId, topic: &str, position: &str) -> String {
    let hash = stream_hash(
        &chain.namespace,
        WORKFLOW_OWNER_KIND,
        &chain.workflow_id,
        topic,
    );
    mint_cursor(store.name(), &hash, position)
}

/// The cursor and counter that tell a reader about the record at `position`.
fn notification_at(
    store: &dyn StreamStore,
    chain: &ChainId,
    topic: &str,
    position: &str,
) -> Option<(String, i64)> {
    // The write landed, so a notification that can't go must not turn it into an error the
    // caller might retry with other content.
    let Some(counter) = progress_counter(position) else {
        tracing::warn!(
            workflow_id = %chain.workflow_id,
            topic,
            position,
            "No notifier counter comes from this position, so the write isn't notified"
        );
        return None;
    };
    Some((cursor_of(store, chain, topic, position), counter))
}

#[async_trait::async_trait]
impl StreamStore for NotifyingStore {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
        let chain = request.chain.clone();
        let topic = request.topic.clone();
        let landed = self.inner.append(request).await?;
        if let Some(chain) = &chain {
            self.notify_at(chain, &topic, &landed.last_position);
        }
        Ok(landed)
    }

    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse> {
        self.inner.read(request).await
    }

    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
        self.inner.latest(request).await
    }

    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>> {
        self.inner.record_at(chain, topic, position).await
    }

    async fn newest_ever(&self, chain: &ChainId, topic: &str) -> StreamResult<String> {
        self.inner.newest_ever(chain, topic).await
    }

    async fn dropped_after(
        &self,
        chain: &ChainId,
        topic: &str,
        mark: &str,
    ) -> StreamResult<Option<String>> {
        self.inner.dropped_after(chain, topic, mark).await
    }

    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.inner.close_chain(chain).await?;
        self.notifier.forget_chain(chain);
        Ok(())
    }

    async fn close_topic(
        &self,
        chain: &ChainId,
        topic: &str,
        result: Option<Payload>,
    ) -> StreamResult<()> {
        self.inner.close_topic(chain, topic, result).await?;
        self.notifier.forget_stream(chain, topic);
        Ok(())
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        self.inner.delete_owner(request).await
    }

    async fn flush_notifications(&self) {
        self.notifier.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryStore, proto::StoreAppendRequest};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    /// Records what it was sent. While `holding`, each call waits for a permit, so a test
    /// decides when the call in flight returns.
    struct Recorder {
        sent: Mutex<Vec<StreamNotification>>,
        holding: AtomicBool,
        gate: Semaphore,
        fail: AtomicBool,
        /// How many of the next calls fail.
        failures: AtomicUsize,
        unsupported: AtomicBool,
    }

    impl Default for Recorder {
        fn default() -> Self {
            Self {
                sent: Mutex::default(),
                holding: AtomicBool::new(false),
                gate: Semaphore::new(0),
                fail: AtomicBool::new(false),
                failures: AtomicUsize::new(0),
                unsupported: AtomicBool::new(false),
            }
        }
    }

    impl Recorder {
        fn holding() -> Arc<Self> {
            let recorder = Arc::new(Self::default());
            recorder.holding.store(true, Ordering::SeqCst);
            recorder
        }

        fn sent(&self) -> Vec<(String, i64)> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|n| (n.position.clone(), n.counter))
                .collect()
        }

        fn release(&self, calls: usize) {
            self.gate.add_permits(calls);
        }
    }

    #[async_trait::async_trait]
    impl NotifierClient for Recorder {
        async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError> {
            self.sent.lock().unwrap().push(notification);
            if self.holding.load(Ordering::SeqCst) {
                self.gate.acquire().await.unwrap().forget();
            }
            if self.unsupported.load(Ordering::SeqCst) {
                return Err(NotifyError::Unsupported("unimplemented".to_string()));
            }
            let failing = self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if failing || self.fail.load(Ordering::SeqCst) {
                return Err(NotifyError::Failed("down".to_string()));
            }
            Ok(())
        }
    }

    fn chain(first_run_id: &str) -> ChainId {
        ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: first_run_id.to_string(),
        }
    }

    /// Lets every spawned send run until it waits.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn counters_come_from_the_stores_position() {
        assert_eq!(progress_counter(""), Some(0));
        assert_eq!(progress_counter("0"), Some(1));
        assert_eq!(progress_counter("41"), Some(42));
        assert_eq!(
            progress_counter("1700000000000-3"),
            Some((1_700_000_000_000 << 20) | 3)
        );
        assert_eq!(
            progress_counter("5-2000000"),
            Some((5 << 20) | ((1 << 20) - 1)),
            "a sequence past 20 bits is held at the largest"
        );
        assert!(progress_counter("1-1") < progress_counter("1-2"));
        assert!(progress_counter("1-2") < progress_counter("2-0"));
        assert_eq!(progress_counter("not a position"), None);
        assert_eq!(progress_counter("9223372036854775807-0"), None, "past i64");
    }

    #[tokio::test]
    async fn notifications_fold_with_one_call_in_flight() {
        let recorder = Recorder::holding();
        let notifier = Notifier::new(recorder.clone(), 10);
        notifier.notify(&chain("run-1"), "t", "c1".to_string(), 1);
        settle().await;
        notifier.notify(&chain("run-1"), "t", "c3".to_string(), 3);
        notifier.notify(&chain("run-1"), "t", "c2".to_string(), 2);
        settle().await;
        assert_eq!(
            recorder.sent(),
            vec![("c1".to_string(), 1)],
            "one call in flight"
        );
        recorder.release(2);
        notifier.flush().await;
        assert_eq!(
            recorder.sent(),
            vec![("c1".to_string(), 1), ("c3".to_string(), 3)],
            "the burst folds into one call with the highest counter"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_last_notification_is_tried_twice_more() {
        let recorder = Arc::new(Recorder::default());
        recorder.failures.store(1, Ordering::SeqCst);
        let notifier = Notifier::new(recorder.clone(), 10);
        notifier.notify(&chain("run-1"), "t", "c1".to_string(), 1);
        notifier.flush().await;
        assert_eq!(
            recorder.sent(),
            vec![("c1".to_string(), 1), ("c1".to_string(), 1)],
            "a quiet stream gets its last notification again"
        );

        recorder.fail.store(true, Ordering::SeqCst);
        notifier.notify(&chain("run-1"), "t", "c2".to_string(), 2);
        notifier.flush().await;
        assert_eq!(
            recorder.sent().len(),
            5,
            "two more tries, then it's dropped"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_newer_notification_goes_instead_of_a_retry() {
        let recorder = Arc::new(Recorder::default());
        recorder.failures.store(1, Ordering::SeqCst);
        let notifier = Notifier::new(recorder.clone(), 10);
        notifier.notify(&chain("run-1"), "t", "c1".to_string(), 1);
        settle().await;
        notifier.notify(&chain("run-1"), "t", "c2".to_string(), 2);
        notifier.flush().await;
        assert_eq!(
            recorder.sent(),
            vec![("c1".to_string(), 1), ("c2".to_string(), 2)]
        );
    }

    #[tokio::test]
    async fn a_server_without_the_notifier_costs_one_call() {
        let recorder = Arc::new(Recorder::default());
        recorder.unsupported.store(true, Ordering::SeqCst);
        let notifier = Notifier::new(recorder.clone(), 10);
        notifier.notify(&chain("run-1"), "a", "a1".to_string(), 1);
        notifier.flush().await;
        notifier.notify(&chain("run-1"), "b", "b1".to_string(), 1);
        notifier.notify(&chain("run-2"), "a", "a1".to_string(), 1);
        notifier.flush().await;
        notifier
            .close(
                &chain("run-1"),
                "a",
                "a1".to_string(),
                2,
                Payload::default(),
            )
            .await
            .unwrap();
        assert_eq!(recorder.sent().len(), 1);
    }

    #[tokio::test]
    async fn close_waits_for_the_call_in_flight_and_carries_the_result() {
        let recorder = Recorder::holding();
        let notifier = Arc::new(Notifier::new(recorder.clone(), 10));
        notifier.notify(&chain("run-1"), "t", "c1".to_string(), 1);
        settle().await;
        let closing = {
            let notifier = notifier.clone();
            tokio::spawn(async move {
                let result = Payload {
                    data: b"summary".to_vec(),
                    ..Default::default()
                };
                notifier
                    .close(&chain("run-1"), "t", "c1".to_string(), 2, result)
                    .await
            })
        };
        settle().await;
        assert_eq!(
            recorder.sent().len(),
            1,
            "the close waits for the call in flight"
        );
        recorder.release(2);
        closing.await.unwrap().unwrap();
        let sent = recorder.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].counter, 2);
        assert_eq!(sent[1].close_result.as_ref().unwrap().data, b"summary");
    }

    #[tokio::test]
    async fn a_notification_after_close_sends_nothing() {
        let recorder = Arc::new(Recorder::default());
        let notifier = Notifier::new(recorder.clone(), 10);
        notifier.notify(&chain("run-1"), "t", "c1".to_string(), 1);
        let slot = notifier.slot(StreamKey::new(&chain("run-1"), "t"));
        slot.close().await;
        slot.notify(&notifier.client, "c2".to_string(), 2);
        notifier.flush().await;
        assert_eq!(recorder.sent(), vec![("c1".to_string(), 1)]);
    }

    #[tokio::test]
    async fn notifiers_beyond_the_cap_are_dropped_least_recent_first() {
        let recorder = Recorder::holding();
        let notifier = Notifier::new(recorder.clone(), 2);
        notifier.notify(&chain("run-1"), "a", "a1".to_string(), 1);
        notifier.notify(&chain("run-1"), "b", "b1".to_string(), 1);
        settle().await;
        notifier.notify(&chain("run-1"), "a", "a2".to_string(), 2);
        notifier.notify(&chain("run-1"), "c", "c1".to_string(), 1);
        let kept: Vec<String> = notifier
            .slots
            .lock()
            .unwrap()
            .iter()
            .map(|(key, _)| key.topic.clone())
            .collect();
        assert_eq!(kept, vec!["c", "a"], "b was the least recently used");
        recorder.release(10);
        notifier.flush().await;
        let mut sent = recorder.sent();
        sent.sort();
        assert_eq!(
            sent,
            vec![
                ("a1".to_string(), 1),
                ("a2".to_string(), 2),
                ("b1".to_string(), 1),
                ("c1".to_string(), 1)
            ],
            "a dropped notifier still sends what it holds"
        );
        assert!(notifier.retired.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_ended_chain_drops_its_notifiers() {
        let recorder = Arc::new(Recorder::default());
        let notifier = Notifier::new(recorder.clone(), 10);
        notifier.notify(&chain("run-1"), "a", "a1".to_string(), 1);
        notifier.notify(&chain("run-1"), "b", "b1".to_string(), 1);
        notifier.notify(&chain("run-9"), "a", "x1".to_string(), 1);
        notifier.forget_chain(&chain("run-1"));
        notifier.forget_stream(&chain("run-9"), "a");
        assert_eq!(notifier.slots.lock().unwrap().len(), 0);
        notifier.flush().await;
        assert_eq!(
            recorder.sent().len(),
            3,
            "the dropped ones still sent what they held"
        );
    }

    #[tokio::test]
    async fn flush_waits_for_every_notification_in_flight() {
        let recorder = Recorder::holding();
        let notifier = Arc::new(Notifier::new(recorder.clone(), 10));
        notifier.notify(&chain("run-1"), "a", "a1".to_string(), 1);
        notifier.notify(&chain("run-1"), "b", "b1".to_string(), 1);
        let flushing = {
            let notifier = notifier.clone();
            tokio::spawn(async move { notifier.flush().await })
        };
        settle().await;
        assert!(!flushing.is_finished());
        recorder.release(1);
        settle().await;
        assert!(!flushing.is_finished(), "one is still out");
        recorder.release(1);
        flushing.await.unwrap();
    }

    fn append_request(chain: &ChainId, topic: &str, sequence: i64) -> StoreAppendRequest {
        StoreAppendRequest {
            chain: Some(chain.clone()),
            topic: topic.to_string(),
            producer_id: "p".to_string(),
            attempt: 1,
            sequence,
            digest: vec![sequence as u8],
            records: vec![b"record".to_vec()],
        }
    }

    fn notifying_store(recorder: &Arc<Recorder>) -> (Arc<MemoryStore>, NotifyingStore) {
        let inner = Arc::new(MemoryStore::new());
        let notifier = Arc::new(Notifier::new(recorder.clone(), 10));
        (inner.clone(), NotifyingStore::new(inner, notifier))
    }

    #[tokio::test]
    async fn an_append_notifies_the_chain_it_wrote_to() {
        let recorder = Arc::new(Recorder::default());
        let (_, store) = notifying_store(&recorder);
        let landed = store
            .append(append_request(&chain("run-1"), "t", 1))
            .await
            .unwrap();
        store.flush_notifications().await;
        let sent = recorder.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].chain, chain("run-1"));
        assert_eq!(sent[0].topic, "t");
        assert_eq!(
            sent[0].counter,
            progress_counter(&landed.last_position).unwrap()
        );
        assert_eq!(
            sent[0].position,
            mint_cursor(
                "memory",
                &stream_hash("ns", WORKFLOW_OWNER_KIND, "wf", "t"),
                &landed.last_position
            ),
            "the position is the cursor a reader resumes from"
        );
    }

    /// A store whose appends land at a position no counter comes from.
    struct OddPositions(MemoryStore);

    #[async_trait::async_trait]
    impl StreamStore for OddPositions {
        fn name(&self) -> &str {
            "odd"
        }
        async fn append(&self, r: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
            self.0.append(r).await?;
            Ok(StoreAppendResponse {
                first_position: "odd".to_string(),
                last_position: "odd".to_string(),
            })
        }
        async fn read(&self, r: StoreReadRequest) -> StreamResult<StoreReadResponse> {
            self.0.read(r).await
        }
        async fn latest(&self, r: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
            self.0.latest(r).await
        }
        async fn record_at(&self, c: &ChainId, t: &str, p: &str) -> StreamResult<Option<Vec<u8>>> {
            self.0.record_at(c, t, p).await
        }
        async fn close_chain(&self, c: &ChainId) -> StreamResult<()> {
            self.0.close_chain(c).await
        }
        async fn close_topic(&self, c: &ChainId, t: &str, r: Option<Payload>) -> StreamResult<()> {
            self.0.close_topic(c, t, r).await
        }
        async fn delete_owner(&self, r: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
            self.0.delete_owner(r).await
        }
    }

    #[tokio::test]
    async fn a_position_without_a_counter_does_not_fail_the_append() {
        let recorder = Arc::new(Recorder::default());
        let notifier = Arc::new(Notifier::new(recorder.clone(), 10));
        let store = NotifyingStore::new(Arc::new(OddPositions(MemoryStore::new())), notifier);
        store
            .append(append_request(&chain("run-1"), "t", 1))
            .await
            .unwrap();
        store.flush_notifications().await;
        assert!(recorder.sent().is_empty());
    }

    #[tokio::test]
    async fn a_closed_streams_notifier_is_dropped() {
        let recorder = Arc::new(Recorder::default());
        let (_, store) = notifying_store(&recorder);
        let run = chain("run-1");
        store.append(append_request(&run, "a", 1)).await.unwrap();
        store.append(append_request(&run, "b", 1)).await.unwrap();
        store.close_topic(&run, "a", None).await.unwrap();
        assert_eq!(store.notifier().slots.lock().unwrap().len(), 1);
        store.close_chain(&run).await.unwrap();
        assert_eq!(store.notifier().slots.lock().unwrap().len(), 0);
    }
}
