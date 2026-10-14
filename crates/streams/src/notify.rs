//! Telling a stream's notifier on the server that the stream moved or closed.
//!
//! The notifier holds the callbacks of the readers that wait on the stream. A notification is a
//! hint that there is something new to read. The records stay on the read path.

use crate::{
    CloseOutcome, NewestOrArm, StreamResult, StreamStore, WORKFLOW_OWNER_KIND, mint_cursor,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
    },
    stream_hash,
};
use std::{
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

/// How long a stopping process waits for the notifications still out.
pub const FLUSH_LIMIT: Duration = Duration::from_secs(10);

/// How long a stream's slot may send nothing before an append sends its last counter again, so
/// the notifier of a busy stream that never arms sees activity before its idle timeout.
pub const NOTIFIER_REFRESH: Duration = Duration::from_secs(24 * 60 * 60);

/// The wait before a failed notifier close is tried again. It doubles up to [CLOSE_RETRY_CAP].
pub const CLOSE_RETRY_FIRST: Duration = Duration::from_secs(1);
/// The longest wait between two tries of a notifier close.
pub const CLOSE_RETRY_CAP: Duration = Duration::from_secs(60);

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
    /// Set on the close, which completes every attached callback with it.
    pub close: Option<CloseOutcome>,
}

impl StreamNotification {
    /// The result a close carries, or `None` for a close with a failure and for progress.
    pub fn close_result(&self) -> Option<Payload> {
        self.close.as_ref().and_then(CloseOutcome::result)
    }
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

/// A Workflow's reader, to attach to a stream's notifier as a wake target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowAttachment {
    /// The run chain whose stream it reads. Its first run keys the notifier.
    pub chain: ChainId,
    /// The stream's topic.
    pub topic: String,
    /// The attach is idempotent by it, and a detach names it.
    pub request_id: String,
    /// The reader's Workflow, in the chain's namespace.
    pub workflow_id: String,
    /// The reader's run.
    pub run_id: String,
    /// Names the reader in its run.
    pub subscription_id: String,
}

/// What a stream's notifier answered a client's wait with.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NotifierPoll {
    /// The highest counter the notifier has seen.
    pub counter: i64,
    /// The position that came with `counter`.
    pub position: String,
    /// Whether the stream closed.
    pub closed: bool,
    /// The result the stream closed with, when it gave one.
    pub close_result: Option<Payload>,
    /// The failure the stream closed with, when it closed with one.
    pub close_failure: Option<temporalio_common::protos::temporal::api::failure::v1::Failure>,
}

/// One Nexus caller's callback, to attach to a stream's notifier.
#[derive(Debug, Clone, PartialEq)]
pub struct CallerAttachment {
    /// The run chain whose stream it is. Its first run keys the notifier.
    pub chain: ChainId,
    /// The stream's topic.
    pub topic: String,
    /// The start's request id. The attach is idempotent by it, and a detach names it.
    pub request_id: String,
    /// Where the server delivers progress and the completion.
    pub callback_url: String,
    /// Headers the server sends with each delivery.
    pub callback_headers: std::collections::HashMap<String, String>,
    /// The operation's token, which the server hands back with each delivery.
    pub operation_token: String,
    /// When the operation started, which the server hands back too.
    pub start_time: std::time::SystemTime,
}

/// Sends notifications to the stream notifier on the server, attaches and detaches the
/// Workflows it wakes and the Nexus callers it completes, and waits on it for clients.
#[async_trait::async_trait]
pub trait NotifierClient: Send + Sync {
    /// Sends one notification.
    async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError>;

    /// Attaches a Workflow's reader to a stream's notifier. Idempotent by its request id.
    async fn attach_workflow(&self, attachment: WorkflowAttachment) -> Result<(), NotifyError> {
        let _ = attachment;
        Err(NotifyError::Unsupported(
            "this client can't attach readers".to_string(),
        ))
    }

    /// Attaches a Nexus caller's callback to a stream's notifier. Idempotent by its request id.
    async fn attach_caller(&self, attachment: CallerAttachment) -> Result<(), NotifyError> {
        let _ = attachment;
        Err(NotifyError::Unsupported(
            "this client can't attach callers".to_string(),
        ))
    }

    /// Detaches what the attach with `request_id` attached to `chain`'s stream on `topic`.
    async fn detach(
        &self,
        chain: ChainId,
        topic: String,
        request_id: String,
    ) -> Result<(), NotifyError> {
        let _ = (chain, topic, request_id);
        Err(NotifyError::Unsupported(
            "this client can't detach readers".to_string(),
        ))
    }

    /// Waits until the notifier of `chain`'s stream on `topic` has a counter above
    /// `after_counter`, or the stream closed. Answers `None` when the wait reached its limit with
    /// no answer from the server.
    async fn poll(
        &self,
        chain: ChainId,
        topic: String,
        after_counter: i64,
    ) -> Result<Option<NotifierPoll>, NotifyError> {
        let _ = (chain, topic, after_counter);
        Err(NotifyError::Unsupported(
            "this client can't wait on a notifier".to_string(),
        ))
    }
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

/// An arm a notification answers. The slot clears it in the store once the notification lands,
/// so a send that fails for good leaves the arm for the next append to report.
#[derive(Clone)]
pub struct Arm {
    /// The generation the store reported.
    pub generation: u64,
    /// The store that holds the arm.
    pub store: Arc<dyn StreamStore>,
}

impl std::fmt::Debug for Arm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arm")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// The newer of two arms, which a disarm of the older would leave set.
fn newer(a: Option<Arm>, b: Option<Arm>) -> Option<Arm> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if b.generation > a.generation { b } else { a }),
        (a, b) => a.or(b),
    }
}

#[derive(Debug)]
struct Pending {
    position: String,
    counter: i64,
    arm: Option<Arm>,
}

#[derive(Debug, Default)]
struct SlotState {
    pending: Option<Pending>,
    sending: bool,
    closed: bool,
    /// The last notification that landed, which a refresh sends again.
    last_sent: Option<(String, i64)>,
    /// When the slot last sent or scheduled a send, or was made.
    active_at: Option<tokio::time::Instant>,
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
            state: Mutex::new(SlotState {
                active_at: Some(tokio::time::Instant::now()),
                ..SlotState::default()
            }),
            idle: watch::Sender::new(true),
        })
    }

    /// Whether the slot sent nothing for [NOTIFIER_REFRESH], answering the last notification
    /// that landed, if any. Counts as activity, so the writes that follow don't refresh again.
    fn refresh_due(&self) -> Option<Option<(String, i64)>> {
        let mut state = self.state.lock().unwrap();
        let now = tokio::time::Instant::now();
        if state.closed
            || state.pending.is_some()
            || state
                .active_at
                .is_some_and(|at| now.duration_since(at) < NOTIFIER_REFRESH)
        {
            return None;
        }
        state.active_at = Some(now);
        Some(state.last_sent.clone())
    }

    /// Records the newest position and returns at once. While a call is out, later
    /// notifications fold into the one that goes next, so a burst costs at most two calls.
    fn notify(
        self: &Arc<Self>,
        client: &Arc<dyn NotifierClient>,
        position: String,
        counter: i64,
        arm: Option<Arm>,
    ) {
        let mut state = self.state.lock().unwrap();
        self.hold(&mut state, client, position, counter, arm);
    }

    fn hold(
        self: &Arc<Self>,
        state: &mut SlotState,
        client: &Arc<dyn NotifierClient>,
        position: String,
        counter: i64,
        arm: Option<Arm>,
    ) {
        if state.closed {
            return;
        }
        state.active_at = Some(tokio::time::Instant::now());
        match &mut state.pending {
            Some(held) => {
                if counter >= held.counter {
                    held.position = position;
                    held.counter = counter;
                }
                held.arm = newer(held.arm.take(), arm);
            }
            None => {
                state.pending = Some(Pending {
                    position,
                    counter,
                    arm,
                })
            }
        }
        if !state.sending {
            state.sending = true;
            self.idle.send_replace(false);
            tokio::spawn(self.clone().send_pending(client.clone()));
        }
    }

    fn mark_idle_if_done(&self, state: &SlotState) {
        if !state.sending {
            self.idle.send_replace(true);
        }
    }

    async fn send_pending(self: Arc<Self>, client: Arc<dyn NotifierClient>) {
        loop {
            let Pending {
                position,
                counter,
                arm,
            } = {
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
                position: position.clone(),
                counter,
                close: None,
            };
            if self.send(&client, notification).await {
                self.state.lock().unwrap().last_sent = Some((position, counter));
                if let Some(arm) = arm {
                    self.disarm(arm).await;
                }
            } else if let Some(held) = self.state.lock().unwrap().pending.as_mut() {
                // The newer notification answers this one's arm too.
                held.arm = newer(held.arm.take(), arm);
            }
        }
    }

    /// Clears the arm a landed notification answered, unless a reader armed again since.
    async fn disarm(&self, arm: Arm) {
        if let Err(error) = arm
            .store
            .disarm(&self.key.chain(), &self.key.topic, arm.generation)
            .await
        {
            tracing::warn!(
                workflow_id = %self.key.workflow_id,
                topic = %self.key.topic,
                %error,
                "Could not clear a stream's arm, so the next write notifies again"
            );
        }
    }

    /// Sends one notification, trying a failed one again. Answers whether it landed.
    async fn send(
        &self,
        client: &Arc<dyn NotifierClient>,
        notification: StreamNotification,
    ) -> bool {
        let mut delay = NOTIFY_RETRY_FIRST;
        for tries_left in (0..=NOTIFY_RETRIES).rev() {
            // A notification is only a hint, and the reader reads from its own cursor, so a
            // notification that can't go never fails the write.
            match client.notify(notification.clone()).await {
                Ok(()) => return true,
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
                    return false;
                }
            }
            tokio::time::sleep(delay).await;
            delay *= 2;
            if self.superseded() {
                return false;
            }
        }
        false
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
    /// How many closes are still being tried in the background.
    closing: watch::Sender<usize>,
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
            closing: watch::Sender::new(0),
        }
    }

    /// Runs `attempt`, a close of `chain`'s stream on `topic`, in the background, trying again
    /// until it lands or the server refuses it. The wait between tries starts at
    /// [CLOSE_RETRY_FIRST] and doubles up to [CLOSE_RETRY_CAP]. [Notifier::flush] waits for it.
    ///
    /// The close completes every attached callback, and it lives only in this process, so one
    /// error must not drop it. Closing is idempotent on the server.
    pub fn close_retrying<F, Fut>(self: &Arc<Self>, chain: ChainId, topic: String, attempt: F)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), NotifyError>> + Send,
    {
        self.closing.send_modify(|closing| *closing += 1);
        let notifier = self.clone();
        tokio::spawn(async move {
            let mut delay = CLOSE_RETRY_FIRST;
            loop {
                match attempt().await {
                    Ok(()) => break,
                    Err(NotifyError::Refused(error) | NotifyError::Unsupported(error)) => {
                        tracing::warn!(
                            workflow_id = %chain.workflow_id,
                            topic,
                            error,
                            "The server refused the close of a stream's notifier"
                        );
                        break;
                    }
                    Err(NotifyError::Failed(error)) => tracing::warn!(
                        workflow_id = %chain.workflow_id,
                        topic,
                        error,
                        "Could not close a stream's notifier; trying again in {delay:?}"
                    ),
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(CLOSE_RETRY_CAP);
            }
            notifier.closing.send_modify(|closing| *closing -= 1);
        });
    }

    /// Tells the notifier of `chain`'s stream on `topic` that it moved to `position`. Never
    /// waits, and never fails: a notification that can't go is logged and dropped.
    pub fn notify(&self, chain: &ChainId, topic: &str, position: String, counter: i64) {
        self.notify_armed(chain, topic, position, counter, None);
    }

    /// Like [Notifier::notify], for a write that found `arm` set. Once the notification lands,
    /// the arm is cleared, unless a reader armed again since.
    pub fn notify_armed(
        &self,
        chain: &ChainId,
        topic: &str,
        position: String,
        counter: i64,
        arm: Option<Arm>,
    ) {
        self.slot(StreamKey::new(chain, topic))
            .notify(&self.client, position, counter, arm);
    }

    /// For a write that owes no notification: once the stream's slot sent nothing for
    /// [NOTIFIER_REFRESH], answers what keeps the notifier active. That is the last counter that
    /// landed, which the notifier takes as activity and delivers to no one, or `None` when the
    /// slot never sent, for the caller to send the newest record.
    pub fn refresh_due(&self, chain: &ChainId, topic: &str) -> Option<Option<(String, i64)>> {
        self.slot(StreamKey::new(chain, topic)).refresh_due()
    }

    /// Closes the stream on the server with `outcome`, after the notification in flight, and
    /// drops its slot. `counter` must rank above the stream's last notification.
    pub async fn close(
        &self,
        chain: &ChainId,
        topic: &str,
        position: String,
        counter: i64,
        outcome: CloseOutcome,
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
                close: Some(outcome),
            })
            .await
    }

    /// Waits until no notification is out or waiting, including a close still being retried, so
    /// bound the wait when the server may be down.
    pub async fn flush(&self) {
        let mut closing = self.closing.subscribe();
        // The sender lives as long as the notifier, so the wait can't fail.
        let _ = closing.wait_for(|closing| *closing == 0).await;
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

/// The cursor and counter a close carries: the newest record's cursor, and one above its
/// counter, so the close outranks every notification of the stream.
async fn close_position(
    store: &dyn StreamStore,
    chain: &ChainId,
    topic: &str,
) -> StreamResult<(String, i64)> {
    let position = store
        .latest(StoreLatestRequest {
            chain: Some(chain.clone()),
            topic: topic.to_string(),
        })
        .await?
        .position;
    let counter = progress_counter(&position).unwrap_or(0).saturating_add(1);
    let cursor = if position.is_empty() {
        String::new()
    } else {
        cursor_of(store, chain, topic, &position)
    };
    Ok((cursor, counter))
}

/// Closes the notifier of `chain`'s stream on `topic` in the background, trying again until it
/// lands or the server refuses it. Each try reads the newest position, so a failed read is tried
/// again too.
fn close_notifier_in_background(
    inner: &Arc<dyn StreamStore>,
    notifier: &Arc<Notifier>,
    chain: &ChainId,
    topic: &str,
    outcome: CloseOutcome,
) {
    let (inner, closing) = (inner.clone(), notifier.clone());
    let (chain, topic) = (chain.clone(), topic.to_string());
    notifier.close_retrying(chain.clone(), topic.clone(), move || {
        let (inner, notifier) = (inner.clone(), closing.clone());
        let (chain, topic, outcome) = (chain.clone(), topic.clone(), outcome.clone());
        async move {
            let (cursor, counter) = close_position(inner.as_ref(), &chain, &topic)
                .await
                .map_err(|error| NotifyError::Failed(error.to_string()))?;
            notifier
                .close(&chain, &topic, cursor, counter, outcome)
                .await
        }
    });
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

/// A store that closes each stream's notifier after the store closes the stream, and with
/// progress on, notifies it after an append lands, when the store reports that a reader armed
/// the stream or can't tell. A landed notification clears the arm it answered.
///
/// Wraps the store a process hands to its stream calls, so every write path notifies without
/// knowing about notifiers. It forwards every trait method by name, since a
/// method left to its default would answer for the inner store.
pub struct NotifyingStore {
    inner: Arc<dyn StreamStore>,
    notifier: Arc<Notifier>,
    progress: bool,
    every_append: bool,
}

impl NotifyingStore {
    /// Wraps `inner`, notifying through `notifier`, with progress on.
    pub fn new(inner: Arc<dyn StreamStore>, notifier: Arc<Notifier>) -> Self {
        Self {
            inner,
            notifier,
            progress: true,
            every_append: false,
        }
    }

    /// Turns progress notifications on or off. Closes reach the notifier either way, since the
    /// close outcome completes every attached callback.
    pub fn with_progress(mut self, progress: bool) -> Self {
        self.progress = progress;
        self
    }

    /// With progress on, notifies after every write whether or not a reader armed the stream, for
    /// readers that can't arm.
    pub fn with_every_append(mut self, every_append: bool) -> Self {
        self.every_append = every_append;
        self
    }

    /// The notifier this store notifies through.
    pub fn notifier(&self) -> &Arc<Notifier> {
        &self.notifier
    }

    /// Closes the stream's notifier in the background, after the store closed the topic. It never
    /// fails the store close: the topic is closed, and only the callers wait for the notifier.
    fn close_notifier_in_background(&self, chain: &ChainId, topic: &str, outcome: CloseOutcome) {
        close_notifier_in_background(&self.inner, &self.notifier, chain, topic, outcome);
    }

    /// What a write that found `arm` owes: `None` when no reader armed the topic, so nothing is
    /// sent, and otherwise the arm to clear once the notification lands. A store that can't tell
    /// reports no arm, and its writes notify without one.
    fn owed(&self, arm: Option<u64>) -> Option<Option<Arm>> {
        if self.every_append {
            return Some(None);
        }
        match arm {
            Some(0) => None,
            Some(generation) => Some(Some(Arm {
                generation,
                store: self.inner.clone(),
            })),
            None => Some(None),
        }
    }

    fn notify_at(&self, chain: &ChainId, topic: &str, position: &str, arm: Option<Arm>) {
        if let Some((cursor, counter)) =
            notification_at(self.inner.as_ref(), chain, topic, position)
        {
            self.notifier
                .notify_armed(chain, topic, cursor, counter, arm);
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
pub(crate) fn notification_at(
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
        if self.progress
            && let Some(chain) = &chain
        {
            match self.owed(landed.arm) {
                Some(arm) => self.notify_at(chain, &topic, &landed.last_position, arm),
                None => match self.notifier.refresh_due(chain, &topic) {
                    Some(Some((cursor, counter))) => {
                        self.notifier.notify(chain, &topic, cursor, counter)
                    }
                    Some(None) => self.notify_at(chain, &topic, &landed.last_position, None),
                    None => {}
                },
            }
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
        // A reader that finds the owner ended closes only the store. The notifier's own owner
        // check closes it on the server, with the outcome the owner ended with.
        self.inner.close_chain(chain).await
    }

    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()> {
        self.close_topic_with(chain, topic, StoredClose::default())
            .await
    }

    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<()> {
        let outcome = CloseOutcome::of(&close);
        self.inner.close_topic_with(chain, topic, close).await?;
        self.close_notifier_in_background(chain, topic, outcome);
        Ok(())
    }

    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()> {
        self.inner
            .retire_producer(chain, topic, producer_id, attempt)
            .await
    }

    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
        self.inner.delete_owner(request).await
    }

    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        self.inner.hold_floor(chain, topic, holder, position).await
    }

    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        self.inner.release_floor(chain, topic, holder).await
    }

    async fn close_stream(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<()> {
        let outcome = CloseOutcome::of(&close);
        self.inner.close_topic_with(chain, topic, close).await?;
        let closed = match close_position(self.inner.as_ref(), chain, topic).await {
            Ok((cursor, counter)) => {
                self.notifier
                    .close(chain, topic, cursor, counter, outcome.clone())
                    .await
            }
            Err(error) => Err(NotifyError::Failed(error.to_string())),
        };
        closed.map_err(|error| match error {
            NotifyError::Refused(message) => crate::StreamError::refused(message),
            NotifyError::Unsupported(message) => crate::StreamError::unsupported(message),
            NotifyError::Failed(message) => {
                // The caller may not try again, and the topic is closed in the store, so no
                // chain end lists it later.
                self.close_notifier_in_background(chain, topic, outcome);
                crate::StreamError::storage(message)
            }
        })
    }

    async fn flush_notifications(&self) {
        self.notifier.flush().await;
    }

    async fn disarm(&self, chain: &ChainId, topic: &str, generation: u64) -> StreamResult<()> {
        self.inner.disarm(chain, topic, generation).await
    }

    async fn newest_or_arm(&self, chain: &ChainId, topic: &str) -> StreamResult<NewestOrArm> {
        self.inner.newest_or_arm(chain, topic).await
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
                CloseOutcome::Result(Payload::default()),
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
                    .close(
                        &chain("run-1"),
                        "t",
                        "c1".to_string(),
                        2,
                        CloseOutcome::Result(result),
                    )
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
        assert_eq!(sent[1].close_result().as_ref().unwrap().data, b"summary");
    }

    #[tokio::test]
    async fn a_notification_after_close_sends_nothing() {
        let recorder = Arc::new(Recorder::default());
        let notifier = Notifier::new(recorder.clone(), 10);
        notifier.notify(&chain("run-1"), "t", "c1".to_string(), 1);
        let slot = notifier.slot(StreamKey::new(&chain("run-1"), "t"));
        slot.close().await;
        slot.notify(&notifier.client, "c2".to_string(), 2, None);
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
        let (inner, store) = notifying_store(&recorder);
        arm(inner.as_ref(), &chain("run-1"), "t").await;
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
                arm: None,
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
        async fn close_topic(&self, c: &ChainId, t: &str) -> StreamResult<()> {
            self.0.close_topic(c, t).await
        }
        async fn retire_producer(&self, c: &ChainId, t: &str, p: &str, a: i64) -> StreamResult<()> {
            self.0.retire_producer(c, t, p, a).await
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
        let (inner, store) = notifying_store(&recorder);
        let run = chain("run-1");
        arm(inner.as_ref(), &run, "a").await;
        arm(inner.as_ref(), &run, "b").await;
        store.append(append_request(&run, "a", 1)).await.unwrap();
        store.append(append_request(&run, "b", 1)).await.unwrap();
        store.close_topic(&run, "a").await.unwrap();
        store.flush_notifications().await;
        assert_eq!(store.notifier().slots.lock().unwrap().len(), 1);
        store.close_topic(&run, "b").await.unwrap();
        store.flush_notifications().await;
        assert_eq!(store.notifier().slots.lock().unwrap().len(), 0);
        let closes: Vec<(String, i64)> = recorder
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|n| n.close_result().is_some())
            .map(|n| (n.topic.clone(), n.counter))
            .collect();
        assert_eq!(
            closes,
            [("a".to_string(), 2), ("b".to_string(), 2)],
            "a closed topic ends its stream on the server too"
        );
    }

    /// Tells, on each notification, whether the store already refuses appends to the topic.
    struct SeesTheStore {
        store: Arc<MemoryStore>,
        refused_then: Mutex<Vec<bool>>,
        answer: Option<NotifyError>,
        sent: Mutex<Vec<StreamNotification>>,
    }

    #[async_trait::async_trait]
    impl NotifierClient for SeesTheStore {
        async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError> {
            // Probed only on the close, since an append to an open topic would add a record.
            let refused = notification.close.is_some()
                && self
                    .store
                    .append(append_request(&notification.chain, &notification.topic, 99))
                    .await
                    .is_err();
            self.refused_then.lock().unwrap().push(refused);
            self.sent.lock().unwrap().push(notification);
            self.answer.clone().map_or(Ok(()), Err)
        }
    }

    fn sees_the_store(answer: Option<NotifyError>) -> (Arc<SeesTheStore>, NotifyingStore) {
        let inner = Arc::new(MemoryStore::new());
        let client = Arc::new(SeesTheStore {
            store: inner.clone(),
            refused_then: Mutex::default(),
            answer,
            sent: Mutex::default(),
        });
        let notifier = Arc::new(Notifier::new(client.clone(), 10));
        (client, NotifyingStore::new(inner, notifier))
    }

    fn summary() -> Payload {
        Payload {
            data: b"summary".to_vec(),
            ..Default::default()
        }
    }

    fn gave_up() -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        temporalio_common::protos::temporal::api::failure::v1::Failure {
            message: "the agent gave up".to_string(),
            ..Default::default()
        }
    }

    fn is_null(outcome: &Option<CloseOutcome>) -> bool {
        matches!(
            outcome,
            Some(CloseOutcome::Result(result))
                if result.metadata.get("encoding").map(Vec::as_slice) == Some(b"binary/null")
                    && result.data.is_empty()
        )
    }

    #[tokio::test]
    async fn a_topic_closed_without_a_result_closes_its_notifier_with_a_null_payload() {
        let (client, store) = sees_the_store(None);
        let run = chain("run-1");
        store.append(append_request(&run, "t", 1)).await.unwrap();
        store.close_topic(&run, "t").await.unwrap();
        store.flush_notifications().await;
        let close = client.sent.lock().unwrap().last().cloned().unwrap();
        assert!(is_null(&close.close), "{close:?}");
    }

    #[tokio::test]
    async fn a_topic_closed_with_a_failure_closes_its_notifier_with_it() {
        let (client, store) = sees_the_store(None);
        let run = chain("run-1");
        store.append(append_request(&run, "t", 1)).await.unwrap();
        store
            .close_topic_with(&run, "t", CloseOutcome::Failure(gave_up()).into())
            .await
            .unwrap();
        store.flush_notifications().await;
        let sent = client.sent.lock().unwrap().clone();
        let close = sent.last().unwrap();
        assert_eq!(close.close, Some(CloseOutcome::Failure(gave_up())));
        assert_eq!(close.close_result(), None);
        assert_eq!(
            client.refused_then.lock().unwrap().last(),
            Some(&true),
            "the store closed before the notifier heard the failure"
        );
    }

    #[tokio::test]
    async fn a_close_closes_the_store_then_the_notifier_above_every_notification() {
        let (client, store) = sees_the_store(None);
        let run = chain("run-1");
        store.append(append_request(&run, "t", 1)).await.unwrap();
        store.append(append_request(&run, "t", 2)).await.unwrap();
        store.flush_notifications().await;
        store
            .close_stream(&run, "t", CloseOutcome::Result(summary()).into())
            .await
            .unwrap();
        let sent = client.sent.lock().unwrap().clone();
        let close = sent.last().unwrap();
        assert_eq!(close.close_result(), Some(summary()));
        assert_eq!(close.counter, 3, "one above the newest record's counter");
        assert!(
            sent[..sent.len() - 1]
                .iter()
                .all(|n| n.counter < close.counter)
        );
        assert_eq!(
            client.refused_then.lock().unwrap().last(),
            Some(&true),
            "the store refused appends before the notifier heard the close"
        );
        assert!(store.notifier().slots.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_close_of_an_empty_stream_carries_the_first_counter() {
        let (client, store) = sees_the_store(None);
        store
            .close_stream(&chain("run-1"), "t", CloseOutcome::Result(summary()).into())
            .await
            .unwrap();
        let sent = client.sent.lock().unwrap().clone();
        assert_eq!((sent[0].position.as_str(), sent[0].counter), ("", 1));
    }

    #[tokio::test]
    async fn a_close_says_whether_trying_again_can_help() {
        let (_, store) = sees_the_store(Some(NotifyError::Refused("no notifier".to_string())));
        let error = store
            .close_stream(&chain("run-1"), "t", CloseOutcome::Result(summary()).into())
            .await
            .unwrap_err();
        assert_eq!(error.kind, crate::proto::StreamFailureKind::Refused);
        let (_, store) = sees_the_store(Some(NotifyError::Failed("down".to_string())));
        let error = store
            .close_stream(&chain("run-1"), "t", CloseOutcome::Result(summary()).into())
            .await
            .unwrap_err();
        assert_ne!(error.kind, crate::proto::StreamFailureKind::Refused);
    }

    #[tokio::test]
    async fn a_store_without_notifications_only_closes_the_topic() {
        let store = MemoryStore::new();
        let run = chain("run-1");
        store
            .close_stream(&run, "t", CloseOutcome::Result(summary()).into())
            .await
            .unwrap();
        let error = store
            .append(append_request(&run, "t", 1))
            .await
            .unwrap_err();
        assert_eq!(error.kind, crate::proto::StreamFailureKind::Closed);
    }

    /// Fails the first `failures` closes, then takes them, and fails every progress call.
    struct FlakyCloses {
        failures: Mutex<usize>,
        refuse: bool,
        at: Mutex<Vec<tokio::time::Instant>>,
    }

    #[async_trait::async_trait]
    impl NotifierClient for FlakyCloses {
        async fn notify(&self, notification: StreamNotification) -> Result<(), NotifyError> {
            assert!(notification.close_result().is_some());
            self.at.lock().unwrap().push(tokio::time::Instant::now());
            if self.refuse {
                return Err(NotifyError::Refused("no notifier".to_string()));
            }
            let mut failures = self.failures.lock().unwrap();
            if *failures == 0 {
                return Ok(());
            }
            *failures -= 1;
            Err(NotifyError::Failed("down".to_string()))
        }
    }

    fn flaky(failures: usize, refuse: bool) -> Arc<FlakyCloses> {
        Arc::new(FlakyCloses {
            failures: Mutex::new(failures),
            refuse,
            at: Mutex::default(),
        })
    }

    /// A notifying store over memory whose notifier closes go to `client`, after one append
    /// on topic `t`.
    async fn closing_through(client: Arc<FlakyCloses>) -> NotifyingStore {
        let inner = Arc::new(MemoryStore::new());
        inner
            .append(append_request(&chain("run-1"), "t", 1))
            .await
            .unwrap();
        NotifyingStore::new(inner, Arc::new(Notifier::new(client, 10)))
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_notifier_close_is_retried_with_backoff_until_it_lands() {
        let client = flaky(3, false);
        let store = closing_through(client.clone()).await;
        store.close_topic(&chain("run-1"), "t").await.unwrap();
        store.flush_notifications().await;
        let at = client.at.lock().unwrap().clone();
        assert_eq!(at.len(), 4, "three failures, then it lands");
        let gaps: Vec<_> = at.windows(2).map(|pair| pair[1] - pair[0]).collect();
        assert_eq!(
            gaps,
            [
                CLOSE_RETRY_FIRST,
                CLOSE_RETRY_FIRST * 2,
                CLOSE_RETRY_FIRST * 4
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_notifier_close_retry_waits_at_most_the_cap() {
        let client = flaky(12, false);
        let store = closing_through(client.clone()).await;
        store.close_topic(&chain("run-1"), "t").await.unwrap();
        store.flush_notifications().await;
        let at = client.at.lock().unwrap().clone();
        let longest = at.windows(2).map(|pair| pair[1] - pair[0]).max().unwrap();
        assert_eq!(longest, CLOSE_RETRY_CAP);
    }

    #[tokio::test(start_paused = true)]
    async fn a_notifier_close_the_server_refuses_is_not_retried() {
        let client = flaky(0, true);
        let store = closing_through(client.clone()).await;
        store.close_topic(&chain("run-1"), "t").await.unwrap();
        store.flush_notifications().await;
        tokio::time::sleep(CLOSE_RETRY_CAP * 2).await;
        assert_eq!(client.at.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_chain_close_from_a_reader_closes_only_the_store() {
        let recorder = Arc::new(Recorder::default());
        let (inner, store) = notifying_store(&recorder);
        let run = chain("run-1");
        store.append(append_request(&run, "a", 1)).await.unwrap();
        store.flush_notifications().await;
        store.close_chain(&run).await.unwrap();
        store.flush_notifications().await;
        assert!(
            recorder
                .sent
                .lock()
                .unwrap()
                .iter()
                .all(|n| n.close_result().is_none()),
            "the Workflow's own close of the topic may still be on its way"
        );
        let error = inner
            .append(append_request(&run, "a", 2))
            .await
            .unwrap_err();
        assert_eq!(error.kind, crate::proto::StreamFailureKind::Closed);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_outside_close_raises_and_is_retried_in_the_background() {
        let client = flaky(1, false);
        let store = closing_through(client.clone()).await;
        let error = store
            .close_stream(&chain("run-1"), "t", CloseOutcome::Result(summary()).into())
            .await
            .unwrap_err();
        assert_ne!(error.kind, crate::proto::StreamFailureKind::Refused);
        store.flush_notifications().await;
        assert_eq!(client.at.lock().unwrap().len(), 2, "the retry lands");
    }

    /// Arms `topic` as a reader that found nothing after the newest record does.
    async fn arm(store: &dyn StreamStore, chain: &ChainId, topic: &str) {
        let after = store
            .latest(StoreLatestRequest {
                chain: Some(chain.clone()),
                topic: topic.to_string(),
            })
            .await
            .unwrap()
            .position;
        let read = store
            .read(StoreReadRequest {
                chain: Some(chain.clone()),
                topic: topic.to_string(),
                after_position: after,
                arm: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(read.armed);
    }

    /// The arm an append straight to `store` reports, which sends no notification.
    async fn arm_now(store: &MemoryStore, chain: &ChainId, topic: &str, sequence: i64) -> u64 {
        let mut request = append_request(chain, topic, sequence);
        request.producer_id = "probe".to_string();
        store.append(request).await.unwrap().arm.unwrap()
    }

    #[tokio::test]
    async fn an_append_with_nothing_armed_sends_nothing() {
        let recorder = Arc::new(Recorder::default());
        let (_, store) = notifying_store(&recorder);
        store
            .append(append_request(&chain("run-1"), "t", 1))
            .await
            .unwrap();
        store.flush_notifications().await;
        assert!(recorder.sent().is_empty(), "no reader sleeps on the stream");
    }

    #[tokio::test]
    async fn a_send_disarms_only_after_it_lands() {
        let recorder = Recorder::holding();
        let (inner, store) = notifying_store(&recorder);
        let run = chain("run-1");
        arm(inner.as_ref(), &run, "t").await;
        store.append(append_request(&run, "t", 1)).await.unwrap();
        settle().await;
        assert_eq!(recorder.sent().len(), 1, "the notification is in flight");
        let armed = arm_now(&inner, &run, "t", 1).await;
        assert!(armed > 0, "the arm stays while the notification is out");
        recorder.release(1);
        store.flush_notifications().await;
        assert_eq!(
            arm_now(&inner, &run, "t", 2).await,
            0,
            "cleared once it landed"
        );
    }

    #[tokio::test]
    async fn a_disarm_keeps_an_arm_a_reader_set_while_the_send_was_out() {
        let recorder = Recorder::holding();
        let (inner, store) = notifying_store(&recorder);
        let run = chain("run-1");
        arm(inner.as_ref(), &run, "t").await;
        store.append(append_request(&run, "t", 1)).await.unwrap();
        settle().await;
        // The reader woke from another notification, read everything and armed again.
        arm(inner.as_ref(), &run, "t").await;
        recorder.release(1);
        store.flush_notifications().await;
        assert!(arm_now(&inner, &run, "t", 1).await > 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_send_keeps_the_arm_for_the_next_append() {
        let recorder = Arc::new(Recorder::default());
        recorder.fail.store(true, Ordering::SeqCst);
        let (inner, store) = notifying_store(&recorder);
        let run = chain("run-1");
        arm(inner.as_ref(), &run, "t").await;
        store.append(append_request(&run, "t", 1)).await.unwrap();
        store.flush_notifications().await;
        assert_eq!(recorder.sent().len(), 3, "three tries, then it's dropped");
        recorder.fail.store(false, Ordering::SeqCst);
        store.append(append_request(&run, "t", 2)).await.unwrap();
        store.flush_notifications().await;
        assert_eq!(recorder.sent().len(), 4, "the next append notifies again");
        assert_eq!(arm_now(&inner, &run, "t", 1).await, 0);
    }

    #[tokio::test]
    async fn a_local_activitys_appends_notify_only_armed_topics() {
        let recorder = Arc::new(Recorder::default());
        let (inner, store) = notifying_store(&recorder);
        let run = chain("run-1");
        arm(inner.as_ref(), &run, "a").await;
        for topic in ["a", "b"] {
            store
                .append(StoreAppendRequest {
                    producer_id: "__temporal_local_1@run-1".to_string(),
                    ..append_request(&run, topic, 1)
                })
                .await
                .unwrap();
        }
        store.flush_notifications().await;
        let topics: Vec<String> = recorder
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|n| n.topic.clone())
            .collect();
        assert_eq!(topics, ["a"]);
        assert_eq!(arm_now(&inner, &run, "a", 1).await, 0);
    }

    /// A memory store that reports no arm, as a store that can't arm does.
    struct CantTell(MemoryStore);

    #[async_trait::async_trait]
    impl StreamStore for CantTell {
        fn name(&self) -> &str {
            self.0.name()
        }
        async fn append(&self, r: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
            let landed = self.0.append(r).await?;
            Ok(StoreAppendResponse {
                arm: None,
                ..landed
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
        async fn close_topic(&self, c: &ChainId, t: &str) -> StreamResult<()> {
            self.0.close_topic(c, t).await
        }
        async fn retire_producer(&self, c: &ChainId, t: &str, p: &str, a: i64) -> StreamResult<()> {
            self.0.retire_producer(c, t, p, a).await
        }
        async fn delete_owner(&self, r: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse> {
            self.0.delete_owner(r).await
        }
    }

    #[tokio::test]
    async fn a_store_that_cant_tell_still_notifies() {
        let recorder = Arc::new(Recorder::default());
        let notifier = Arc::new(Notifier::new(recorder.clone(), 10));
        let store = NotifyingStore::new(Arc::new(CantTell(MemoryStore::new())), notifier);
        let run = chain("run-1");
        store.append(append_request(&run, "t", 1)).await.unwrap();
        store.append(append_request(&run, "b", 1)).await.unwrap();
        store.flush_notifications().await;
        let mut topics: Vec<String> = recorder
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|n| n.topic.clone())
            .collect();
        topics.sort();
        assert_eq!(topics, ["b", "t"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_stream_that_never_arms_refreshes_its_notifier_once_a_day() {
        // Its readers always find records, so without a refresh the notifier would see no
        // activity and fail its callers at its idle timeout.
        let recorder = Arc::new(Recorder::default());
        let (_, store) = notifying_store(&recorder);
        let run = chain("run-1");
        let mut sequence = 0;
        let mut append_after = async |wait: Duration| {
            tokio::time::sleep(wait).await;
            sequence += 1;
            let landed = store
                .append(append_request(&run, "t", sequence))
                .await
                .unwrap();
            store.flush_notifications().await;
            landed
        };
        append_after(Duration::ZERO).await;
        append_after(NOTIFIER_REFRESH - Duration::from_secs(60)).await;
        assert!(recorder.sent().is_empty(), "a day has not passed");
        let landed = append_after(Duration::from_secs(60)).await;
        let first = progress_counter(&landed.last_position).unwrap();
        assert_eq!(
            recorder.sent().iter().map(|(_, c)| *c).collect::<Vec<_>>(),
            [first],
            "a slot that never sent sends the newest record's counter"
        );
        append_after(Duration::from_secs(60)).await;
        assert_eq!(recorder.sent().len(), 1, "once a day");
        append_after(NOTIFIER_REFRESH).await;
        assert_eq!(
            recorder.sent().iter().map(|(_, c)| *c).collect::<Vec<_>>(),
            [first, first],
            "the last counter it sent again, which the notifier takes as activity only"
        );
    }
}
