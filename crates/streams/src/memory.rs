//! The memory Store, for tests. It keeps the same fields as the Redis layout and runs the steps of
//! the Lua scripts in the same order, so that both Stores return the same results. Each call holds
//! one lock, since Redis runs one script at a time.

use crate::{
    Reason, StreamError, StreamResult,
    digest::DIGEST_LEN,
    epoch::{EPOCH_KEY_LEN, ProducerKind, epoch_key},
    ids::StreamKeys,
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{Producer, StreamRef},
    store::{
        Appended, Batch, Capabilities, Closed, ConflictPolicy, Create, CreateOutcome, Created,
        Entry, FORMAT_VERSION, MAX_SEQUENCE, Owner, Store, StreamState, TOMBSTONE, owner_chain,
    },
};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Notify, time::Instant};

/// The number of sequence numbers in one millisecond, since an offset packs them in 20 bits.
const SEQ_LIMIT: u64 = 1 << crate::offset::SEQ_BITS;
/// The most Producers that one append prunes, as `PRUNE_LIMIT` in `lib.lua`.
const PRUNE_LIMIT: usize = 64;

#[derive(Clone, Default)]
struct Fields {
    record: Option<Vec<u8>>,
    superseded: Option<Vec<u8>>,
}

/// One Stream's meta hash and log, each with an expiry time, as Redis keys have.
#[derive(Default)]
struct Keys {
    /// The meta hash, whose field names and values are bytes as in Redis.
    meta: HashMap<Vec<u8>, Vec<u8>>,
    meta_expires: i64,
    /// `None` when the log key doesn't exist.
    log: Option<BTreeMap<(u64, u64), Fields>>,
    log_expires: i64,
    /// The newest id of the log, which Redis keeps after a trim and drops with the expired key.
    last_id: (u64, u64),
    /// The `producers` sorted set, each `<kind>|<producer id>` with the ms of its last append.
    producers: HashMap<String, i64>,
    producers_expires: i64,
}

/// A Store in process memory, with the rules of the Redis scripts.
pub struct MemoryStore {
    prefix: String,
    retention: Duration,
    safety_ttl: Duration,
    streams: Mutex<HashMap<String, Keys>>,
    appended: Notify,
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("retention", &self.retention)
            .field("safety_ttl", &self.safety_ttl)
            .finish_non_exhaustive()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn refuse(reason: Reason, message: &str) -> StreamError {
    StreamError::new(reason, message)
}

fn id_text((ms, seq): (u64, u64)) -> String {
    format!("{ms}-{seq}")
}

fn parse_id(text: &str) -> Option<(u64, u64)> {
    let (ms, seq) = text.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// The length of a [ProducerField] value.
const PRODUCER_FIELD_LEN: usize = 1 + 2 * EPOCH_KEY_LEN + 8 + 2 * 12 + DIGEST_LEN;
/// The flag of a [ProducerField] that has a held epoch. The other flag bits are zero.
const HELD: u8 = 1;

/// An entry id as a [ProducerField] keeps it, the ms and the seq.
pub(crate) type EntryId = (u64, u32);

/// The `p|<kind>|<producer id>` field, one per Producer. Its value is 129 bytes: a flag byte, the
/// fence, the held epoch key or zeros, and the last batch's sequence (8), first and last entry ids
/// (12 each, the ms in 8 and the seq in 4, zeros for none) and digest. Numbers are big-endian.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProducerField {
    /// The newest epoch key of the Producer, which is also the epoch of its last batch.
    pub(crate) fence: [u8; EPOCH_KEY_LEN],
    /// The newest epoch key that still has Records.
    pub(crate) held: Option<[u8; EPOCH_KEY_LEN]>,
    /// The last batch's sequence.
    pub(crate) sequence: i64,
    /// The last batch's first and last entry ids, `None` when it wrote no entry.
    pub(crate) entries: Option<(EntryId, EntryId)>,
    /// The last batch's digest.
    pub(crate) digest: [u8; DIGEST_LEN],
}

impl ProducerField {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(PRODUCER_FIELD_LEN);
        value.push(if self.held.is_some() { HELD } else { 0 });
        value.extend_from_slice(&self.fence);
        value.extend_from_slice(&self.held.unwrap_or([0; EPOCH_KEY_LEN]));
        value.extend_from_slice(&self.sequence.to_be_bytes());
        let (first, last) = self.entries.unwrap_or_default();
        for (ms, seq) in [first, last] {
            value.extend_from_slice(&ms.to_be_bytes());
            value.extend_from_slice(&seq.to_be_bytes());
        }
        value.extend_from_slice(&self.digest);
        value
    }
}

/// Refuses a value of another length or with another flag bit, which a later format may write.
impl TryFrom<&[u8]> for ProducerField {
    type Error = StreamError;

    fn try_from(value: &[u8]) -> StreamResult<Self> {
        let unknown = || {
            refuse(
                Reason::StorageCorrupt,
                "the Producer field has an unknown format",
            )
        };
        let Some((&flags, rest)) = value.split_first() else {
            return Err(unknown());
        };
        if value.len() != PRODUCER_FIELD_LEN || flags & !HELD != 0 {
            return Err(unknown());
        }
        let (fence, rest) = rest.split_at(EPOCH_KEY_LEN);
        let (held, rest) = rest.split_at(EPOCH_KEY_LEN);
        let (sequence, rest) = rest.split_at(8);
        let (first, rest) = rest.split_at(12);
        let (last, digest) = rest.split_at(12);
        let entry_id = |bytes: &[u8]| -> StreamResult<EntryId> {
            let (ms, seq) = bytes.split_at(8);
            let ms = ms.try_into().map_err(|_| unknown())?;
            let seq = seq.try_into().map_err(|_| unknown())?;
            Ok((u64::from_be_bytes(ms), u32::from_be_bytes(seq)))
        };
        // A batch has both entry ids or neither.
        let entries = match (entry_id(first)?, entry_id(last)?) {
            ((0, 0), (0, 0)) => None,
            ((0, 0), _) | (_, (0, 0)) => return Err(unknown()),
            ids => Some(ids),
        };
        Ok(Self {
            fence: fence.try_into().map_err(|_| unknown())?,
            held: if flags & HELD != 0 {
                Some(held.try_into().map_err(|_| unknown())?)
            } else {
                None
            },
            sequence: i64::from_be_bytes(sequence.try_into().map_err(|_| unknown())?),
            entries,
            digest: digest.try_into().map_err(|_| unknown())?,
        })
    }
}

/// The [EntryId] of an entry id's text, `<ms>-<seq>`.
fn entry_id(text: &str) -> StreamResult<EntryId> {
    let bad = || refuse(Reason::StorageCorrupt, "an entry id isn't <ms>-<seq>");
    let (ms, seq) = text.split_once('-').ok_or_else(bad)?;
    Ok((
        ms.parse().map_err(|_| bad())?,
        seq.parse().map_err(|_| bad())?,
    ))
}

impl Keys {
    fn get(&self, field: impl AsRef<[u8]>) -> Option<&[u8]> {
        self.meta.get(field.as_ref()).map(Vec::as_slice)
    }

    /// A field that holds text, such as `own` or `newest`. `None` when it's absent or not text.
    fn text(&self, field: impl AsRef<[u8]>) -> Option<&str> {
        self.get(field)
            .and_then(|value| std::str::from_utf8(value).ok())
    }

    /// A field that holds a number, zero when it's absent.
    fn number(&self, field: &str) -> i64 {
        self.text(field)
            .and_then(|number| number.parse().ok())
            .unwrap_or(0)
    }

    fn set(&mut self, field: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.meta.insert(field.into(), value.into());
    }

    fn remove(&mut self, field: impl AsRef<[u8]>) {
        self.meta.remove(field.as_ref());
    }

    /// The `check_live` step, which checks the create key, the format version and the expiry.
    fn check_live(&self, now: i64) -> StreamResult<()> {
        if self.get("v").is_none() {
            return Err(refuse(Reason::NotFound, "nobody created the Stream"));
        }
        let v = self.number("v");
        if v > i64::from(FORMAT_VERSION) {
            return Err(refuse(
                Reason::FormatTooNew,
                &format!("format {v} is newer than {FORMAT_VERSION}"),
            ));
        }
        if self
            .text("exp")
            .and_then(|exp| exp.parse::<i64>().ok())
            .is_some_and(|exp| exp <= now)
        {
            return Err(refuse(Reason::Expired, "the Stream expired"));
        }
        Ok(())
    }

    /// The `check_owner` step of a close, as in `lib.lua`.
    fn check_owner(&self, kind: ProducerKind, chain: &str) -> StreamResult<()> {
        if self.text("own") == Some("wf") {
            if kind != ProducerKind::Workflow {
                return Err(refuse(
                    Reason::NotOwner,
                    "only the close Activity of the owner run chain closes a Workflow-owned Stream",
                ));
            }
            if self.text("oc").is_some_and(|owner| chain != owner) {
                return Err(refuse(
                    Reason::NotOwner,
                    "the ref has another run chain than the owner",
                ));
            }
        } else if !matches!(kind, ProducerKind::Workflow | ProducerKind::Client) {
            return Err(refuse(
                Reason::NotOwner,
                "only a Workflow or a client closes a Stream with no owner",
            ));
        }
        Ok(())
    }

    fn refresh(&mut self, now: i64, safety: i64, tombstone: i64) {
        let retention = self.number("ret");
        let mut ttl = retention;
        if self.text("own") == Some("wf") && self.get("closed").is_none() {
            ttl = safety;
        }
        let exp = now.saturating_add(ttl);
        self.set("exp", exp.to_string());
        self.meta_expires = exp.saturating_add(tombstone);
        if self.log.is_some() {
            // A read hides every Record one Retention after the last write, so the log goes then.
            self.log_expires = exp.min(now.saturating_add(retention));
        }
        if !self.producers.is_empty() {
            self.producers_expires = exp;
        }
    }

    /// Removes the `p|` field of the Producers whose last append is older than the Retention, as
    /// `prune` in `lib.lua`. Their Records are older too, so they hold none.
    fn prune(&mut self, now: i64) {
        let retention = self.number("ret");
        let min = now - retention;
        let mut old: Vec<_> = self
            .producers
            .iter()
            .filter(|(_, at)| **at < min)
            .map(|(producer, at)| (*at, producer.clone()))
            .collect();
        old.sort();
        for (_, producer) in old.into_iter().take(PRUNE_LIMIT) {
            self.remove(format!("p|{producer}"));
            self.producers.remove(&producer);
        }
    }

    /// The newest id the Stream gave. Redis drops it with an expired log, so the meta's `newest`
    /// keeps the next ids above the old ones.
    fn newest_id(&self) -> (u64, u64) {
        let newest = self.text("newest").and_then(parse_id);
        self.last_id.max(newest.unwrap_or_default())
    }

    /// The ms of the ids that the next `add` gives, as `next_ms` in `lib.lua`.
    fn next_ms(&self, now: i64) -> i64 {
        now.max(self.newest_id().0 as i64)
    }

    /// Refuses a create that names another owner than the Stream has, as `check_identity` in
    /// `lib.lua`.
    fn check_identity(&self, owner: &str, chain: &str) -> StreamResult<()> {
        let stored = self.text("oc").unwrap_or_default();
        if self.text("own") != Some(owner) || stored != chain {
            return Err(refuse(
                Reason::NotOwner,
                "the Stream has another owner than the create names",
            ));
        }
        Ok(())
    }

    fn trim(&mut self, now: i64) {
        if self.log.is_none() {
            // The expired log took every Record, so a read from an older offset must fail.
            if let Some(newest) = self.text("newest").and_then(parse_id) {
                self.raise_trimmed(newest);
            }
            return;
        }
        let retention = self.number("ret");
        let min = now - retention;
        if min <= 0 {
            return;
        }
        let Some(log) = self.log.as_ref() else {
            return;
        };
        let old: Vec<_> = log.range(..(min as u64, 0)).map(|(id, _)| *id).collect();
        let Some(newest_old) = old.last().copied() else {
            return;
        };
        self.raise_trimmed(newest_old);
        if let Some(log) = self.log.as_mut() {
            for id in old {
                log.remove(&id);
            }
        }
    }

    /// Sets the trimmed offset to `id`, unless it is already at or past it.
    fn raise_trimmed(&mut self, id: (u64, u64)) {
        let moves = self
            .text("trimmed")
            .and_then(parse_id)
            .is_none_or(|trimmed| trimmed < id);
        if moves {
            self.set("trimmed", id_text(id));
        }
    }

    /// Refuses `count` entries whose ids would not fit an offset. The ids go on from the newest id
    /// the Stream gave, as `check_ids_fit` in `lib.lua`.
    fn check_ids_fit(&self, now: i64, count: u64) -> StreamResult<()> {
        let (ms, seq) = self.newest_id();
        let first = if now > ms as i64 { 0 } else { seq + 1 };
        if first + count > SEQ_LIMIT {
            return Err(refuse(
                Reason::StorageUnavailable,
                "the entry ids of this millisecond would not fit an offset",
            ));
        }
        Ok(())
    }

    /// Adds an entry as `add` in `lib.lua` does, with the next id after the newest one, in this
    /// millisecond or a later one.
    fn add(&mut self, now: i64, fields: Fields) -> String {
        let now = now.max(0) as u64;
        let (last_ms, last_seq) = self.newest_id();
        let id = if now > last_ms {
            (now, 0)
        } else {
            (last_ms, last_seq + 1)
        };
        self.last_id = id;
        self.log
            .get_or_insert_with(BTreeMap::new)
            .insert(id, fields);
        id_text(id)
    }
}

impl MemoryStore {
    /// An empty Store.
    pub fn new(retention: Duration, safety_ttl: Duration) -> Self {
        Self {
            prefix: crate::ids::DEFAULT_KEY_PREFIX.to_string(),
            retention,
            safety_ttl,
            streams: Mutex::default(),
            appended: Notify::new(),
        }
    }

    fn key(&self, namespace: &str, stream_id: &str) -> String {
        StreamKeys::new(&self.prefix, namespace, stream_id).meta()
    }

    /// Runs `step` on one Stream's keys after it drops the expired keys, as Redis does.
    fn with<T>(
        &self,
        namespace: &str,
        stream_id: &str,
        step: impl FnOnce(&mut Keys, i64) -> T,
    ) -> T {
        let now = now_ms();
        let mut streams = self.streams.lock();
        let key = self.key(namespace, stream_id);
        let keys = streams.entry(key.clone()).or_default();
        if keys.meta_expires > 0 && keys.meta_expires <= now {
            keys.meta.clear();
            keys.meta_expires = 0;
        }
        if keys.log_expires > 0 && keys.log_expires <= now {
            keys.log = None;
            keys.log_expires = 0;
            keys.last_id = (0, 0);
        }
        if keys.producers_expires > 0 && keys.producers_expires <= now {
            keys.producers.clear();
            keys.producers_expires = 0;
        }
        let result = step(keys, now);
        // Once every key is gone, Redis has no state of the Stream, not even its newest id.
        if keys.meta.is_empty() && keys.log.is_none() && keys.producers.is_empty() {
            streams.remove(&key);
        }
        result
    }
}

#[cfg(any(test, feature = "test-support"))]
impl MemoryStore {
    /// Changes one meta field, for tests of what the scripts refuse.
    #[doc(hidden)]
    pub fn test_set_meta(&self, namespace: &str, stream_id: &str, field: &[u8], value: &[u8]) {
        self.with(namespace, stream_id, |keys, _| keys.set(field, value));
    }

    /// One meta field, for tests.
    #[doc(hidden)]
    pub fn test_meta(&self, namespace: &str, stream_id: &str, field: &[u8]) -> Option<Vec<u8>> {
        self.with(namespace, stream_id, |keys, _| {
            keys.get(field).map(<[u8]>::to_vec)
        })
    }

    /// The ms left on the log and on the meta, -2 for a key that doesn't exist, as `PTTL`.
    #[doc(hidden)]
    pub fn test_ttls(&self, namespace: &str, stream_id: &str) -> (i64, i64) {
        self.with(namespace, stream_id, |keys, now| {
            let log = if keys.log.is_some() {
                keys.log_expires - now
            } else {
                -2
            };
            let meta = if keys.meta.is_empty() {
                -2
            } else {
                keys.meta_expires - now
            };
            (log, meta)
        })
    }

    /// Adds an entry with an explicit id, as `XADD <id>`, for tests.
    #[doc(hidden)]
    pub fn test_add_entry(&self, namespace: &str, stream_id: &str, id: &str) {
        self.with(namespace, stream_id, |keys, _| {
            let id = parse_id(id).expect("an entry id");
            keys.last_id = keys.last_id.max(id);
            keys.log
                .get_or_insert_with(BTreeMap::new)
                .insert(id, Fields::default());
        });
    }

    /// The number of log entries, as `XLEN`, for tests.
    #[doc(hidden)]
    pub fn test_log_len(&self, namespace: &str, stream_id: &str) -> usize {
        self.with(namespace, stream_id, |keys, _| {
            keys.log.as_ref().map_or(0, BTreeMap::len)
        })
    }
}

fn offsets(first: &str, last: &str) -> StreamResult<Option<(i64, i64)>> {
    if first.is_empty() || last.is_empty() {
        return Ok(None);
    }
    Ok(Some((
        offset_of_entry_id(first)?,
        offset_of_entry_id(last)?,
    )))
}

#[async_trait::async_trait]
impl Store for MemoryStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL.without(Capabilities::RUN_FENCE)
    }

    async fn create(
        &self,
        namespace: &str,
        stream_ref: &StreamRef,
        create: &Create<'_>,
    ) -> StreamResult<Created> {
        let stream_id = stream_ref.id.as_str();
        let retention = create.retention.filter(|r| !r.is_zero());
        let retention = millis(retention.unwrap_or(self.retention));
        let request_id = create.request_id;
        let owner = match create.owner {
            Owner::Workflow => "wf",
            Owner::Client => "client",
        };
        let (safety, tombstone) = (millis(self.safety_ttl), millis(TOMBSTONE));
        let chain = if owner == "wf" {
            owner_chain(stream_ref)
        } else {
            String::new()
        };
        let created = self.with(namespace, stream_id, |keys, now| {
            if keys.get("ret").is_none() {
                keys.set("v", FORMAT_VERSION.to_string());
                keys.set("ret", retention.to_string());
                keys.set("own", owner);
                if !chain.is_empty() {
                    keys.set("oc", chain.clone());
                }
                if !request_id.is_empty() {
                    keys.set("rid", request_id);
                }
                keys.refresh(now, safety, tombstone);
                return Ok((CreateOutcome::Created, retention));
            }
            keys.check_live(now)?;
            keys.check_identity(owner, &chain)?;
            if keys.get("closed").is_some() {
                return Err(refuse(Reason::Closed, "the Stream is closed"));
            }
            // A retry of the create that made the Stream gets its answer again.
            if !request_id.is_empty() && keys.get("rid") == Some(request_id.as_bytes()) {
                return Ok((CreateOutcome::Created, keys.number("ret")));
            }
            if create.conflict == ConflictPolicy::Fail {
                return Err(refuse(Reason::AlreadyExists, "the Stream exists"));
            }
            Ok((CreateOutcome::Exists, keys.number("ret")))
        });
        let (outcome, stored) = created?;
        Ok(Created {
            outcome,
            retention: Duration::from_millis(stored.max(0) as u64),
            flagged: false,
        })
    }

    async fn append(
        &self,
        namespace: &str,
        stream_ref: &StreamRef,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let stream_id = stream_ref.id.as_str();
        let producer = batch.producer;
        let kind = ProducerKind::try_from(producer)?;
        let epoch = epoch_key(&producer.epoch.unwrap_or_default());
        let withdrawal = &batch.withdrawal;
        let (safety, tombstone) = (millis(self.safety_ttl), millis(TOMBSTONE));
        let field = format!("p|{}|{}", kind as i32, producer.id);
        let member = format!("{}|{}", kind as i32, producer.id);
        let appended = self.with(namespace, stream_id, |keys, now| {
            // The format version comes first, since a newer format can change the Producer field.
            keys.check_live(now)?;
            let stored = keys.get(&field).map(ProducerField::try_from).transpose()?;
            // Every append sets the fence to its epoch, so the fence is the last batch's epoch.
            let previous = stored.filter(|stored| stored.fence == epoch);
            // The lock also covers the dedupe check, so no other batch takes the same number.
            let sequence = match batch.sequence {
                0 => previous.map_or(1, |previous| previous.sequence + 1),
                sequence => sequence,
            };
            if !(1..=MAX_SEQUENCE).contains(&sequence) {
                return Err(refuse(
                    Reason::Invalid,
                    "the sequence must be from 1 to 2^53 - 1",
                ));
            }
            if let Some(previous) = previous {
                if sequence == previous.sequence {
                    if previous.digest != batch.digest {
                        return Err(refuse(
                            Reason::SeqDivergent,
                            &format!(
                                "sequence {sequence} has other content than the batch the Store \
                                 has"
                            ),
                        ));
                    }
                    let (first, last) = previous
                        .entries
                        .map(|((ms, seq), (last_ms, last_seq))| {
                            (format!("{ms}-{seq}"), format!("{last_ms}-{last_seq}"))
                        })
                        .unwrap_or_default();
                    return Ok((true, first, last, false));
                }
                if sequence < previous.sequence {
                    return Err(refuse(
                        Reason::SequenceTooLow,
                        &format!(
                            "sequence {sequence} is below the last one, {}",
                            previous.sequence
                        ),
                    ));
                }
            }
            if keys.get("closed").is_some() {
                return Err(refuse(Reason::Closed, "the Stream is closed"));
            }
            if stored.is_some_and(|stored| epoch < stored.fence) {
                return Err(refuse(
                    Reason::Fenced,
                    "the epoch is lower than the newest one of the Producer",
                ));
            }
            let records = batch.records.len() as u64;
            keys.check_ids_fit(now, records + 1)?;

            // A client's newer epoch only fences, as in `append.lua`.
            let flagged = kind != ProducerKind::Client
                && stored
                    .and_then(|stored| stored.held)
                    .is_some_and(|held| held < epoch);

            keys.trim(now);
            keys.prune(now);
            let at = keys.next_ms(now);
            let (mut first, mut last) = (String::new(), String::new());
            if records == 0 && flagged {
                last = keys.add(
                    now,
                    Fields {
                        record: None,
                        superseded: Some(withdrawal.superseded.to_vec()),
                    },
                );
                first = last.clone();
            }
            for (i, record) in batch.records.iter().enumerate() {
                let superseded = (i == 0 && flagged).then(|| withdrawal.superseded.to_vec());
                last = keys.add(
                    now,
                    Fields {
                        record: Some(record.clone()),
                        superseded,
                    },
                );
                if first.is_empty() {
                    first = last.clone();
                }
            }

            let held = if records > 0 {
                Some(epoch)
            } else if flagged {
                None
            } else {
                stored.and_then(|stored| stored.held)
            };
            let entries = if last.is_empty() {
                None
            } else {
                Some((entry_id(&first)?, entry_id(&last)?))
            };
            let value = ProducerField {
                fence: epoch,
                held,
                sequence,
                entries,
                digest: batch.digest,
            };
            keys.set(field.as_str(), value.encode());
            if !last.is_empty() {
                keys.set("newest", last.clone());
            }
            keys.producers.insert(member.clone(), at);
            keys.refresh(now, safety, tombstone);
            Ok((false, first, last, flagged))
        });
        let (resend, first, last, flagged) = appended?;
        if !resend && !last.is_empty() {
            self.appended.notify_waiters();
        }
        Ok(Appended {
            offsets: offsets(&first, &last)?,
            resend,
            flagged,
        })
    }

    async fn close(
        &self,
        namespace: &str,
        stream_ref: &StreamRef,
        producer: &Producer,
    ) -> StreamResult<Closed> {
        let stream_id = stream_ref.id.as_str();
        let kind = ProducerKind::try_from(producer)?;
        let epoch = epoch_key(&producer.epoch.unwrap_or_default());
        let field = format!("p|{}|{}", kind as i32, producer.id);
        let tombstone = millis(TOMBSTONE);
        let chain = owner_chain(stream_ref);
        let closed = self.with(namespace, stream_id, |keys, now| {
            keys.check_live(now)?;
            keys.check_owner(kind, &chain)?;
            if keys.get("closed").is_some() {
                return Ok(true);
            }
            let stored = keys.get(&field).map(ProducerField::try_from).transpose()?;
            if stored.is_some_and(|stored| epoch < stored.fence) {
                return Err(refuse(
                    Reason::Fenced,
                    "the epoch is lower than the newest one of the Producer",
                ));
            }
            keys.set("closed", "1");
            keys.refresh(now, 0, tombstone);
            Ok(false)
        });
        let already = closed?;
        // No entry wakes a waiting read, so the close wakes it.
        if !already {
            self.appended.notify_waiters();
        }
        Ok(Closed { already })
    }

    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        let start = if from > 0 {
            parse_id(&entry_id_of_offset(from)).unwrap_or((0, 0))
        } else {
            (0, 0)
        };
        self.with(namespace, stream_id, |keys, now| {
            let offset = |field: &str| -> StreamResult<i64> {
                keys.text(field).map_or(Ok(0), offset_of_entry_id)
            };
            let number = |field: &str| keys.number(field);
            let state = StreamState {
                exists: keys.get("v").is_some(),
                version: number("v") as u32,
                closed: keys.get("closed").is_some(),
                newest: offset("newest")?,
                trimmed: offset("trimmed")?,
                retention: Duration::from_millis(number("ret").max(0) as u64),
                expires_ms: number("exp"),
                now_ms: now,
            };
            let mut entries = vec![];
            if let Some(log) = keys.log.as_ref() {
                for (id, fields) in log.range(start..).take(count) {
                    entries.push(Entry {
                        offset: offset_of_entry_id(&id_text(*id))?,
                        record: fields.record.clone(),
                        superseded: fields.superseded.clone(),
                    });
                }
            }
            Ok((state, entries))
        })
    }

    async fn read_back(
        &self,
        namespace: &str,
        stream_id: &str,
        before: i64,
        count: usize,
    ) -> StreamResult<Vec<Entry>> {
        let before = parse_id(&entry_id_of_offset(before)).unwrap_or((0, 0));
        self.with(namespace, stream_id, |keys, _| {
            let Some(log) = keys.log.as_ref() else {
                return Ok(vec![]);
            };
            log.range(..=before)
                .rev()
                .take(count)
                .map(|(id, fields)| {
                    Ok(Entry {
                        offset: offset_of_entry_id(&id_text(*id))?,
                        record: fields.record.clone(),
                        superseded: fields.superseded.clone(),
                    })
                })
                .collect()
        })
    }

    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        let after = if after > 0 {
            parse_id(&entry_id_of_offset(after)).unwrap_or((0, 0))
        } else {
            (0, 0)
        };
        loop {
            // Listen before the check, so an append between the two still wakes the wait.
            let appended = self.appended.notified();
            let found = self.with(namespace, stream_id, |keys, _| {
                keys.get("closed").is_some()
                    || keys
                        .log
                        .as_ref()
                        .and_then(|log| log.keys().next_back())
                        .is_some_and(|id| *id > after)
            });
            if found {
                return Ok(true);
            }
            if tokio::time::timeout_at(deadline, appended).await.is_err() {
                return Ok(false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_follows_the_newest_one_within_a_millisecond() {
        let mut keys = Keys::default();
        assert_eq!(keys.add(5, Fields::default()), "5-0");
        assert_eq!(keys.add(5, Fields::default()), "5-1");
        // The ids keep increasing when the clock steps back.
        assert_eq!(keys.add(3, Fields::default()), "5-2");
        assert_eq!(keys.add(9, Fields::default()), "9-0");
    }

    #[test]
    fn a_producer_field_matches_the_shared_vectors() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/fields.json")).unwrap();
        let hex = |text: &str| -> Vec<u8> {
            (0..text.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
                .collect()
        };
        for case in vectors["producer_values"].as_array().unwrap() {
            let text = |name: &str| case[name].as_str().unwrap();
            let key = |name: &str| case[name].as_str().map(|key| hex(key).try_into().unwrap());
            let entries = (!text("first").is_empty()).then(|| {
                (
                    entry_id(text("first")).unwrap(),
                    entry_id(text("last")).unwrap(),
                )
            });
            let field = ProducerField {
                fence: key("fence").unwrap(),
                held: key("held"),
                sequence: case["sequence"].as_i64().unwrap(),
                entries,
                digest: hex(text("digest")).try_into().unwrap(),
            };
            let value = hex(text("value"));
            assert_eq!(field.encode(), value, "{}", text("name"));
            assert_eq!(ProducerField::try_from(value.as_slice()).unwrap(), field);
        }
    }
}
