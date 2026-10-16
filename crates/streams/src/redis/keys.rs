//! The Redis key layout, shared with every SDK that wrote streams before the store moved into
//! Core.
//!
//! Every key of one run chain shares a Redis Cluster hash tag, the chain itself, so one script
//! can touch all of them:
//!
//! ```text
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>        the log
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>:meta   its meta
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>:floors the readers that hold it
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:chain            the close flag
//! ```
//!
//! The notification log lives apart from the chains, one stream per Cluster slot and a few
//! doorbells, each under the hash tag of a slot (see `slots.rs`). A chain's appends go to the
//! stream of the chain's own slot, so the append script writes both:
//!
//! ```text
//! <prefix>:{<tag of slot s>}:n      slot s's notification entries
//! <prefix>:{<tag of slot d * 16384 / doorbells>}:bell
//!                                    doorbell d, which slots s with s % doorbells == d ring
//! ```
//!
//! Each part, the prefix included, is percent-encoded, so a `:` or a brace in an id cannot make
//! two streams share a key.

use super::{conn::slot, slots};
use crate::proto::ChainId;

/// Percent-encodes every byte but the unreserved ones, as Python's `quote(text, safe="")` does.
pub(crate) fn part(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// The keys of one run chain's streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChainKeys {
    base: String,
}

impl ChainKeys {
    pub(crate) fn new(prefix: &str, chain: &ChainId) -> Self {
        Self {
            base: format!(
                "{}:{{{}:{}:{}}}",
                part(prefix),
                part(&chain.namespace),
                part(&chain.workflow_id),
                part(&chain.first_run_id)
            ),
        }
    }

    pub(crate) fn log(&self, topic: &str) -> String {
        format!("{}:t:{}", self.base, part(topic))
    }

    pub(crate) fn meta(&self, topic: &str) -> String {
        format!("{}:meta", self.log(topic))
    }

    pub(crate) fn chain(&self) -> String {
        format!("{}:chain", self.base)
    }

    /// The positions-only readers that hold a topic's records, so a replay finds them again.
    pub(crate) fn floors(&self, topic: &str) -> String {
        format!("{}:floors", self.log(topic))
    }

    /// The Cluster slot every key of the chain lands in.
    pub(crate) fn slot(&self) -> u16 {
        slot(&self.base)
    }
}

/// Slot `slot`'s notification stream.
pub(crate) fn notifications(prefix: &str, slot: u16) -> String {
    format!("{}:{{{}}}:n", part(prefix), slots::tag(slot))
}

/// Doorbell `doorbell`'s stream, of `doorbells`. The doorbells spread over the slot range, so
/// a cluster's primaries share them.
pub(crate) fn doorbell(prefix: &str, doorbell: u32, doorbells: u32) -> String {
    let slot = u64::from(doorbell) * slots::SLOTS as u64 / u64::from(doorbells.max(1));
    format!("{}:{{{}}}:bell", part(prefix), slots::tag(slot as u16))
}

/// The chain and topic of a log key that a notification entry names, or `None` when the key is
/// not a log key under `prefix`.
pub(crate) fn chain_of_log(prefix: &str, key: &str) -> Option<(ChainId, String)> {
    let rest = key.strip_prefix(&format!("{}:{{", part(prefix)))?;
    let (tag, topic) = rest.split_once("}:t:")?;
    let mut parts = tag.split(':');
    let (namespace, workflow_id, first_run_id) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    Some((
        ChainId {
            namespace: unpart(namespace)?,
            workflow_id: unpart(workflow_id)?,
            first_run_id: unpart(first_run_id)?,
            ..Default::default()
        },
        unpart(topic)?,
    ))
}

/// Undoes [part].
fn unpart(text: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut input = text.bytes();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let hex = [input.next()?, input.next()?];
            bytes.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).ok()
}

/// The meta field that holds one producer attempt's newest batch.
///
/// Length-prefixed, so an id that holds `:` cannot name another attempt. The length counts UTF-8
/// bytes, so every language counts the same.
pub(crate) fn session_field(producer_id: &str, attempt: i64) -> String {
    format!("hw:{}:{producer_id}:{attempt}", producer_id.len())
}

/// The pattern that matches every key of one Workflow id, across all its chains.
pub(crate) fn owner_pattern(prefix: &str, namespace: &str, workflow_id: &str) -> String {
    format!(
        "{}:{{{}:{}:*",
        part(prefix),
        part(namespace),
        part(workflow_id)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn text<'a>(case: &'a Value, field: &str) -> &'a str {
        case[field].as_str().unwrap()
    }

    #[test]
    fn keys_match_the_python_sdk() {
        // Streams written by an SDK before the store moved into Core must stay readable.
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../testdata/redis/keys.json")).unwrap();
        assert!(cases.len() >= 4);
        for case in &cases {
            let keys = ChainKeys::new(
                text(case, "prefix"),
                &ChainId {
                    namespace: text(case, "namespace").to_string(),
                    workflow_id: text(case, "workflow_id").to_string(),
                    first_run_id: text(case, "first_run_id").to_string(),
                    ..Default::default()
                },
            );
            let topic = text(case, "topic");
            assert_eq!(keys.log(topic), text(case, "log"));
            assert_eq!(keys.meta(topic), text(case, "meta"));
            assert_eq!(keys.chain(), text(case, "chain"));
        }
    }

    #[test]
    fn a_chains_notification_stream_shares_its_slot() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../testdata/redis/keys.json")).unwrap();
        for case in &cases {
            let chain = ChainId {
                namespace: text(case, "namespace").to_string(),
                workflow_id: text(case, "workflow_id").to_string(),
                first_run_id: text(case, "first_run_id").to_string(),
                ..Default::default()
            };
            let prefix = text(case, "prefix");
            let keys = ChainKeys::new(prefix, &chain);
            let topic = text(case, "topic");
            let notifications = notifications(prefix, keys.slot());
            assert_eq!(slot(&notifications), slot(&keys.log(topic)));
            assert_eq!(slot(&notifications), slot(&keys.meta(topic)));
            assert_eq!(
                chain_of_log(prefix, &keys.log(topic)),
                Some((chain, topic.to_string()))
            );
        }
        assert_eq!(chain_of_log("p", "q:{ns:wf:run}:t:out"), None);
        assert_eq!(chain_of_log("p", "p:{ns:wf}:t:out"), None);
    }

    #[test]
    fn session_fields_match_the_python_sdk() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../testdata/redis/session_fields.json")).unwrap();
        for case in &cases {
            assert_eq!(
                session_field(text(case, "producer_id"), case["attempt"].as_i64().unwrap()),
                text(case, "field")
            );
        }
    }

    #[test]
    fn an_owner_pattern_covers_every_chain_of_one_workflow_only() {
        let chain = ChainId {
            namespace: "ns".to_string(),
            workflow_id: "wf".to_string(),
            first_run_id: "run-1".to_string(),
            ..Default::default()
        };
        let pattern = owner_pattern("p", "ns", "wf");
        assert_eq!(pattern, "p:{ns:wf:*");
        let matches = |key: &str| key.starts_with(pattern.trim_end_matches('*'));
        assert!(matches(&ChainKeys::new("p", &chain).chain()));
        // The `:` after the id keeps `wf` from matching the chains of `wf2`.
        let other = ChainId {
            workflow_id: "wf2".to_string(),
            ..chain
        };
        assert!(!matches(&ChainKeys::new("p", &other).chain()));
        // Each part is encoded as the keys encode it, so a glob character matches only itself.
        assert_eq!(owner_pattern("p", "ns", "a*b"), "p:{ns:a%2Ab:*");
    }

    #[test]
    fn the_doorbells_spread_over_the_slot_range() {
        let slots: Vec<u16> = (0..16)
            .map(|d| super::super::conn::slot(&doorbell("p", d, 16)))
            .collect();
        assert_eq!(slots, (0..16).map(|d| d * 1024).collect::<Vec<u16>>());
    }
}
