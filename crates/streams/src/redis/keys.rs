//! The Redis key layout, shared with every SDK that wrote streams before the store moved into
//! Core.
//!
//! Every key of one run chain shares a Redis Cluster hash tag, the chain itself, so one script
//! can touch all of them:
//!
//! ```text
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>        the log
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:t:<topic>:meta   its meta
//! <prefix>:{<namespace>:<workflow id>:<first run id>}:chain            the close flag
//! ```
//!
//! Each part, the prefix included, is percent-encoded, so a `:` or a brace in an id cannot make
//! two streams share a key.

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
}

/// The meta field that holds one producer attempt's newest batch.
///
/// Length-prefixed, so an id that holds `:` cannot name another attempt. The length counts UTF-8
/// bytes, so every language counts the same.
pub(crate) fn session_field(producer_id: &str, attempt: i64) -> String {
    format!("hw:{}:{producer_id}:{attempt}", producer_id.len())
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
                },
            );
            let topic = text(case, "topic");
            assert_eq!(keys.log(topic), text(case, "log"));
            assert_eq!(keys.meta(topic), text(case, "meta"));
            assert_eq!(keys.chain(), text(case, "chain"));
        }
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
}
