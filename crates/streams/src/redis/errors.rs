//! Turning Redis client errors into stream errors, so no Redis error reaches lang.

use crate::{StreamError, proto::StreamFailureKind};
use redis::RedisError;

/// Whether a failed call may have changed the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Call {
    Read,
    Write,
    /// A read a write makes first. Nothing is written yet, but a refusal refuses the write.
    BeforeWrite,
}

/// The stream error a Redis error stands for.
///
/// A connection or timeout failure during a write leaves its outcome unknown. A reply the server
/// sent means it refused the write. The scripts name their own refusals with a `STREAMS_` code.
pub(crate) fn stream_error(error: &RedisError, call: Call) -> StreamError {
    if error.is_io_error() || error.is_timeout() || error.is_connection_dropped() {
        return match call {
            Call::Write => StreamError::outcome_unknown(format!(
                "the write may or may not have been applied: {error}"
            )),
            Call::Read | Call::BeforeWrite => {
                StreamError::storage(format!("Redis could not be reached: {error}"))
            }
        };
    }
    let (code, detail) = server_reply(error).unzip();
    let detail = detail.unwrap_or_default();
    match code.as_deref() {
        Some("STREAMS_DIVERGENT") => StreamError::new(StreamFailureKind::ProducerDivergent, detail),
        Some("STREAMS_STALE") => StreamError::new(StreamFailureKind::ProducerStale, detail),
        Some("STREAMS_CLOSED") => StreamError::closed(detail),
        // The reply as the server wrote it, so the message names the code the Redis docs use.
        Some(code) if call != Call::Read => {
            StreamError::refused(format!("Redis refused the write: {code} {detail}"))
        }
        Some(code) => StreamError::storage(format!("Redis refused the read: {code} {detail}")),
        None => StreamError::storage(format!("Redis failed: {error}")),
    }
}

/// The code and detail of the reply the server refused with. A pipeline reports its failed
/// commands together, and the first one names the refusal.
fn server_reply(error: &RedisError) -> Option<(String, String)> {
    if let Some(code) = error.code() {
        return Some((
            code.to_string(),
            error.detail().unwrap_or_default().to_string(),
        ));
    }
    let failed = error.clone().into_server_errors()?;
    let (_, first) = failed.first()?;
    Some((
        first.code().to_string(),
        first.details().unwrap_or_default().to_string(),
    ))
}

/// Maps a Redis result for a call.
pub(crate) trait Mapped<T> {
    fn mapped(self, call: Call) -> Result<T, StreamError>;
}

impl<T> Mapped<T> for Result<T, RedisError> {
    fn mapped(self, call: Call) -> Result<T, StreamError> {
        self.map_err(|error| stream_error(&error, call))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::{ErrorKind, ServerErrorKind};

    fn server(code: &str, detail: &str) -> RedisError {
        // Parsed as the wire carries it, so the code and detail split as a real reply's do.
        redis::parse_redis_value(format!("-{code} {detail}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    #[test]
    fn script_refusals_keep_their_class() {
        for (code, kind) in [
            ("STREAMS_DIVERGENT", StreamFailureKind::ProducerDivergent),
            ("STREAMS_STALE", StreamFailureKind::ProducerStale),
            ("STREAMS_CLOSED", StreamFailureKind::Closed),
        ] {
            for call in [Call::Read, Call::Write] {
                let error = stream_error(&server(code, "sequence 3 is below"), call);
                assert_eq!(error.kind, kind, "{code}");
                assert_eq!(error.message, "sequence 3 is below");
            }
        }
    }

    #[test]
    fn a_reply_refuses_a_write_and_fails_a_read() {
        let refused = server("NOPERM", "this user has no permissions");
        assert_eq!(refused.kind(), ErrorKind::Server(ServerErrorKind::NoPerm));
        let error = stream_error(&refused, Call::Write);
        assert_eq!(error.kind, StreamFailureKind::Refused);
        assert!(error.message.contains("NOPERM"), "{error}");
        assert_eq!(
            stream_error(&refused, Call::Read).kind,
            StreamFailureKind::Storage
        );
    }

    #[test]
    fn a_read_before_a_write_refuses_it_but_writes_nothing() {
        let refused = stream_error(&server("WRONGTYPE", "wrong kind"), Call::BeforeWrite);
        assert_eq!(refused.kind, StreamFailureKind::Refused);
        let lost = RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert_eq!(
            stream_error(&lost, Call::BeforeWrite).kind,
            StreamFailureKind::Storage
        );
    }

    #[test]
    fn a_lost_connection_leaves_a_write_unknown() {
        let lost = RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert_eq!(
            stream_error(&lost, Call::Write).kind,
            StreamFailureKind::OutcomeUnknown
        );
        assert_eq!(
            stream_error(&lost, Call::Read).kind,
            StreamFailureKind::Storage
        );
    }
}
