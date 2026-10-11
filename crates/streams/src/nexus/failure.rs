//! How a stream call's failure crosses to a Nexus caller.
//!
//! The mapping and the class names are N0b's, so a caller in any language raises the class the
//! store raised, and callers saw no change when the handler moved into Core.

use crate::{StreamError, proto::NexusFailure, proto::StreamFailureKind};
use temporalio_common::protos::temporal::api::{
    enums::v1::NexusHandlerErrorRetryBehavior,
    failure::v1::{ApplicationFailureInfo, Failure, NexusHandlerFailureInfo, failure::FailureInfo},
};

/// The class a caller raises for a mistake in the call's input, the same in every SDK.
pub(crate) const INPUT_MISTAKE: &str = "ValueError";

/// The Nexus `HandlerErrorType` names this table uses.
pub(crate) const BAD_REQUEST: &str = "BAD_REQUEST";
pub(crate) const NOT_FOUND: &str = "NOT_FOUND";
pub(crate) const NOT_IMPLEMENTED: &str = "NOT_IMPLEMENTED";
pub(crate) const INTERNAL: &str = "INTERNAL";
pub(crate) const UNAVAILABLE: &str = "UNAVAILABLE";

/// The class name, the Nexus error type and whether a retry may help, per stream failure.
pub(crate) fn classify(kind: StreamFailureKind) -> (&'static str, &'static str, bool) {
    use StreamFailureKind::*;
    match kind {
        NotFound => ("StreamNotFoundError", NOT_FOUND, false),
        Cursor => ("StreamCursorError", BAD_REQUEST, false),
        Expired => ("StreamExpiredError", BAD_REQUEST, false),
        ProducerDivergent | ProducerStale => ("StreamProducerError", BAD_REQUEST, false),
        Closed => ("StreamClosedError", BAD_REQUEST, false),
        Unsupported => ("StreamUnsupportedError", NOT_IMPLEMENTED, false),
        // Nothing was written, and the same call gets the same answer until the store changes.
        Refused => ("StreamRefusedError", INTERNAL, false),
        // The write may have landed. The store deduplicates a repeat, so a retry is safe.
        OutcomeUnknown => ("StreamOutcomeUnknownError", UNAVAILABLE, true),
        Storage => ("StreamStorageError", UNAVAILABLE, true),
        Record => ("StreamRecordError", INTERNAL, true),
        Unspecified => ("StreamError", INTERNAL, true),
    }
}

/// The failure a stream error crosses as.
pub(crate) fn of_stream(error: &StreamError) -> NexusFailure {
    let (class, kind, retryable) = classify(error.kind);
    failure(class, kind, retryable, &error.message)
}

/// The failure a mistake in the call's input crosses as.
pub(crate) fn input_mistake(message: impl Into<String>) -> NexusFailure {
    failure(INPUT_MISTAKE, BAD_REQUEST, false, &message.into())
}

/// A failure of `kind` with the cause class `class`. The message leads with the class, and the
/// failure carries no stack, so the handler's frames stay on this side.
pub(crate) fn failure(class: &str, kind: &str, retryable: bool, message: &str) -> NexusFailure {
    let retry_behavior = if retryable {
        NexusHandlerErrorRetryBehavior::Retryable
    } else {
        NexusHandlerErrorRetryBehavior::NonRetryable
    };
    NexusFailure {
        handler_error_type: kind.to_string(),
        retryable,
        message: format!("{class}: {message}"),
        failure: Some(Failure {
            cause: Some(Box::new(Failure {
                message: message.to_string(),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        r#type: class.to_string(),
                        non_retryable: !retryable,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
                NexusHandlerFailureInfo {
                    r#type: kind.to_string(),
                    retry_behavior: retry_behavior as i32,
                },
            )),
            ..Default::default()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_stream_condition_crosses_as_its_own_class() {
        use StreamFailureKind::*;
        for (kind, class, nexus, retryable) in [
            (NotFound, "StreamNotFoundError", NOT_FOUND, false),
            (Cursor, "StreamCursorError", BAD_REQUEST, false),
            (Expired, "StreamExpiredError", BAD_REQUEST, false),
            (ProducerDivergent, "StreamProducerError", BAD_REQUEST, false),
            (ProducerStale, "StreamProducerError", BAD_REQUEST, false),
            (Closed, "StreamClosedError", BAD_REQUEST, false),
            (
                Unsupported,
                "StreamUnsupportedError",
                NOT_IMPLEMENTED,
                false,
            ),
            (Refused, "StreamRefusedError", INTERNAL, false),
            (
                OutcomeUnknown,
                "StreamOutcomeUnknownError",
                UNAVAILABLE,
                true,
            ),
            (Storage, "StreamStorageError", UNAVAILABLE, true),
        ] {
            let crossed = of_stream(&StreamError::new(kind, "why"));
            assert_eq!(crossed.handler_error_type, nexus, "{kind:?}");
            assert_eq!(crossed.retryable, retryable, "{kind:?}");
            assert_eq!(crossed.message, format!("{class}: why"));
            let failure = crossed.failure.unwrap();
            assert!(failure.stack_trace.is_empty());
            let Some(FailureInfo::NexusHandlerFailureInfo(info)) = failure.failure_info else {
                panic!("no Nexus handler info");
            };
            assert_eq!(info.r#type, nexus);
            let cause = failure.cause.unwrap();
            let Some(FailureInfo::ApplicationFailureInfo(application)) = cause.failure_info else {
                panic!("no application info");
            };
            assert_eq!(application.r#type, class);
            assert_eq!(application.non_retryable, !retryable);
            assert_eq!(cause.message, "why");
        }
    }

    #[test]
    fn an_input_mistake_is_a_bad_request() {
        let crossed = input_mistake("an append carries payloads or finishes");
        assert_eq!(crossed.handler_error_type, BAD_REQUEST);
        assert!(!crossed.retryable);
        assert!(crossed.message.starts_with("ValueError: "));
    }
}
