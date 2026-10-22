//! A plugin component that a worker carries, which lang calls by method name.

use temporalio_client::Connection;

/// A component that lang passes to the worker through [crate::WorkerConfig::extension].
///
/// Core only gives it the worker's connection. Core never calls [WorkerExtension::call] or
/// [WorkerExtension::call_async]. A bridge exposes them to lang, so lang code reaches the
/// extension in the same native library without a bridge function of its own.
#[async_trait::async_trait]
pub trait WorkerExtension: Send + Sync + 'static {
    /// A name that identifies the extension in logs.
    fn name(&self) -> &str;

    /// Gives the extension the worker's client connection. Core calls it once when the worker
    /// starts, before any poll, and again with the new connection after
    /// [crate::Worker::replace_client]. The extension then calls the server with the worker's
    /// TLS, API key and headers, and it needs no connection of its own. It must not block.
    fn attach(&self, _connection: Connection) {}

    /// Runs `method` with encoded input and returns the encoded output. It must not block, since
    /// lang may call it from workflow code.
    fn call(&self, method: &str, _input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
        Err(ExtensionError::unknown_method(method))
    }

    /// Like [WorkerExtension::call], for work that waits on I/O.
    async fn call_async(&self, method: String, _input: Vec<u8>) -> Result<Vec<u8>, ExtensionError> {
        Err(ExtensionError::unknown_method(&method))
    }
}

/// The error of a [WorkerExtension] call. It has the fields of a gRPC status, so a bridge can turn
/// it into the same lang error as a failed service call.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{code:?}: {message}")]
#[non_exhaustive]
pub struct ExtensionError {
    /// The status code.
    pub code: tonic::Code,
    /// The message for the caller.
    pub message: String,
    /// Encoded error details, empty if there are none.
    pub details: Vec<u8>,
}

impl ExtensionError {
    /// Creates an error with no details.
    pub fn new(code: tonic::Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: vec![],
        }
    }

    /// Adds encoded error details.
    pub fn with_details(mut self, details: Vec<u8>) -> Self {
        self.details = details;
        self
    }

    /// The error for a method the extension does not have.
    pub fn unknown_method(method: &str) -> Self {
        Self::new(
            tonic::Code::Unimplemented,
            format!("Unknown extension method {method}"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct Named;

    impl WorkerExtension for Named {
        fn name(&self) -> &str {
            "named"
        }
    }

    #[tokio::test]
    async fn calls_to_an_extension_without_methods_are_unimplemented() {
        let extension: Arc<dyn WorkerExtension> = Arc::new(Named);
        let err = extension.call("Append", b"x").unwrap_err();
        assert_eq!(err.code, tonic::Code::Unimplemented);
        assert!(err.message.contains("Append"));
        let err = extension
            .call_async("Append".to_string(), vec![])
            .await
            .unwrap_err();
        assert_eq!(err.code, tonic::Code::Unimplemented);
    }

    struct Echo;

    #[async_trait::async_trait]
    impl WorkerExtension for Echo {
        fn name(&self) -> &str {
            "echo"
        }

        fn call(&self, method: &str, input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
            match method {
                "Echo" => Ok(input.to_vec()),
                _ => {
                    Err(ExtensionError::new(tonic::Code::NotFound, "no stream")
                        .with_details(vec![7]))
                }
            }
        }

        async fn call_async(
            &self,
            method: String,
            input: Vec<u8>,
        ) -> Result<Vec<u8>, ExtensionError> {
            self.call(&method, &input)
        }
    }

    #[tokio::test]
    async fn calls_reach_the_extension_methods() {
        let extension: Arc<dyn WorkerExtension> = Arc::new(Echo);
        assert_eq!(extension.call("Echo", b"abc").unwrap(), b"abc");
        assert_eq!(
            extension
                .call_async("Echo".to_string(), b"abc".to_vec())
                .await
                .unwrap(),
            b"abc"
        );
        let err = extension.call("Read", b"").unwrap_err();
        assert_eq!(err.code, tonic::Code::NotFound);
        assert_eq!(err.details, [7]);
    }
}
