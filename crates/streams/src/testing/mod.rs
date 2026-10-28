//! The shared suites of the Stream rules. Each Store crate runs them on its own backends, so that
//! every Store keeps the same rules. This crate runs them on the memory Store.

pub(crate) mod support;

pub mod scope;
pub mod service;
pub mod store;
pub mod writer;

pub use support::Needs;

use crate::{
    MemoryStreamRuntime, StreamRuntime,
    memory::MemoryStore,
    proto::MemoryStreamConfig,
    store::{DEFAULT_RETENTION, DEFAULT_SAFETY_TTL, Store},
};
use futures_util::{FutureExt, future::join_all};
use prost::Message;
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

/// The state under a Store's calls, which some cases read or change.
#[async_trait::async_trait]
pub trait RawKeys: Send + Sync {
    /// Sets one meta field of a Stream.
    async fn set_meta(&self, namespace: &str, stream: &str, field: &str, value: &str);
    /// One meta field of a Stream.
    async fn meta(&self, namespace: &str, stream: &str, field: &str) -> Option<String>;
    /// The ms left on the log and on the meta, as `PTTL` gives them.
    async fn ttls(&self, namespace: &str, stream: &str) -> (i64, i64);
    /// Adds a log entry with an explicit id, as `XADD <id>`.
    async fn add_raw_entry(&self, namespace: &str, stream: &str, id: &str);
    /// The number of log entries.
    async fn log_len(&self, namespace: &str, stream: &str) -> usize;
}

/// A backend that the suites run on.
#[async_trait::async_trait]
pub trait TestBackend: Send + Sync {
    /// What the backend gives. A case that needs more doesn't run on it.
    fn gives(&self) -> Needs;
    /// A Store for one case, and the state under it.
    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>);
    /// A runtime on the backend, whose refs name `backend`.
    fn runtime(&self, backend: &str) -> Arc<StreamRuntime>;
}

/// The memory Store, with a new Store for each case.
#[derive(Debug, Default)]
pub struct MemoryBackend;

#[async_trait::async_trait]
impl TestBackend for MemoryBackend {
    fn gives(&self) -> Needs {
        Needs::ALL
    }

    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>) {
        let store = Arc::new(MemoryStore::new(DEFAULT_RETENTION, DEFAULT_SAFETY_TTL));
        (store.clone(), store)
    }

    // Through the config bytes, as a bridge builds it.
    fn runtime(&self, backend: &str) -> Arc<StreamRuntime> {
        let config = MemoryStreamConfig {
            backend: backend.to_string(),
            ..Default::default()
        };
        MemoryStreamRuntime::from_config_bytes(&config.encode_to_vec(), None).unwrap()
    }
}

#[async_trait::async_trait]
impl RawKeys for MemoryStore {
    async fn set_meta(&self, namespace: &str, stream: &str, field: &str, value: &str) {
        self.test_set_meta(namespace, stream, field, value)
    }

    async fn meta(&self, namespace: &str, stream: &str, field: &str) -> Option<String> {
        self.test_meta(namespace, stream, field)
    }

    async fn ttls(&self, namespace: &str, stream: &str) -> (i64, i64) {
        self.test_ttls(namespace, stream)
    }

    async fn add_raw_entry(&self, namespace: &str, stream: &str, id: &str) {
        self.test_add_entry(namespace, stream, id)
    }

    async fn log_len(&self, namespace: &str, stream: &str) -> usize {
        self.test_log_len(namespace, stream)
    }
}

/// One case of a suite.
pub struct Case {
    /// The name of the case, for the report.
    pub name: &'static str,
    /// Runs the case on each backend under test that gives what it needs.
    pub run: fn() -> Pin<Box<dyn Future<Output = ()>>>,
}

macro_rules! case {
    ($name:ident) => {
        $crate::testing::Case {
            name: stringify!($name),
            run: || Box::pin($name()),
        }
    };
}
pub(crate) use case;

tokio::task_local! {
    static BACKENDS: Vec<Arc<dyn TestBackend>>;
}

/// The backends of the running suite.
pub(crate) fn backends() -> Vec<Arc<dyn TestBackend>> {
    BACKENDS.with(Clone::clone)
}

/// Runs `cases` on `backends`, all at once, and then fails with the names of the cases that
/// failed. A case that fails prints its panic as it goes.
pub async fn run(backends: Vec<Arc<dyn TestBackend>>, cases: &[Case]) {
    let outcomes = BACKENDS
        .scope(
            backends,
            join_all(cases.iter().map(|case| async move {
                let passed = AssertUnwindSafe((case.run)()).catch_unwind().await.is_ok();
                (case.name, passed)
            })),
        )
        .await;
    let failed: Vec<_> = outcomes
        .into_iter()
        .filter(|(_, passed)| !passed)
        .map(|(name, _)| name)
        .collect();
    assert!(failed.is_empty(), "these cases failed: {failed:?}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_store_suite_passes_on_the_memory_store() {
        run(vec![Arc::new(MemoryBackend)], store::CASES).await;
    }

    #[tokio::test]
    async fn the_service_suite_passes_on_the_memory_store() {
        run(vec![Arc::new(MemoryBackend)], service::CASES).await;
    }

    #[tokio::test]
    async fn the_writer_suite_passes_on_the_memory_store() {
        run(vec![Arc::new(MemoryBackend)], writer::CASES).await;
    }

    #[tokio::test]
    async fn the_scope_suite_passes_on_the_memory_store() {
        run(vec![Arc::new(MemoryBackend)], scope::CASES).await;
    }
}
