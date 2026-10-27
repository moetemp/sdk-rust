#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs. It is experimental.
//!
//! The crate serves the Stream service on a Stream Store. Lang's bridge builds the runtime per
//! client and calls it with an operation name and proto bytes. Core has no hook for it. A Workflow
//! makes each call from a Local Activity.

pub mod digest;
pub mod epoch;
mod error;
pub mod ids;
pub mod memory;
mod metrics;
pub mod offset;
pub mod protos;
mod record;
mod runtime;
mod service;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
mod writer;

pub use error::{Reason, StreamError, StreamResult};
pub use runtime::{MEMORY_RUNTIME_NAME, MemoryStreamRuntime, StoreFactory, StreamRuntime};
pub use service::{MAX_RECORDS, MAX_WAIT};

/// The protos that lang and the crate exchange.
pub mod proto {
    pub use crate::protos::temporal::{
        api::{enums::v1::*, stream::v1::*, streamservice::v1::*},
        sdk::streams::v1::*,
    };
}
