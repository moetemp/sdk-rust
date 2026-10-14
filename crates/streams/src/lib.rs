#![warn(missing_docs)]

//! Stream stores for Temporal Workflows.
//!
//! A stream is a topic of a Workflow, kept in a store outside Temporal and keyed by the
//! Workflow's run chain. This crate holds what every store and every SDK share: the
//! [StreamStore] trait a store implements, the cursor format, the record helpers that stamp
//! records, and the error every call fails with. Lang reaches a store through Core's
//! `StreamService`.

mod cursor;
mod error;
mod memory;
mod reader;
mod record;
mod store;

pub use cursor::{BEGINNING, END, cursor_position, mint_cursor, stream_hash};
pub use error::{StreamError, StreamResult};
pub use memory::MemoryStore;
pub use reader::{
    DEFAULT_MAX_RECORDS, MAX_STATE_BYTES, MAX_TRACKED_PRODUCERS, ReadTarget, read_page,
};
pub use record::{
    WORKFLOW_OWNER_KIND, activity_producer_id, check_append_digest, stored_append_record,
};
pub use store::StreamStore;

/// The protos stores and lang exchange.
pub mod proto {
    pub use temporalio_common::protos::{
        coresdk::streams::*,
        temporal::sdk::streams::v1::{StreamRecord, StreamRecordKind},
    };
}
