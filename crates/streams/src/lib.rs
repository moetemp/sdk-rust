#![warn(missing_docs)]

//! Stream stores for Temporal Workflows.
//!
//! A stream is a topic of a Workflow, kept in a store outside Temporal and keyed by the
//! Workflow's run chain. This crate holds what every store and every SDK share: the
//! [StreamStore] trait a store implements, the cursor format, the record helpers that stamp
//! records, and the error every call fails with. Lang reaches a store through Core's
//! `StreamService`, and Core's Worker appends a Workflow's own output through the same trait.

mod cursor;
mod driver;
mod error;
mod handle;
mod memory;
mod notify;
mod notifylog;
mod owner;
mod reader;
mod record;
#[cfg(feature = "redis")]
mod redis;
mod service;
mod store;
mod subscribe;

#[cfg(feature = "redis")]
pub use self::redis::{DEFAULT_NOTIFY_DOORBELLS, RedisStore, RedisStoreOptions};
pub use cursor::{BEGINNING, END, cursor_position, mint_cursor, stream_hash};
pub use driver::{StreamDriver, StreamDriverOptions};
pub use error::{StreamError, StreamResult};
pub use handle::{OWNER_RECHECK, OWNER_START_GRACE, Streams, StreamsOptions};
pub use memory::MemoryStore;
pub use notify::{
    CLOSE_RETRY_CAP, CLOSE_RETRY_FIRST, CLOSE_RETRY_LIMIT, DEFAULT_MAX_NOTIFIERS, FLUSH_LIMIT,
    Notifier, NotifierClient, NotifierPoll, NotifyError, NotifyingStore, StreamNotification,
    WorkflowAttachment, progress_counter,
};
pub use notifylog::{NotificationEntry, NotificationLog};
pub use owner::{OwnerClient, OwnerDescription, OwnerError};
pub use reader::{
    DEFAULT_MAX_RECORDS, MAX_STATE_BYTES, MAX_TRACKED_PRODUCERS, ReadTarget, read_page,
};
pub use record::{
    WORKFLOW_OWNER_KIND, activity_producer_id, check_append_digest, stored_append_record,
    workflow_batch_digest, workflow_producer_id,
};
pub use service::StreamService;
pub use store::{
    CloseOutcome, MAX_CLOSE_BYTES, MAX_FLOOR_HOLDERS, StreamStore, empty_close_result,
};

/// The protos stores and lang exchange.
pub mod proto {
    pub use temporalio_common::protos::{
        coresdk::streams::*,
        temporal::api::common::v1::Payload,
        temporal::sdk::streams::v1::{StreamRecord, StreamRecordKind},
    };
}
