#![warn(missing_docs)]

//! Stream stores for Temporal Workflows.
//!
//! A stream is a topic of a Workflow or a standalone Activity, kept in a store outside Temporal
//! and keyed by the owner's run chain. An Activity run is a chain of one run. This crate holds
//! what every store and every SDK share: the [StreamStore] trait a store implements, the cursor
//! format, the record helpers that stamp records, and the error every call fails with. Lang
//! reaches a store through Core's `StreamService`, and Core's Worker appends a Workflow's own
//! output through the same trait.

mod archive;
mod cursor;
mod driver;
mod error;
mod follow;
mod handle;
mod links;
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
mod tiered;

#[cfg(feature = "redis")]
pub use self::redis::{DEFAULT_NOTIFY_DOORBELLS, RedisStore, RedisStoreOptions};
#[cfg(feature = "archive")]
pub use archive::ObjectStoreArchive;
pub use archive::{ArchiveProgress, LocalFileArchive, SegmentLimits, StreamArchive, archive_topic};
pub use cursor::{BEGINNING, END, cursor_position, mint_cursor, stream_hash};
pub use driver::{ArchiveSink, StreamDriver, StreamDriverOptions};
pub use error::{StreamError, StreamResult};
pub use handle::{OWNER_RECHECK, OWNER_START_GRACE, Streams, StreamsOptions};
pub use memory::MemoryStore;
pub use notify::{
    CLOSE_RETRY_CAP, CLOSE_RETRY_FIRST, DEFAULT_MAX_NOTIFIERS, FLUSH_LIMIT, Notifier,
    NotifierClient, NotifierPoll, NotifyError, NotifyingStore, StreamNotification,
    WorkflowAttachment, progress_counter,
};
pub use notifylog::{ArchiveMark, NotificationEntry, NotificationLog};
pub use owner::{OwnerClient, OwnerDescription, OwnerError, OwnerId, OwnerStatus};
pub use reader::{
    DEFAULT_MAX_RECORDS, MAX_STATE_BYTES, MAX_TRACKED_PRODUCERS, ReadTarget, read_page,
};
pub use record::{
    ACTIVITY_OWNER_KIND, MAX_KEYED_ACTIVITY_ATTEMPT, WORKFLOW_ACTIVITY_OWNER_KIND,
    WORKFLOW_OWNER_KIND, activity_attempt, activity_producer_id, chain_owner, check_append_digest,
    stored_append_record, workflow_batch_digest, workflow_producer_id,
};
pub use service::StreamService;
pub use store::{ARCHIVE_HOLDER, CloseOutcome, MAX_CLOSE_BYTES, StreamStore, empty_close_result};
pub use tiered::TieredStore;

/// The protos stores and lang exchange.
pub mod proto {
    pub use temporalio_common::protos::{
        coresdk::streams::*,
        temporal::api::common::v1::Payload,
        temporal::sdk::streams::v1::{StreamLink, StreamRecord, StreamRecordKind, stream_link},
    };
}
