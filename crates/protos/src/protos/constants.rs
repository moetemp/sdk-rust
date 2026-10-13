//! Contains various constants that are used by core when storing/serializing data

/// Used as `marker_name` field when recording patch markers
pub const PATCH_MARKER_NAME: &str = "core_patch";

/// Used as `marker_name` field when recording local activity markers
pub const LOCAL_ACTIVITY_MARKER_NAME: &str = "core_local_activity";

/// Used as `marker_name` field when recording external stream markers
///
/// Kept out of the `core_workflow_stream*` namespace so it cannot collide in History with the
/// shipped `temporalio.contrib.workflow_streams`, which it coexists with.
pub const EXTERNAL_STREAM_MARKER_NAME: &str = "core_external_stream";
