<!--
High-level release notes.
Loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This file serves users of the other Temporal SDKs, whose workers and clients run on
Core. The repository-root CHANGELOG.md serves users of the Rust SDK.

Log a change here only if a user of one of those SDKs can observe it: different behavior,
an option surfaced to them, a new log or metric, a different interaction with the server.
The question is what the user sees, not which crate or which files your PR touched.

The Rust SDK runs on Core too, so a user-observable change in Core behavior normally
belongs in the root changelog as well, worded for each audience. What belongs only here is
what only the other SDKs' users can see — a Core capability the Rust SDK does not surface,
or a C-bridge change. A Rust-level API change that a language SDK absorbs inside its own
bridge, without its users noticing, belongs in neither file.

When your PR includes a user-facing change, add an entry below under the
appropriate heading (create the heading if it does not yet exist) in the
Unreleased section — never under a released version. Within each heading content
can be free-form. Feel free to include examples, links to docs, or any other
relevant information.

### Added            — new features
### Changed          — changes in existing functionality
### Deprecated       — soon-to-be-removed features
### Breaking Changes — removed or backwards-incompatible features
### Fixed            — notable bug fixes
### Security         — notable security fixes
-->

# Changelog

## Unreleased

### Added
* Experimental: behind the new `streams` feature, `streams::connect_stream_service` builds one
  stream store from a `StreamStoreConfig` (Redis or memory) and serves lang's stream calls in
  process: append, read, latest, close and owner delete, as serialized `coresdk.streams`
  requests.
* Experimental: the C bridge has `temporal_core_stream_store_new`, `_call` and `_free` for the
  stream service, built with the Redis store.
* Experimental: with `StreamStoreConfig.notify_on_append`, Core tells each stream's notifier on
  the server when the stream moves, after every append. That's how a reader that waits on the
  notifier wakes. An empty read answer carries `wake_after`, the counter of the read's position,
  and the reader waits for a counter above it. Notifications for one stream fold into one call in
  flight, a failed last notification is tried twice more, and a notification never fails the
  write. `max_notifiers` caps how many streams the process keeps a notifier for (1000 when zero).
* Experimental: a stream store can keep a notification log, one entry per append written in the
  same store step. With `StreamStoreConfig.notification_log`, producers send no notifications and
  a driver reads the log through `StreamStore::notification_log`, a `NotificationLog` of
  partitions, doorbells, consumer-group takes, claims after an idle time, acks and a sweep. The
  memory store keeps one, and a store without one refuses the setting. `NotifierClient` gains
  `notify_streams`, which Core's client sends as one `NotifyStreams` call per namespace, and the C
  bridge takes `NotifyStreams` as a raw call.
* Experimental: the Redis store keeps a notification log with `notification_log`. The append
  script (hash `9fdc33e7`) also adds an entry to the stream of the chain's Cluster slot,
  `<prefix>:{<slot tag>}:n`, and Core rings one of `notify_doorbells` (16 when zero) doorbell
  streams when that stream takes its first entry. `testdata/redis/notify_slots.json` pins the slot
  tags for other SDKs. Drivers read as the consumer group `streams-driver`.
* Experimental: the new `RunStreamDriver` stream call runs a stream driver until the caller
  cancels it. The driver blocks on each doorbell and sweeps every busy slot every
  `sweep_interval` (30 seconds). It folds each drain to the newest counter per stream, sends one
  `NotifyStreams` call per namespace, and acks the entries only once the call landed. It takes
  entries another driver left unacked for `claim_idle` (60 seconds). Each driver needs its own
  `consumer`. `StreamDriver` runs the same in Rust.
* Experimental: a reader reads first and subscribes only when a read comes back empty, since a
  notification for a stream with no notifier is dropped. `SubscribeResponse.newest_counter` names
  the newest record after the attach landed, so a reader whose read stopped below it reads once
  more before it sleeps. `WaitForNotification` reads the stream once more just after it sends the
  poll, and answers at once for a record that landed before the notifier existed.
* Experimental: the new `FlushNotifications` stream call waits up to 10 seconds for the
  notifications still out. A process calls it before it stops.
* Experimental: a server without the stream notifier answers the first notifier call with
  `UNIMPLEMENTED`. Core then stops notifier calls in that process and logs it once at debug.
* Experimental: Core closes a stream's notifier after the store closes the stream, whether or
  not `notify_on_append` is on, so attached callbacks complete with the close outcome. A
  `CloseRequest` carries it in the new `result` or `failure` field. With one of them, the call
  fails when the server doesn't take the close, and Core keeps trying in the background too.
  Without them, the notifier closes in the background with a `binary/null` result. A reader that
  finds the owner ended closes only the store. The notifier's owner check closes the notifier.
* Experimental: `StreamStore` has `close_topic_with` and `close_stream`, which carry the close
  outcome to a wrapper that tells the notifier.
* Experimental: the new `Subscribe` and `Unsubscribe` stream calls attach a Workflow's reader to
  a stream's notifier as a `workflow_target`, and detach it. The attach's request id is
  `jetty/<run id>/<subscription id>`, so a rerun attaches as the same reader. After a wake with
  `resubscribe`, lang raises `SubscribeRequest.generation`, which makes a new attach. A process
  attaches each subscription once and answers a repeat from memory. The attach names the chain
  that holds the Workflow id now. The attach only announces the reader, and the first append
  wakes the Workflow.
* Experimental: the new `WaitForNotification` stream call waits on a stream's notifier with the
  server's `PollStreamNotifier` long poll, for a client reader whose read came back empty. It
  returns when the notifier's counter passes the read's `wake_after`, when the stream closed, or
  with `timed_out` at the server's wait limit. The reader then reads again.
* Experimental: a stream wake for one of a Workflow's readers reaches lang as a
  `ResolveStreamWake` job, at most one per subscription per workflow task, folded from the
  `stream_wakes` on the task's scheduled events. Replay delivers the same jobs in the same
  activations. A task scheduled with wakes ends a workflow task heartbeat chain, so it keeps an
  activation of its own on replay.
* Experimental: `Success.heartbeat_task` on a workflow activation completion asks Core to
  complete the workflow task now while local activities run, as at a local activity heartbeat.
  The task carries the commands and markers so far and asks for a new task. Core ignores the
  flag while replaying, since lang sets it from a count or a clock.
* Experimental: a store keeps the first close of each topic, with its result or failure and the
  new `CloseRequest.closed_by`, at most 1.5 MiB. A read that ends with `done` answers it as
  `close_result`, `close_failure` and `closed_by`, so a reader that reaches the end before a
  close wake still sees the outcome. A chain close, when the owner ended, keeps none. A CLOSED
  append refusal names the close's `closed_by` in `StreamFailure.closed_by`. The Redis store
  keeps it in the topic's meta as `close`, set only once by the close script. A later close
  tells the stream's notifier that first close too, not its own.
  `StreamStore::close_topic_with` and `close_stream` take a `StoredClose`, and
  `close_topic_with` answers the close the store keeps.
* Experimental: a positions-only Workflow reader can record what it read in place of the records.
  `ReadResponse` gains `records_digest`, `chain_first_run_id` and `start_cursor`, and the new
  `ReadRange` stream call reads that exact range again on replay and checks the digest. Other
  records fail as `REPLAY_MISMATCH`, and records the store no longer holds as
  `REPLAY_RANGE_LOST` (REC-01). `ReadRequest.floor_holder` holds the records a reader read
  against retention, under a lease that each read renews, and the new `ReleaseFloor` call ends
  the hold after a grace. The Redis store keeps holds in a `floors` hash per topic, and its append
  script trims around them. `RedisStoreConfig` gains `floor_lease` and `floor_release_grace`.
  Lang sets `ReadRangeRequest.start_from_end` when the read asked for `$end`.
* Experimental: the stream service's `RetireProducer` call drops one producer attempt's dedupe
  state on a stream, which the Redis store keeps as one field of the topic's meta hash until
  retention. A producer that writes once per short-lived attempt calls it after the attempt's
  last append returned, so the meta hash tracks the attempts in flight. Calling it earlier lets a
  retried batch land twice.
* Experimental: a Workflow publishes stream records with `WorkflowOutputStreamCommit` in its
  activation completion. The Worker appends them to its `stream_store` before it reports the
  completion, so readers see them while the Workflow Task runs, and nothing goes to History. The
  producer is `@<run id>`. The attempt is the last completed task's WorkflowTaskStarted event id
  shifted left 16 bits, plus 1 and the task's failed or timed-out reruns since. Sequences start at
  1 per attempt. Replay appends nothing. A store that refuses the append fails the Workflow Task.
  Once the server accepts a task, Core retires the dedupe sessions of the run's earlier attempts.
  The C bridge's worker options take the process's stream store as `stream_store`.
* Experimental: a read reports `SUPERSEDED` for a Workflow producer (`@<run id>`) only when its
  attempt keeps the high bits (the same task) and raises the low 16 bits (a rerun). A higher
  prefix is the next task and reads with no supersession.
* Experimental: a read of a Workflow that Temporal doesn't know yet answers empty for
  `StreamStoreConfig.owner_start_grace` (60 seconds unless set, counted from the read's first
  owner check), so a reader can start before its owner. After the grace it fails as `NOT_FOUND`
  as before. Zero turns the grace off.
* Experimental: a Workflow Task append that the store refuses as closed drops that topic's
  records, logs a warning, counts `workflow_stream_output_dropped`, and the task goes on. A
  retry would be refused the same way, also when the run closed the topic itself before its task
  reran. Any other store failure still fails the task. Local Activities that a completion with
  stream output schedules start only once that output is in the store.
* Experimental: the stream protos name a standalone Activity owner. `StreamOwnerKind` gains
  `ACTIVITY`, `StreamAddress` and `DeleteOwnerRequest` gain `activity_id`, and `ChainId` gains
  `owner_kind` and `activity_id`. `ActivityProducer` gains `attempt_started_time`. The vendored
  API's `StreamReference` gains `activity_id` and its owner kind gains `ACTIVITY`. Core still
  refuses the Activity kind as `UNSUPPORTED`.
* Experimental: a standalone Activity owns streams. A `StreamAddress` of the `ACTIVITY` kind names
  the Activity id and, optionally, its run. One Activity run is a chain of one run, so every chain
  rule holds with the run as `first_run_id`. Core checks the owner with
  `DescribeActivityExecution`. Any terminal status ends the run's streams, and retries, backoff,
  pause and reset keep them open. A new run of the Activity id ends the earlier run's streams.
  Cursors hash under the kind `activity`. The Redis keys of an Activity run have the hash tag
  `{<namespace>:!a:<activity id>:<run id>}`, which no Workflow id makes. `DeleteOwner` takes the
  Activity kind. `OwnerClient::describe` takes an `OwnerId`, and `OwnerDescription.status` is an
  `OwnerStatus`. A paused Workflow run now counts as running for a read pinned to it.
* Experimental: notifier calls name an Activity-owned stream by its kind, its Activity id and its
  run. Notifications, closes, Workflow reader attaches, detaches and `WaitForNotification` polls
  all do, and a stream driver turns an Activity log key into that reference. Their cursors hash
  under the kind `activity`.
* Experimental: an `ActivityProducer` with `attempt_started_time` writes as the attempt
  `started_unix_ms << 16 | min(attempt, 65535)`. A retry and a reset attempt start later, so they
  supersede the earlier attempt, also after a reset sets the Temporal attempt back to 1. An
  attempt that writes on after a newer one started reads as stale. Without the field the attempt
  is the Temporal attempt as before. `testdata/streams/activity_attempts.json` in
  `temporalio-streams` pins the formula for other SDKs.
* Experimental: the Activity attempt key is `started_unix_ms << 11 | min(attempt, 2047)`, so it
  fits 53 bits and a JSON number holds it exactly. The Nexus contract's models and JavaScript
  refused the earlier 16-bit form. A start time from May 2109 on is refused. The vector file
  holds the new keys.

### Breaking Changes
* Replay detects changes to the service or operation of scheduled Nexus operations, reporting
  nondeterminism while retaining compatibility with histories recorded before these checks.

### Fixed
* Worker heartbeats now report correct task-slot and poller counts when using buffered or custom
  metrics backends, including the Python SDK's `MetricBuffer`. Previously these counts could
  remain `0` because heartbeat accounting depended on reading labels from backend-owned attributes.
* Local activities still running when their workflow is evicted or the worker shuts down now
  always receive a cancellation. Local activities no longer retry ordinary failures or
  start-to-close timeouts after cancellation has been requested, matching non-local activities.
  Workflow code no longer observes a spurious local activity cancellation when its run is evicted
  or completes while the local activity is backing off between retries.
* Workers no longer panic when evicting a workflow with buffered local activity results. Completed
  activations' output is recorded before eviction, without recording eviction-induced cancellations.

## [0.10.0] - 2026-10-06

### Added
* Child workflow commands, including those submitted through the C bridge, support pinned,
  auto-upgrade, and one-time worker deployment versioning overrides on Temporal Server 1.32.0
  or later. Child-start resolutions distinguish invalid versioning overrides and missing
  namespaces from other start failures.

### Fixed
* Task-poll targets no longer decrease after cancelled or timed-out polls. Affected pollers still
  retain their slot during backoff, while resource-exhaustion errors still reduce the target.
* Replay now preserves which local activity results were delivered together for newly recorded
  histories. This prevents local activity scheduling which was conditional on local activity
  resolution order from assigning recorded results to different handles. Older markers keep their
  existing behavior.
* Workflow poll balancing now lets non-sticky pollers use capacity after sticky pollers reach their
  configured or autoscaled polling limit.
* Every path that fails a workflow task now only reports the failure to server
  on the task's first attempt, and later attempts are left to time out. Previously `PayloadsTooLarge`
  failures and history fetch failures were re-reported on every attempt.
* The `workflow_task_execution_failed` metric is now recorded for every failed workflow task
  attempt, including attempts whose failure was not sent to the server, and its `failure_reason`
  tag distinguishes `GrpcMessageTooLarge`, `PayloadsTooLarge`, and `RequestTooLarge` on every path.

## [0.9.0] - 2026-09-04

## [0.8.0] - 2026-09-02

### Added
* External workflow signal and cancellation resolution activations now include the typed server
  failure cause alongside the existing failure.
* Language SDKs can opt in to recording local activity arguments in the local activity marker's
  `input` detail.
* Core console logs can now be emitted as newline-delimited JSON when an SDK selects the JSON log
  format. Configured log filters continue to apply to JSON output.
* Workflow completion-as-cancelled commands can now carry details for recording on the terminal
  history event.
* Worker heartbeats now report the SDK runtime, hosting environments, operating system, and
  architecture once per worker, retrying until the first successful delivery. Runtime options can
  disable the reporting.
* Workers now log a `[TMPRL1104]` warning when a workflow task takes longer than 5 seconds. Set
  `TEMPORAL_WORKFLOW_TASK_DURATION_WARN_SECONDS` to change the threshold.
* Core now supports attaching `EventGroupMarker`s to most workflow commands.
* The `temporal_activity_execution_failed` and `temporal_local_activity_execution_failed` worker
  metrics now carry a `failure_reason` attribute. Each is now split into one time series per
  reason, which may affect existing dashboards.
* Workflow task completions larger than the gRPC request size limit are now paginated automatically when the namespace supports it. Paginated workflow task completions require Temporal Server 1.32.0 or later.

### Breaking Changes :boom:
* The following types are now non-exhaustive: `Priority`, `WorkerDeploymentVersion`,
  `WorkerCallbacks`, `WorkflowExecutionInfo`, `ActivityCloseTimeouts`,
  `ActivityExecutionDecodeHint`, child-workflow and signal decode hints,
  `SerializationContext`, `SerializationContextData`, `PayloadConverter`, `IncomingError`,
  `ScheduleSpec`, and `ScheduleOverlapPolicy`. Construct structs using their respective builders
  or constructors (`WorkerCallbacks::new`, `ActivityExecutionDecodeHint::new`, or
  `SerializationContext::new`); use `Default` for `PayloadConverter`; and add wildcard branches
  when matching enums.
* Renamed `ActivityCloseTimeouts::Both` to `ActivityCloseTimeouts::ScheduleAndStartToClose`.
* Removed the unused `ActExitValue` type. Use `ActivityError::WillCompleteAsync` to mark an
  activity for asynchronous completion.
* Removed the test-only `FailOnNondeterminismInterceptor` from the public API.
* `TaskToken` no longer exposes its underlying bytes directly. Use `TaskToken::into_inner()` to
  consume a token into its bytes.
* Activity failures now include the latest heartbeat details atomically instead of force-flushing a
  throttled heartbeat first. Temporal Server 1.16.0 or newer is required to guarantee those details
  are preserved on failure; workers warn when the server does not advertise support.

### Fixed
* Worker shutdown now drains activity completions that are still flushing their result to the
  server before finishing. Previously such a completion — typically one whose final heartbeat RPC
  was still in flight — could be permanently stranded by shutdown: the activity's result was
  never reported (the server had to time the attempt out before retrying it), and workers missed
  shutdown's slot-permit release deadline, panicking in debug builds.
* The Prometheus exporter now appends `_total` to counter metric names when an SDK enables the
  counter suffix option.
* Update-with-start `ExecuteMultiOperation` calls now use Core's long-poll timeout instead of the
  normal RPC timeout, avoiding premature failures while waiting for an update to reach its
  requested stage.
* An activity failure caused by oversized final heartbeat details is now counted in the
  `temporal_activity_execution_failed` metric as `failure_reason="PayloadsTooLarge"`. Previously it
  was counted under the reason for the failure the activity itself reported, and was not counted at
  all when that failure was benign, even though a payload-limit failure was reported instead.
* Workers now warn when autoscaling task polling encounters errors continuously for one minute.
  Repeated warnings use exponential backoff up to 15-minute intervals and stop after polling
  recovers.
* Workers no longer send worker heartbeats or appear in centralized heartbeat reports before they
  begin polling.
* Ephemeral server processes no longer leak on failed start.
* Local activity resolutions are now delivered to workflows as each activity completes instead of
  waiting for every local activity in the workflow task. This allows sequences of short local
  activities to make progress while a long-running local activity executes in parallel, while
  preserving the resolution ordering recorded in existing histories during replay.
* Try-cancel child workflows no longer cause nondeterminism when they complete or fail after their
  cancellation was requested.
* Nexus tasks are now timed out locally even when the server sends a `request-timeout` header that
  falls outside the Nexus duration grammar, such as a negative value for a task whose deadline has
  already elapsed, a sub-millisecond unit, or a multi-unit value like `1m30s`. Previously such a
  header was ignored entirely, so the handler was never told the task had timed out, and a task
  left unanswered could block worker shutdown indefinitely.
* Workers with a small workflow cache no longer briefly stop accepting new workflows. Sticky
  workflow-task pollers could consume every workflow-cache permit and starve the non-sticky poller,
  so the worker would stop picking up new workflows until a poll timed out (up to ~60s). The poll
  balancer now reserves a non-sticky slot against the workflow cache size rather than the slot
  supplier size.
