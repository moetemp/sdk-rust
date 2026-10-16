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
* Experimental: `StreamStoreConfig.archive` keeps streams in object storage after the store drops
  them, so they stay readable for longer than the store's retention. `url` is
  `s3://<bucket>/<prefix>` or `file:///<directory>`, and S3 settings come from the `AWS_*`
  variables. The store holds records the archive doesn't hold yet for `stall_limit` (seven days).
  The `RunStreamDriver` call writes the archive: it takes a stream's records in segments of about
  `segment_bytes` (4 MiB), once the stream is closed or idle for `segment_idle` (five minutes), and
  at least that often while it keeps taking writes. Reads move from the archive to the store with
  the same cursors. An archive needs `notification_log`. The C bridge builds the S3 archive.
* Experimental: `StreamStoreConfig.postgres` keeps streams in a PostgreSQL database, 14 or later,
  in one set of tables in `schema` (`temporal_streams`). The store applies its migrations at
  connect unless `skip_migrations` is set. A janitor trims records past `retention` (seven days)
  every `janitor_interval` (one minute). Waiting reads hold no connection and wake within
  `poll_interval` (100 ms). TLS follows the URL's `sslmode` and `sslrootcert`. The C bridge builds
  the store.
* Experimental: the PostgreSQL store keeps a notification log with `notification_log`, a
  `notifications` table with 64 partitions that the append writes in its transaction. Drivers take
  entries with `FOR UPDATE SKIP LOCKED`, and a `NOTIFY` doorbell rings when a partition takes its
  first entry. `listen_url` names a direct connection for the listener and the migrations when
  `url` goes through PgBouncer in transaction mode. `notify_reads` sends a `NOTIFY` per append, so
  waiting reads in other processes wake at once.
* Experimental: `StreamStoreConfig.archive` works with the PostgreSQL store. A topic's first record
  makes the archive's hold as a `floors` row, and the janitor drops only what the archive holds
  while the driver keeps up. The archive is off unless the config sets it.
* Experimental: the Redis append no longer costs more the longer a topic's retained log is. Its
  trim finds the newest record to drop with `XRANGE` forward from the oldest, where the SDKs'
  script read the whole log with `XREVRANGE`. At 100,000 retained records an append took about
  4 ms before and takes about the same as on a short log now. The append script's hashes are now
  `188afac5`, `eeed648a` with the notification log, and `337494c0` with the archive.
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
* Experimental: progress of a started Nexus operation reaches lang as a
  `ResolveNexusOperationProgress` job, at most one per operation per workflow task, with the
  highest counter the server folded onto the task's scheduled event. Replay delivers the same jobs
  in the same activations. Progress for an operation that is unknown, not started, already
  resolved, or below a counter already delivered is dropped. A task scheduled with progress ends
  a workflow task heartbeat chain, as one with stream wakes does.
* Experimental: the stream calls `AttachCaller` and `DetachCaller` serve a stream-returning Nexus
  operation. `AttachCaller` resolves the stream's run chain, mints the operation token and
  attaches the start's callback to the stream's notifier with the token and start time.
  `DetachCaller` decodes the token and detaches from the chain the start attached to. The token
  format is in `crates/streams/nexus/operation_token_vectors.json`, which every SDK's encoder and
  decoder check. Both answer a failure in the response as a `NexusFailure`, from the same table as
  the Nexus calls, so lang's handler raises it as it is. A mistake in the stream, an empty callback
  URL or request id, and a token that names no stream are `BAD_REQUEST`. An attach for the latest
  run describes the Workflow fresh, so it never lands on a chain that already ended.
* Experimental: `AttachCaller` attaches the caller's callback as an idle one, which hears no
  progress until a wait after a counter and still gets the completion. A reader reads first, so a
  busy one costs no deliveries. `NotifierClient::wait_callback` sends the new
  `WaitStreamCallback`, and the C bridge takes it as a raw call. The API protos carry the
  `NexusHandler` progress fields and `nexus/notification-service.wit`.
* Experimental: the Nexus `read` operation takes `wake_token`, the reader's stream operation
  token. When a read answers no records and not done, Core asks the notifier to wake that
  operation's idle callback once the stream moves past the answer, then reads once more and
  answers what that finds. A reader that finds records on each call never waits. Reads through a
  transport, positions-only ones too, take the same path.
* Experimental: `AttachCallerRequest` takes `wake_after_counter`. With it Core asks the notifier
  for the idle callback's first wake right after the attach, once the stream holds a record above
  that counter. A follower that reads only when woken sets it. A reader that reads first leaves it
  unset and waits through its reads' `wake_token`.
* Experimental: after the first wait, Core notifies the stream's newest record when it is above
  `wake_after_counter`. A standalone start runs its handler after the caller has the answer, so a
  record can land before the attach, and the server drops its notification while no notifier
  exists.
* Experimental: the Nexus `read` takes `floor_holder`. With it the read holds the floor and
  answers `replay`, the range of the records that crossed after the byte cut, with Core's digest
  over them. The new Nexus `read_range` operation, `NexusReadRange` for lang, reads that range
  again through the codec step and checks the digest. Its answer has no byte budget and carries
  only the records and `next_token`. It checks that the chain it names is one of the Workflow's.
* Experimental: the API protos carry `WorkflowService.ExecuteSyncNexusOperation`, which runs one
  synchronous Nexus operation for a caller namespace and persists nothing. The client and the C
  bridge call it, and its `input` counts against the blob size limit.
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
* Experimental: the stream service contract's `StreamRef` gains the optional member
  `activity_id`, and `kind` takes `"activity"`. An Activity's ref sends `workflow_id` as `""`,
  and its `chain_run_id` is the Activity run. A stream operation token carries the kind and the
  Activity id, keeps version 2, and leaves `activity_id` out of a Workflow's ref, so a Workflow's
  token keeps its bytes. `operation_token_vectors.json` has Activity tokens. `CodecWork` gains
  `owner_kind` and `activity_id`. The Nexus calls still refuse an Activity ref.
* Experimental: the stream service's Nexus calls serve streams that a standalone Activity owns.
  `append`, `read`, `read_range`, `latest_only`, the floor holder, `AttachCaller` with
  `wake_after_counter`, `DetachCaller` and the read-first wake all take an Activity ref. A read's
  `wake_token` must name the read's owner kind, Workflow id, Activity id and topic, so a token of
  one Activity never wakes another Activity's reader on the same topic. `CodecWork` names the
  Activity, so lang's codec context is the namespace and the Activity id. A ref of kind
  `activity` with a Workflow id, or of kind `workflow` with an Activity id, is a bad request.
* Experimental: the stream service contract and its Nexus calls serve Workflow Activity streams.
  `StreamRef.kind` takes `"workflow_activity"`, with `workflow_id`, `activity_id` and the
  scheduling run as `chain_run_id`, all required, and no new member. A stream operation token
  carries the kind and the Activity id, keeps version 2, and its chain is the scheduling run.
  Reads and replays keep the run, since it names the stream. A carried state must name that run.
  A read's `wake_token` must also name the read's run for this kind. `CodecWork` names the
  Workflow and the Activity. `operation_token_vectors.json` has Workflow Activity tokens.
* Experimental: the stream service contract gains the `follow` and `follow_range` operations,
  served in Core by `Follow` and `FollowRange` behind `NexusFollow` and `NexusFollowRange`. A
  follow answers `FollowedWire` items, each a `RecordWire` or a `LinkStateWire` with its source,
  the sources it names, and the composite state (at most 64 KiB). Each owner's bodies cross
  decoded with that owner's codec context, so a follow may ask lang for codec work once per
  owner. A follow with a `floor_holder` answers `replay`, opaque ranges and a digest that
  `follow_range` reads again and checks. Links to another namespace or endpoint are skipped. A
  `wake_token` wakes the caller's operation on the root's notifier.
* Experimental: a `follow` with a `wake_token` that finds nothing new while a linked stream is
  open waits on every open stream inside the call, for up to half its deadline, in place of the
  root's wake. Only a follow with no open linked stream sends the root's `WaitStreamCallback`.
* Experimental: the Activity attempt key is `started_unix_ms << 11 | min(attempt, 2047)`, so it
  fits 53 bits and a JSON number holds it exactly. The Nexus contract's models and JavaScript
  refused the earlier 16-bit form. A start time from May 2109 on is refused. The vector file
  holds the new keys.
* Experimental: the stream protos name a Workflow Activity owner. `StreamOwnerKind` gains
  `WORKFLOW_ACTIVITY`, an Activity that a Workflow run scheduled. Its `StreamAddress` and
  `ChainId` set `workflow_id`, the scheduling run and `activity_id`, with no new field. The
  vendored API's owner kind gains `WORKFLOW_ACTIVITY`. Core still refuses the kind as
  `UNSUPPORTED`.
* Experimental: a Workflow Activity owns streams. Its `StreamAddress` names the Workflow, the
  scheduling run and the Activity id, all required. It is a chain of one, keyed by that run, so
  Activity `1` of the next run after Continue-as-New is another stream. Core describes the run
  and looks up the Activity in `pending_activities`. A closed run ends reads at once. A running
  run that no longer lists the Activity ends reads only after the owner start grace from the
  first check that found it so, and refuses producers at once. Cursors hash under the kind
  `workflow_activity` with the run in the owner id. The Redis keys have the hash tag
  `{<namespace>:!wa:<workflow id>:<run id>:<activity id>}`, and the append script (hashes
  `eec20388`, and `418b9d10` with the notification log) adds each log key to the chain's
  `topics` set on a producer attempt's first batch. `StreamStore::written_topics` answers that
  set, and every store and wrapper must implement it. A Workflow's `DeleteOwner` also deletes
  its Activities' streams. `OwnerId` gains `WorkflowActivity` and `OwnerStatus` gains
  `ActivityGone`. The documented Redis ACL gains `+sadd +smembers`.
* Experimental: the scheduling Workflow's Core closes a Workflow Activity's streams when it
  resolves the Activity. Workflow code marks the Activity with the `ScheduleActivity` header
  `__temporal_stream_owner`. When a marked Activity resolves outside replay, Core closes each
  topic in the Activity's topic set in the background, once the activation completed and its
  stream output was appended. A completed Activity closes with a null result, and a failed,
  timed-out or canceled one with the failure its resolution holds, cut to `MAX_CLOSE_BYTES`. The
  close names `<activity id>@<run id>` as `closed_by` and reaches the stream's notifier. A close
  that fails is logged and does not fail the Workflow Task. Unmarked Activities cost no store
  call. Notifier calls name a Workflow Activity stream by its Workflow, scheduling run and
  Activity id.
* Experimental: streams store link records. `StreamRecordKind` gains `LINK = 4`, and
  `StreamRecord.link` (field 8) holds a new `StreamLink`: an `OPEN` or `JOIN` event, the linked
  `temporal.api.stream.v1.StreamReference`, a name of at most 256 bytes, the `after` and `until`
  cursors, a namespace and an endpoint. The link stays out of the payload codec. An optional
  body goes through it and needs its content hash, as on `DATA`. `AppendRecord.link` (field 4)
  and `OutputRecord.link` (field 5) carry it on append and in a Workflow's commit. Core refuses a
  link with no name, no event, an `OPEN` with no stream or with ids that don't fit its owner
  kind, a `JOIN` with a stream, or a link on another kind. `workflow_batch_digest` takes each
  record's link, and both it and the read digest add the link's fields with length prefixes, so
  a replay with another link fails as `REPLAY_MISMATCH`. A record with no link keeps its digest.
  `temporalio_common::streams::check_link` holds the rules. A reader before this release refuses
  a link as a record of an unknown kind.
* Experimental: `StreamService` gains `Follow` and `FollowRange`. `Follow` reads a root stream
  and the streams its links open, in one order with no clock. Order within each stream is kept.
  A linked stream's records come after its OPEN, and a JOIN holds the linking stream until the
  linked stream reaches the JOIN's `until` or ends. Each item names its source, and a linked
  stream's end, failure or skip comes as a `LinkState` (`OPENED`, `DONE` with the close outcome,
  `UNAVAILABLE` with the error, `SKIPPED` with `depth`, `limit`, `cycle`, `open` or
  `endpoint`). Only a `STORAGE` failure fails the call. The answer's `state` is the composite
  position and resumes every open source. `max_depth` (4) and `max_open` (16) bound a follow. A
  Workflow follower sets `subscriber`, so an empty answer attaches each idle source and lists
  its wake. `FollowRange` reads an earlier answer's ranges again with `ReadRange` and checks the
  `follow_digest`. The state machine is `links.rs`, with no I/O.

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
