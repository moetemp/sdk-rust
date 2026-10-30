# temporalio-streams

Experimental. The Stream runtime that the Temporal SDKs link into their bridges. A bridge builds
the runtime per client, as a plain object, and calls it with a method name and proto bytes. A
Workflow makes each Stream call from a Local Activity. Core has no Stream code.

## Protos and shared vectors

- `protos/temporal/api/stream/v1/message.proto`,
  `protos/temporal/api/streamservice/v1/request_response.proto` and
  `protos/temporal/api/enums/v1/stream.proto` are the Stream protos of the API proposal, byte for
  byte. The API protos of this repo don't have them yet, so the crate generates them. Lang
  generates the same files. They import API protos that the build reads from the descriptor set
  of `temporalio-protos`, and that map to the types of `temporalio-common`. So the packaged crate
  needs no file outside itself.
- `protos/temporal/sdk/streams/v1/runtime.proto` has the crate's own calls, such as the writer
  calls.
- `testdata/*.json` are the shared vectors. They cover Stream ids, Producer ids, Store keys and
  meta fields, offsets, epoch keys, the withdrawal rule and the batch digest of a dedupe entry.
  `testdata/gen_vectors.py` writes them from a second implementation, and the crate's tests read
  each file. The Python SDK also reads `ids.json` and `withdrawals.json`, and the server reads
  `epochs.json` and `withdrawals.json`. Each Store reads `fields.json` through the shared Store
  suite. `digests.json` is the golden of the crate's batch digest.

## Stores and the shared suites

The memory Store is for tests. It keeps the key layout of the Redis Store and applies the same rules
in the same order. The `test-support` feature exports the shared suites in `testing`. A Store crate
implements `testing::TestBackend` for its Store and runs each suite with `testing::run`, so that
every Store keeps one set of rules. This crate runs them on the memory Store in its unit tests.

