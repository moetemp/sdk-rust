# temporalio-streams

Experimental. The Stream runtime that the Temporal SDKs link into their bridges. A bridge builds
the runtime per client, as a plain object, and calls its typed methods, or its byte form with a
method name and proto bytes. A Workflow makes each Stream call from a Local Activity. Core has no
Stream code.

## Protos and shared vectors

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.

- `temporal/api/stream/v1/message.proto`, `temporal/api/streamservice/v1/request_response.proto`
  and `temporal/api/enums/v1/stream.proto` are the Stream protos of the API. They are in
  `crates/protos/protos/api_upstream`.
- `crates/protos/protos/local/temporal/sdk/core/streams/streams.proto`, in the package
  `coresdk.streams`, has the crate's own calls, such as the writer calls.
- `testdata/*.json` are the shared vectors. They cover Stream ids, Producer ids, Store keys and
  meta fields, offsets, epoch keys and the batch digest of a dedupe entry.
  `testdata/gen_vectors.py` writes them from a second implementation, and the crate's tests read
  each file. The Python SDK also reads `ids.json`. Each Store reads `fields.json` through the
  shared Store suite. `digests.json` is the golden of the crate's batch digest.

## The runtime

`StreamRuntime` serves the Stream service on one Store, with one typed method for each operation.
`StreamRuntime::call` is the byte form of the same methods, for a bridge that decodes no proto.
`StreamRuntime::open_writer` gives a `StreamWriter`, which the bridge holds, and dropping it
releases the writer. A Store crate builds the runtime with `StreamRuntime::new` and its Store. A
`store::LazyStore` connects its inner Store on the first call that needs it. So a bridge can build
the runtime with no async runtime, and a Workflow that never publishes never connects.
`MemoryStreamRuntime` builds it on the memory Store, from a `MemoryStreamConfig`. That message has
the field numbers of the same fields in the Redis config, so a bridge can send the memory runtime
the config it builds for Redis.

## Stores and the shared suites

The memory Store is for tests. It keeps the key layout of the Redis Store and applies the same rules
in the same order. The `test-support` feature exports the shared suites in `testing`. A Store crate
implements `testing::TestBackend` for its Store and runs each suite with `testing::run`, so that
every Store keeps one set of rules. This crate runs them on the memory Store in its unit tests.

