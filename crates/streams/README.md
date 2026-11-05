# temporalio-streams

Experimental. The Stream runtime that the Temporal SDKs link into their bridges. A bridge builds
the runtime per client, as a plain object, and calls it with a method name and proto bytes. A
Workflow makes each Stream call from a Local Activity. Core has no Stream code.

## Protos and shared vectors

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.

- `temporal/api/stream/v1/message.proto`, `temporal/api/streamservice/v1/request_response.proto`
  and `temporal/api/enums/v1/stream.proto` are the Stream protos of the API. They are in
  `crates/protos/protos/api_upstream`.
- `crates/protos/protos/local/temporal/sdk/core/streams/streams.proto`, in the package
  `coresdk.streams`, has the crate's own calls, such as the writer calls.
- `testdata/*.json` are the shared vectors. They cover Stream ids, Producer ids, Store keys and
  meta fields, offsets, epoch keys, the withdrawal rule and the batch digest of a dedupe entry.
  `testdata/gen_vectors.py` writes them from a second implementation, and the crate's tests read
  each file. The Python SDK also reads `ids.json` and `withdrawals.json`. `digests.json` is the
  golden of the crate's batch digest.
