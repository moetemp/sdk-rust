//! The byte form of the runtime's calls, for bridges. A bridge passes a method name and the
//! encoded request, and gets the encoded response, so it decodes no proto.

use crate::{
    Reason, StreamError, StreamResult, StreamRuntime,
    offset::offset_to_wire,
    proto::{
        CloseWriterRequest, CloseWriterResponse, CloseWritersRequest, CloseWritersResponse,
        FreeWriterRequest, FreeWriterResponse, OpenWriterRequest, OpenWriterResponse,
        WriteRecordsRequest, WriteRecordsResponse,
    },
};
use prost::Message;

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

impl StreamRuntime {
    /// Runs one method on encoded input and returns the encoded output. The bridge raises an error
    /// with [StreamError::code] and [StreamError::details].
    pub async fn call(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        let output = match method {
            "CreateStream" => self.create_stream(decode(input)?).await?.encode_to_vec(),
            "AppendRecords" => self.append_records(decode(input)?).await?.encode_to_vec(),
            "CloseStream" => self.close_stream(decode(input)?).await?.encode_to_vec(),
            "ReadRecords" => self.read_records(decode(input)?).await?.encode_to_vec(),
            "DescribeStream" => self.describe_stream(decode(input)?).await?.encode_to_vec(),
            "OpenWriter" => {
                let request = decode::<OpenWriterRequest>(input)?;
                let writer_id = self
                    .writers
                    .open(&self.store, &self.metrics, request)
                    .await?;
                OpenWriterResponse { writer_id }.encode_to_vec()
            }
            "WriteRecords" => {
                let request = decode::<WriteRecordsRequest>(input)?;
                let (first_offset, last_offset) = self
                    .writers
                    .write(request.writer_id, request.records)
                    .await?;
                WriteRecordsResponse {
                    first_offset: offset_to_wire(first_offset),
                    last_offset: offset_to_wire(last_offset),
                }
                .encode_to_vec()
            }
            "CloseWriters" => {
                decode::<CloseWritersRequest>(input)?;
                let failed = self.writers.close_all().await;
                CloseWritersResponse {
                    failed: failed as i32,
                }
                .encode_to_vec()
            }
            "CloseWriter" => {
                let request = decode::<CloseWriterRequest>(input)?;
                let last_offset = self.writers.close(request.writer_id).await?;
                CloseWriterResponse {
                    last_offset: offset_to_wire(last_offset),
                }
                .encode_to_vec()
            }
            "FreeWriter" => {
                let request = decode::<FreeWriterRequest>(input)?;
                self.free_writer(request.writer_id)?;
                FreeWriterResponse {}.encode_to_vec()
            }
            _ => {
                return Err(StreamError::new(
                    Reason::Unsupported,
                    format!("Unknown method {method}"),
                ));
            }
        };
        Ok(output)
    }

    /// Releases one reference to a writer with no wait, as the `FreeWriter` method does. A bridge
    /// calls it from a finalizer, which has no async runtime to await on.
    pub fn free_writer(&self, writer_id: u64) -> StreamResult<()> {
        self.writers.free(writer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStreamRuntime,
        ids::client_producer_id,
        proto::{
            AppendRecord, AppendRecordsRequest, AppendRecordsResponse, ClientEpoch, ClientProducer,
            CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
            DescribeStreamRequest, DescribeStreamResponse, MemoryStreamConfig, Producer,
            ReadRecordsRequest, ReadRecordsResponse, StreamRef, StreamStatus, producer::Kind,
        },
    };
    use std::sync::Arc;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    const METHODS: [&str; 10] = [
        "CreateStream",
        "AppendRecords",
        "CloseStream",
        "ReadRecords",
        "DescribeStream",
        "OpenWriter",
        "WriteRecords",
        "CloseWriters",
        "CloseWriter",
        "FreeWriter",
    ];

    fn runtime() -> Arc<StreamRuntime> {
        let config = MemoryStreamConfig {
            backend: "b".to_string(),
            ..Default::default()
        };
        MemoryStreamRuntime::from_config_bytes(&config.encode_to_vec(), None).unwrap()
    }

    fn stream_ref() -> Option<StreamRef> {
        Some(StreamRef {
            id: "s".to_string(),
            ..Default::default()
        })
    }

    fn client() -> Option<Producer> {
        Some(Producer {
            id: client_producer_id("me", "p"),
            kind: Some(Kind::Client(ClientProducer {
                epoch: Some(ClientEpoch { attempt: 1 }),
            })),
        })
    }

    fn records() -> Vec<AppendRecord> {
        vec![AppendRecord {
            body: Some(Payload::default()),
        }]
    }

    async fn call<Req: Message, Resp: Message + Default>(
        runtime: &StreamRuntime,
        method: &str,
        request: Req,
    ) -> Resp {
        let bytes = runtime
            .call(method, &request.encode_to_vec())
            .await
            .unwrap();
        Resp::decode(bytes.as_slice()).unwrap()
    }

    // A typo in a method name would make it unknown, which is unsupported, so the loop also checks
    // the names that a bridge calls.
    #[tokio::test]
    async fn a_bad_request_is_invalid_and_an_unknown_method_is_unsupported() {
        let runtime = runtime();
        for method in METHODS {
            let error = runtime.call(method, &[0xff]).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid, "{method}");
        }
        let error = runtime.call("Nope", &[]).await.unwrap_err();
        assert_eq!(
            (error.reason, error.message.as_str()),
            (Reason::Unsupported, "Unknown method Nope")
        );
    }

    #[tokio::test]
    async fn each_stream_method_round_trips_its_bytes() {
        let runtime = runtime();
        let created: CreateStreamResponse = call(
            &runtime,
            "CreateStream",
            CreateStreamRequest {
                stream_ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(created.r#ref.unwrap().backend, "b");
        let appended: AppendRecordsResponse = call(
            &runtime,
            "AppendRecords",
            AppendRecordsRequest {
                r#ref: stream_ref(),
                producer: client(),
                sequence: 1,
                records: records(),
                ..Default::default()
            },
        )
        .await;
        let read: ReadRecordsResponse = call(
            &runtime,
            "ReadRecords",
            ReadRecordsRequest {
                r#ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(read.last_offset, appended.last_offset);
        let _: CloseStreamResponse = call(
            &runtime,
            "CloseStream",
            CloseStreamRequest {
                r#ref: stream_ref(),
                producer: client(),
                ..Default::default()
            },
        )
        .await;
        let described: DescribeStreamResponse = call(
            &runtime,
            "DescribeStream",
            DescribeStreamRequest {
                r#ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(described.status, StreamStatus::Closed as i32);
    }

    #[tokio::test]
    async fn a_writer_round_trips_its_bytes() {
        let runtime = runtime();
        let _: CreateStreamResponse = call(
            &runtime,
            "CreateStream",
            CreateStreamRequest {
                stream_ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        let opened: OpenWriterResponse = call(
            &runtime,
            "OpenWriter",
            OpenWriterRequest {
                r#ref: stream_ref(),
                producer: client(),
                ..Default::default()
            },
        )
        .await;
        let written: WriteRecordsResponse = call(
            &runtime,
            "WriteRecords",
            WriteRecordsRequest {
                writer_id: opened.writer_id,
                records: records(),
            },
        )
        .await;
        let closed: CloseWriterResponse = call(
            &runtime,
            "CloseWriter",
            CloseWriterRequest {
                writer_id: opened.writer_id,
            },
        )
        .await;
        assert_eq!(closed.last_offset, written.last_offset);
    }
}
