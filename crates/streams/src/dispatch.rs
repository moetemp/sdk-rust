//! The byte form of the runtime's and the writer's calls, for bridges. A bridge passes the encoded
//! request and gets the encoded response, so it decodes no proto.

use crate::{
    Reason, StreamError, StreamResult, StreamRuntime, StreamWriter, proto::WriteRecordsRequest,
};
use prost::Message;

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

impl StreamRuntime {
    /// Runs one of the five Stream service operations on encoded input, by its name, and returns
    /// the encoded output. The bridge raises an error with [StreamError::code] and
    /// [StreamError::details].
    pub async fn call(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        let output = match method {
            "CreateStream" => self.create_stream(decode(input)?).await?.encode_to_vec(),
            "AppendRecords" => self.append_records(decode(input)?).await?.encode_to_vec(),
            "CloseStream" => self.close_stream(decode(input)?).await?.encode_to_vec(),
            "ReadRecords" => self.read_records(decode(input)?).await?.encode_to_vec(),
            "DescribeStream" => self.describe_stream(decode(input)?).await?.encode_to_vec(),
            _ => {
                return Err(StreamError::new(
                    Reason::Unsupported,
                    format!("Unknown method {method}"),
                ));
            }
        };
        Ok(output)
    }

    /// Opens a writer from an encoded `OpenWriterRequest`.
    pub async fn open_writer_bytes(&self, input: &[u8]) -> StreamResult<StreamWriter> {
        self.open_writer(decode(input)?).await
    }
}

impl StreamWriter {
    /// Writes an encoded `WriteRecordsRequest`, and returns the encoded `WriteRecordsResponse`.
    pub async fn write_bytes(&self, input: &[u8]) -> StreamResult<Vec<u8>> {
        let request = decode::<WriteRecordsRequest>(input)?;
        Ok(self.write(request).await?.encode_to_vec())
    }

    /// Closes the writer, and returns the encoded `CloseWriterResponse`.
    pub async fn close_bytes(&self) -> StreamResult<Vec<u8>> {
        Ok(self.close().await?.encode_to_vec())
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
            CloseStreamRequest, CloseStreamResponse, CloseWriterResponse, CreateStreamRequest,
            CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
            MemoryStreamConfig, OpenWriterRequest, Producer, ReadRecordsRequest,
            ReadRecordsResponse, StreamRef, StreamStatus, WriteRecordsResponse, producer::Kind,
        },
    };
    use std::sync::Arc;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    const METHODS: [&str; 5] = [
        "CreateStream",
        "AppendRecords",
        "CloseStream",
        "ReadRecords",
        "DescribeStream",
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

    async fn create(runtime: &StreamRuntime) -> CreateStreamResponse {
        let request = CreateStreamRequest {
            stream_ref: stream_ref(),
            ..Default::default()
        };
        call(runtime, "CreateStream", request).await
    }

    // A typo in a method name would make it unknown, which is unsupported, so the loop also checks
    // the names that a bridge calls.
    #[tokio::test]
    async fn bad_bytes_are_invalid_and_an_unknown_method_is_unsupported() {
        let runtime = runtime();
        for method in METHODS {
            let error = runtime.call(method, &[0xff]).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid, "{method}");
        }
        let error = runtime.open_writer_bytes(&[0xff]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        create(&runtime).await;
        let request = OpenWriterRequest {
            r#ref: stream_ref(),
            producer: client(),
            ..Default::default()
        };
        let writer = runtime
            .open_writer_bytes(&request.encode_to_vec())
            .await
            .unwrap();
        let error = writer.write_bytes(&[0xff]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        let error = runtime.call("Nope", &[]).await.unwrap_err();
        assert_eq!(
            (error.reason, error.message.as_str()),
            (Reason::Unsupported, "Unknown method Nope")
        );
    }

    #[tokio::test]
    async fn each_stream_method_round_trips_its_bytes() {
        let runtime = runtime();
        assert_eq!(create(&runtime).await.r#ref.unwrap().backend, "b");
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
        create(&runtime).await;
        let request = OpenWriterRequest {
            r#ref: stream_ref(),
            producer: client(),
            ..Default::default()
        };
        let writer = runtime
            .open_writer_bytes(&request.encode_to_vec())
            .await
            .unwrap();
        let write = WriteRecordsRequest { records: records() };
        let written = writer.write_bytes(&write.encode_to_vec()).await.unwrap();
        let written = WriteRecordsResponse::decode(written.as_slice()).unwrap();
        let closed = writer.close_bytes().await.unwrap();
        let closed = CloseWriterResponse::decode(closed.as_slice()).unwrap();
        assert_eq!(closed.last_offset, written.last_offset);
    }
}
