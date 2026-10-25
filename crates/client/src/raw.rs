//! Unary gRPC calls by method path, with encoded protobuf messages. A crate that generates its own
//! messages can use them to call a method that this client doesn't list.

use crate::{Connection, grpc::RawGrpcCaller};
use bytes::{Buf, BufMut, Bytes};
use futures_util::{FutureExt, future::BoxFuture};
use http::uri::PathAndQuery;
use tonic::{
    Request, Response, Status,
    body::Body,
    client::{Grpc, GrpcService},
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
};

/// A unary call by method path on the connection's own gRPC service.
pub(crate) trait RawUnaryService: Send + Sync + dyn_clone::DynClone {
    fn unary(
        &self,
        path: PathAndQuery,
        request: Request<Bytes>,
    ) -> BoxFuture<'static, Result<Response<Bytes>, Status>>;
}
dyn_clone::clone_trait_object!(RawUnaryService);

#[derive(Clone)]
pub(crate) struct GrpcRawUnary<T> {
    pub(crate) grpc: Grpc<T>,
}

impl<T> RawUnaryService for GrpcRawUnary<T>
where
    T: GrpcService<Body> + Send + Sync + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    <T as GrpcService<Body>>::Future: Send,
{
    fn unary(
        &self,
        path: PathAndQuery,
        request: Request<Bytes>,
    ) -> BoxFuture<'static, Result<Response<Bytes>, Status>> {
        let mut grpc = self.grpc.clone();
        async move {
            grpc.ready()
                .await
                .map_err(|e| Status::unknown(format!("Service was not ready: {}", e.into())))?;
            grpc.unary(request, path, BytesCodec).await
        }
        .boxed()
    }
}

/// Passes the encoded message through as it is.
#[derive(Clone, Copy, Default)]
struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        *self
    }

    fn decoder(&mut self) -> Self::Decoder {
        *self
    }
}

impl Encoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

impl Connection {
    /// Makes a unary gRPC call to the method at `path`, such as
    /// `/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo`, with `request` as the
    /// encoded request message. Returns the encoded response message.
    ///
    /// The call uses the same stack as the listed methods, with the connection's TLS, API key,
    /// headers, interceptors, metrics and retry policy. The retry policy uses `path` as the call
    /// name. Request extensions and metadata work as they do on a listed method. Set the
    /// `temporal-namespace` metadata to route by namespace, as the listed methods do.
    pub async fn raw_unary(
        &self,
        path: &'static str,
        request: Request<Bytes>,
    ) -> Result<Response<Bytes>, Status> {
        let method = PathAndQuery::from_maybe_shared(Bytes::from_static(path.as_bytes()))
            .map_err(|e| Status::invalid_argument(format!("{path} is no gRPC method path: {e}")))?;
        let Some(raw) = self.inner.service.raw_unary.clone() else {
            return Err(Status::unimplemented(
                "this connection's service makes no calls by method path",
            ));
        };
        let fact = move |req: Request<Bytes>| raw.unary(method.clone(), req);
        self.clone().call(path, fact, request).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Connection, ConnectionOptions, RetryOptions,
        callback_based::{CallbackBasedGrpcService, GrpcRequest, GrpcSuccessResponse},
    };
    use bytes::Bytes;
    use parking_lot::Mutex;
    use std::{collections::HashMap, sync::Arc, time::Duration};
    use tonic::{Code, Request, Status};
    use url::Url;

    /// The requests that the server got, other than the `GetSystemInfo` call of the connect.
    type Seen = Arc<Mutex<Vec<GrpcRequest>>>;

    /// A connection to a server that answers each call with `answer`, other than the
    /// `GetSystemInfo` call of the connect.
    async fn connection(
        answer: impl Fn(&GrpcRequest, usize) -> Result<Vec<u8>, Status> + Send + Sync + 'static,
    ) -> (Connection, Seen) {
        let seen: Seen = Default::default();
        let recorded = seen.clone();
        let answer = Arc::new(answer);
        let service = CallbackBasedGrpcService {
            callback: Arc::new(move |request| {
                let (recorded, answer) = (recorded.clone(), answer.clone());
                Box::pin(async move {
                    if request.rpc == "GetSystemInfo" {
                        return Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto: vec![],
                        });
                    }
                    let attempt = recorded.lock().len();
                    let answered = answer(&request, attempt);
                    recorded.lock().push(request);
                    answered.map(|proto| GrpcSuccessResponse {
                        headers: Default::default(),
                        proto,
                    })
                })
            }),
        };
        let options = ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
            .service_override(service)
            .dns_load_balancing(None)
            .headers(HashMap::from([(
                "my-header".to_string(),
                "my-value".to_string(),
            )]))
            .retry_options(RetryOptions {
                initial_interval: Duration::from_millis(1),
                max_interval: Duration::from_millis(1),
                ..Default::default()
            })
            .build();
        (Connection::connect(options).await.unwrap(), seen)
    }

    #[tokio::test]
    async fn a_method_the_client_does_not_list_is_called_by_its_path() {
        let (connection, seen) = connection(|_, _| Ok(b"answer".to_vec())).await;
        let mut request = Request::new(Bytes::from_static(b"question"));
        request
            .metadata_mut()
            .insert("temporal-namespace", "ns".parse().unwrap());
        let response = connection
            .raw_unary("/example.v1.ExampleService/NotListed", request)
            .await
            .unwrap();
        assert_eq!(response.into_inner(), Bytes::from_static(b"answer"));
        let seen = seen.lock();
        let [call] = seen.as_slice() else {
            panic!("one call, not {seen:?}");
        };
        assert_eq!(
            (call.service.as_str(), call.rpc.as_str()),
            ("example.v1.ExampleService", "NotListed")
        );
        assert_eq!(call.proto, Bytes::from_static(b"question"));
        assert_eq!(call.headers["my-header"], "my-value");
        assert_eq!(call.headers["temporal-namespace"], "ns");
    }

    #[tokio::test]
    async fn a_call_is_retried_by_the_connections_policy() {
        let (connection, seen) = connection(|_, attempt| {
            if attempt < 2 {
                Err(Status::unavailable("restarting"))
            } else {
                Ok(vec![])
            }
        })
        .await;
        connection
            .raw_unary(
                "/example.v1.ExampleService/Flaky",
                Request::new(Bytes::new()),
            )
            .await
            .unwrap();
        assert_eq!(seen.lock().len(), 3);
    }

    #[tokio::test]
    async fn a_refusal_comes_back_as_it_is() {
        let (connection, seen) =
            connection(|_, _| Err(Status::invalid_argument("no such thing"))).await;
        let status = connection
            .raw_unary(
                "/example.v1.ExampleService/Refused",
                Request::new(Bytes::new()),
            )
            .await
            .unwrap_err();
        assert_eq!(
            (status.code(), status.message()),
            (Code::InvalidArgument, "no such thing")
        );
        assert_eq!(seen.lock().len(), 1, "a refusal is not retried");
    }

    #[tokio::test]
    async fn a_path_that_is_no_method_path_is_refused_before_any_call() {
        let (connection, seen) = connection(|_, _| Ok(vec![])).await;
        let status = connection
            .raw_unary("no path at all", Request::new(Bytes::new()))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(seen.lock().is_empty());
    }
}
