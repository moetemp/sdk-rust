//! The Stream runtime of `temporalio-streams`, for the SDKs over this bridge. The functions exist
//! in every build, so the header doesn't change with the features.

use crate::{ByteArray, ByteArrayRef, CancellationToken, UserDataHandle, runtime::Runtime};

/// The Stream runtime of one backend. Free it with `temporal_core_stream_runtime_free`.
pub struct StreamRuntime {
    #[cfg(feature = "streams")]
    runtime: Runtime,
    #[cfg(feature = "streams")]
    streams: std::sync::Arc<temporalio_streams::StreamRuntime>,
}

/// If `fail` is set, `runtime` is null, and the caller frees `fail`.
#[repr(C)]
pub struct StreamRuntimeOrFail {
    pub runtime: *mut StreamRuntime,
    pub fail: *const ByteArray,
}

/// On failure, `status_code` is the gRPC code, and `failure_details` is the encoded
/// `StreamErrorDetails`, which a cancel (code 1) doesn't set. The caller frees each array.
#[repr(C)]
pub struct StreamCallResult {
    pub success: *const ByteArray,
    pub status_code: u32,
    pub failure_message: *const ByteArray,
    pub failure_details: *const ByteArray,
}

/// The callback of an asynchronous call, with the fields of `StreamCallResult`. It runs on a Tokio
/// thread, or at once in a build with no `streams` feature.
pub type StreamCallCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *const ByteArray,
    status_code: u32,
    failure_message: *const ByteArray,
    failure_details: *const ByteArray,
);

#[cfg(not(feature = "streams"))]
const NO_STREAMS: &str = "This build has no Stream runtime";

/// The bytes of `bytes`. An empty array can come with a null pointer, such as from .NET's `fixed`
/// on an empty array, and a slice of a null pointer is undefined behavior.
#[cfg(feature = "streams")]
fn slice(bytes: &ByteArrayRef) -> &[u8] {
    if bytes.size == 0 {
        &[]
    } else {
        bytes.to_slice()
    }
}

#[cfg(feature = "streams")]
fn text(bytes: &ByteArrayRef) -> &str {
    if bytes.size == 0 { "" } else { bytes.to_str() }
}

fn failed(status_code: u32, message: String, details: Option<Vec<u8>>) -> StreamCallResult {
    StreamCallResult {
        success: std::ptr::null(),
        status_code,
        failure_message: ByteArray::from_utf8(message).into_raw(),
        failure_details: details.map_or(std::ptr::null(), |details| {
            ByteArray::from_vec(details).into_raw()
        }),
    }
}

#[cfg(feature = "streams")]
fn result(output: temporalio_streams::StreamResult<Vec<u8>>) -> StreamCallResult {
    match output {
        Ok(bytes) => StreamCallResult {
            success: ByteArray::from_vec(bytes).into_raw(),
            status_code: 0,
            failure_message: std::ptr::null(),
            failure_details: std::ptr::null(),
        },
        Err(error) => failed(
            error.code() as u32,
            error.message.clone(),
            Some(error.details()),
        ),
    }
}

/// The answer of a call's task, or code 1 when a cancel stops it first. A call that ended before
/// the cancel took effect gives its result, since its effects happened.
#[cfg(feature = "streams")]
async fn answer(
    mut call: tokio::task::JoinHandle<temporalio_streams::StreamResult<Vec<u8>>>,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> StreamCallResult {
    let Some(cancel) = cancel else {
        return joined(call.await);
    };
    tokio::select! {
        biased;
        output = &mut call => return joined(output),
        _ = cancel.cancelled() => {}
    }
    call.abort();
    match call.await {
        Err(error) if error.is_cancelled() => failed(1, "Cancelled".to_string(), None),
        output => joined(output),
    }
}

/// The result of a call's task, with a panic as `INTERNAL`.
#[cfg(feature = "streams")]
fn joined(
    output: Result<temporalio_streams::StreamResult<Vec<u8>>, tokio::task::JoinError>,
) -> StreamCallResult {
    match output {
        Ok(output) => result(output),
        Err(error) => panicked(error.to_string()),
    }
}

/// A call that panicked, as `INTERNAL`, since a panic can't cross the C ABI.
#[cfg(feature = "streams")]
fn panicked(message: String) -> StreamCallResult {
    let message = format!("The Stream call panicked: {message}");
    failed(tonic::Code::Internal as u32, message, None)
}

/// Runs a call on the caller's thread, with a panic as `INTERNAL`.
#[cfg(feature = "streams")]
fn guarded(call: impl FnOnce() -> temporalio_streams::StreamResult<Vec<u8>>) -> StreamCallResult {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(call)) {
        Ok(output) => result(output),
        Err(panic) => panicked(
            (panic.downcast_ref::<&str>().map(|text| text.to_string()))
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default(),
        ),
    }
}

#[cfg(feature = "streams")]
fn build(
    runtime: &Runtime,
    kind: &str,
    config: &[u8],
) -> Result<std::sync::Arc<temporalio_streams::StreamRuntime>, String> {
    let meter = runtime.core.telemetry().get_temporal_metric_meter();
    // The Stream runtime keeps the Tokio runtime current at its build, for the writes of a buffer.
    let _entered = runtime.core.tokio_handle().enter();
    match kind {
        "memory" => temporalio_streams::MemoryStreamRuntime::from_config_bytes(config, meter)
            .map_err(|error| error.to_string()),
        #[cfg(feature = "streams-redis")]
        "redis" => temporalio_streams_redis::RedisStreamRuntime::from_config_bytes(config, meter)
            .map_err(|error| error.to_string()),
        other => Err(format!("This build has no {other} Stream runtime")),
    }
}

/// Builds the Stream runtime of `kind`, `memory` or `redis`, from its encoded config. A build with
/// no `streams` feature has no kind, and one with no `streams-redis` has no `redis`.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_runtime_new(
    runtime: *mut Runtime,
    kind: ByteArrayRef,
    config: ByteArrayRef,
) -> StreamRuntimeOrFail {
    #[cfg(feature = "streams")]
    let built = {
        let runtime = unsafe { &*runtime };
        build(runtime, text(&kind), slice(&config)).map(|streams| StreamRuntime {
            runtime: runtime.clone(),
            streams,
        })
    };
    #[cfg(not(feature = "streams"))]
    let built: Result<StreamRuntime, String> = {
        let _ = (runtime, kind, config);
        Err(NO_STREAMS.to_string())
    };
    match built {
        Ok(streams) => StreamRuntimeOrFail {
            runtime: Box::into_raw(Box::new(streams)),
            fail: std::ptr::null(),
        },
        Err(message) => StreamRuntimeOrFail {
            runtime: std::ptr::null_mut(),
            fail: ByteArray::from_utf8(message).into_raw(),
        },
    }
}

/// The Stream runtime holds the runtime, so either can be freed first. Free neither from inside
/// a callback, since a Tokio thread can't drop the last Core runtime.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_runtime_free(runtime: *mut StreamRuntime) {
    if !runtime.is_null() {
        unsafe {
            let _ = Box::from_raw(runtime);
        }
    }
}

/// Runs a method on Core's Tokio runtime and calls `callback` with the result. The call copies the
/// method and the input, so only the user data must live until the callback.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_call(
    runtime: *mut StreamRuntime,
    method: ByteArrayRef,
    input: ByteArrayRef,
    cancel: *const CancellationToken,
    user_data: *mut libc::c_void,
    callback: StreamCallCallback,
) {
    let user_data = UserDataHandle(user_data);
    #[cfg(feature = "streams")]
    {
        let runtime = unsafe { &*runtime };
        let streams = runtime.streams.clone();
        let (method, input) = (text(&method).to_string(), slice(&input).to_vec());
        let cancel = unsafe { cancel.as_ref() }.map(|cancel| cancel.token.clone());
        let handle = runtime.runtime.core.tokio_handle();
        handle.clone().spawn(async move {
            // The call runs on a task of its own, so a panic in it still answers the callback.
            let call = handle.spawn(async move { streams.call(&method, &input).await });
            let done = answer(call, cancel).await;
            unsafe {
                callback(
                    user_data.into(),
                    done.success,
                    done.status_code,
                    done.failure_message,
                    done.failure_details,
                );
            }
        });
    }
    #[cfg(not(feature = "streams"))]
    {
        let _ = (runtime, method, input, cancel);
        let done = failed(tonic::Code::Unimplemented as u32, NO_STREAMS.into(), None);
        unsafe {
            callback(
                user_data.into(),
                done.success,
                done.status_code,
                done.failure_message,
                done.failure_details,
            );
        }
    }
}

/// Runs a method that never waits, such as a `Free` method, on the caller's thread.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_call_sync(
    runtime: *mut StreamRuntime,
    method: ByteArrayRef,
    input: ByteArrayRef,
) -> StreamCallResult {
    #[cfg(feature = "streams")]
    {
        let runtime = unsafe { &*runtime };
        guarded(|| runtime.streams.call_sync(text(&method), slice(&input)))
    }
    #[cfg(not(feature = "streams"))]
    {
        let _ = (runtime, method, input);
        failed(tonic::Code::Unimplemented as u32, NO_STREAMS.into(), None)
    }
}

/// Runs a static method, such as `DeriveProducer`, which needs no Stream runtime.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_call_static(
    method: ByteArrayRef,
    input: ByteArrayRef,
) -> StreamCallResult {
    #[cfg(feature = "streams")]
    {
        guarded(|| temporalio_streams::call_static(text(&method), slice(&input)))
    }
    #[cfg(not(feature = "streams"))]
    {
        let _ = (method, input);
        failed(tonic::Code::Unimplemented as u32, NO_STREAMS.into(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        RuntimeInfoArray, RuntimeOptions, temporal_core_byte_array_free,
        temporal_core_runtime_free, temporal_core_runtime_new,
    };

    fn runtime() -> *mut Runtime {
        let built = temporal_core_runtime_new(&RuntimeOptions {
            telemetry: std::ptr::null(),
            worker_heartbeat_interval_millis: 0,
            runtime_info: RuntimeInfoArray {
                data: std::ptr::null(),
                size: 0,
            },
            disable_environment_info: false,
        });
        assert!(built.fail.is_null());
        built.runtime
    }

    /// The fields of a result, with each array copied and freed.
    #[derive(Debug)]
    struct Outcome {
        success: Option<Vec<u8>>,
        status_code: u32,
        message: Option<String>,
        details: Option<Vec<u8>>,
    }

    fn take(runtime: *mut Runtime, bytes: *const ByteArray) -> Option<Vec<u8>> {
        if bytes.is_null() {
            return None;
        }
        let copy = unsafe { (*bytes).as_ref() }.to_vec();
        temporal_core_byte_array_free(runtime, bytes);
        Some(copy)
    }

    fn outcome(runtime: *mut Runtime, result: StreamCallResult) -> Outcome {
        Outcome {
            success: take(runtime, result.success),
            status_code: result.status_code,
            message: take(runtime, result.failure_message)
                .map(|bytes| String::from_utf8(bytes).unwrap()),
            details: take(runtime, result.failure_details),
        }
    }

    #[cfg(not(feature = "streams"))]
    #[test]
    fn a_build_with_no_streams_feature_has_no_stream_runtime() {
        let runtime = runtime();
        let built =
            temporal_core_stream_runtime_new(runtime, "memory".into(), b"".as_slice().into());
        assert!(built.runtime.is_null());
        let fail = take(runtime, built.fail).unwrap();
        assert_eq!(String::from_utf8(fail).unwrap(), NO_STREAMS);
        let answer = outcome(
            runtime,
            temporal_core_stream_call_static("Withdraws".into(), b"".as_slice().into()),
        );
        assert_eq!(answer.status_code, tonic::Code::Unimplemented as u32);
        assert_eq!(answer.message.as_deref(), Some(NO_STREAMS));
        assert_eq!((answer.success, answer.details), (None, None));
        temporal_core_runtime_free(runtime);
    }

    #[cfg(feature = "streams")]
    mod with_streams {
        use super::*;
        use crate::{
            temporal_core_cancellation_token_cancel, temporal_core_cancellation_token_free,
            temporal_core_cancellation_token_new,
        };
        use prost::Message;
        use std::{
            sync::mpsc,
            time::{Duration, Instant},
        };
        use temporalio_streams::proto::{
            CreateStreamRequest, CreateStreamResponse, MemoryStreamConfig, OpenClientScopeRequest,
            OpenScopeResponse, ReadRecordsRequest, ScopedStreamIdRequest, ScopedStreamIdResponse,
            StreamErrorDetails, StreamErrorReason, StreamRef,
        };

        unsafe extern "C" fn send(
            user_data: *mut libc::c_void,
            success: *const ByteArray,
            status_code: u32,
            failure_message: *const ByteArray,
            failure_details: *const ByteArray,
        ) {
            let sender = unsafe { Box::from_raw(user_data as *mut mpsc::Sender<StreamCallResult>) };
            let result = StreamCallResult {
                success,
                status_code,
                failure_message,
                failure_details,
            };
            sender.send(result).unwrap();
        }

        fn call(
            streams: *mut StreamRuntime,
            method: &str,
            input: &[u8],
            cancel: *const CancellationToken,
        ) -> mpsc::Receiver<StreamCallResult> {
            let (sender, receiver) = mpsc::channel();
            let user_data = Box::into_raw(Box::new(sender)) as *mut libc::c_void;
            temporal_core_stream_call(
                streams,
                method.into(),
                input.into(),
                cancel,
                user_data,
                send,
            );
            receiver
        }

        fn memory(runtime: *mut Runtime) -> *mut StreamRuntime {
            let config = MemoryStreamConfig {
                backend: "b".into(),
                ..Default::default()
            }
            .encode_to_vec();
            let built = temporal_core_stream_runtime_new(
                runtime,
                "memory".into(),
                config.as_slice().into(),
            );
            assert!(built.fail.is_null());
            built.runtime
        }

        fn create(stream: &str) -> Vec<u8> {
            CreateStreamRequest {
                namespace: "ns".into(),
                stream_id: stream.into(),
                ..Default::default()
            }
            .encode_to_vec()
        }

        #[test]
        fn a_call_answers_through_its_callback() {
            let runtime = runtime();
            let streams = memory(runtime);
            let answers = call(streams, "CreateStream", &create("s"), std::ptr::null());
            let answer = outcome(
                runtime,
                answers.recv_timeout(Duration::from_secs(10)).unwrap(),
            );
            assert_eq!(answer.status_code, 0);
            let created = CreateStreamResponse::decode(answer.success.unwrap().as_slice()).unwrap();
            assert!(created.created);
            assert_eq!(created.stream.unwrap().backend, "b");
            temporal_core_stream_runtime_free(streams);
            temporal_core_runtime_free(runtime);
        }

        #[test]
        fn a_failed_call_gives_its_code_and_reason() {
            let runtime = runtime();
            let streams = memory(runtime);
            let answers = call(streams, "CreateStream", &create(""), std::ptr::null());
            let answer = outcome(
                runtime,
                answers.recv_timeout(Duration::from_secs(10)).unwrap(),
            );
            assert!(answer.success.is_none());
            assert_eq!(answer.status_code, tonic::Code::InvalidArgument as u32);
            assert!(answer.message.unwrap().contains("empty"));
            let details = StreamErrorDetails::decode(answer.details.unwrap().as_slice()).unwrap();
            assert_eq!(details.reason(), StreamErrorReason::Invalid);
            temporal_core_stream_runtime_free(streams);
            temporal_core_runtime_free(runtime);
        }

        #[test]
        fn a_cancelled_call_ends_with_code_1() {
            let runtime = runtime();
            let streams = memory(runtime);
            let created = call(streams, "CreateStream", &create("s"), std::ptr::null());
            outcome(
                runtime,
                created.recv_timeout(Duration::from_secs(10)).unwrap(),
            );
            let read = ReadRecordsRequest {
                namespace: "ns".into(),
                stream: Some(StreamRef {
                    id: "s".into(),
                    ..Default::default()
                }),
                wait: Some(prost_types::Duration {
                    seconds: 30,
                    nanos: 0,
                }),
                ..Default::default()
            };
            let cancel = temporal_core_cancellation_token_new();
            let started = Instant::now();
            let answers = call(streams, "ReadRecords", &read.encode_to_vec(), cancel);
            std::thread::sleep(Duration::from_millis(200));
            temporal_core_cancellation_token_cancel(cancel);
            let answer = outcome(
                runtime,
                answers.recv_timeout(Duration::from_secs(10)).unwrap(),
            );
            assert!(started.elapsed() < Duration::from_secs(10));
            assert_eq!((answer.status_code, answer.details), (1, None));
            temporal_core_cancellation_token_free(cancel);
            temporal_core_stream_runtime_free(streams);
            temporal_core_runtime_free(runtime);
        }

        /// Runs `body` on the Tokio runtime of a new bridge runtime.
        fn on_tokio(body: impl std::future::Future<Output = ()>) {
            let runtime = runtime();
            unsafe { &*runtime }.core.tokio_handle().block_on(body);
            temporal_core_runtime_free(runtime);
        }

        #[test]
        fn a_call_that_ended_before_its_cancel_gives_its_result() {
            on_tokio(async {
                // An unbiased choice would pick the cancel about half the time.
                for _ in 0..50 {
                    let call = tokio::spawn(async { Ok(vec![7]) });
                    while !call.is_finished() {
                        tokio::task::yield_now().await;
                    }
                    let cancel = tokio_util::sync::CancellationToken::new();
                    cancel.cancel();
                    let answer = answer(call, Some(cancel)).await;
                    assert_eq!(answer.status_code, 0);
                    let success = unsafe { (*answer.success).as_ref() }.to_vec();
                    assert_eq!(success, vec![7]);
                    temporal_core_byte_array_free(std::ptr::null_mut(), answer.success);
                }
            });
        }

        #[test]
        fn a_cancel_stops_a_running_call_and_a_panic_is_internal() {
            on_tokio(async {
                let call = tokio::spawn(std::future::pending());
                let cancel = tokio_util::sync::CancellationToken::new();
                cancel.cancel();
                let cancelled = answer(call, Some(cancel)).await;
                assert_eq!(
                    (cancelled.status_code, cancelled.success),
                    (1, std::ptr::null())
                );
                let call = tokio::spawn(async { panic!("the call panics") });
                let panicked = answer(call, None).await;
                assert_eq!(panicked.status_code, tonic::Code::Internal as u32);
            });
        }

        #[test]
        fn an_empty_input_may_have_a_null_pointer() {
            let runtime = runtime();
            let streams = memory(runtime);
            let null = || ByteArrayRef {
                data: std::ptr::null(),
                size: 0,
            };
            let answer = outcome(
                runtime,
                temporal_core_stream_call_sync(streams, "FreeScope".into(), null()),
            );
            assert_eq!(answer.status_code, tonic::Code::NotFound as u32);
            let answer = outcome(runtime, temporal_core_stream_call_static(null(), null()));
            assert_eq!(answer.status_code, tonic::Code::Unimplemented as u32);
            temporal_core_stream_runtime_free(streams);
            temporal_core_runtime_free(runtime);
        }

        #[test]
        fn the_sync_and_static_calls_answer_at_once() {
            let runtime = runtime();
            let streams = memory(runtime);
            let request = OpenClientScopeRequest {
                namespace: "ns".into(),
                identity: "me".into(),
                ..Default::default()
            }
            .encode_to_vec();
            let answer = outcome(
                runtime,
                temporal_core_stream_call_sync(
                    streams,
                    "OpenClientScope".into(),
                    request.as_slice().into(),
                ),
            );
            let opened = OpenScopeResponse::decode(answer.success.unwrap().as_slice()).unwrap();
            assert!(opened.producer.unwrap().id.starts_with("client:me:"));
            let request = ScopedStreamIdRequest {
                workflow_id: "wf".into(),
                first_execution_run_id: "r".into(),
                name: "n".into(),
            }
            .encode_to_vec();
            let answer = outcome(
                runtime,
                temporal_core_stream_call_static(
                    "ScopedStreamId".into(),
                    request.as_slice().into(),
                ),
            );
            let scoped =
                ScopedStreamIdResponse::decode(answer.success.unwrap().as_slice()).unwrap();
            assert_eq!(scoped.id, "wf:r:n");
            temporal_core_stream_runtime_free(streams);
            temporal_core_runtime_free(runtime);
        }

        #[cfg(not(feature = "streams-redis"))]
        #[test]
        fn a_build_with_no_redis_feature_has_no_redis_runtime() {
            let runtime = runtime();
            let built =
                temporal_core_stream_runtime_new(runtime, "redis".into(), b"".as_slice().into());
            assert!(built.runtime.is_null());
            let fail = String::from_utf8(take(runtime, built.fail).unwrap()).unwrap();
            assert_eq!(fail, "This build has no redis Stream runtime");
            temporal_core_runtime_free(runtime);
        }

        #[cfg(feature = "streams-redis")]
        #[test]
        fn a_redis_runtime_builds_from_its_config_with_no_connect() {
            use prost::Message as _;
            let runtime = runtime();
            let config = temporalio_streams_redis::proto::RedisStreamConfig {
                urls: vec!["redis://localhost:1/0".into()],
                ..Default::default()
            }
            .encode_to_vec();
            let built =
                temporal_core_stream_runtime_new(runtime, "redis".into(), config.as_slice().into());
            assert!(built.fail.is_null());
            temporal_core_stream_runtime_free(built.runtime);
            temporal_core_runtime_free(runtime);
        }
    }
}
