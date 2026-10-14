use crate::{
    ByteArray, ByteArrayRef, CancellationToken, UserDataHandle, client::Connection,
    runtime::Runtime,
};
use prost::Message;
use std::sync::Arc;
use temporalio_sdk_core::streams::{StreamService, connect_stream_service, proto};

/// One process's stream store, serving the stream calls lang makes. Workers take it in their
/// options, so a Workflow's output and outside producers share it.
pub struct StreamStore {
    pub(crate) runtime: Runtime,
    pub(crate) service: Arc<StreamService>,
}

/// If success or fail are not null, they must be manually freed when done.
pub type StreamStoreNewCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *mut StreamStore,
    fail: *const ByteArray,
);

/// Connects to the store `config` names, a serialized `coresdk.streams.StreamStoreConfig`. The
/// store asks `client`'s server about streams' owners. Client must live as long as the store, and
/// config and user data must live through the callback.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_store_new(
    client: *mut Connection,
    config: ByteArrayRef,
    user_data: *mut libc::c_void,
    callback: StreamStoreNewCallback,
) {
    let client = unsafe { &*client };
    let mut runtime = client.runtime.clone();
    let config = match proto::StreamStoreConfig::decode(config.to_slice()) {
        Ok(config) => config,
        Err(err) => {
            let fail = runtime.alloc_utf8(&format!("Invalid stream store config: {err}"));
            unsafe { callback(user_data, std::ptr::null_mut(), fail.into_raw()) };
            return;
        }
    };
    let connection = client.core.clone();
    let user_data = UserDataHandle(user_data);
    client.runtime.core.tokio_handle().spawn(async move {
        match connect_stream_service(config, connection).await {
            Ok(service) => {
                let store = Box::into_raw(Box::new(StreamStore {
                    runtime: runtime.clone(),
                    service,
                }));
                unsafe { callback(user_data.into(), store, std::ptr::null()) };
            }
            Err(err) => {
                let fail =
                    ByteArray::from_utf8(format!("Could not connect the stream store: {err}"));
                unsafe { callback(user_data.into(), std::ptr::null_mut(), fail.into_raw()) };
            }
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_store_free(store: *mut StreamStore) {
    unsafe {
        let _ = Box::from_raw(store);
    }
}

/// The failure a call answers with when its caller cancelled it. A write may or may not have
/// landed, but a read only stopped, so it gets a message that says so.
fn cancelled(rpc: &str) -> proto::StreamFailure {
    let message = match rpc {
        "Read" | "Latest" => "Cancelled before the read answered",
        _ => "Cancelled",
    };
    proto::StreamFailure {
        kind: proto::StreamFailureKind::OutcomeUnknown as i32,
        message: message.to_string(),
        cursor: String::new(),
        closed_by: String::new(),
    }
}

/// If success or failure are not null, they must be manually freed when done. Exactly one is
/// set. Success is the serialized response, and failure a serialized
/// `coresdk.streams.StreamFailure`.
pub type StreamStoreCallCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *const ByteArray,
    failure: *const ByteArray,
);

/// Makes one stream call. `rpc` names a `coresdk.streams.StreamService` method and `request` is
/// its serialized request. Cancelling the token drops the call, which a read uses to stop
/// waiting. Store, request and user data must live through the callback.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_stream_store_call(
    store: *mut StreamStore,
    rpc: ByteArrayRef,
    request: ByteArrayRef,
    cancellation_token: *const CancellationToken,
    user_data: *mut libc::c_void,
    callback: StreamStoreCallCallback,
) {
    let store = unsafe { &*store };
    let rpc = rpc.to_string();
    let request = request.to_vec();
    let cancel_token = unsafe { cancellation_token.as_ref() }.map(|v| v.token.clone());
    let service = store.service.clone();
    let user_data = UserDataHandle(user_data);
    store.runtime.core.tokio_handle().spawn(async move {
        let call = service.call(&rpc, &request);
        let result = if let Some(cancel_token) = cancel_token {
            tokio::select! {
                _ = cancel_token.cancelled() => Err(cancelled(&rpc)),
                result = call => result,
            }
        } else {
            call.await
        };
        let (success, failure) = match result {
            Ok(response) => (
                ByteArray::from_vec(response).into_raw(),
                std::ptr::null_mut(),
            ),
            Err(failure) => (
                std::ptr::null_mut(),
                ByteArray::from_vec(failure.encode_to_vec()).into_raw(),
            ),
        };
        unsafe { callback(user_data.into(), success, failure) };
    });
}
