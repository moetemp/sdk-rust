//! The Stores and the Producers of the unit tests of the Stream calls.

use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use prost_types::Timestamp;
use std::sync::atomic::{AtomicBool, Ordering};
use temporalio_streams::{
    AnyError, AppendError, CloseError, CreateError, DescribeError, ReadError, StreamStore,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, ClientProducer, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, Producer, ReadRecordsRequest, ReadRecordsResponse,
        WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
    },
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Request {
    Append(AppendRecordsRequest),
    Read(ReadRecordsRequest),
    Close(CloseStreamRequest),
}

#[derive(Default)]
pub(crate) struct Answers {
    pub(crate) appended: AppendRecordsResponse,
    pub(crate) page: ReadRecordsResponse,
}

#[derive(Default)]
pub(crate) struct Scripted {
    pub(crate) answers: Mutex<Answers>,
    requests: Mutex<Vec<Request>>,
    /// While set, appends and closes get no answer.
    pub(crate) silent: AtomicBool,
    /// The number of appends and closes, from now on, that fail with `StoreUnavailable`.
    pub(crate) unavailable: Mutex<usize>,
}

impl Scripted {
    pub(crate) fn requests(&self) -> Vec<Request> {
        self.requests.lock().clone()
    }

    async fn write(&self) -> Result<(), AnyError> {
        if self.silent.load(Ordering::Acquire) {
            std::future::pending::<()>().await;
        }
        let mut unavailable = self.unavailable.lock();
        if *unavailable > 0 {
            *unavailable -= 1;
            return Err(AnyError::store_unavailable("The Store is down"));
        }
        Ok(())
    }
}

impl StreamStore for Scripted {
    fn create_stream(
        &self,
        _: CreateStreamRequest,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        unimplemented!("A scripted Store only appends, reads and closes")
    }

    fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Append(request));
            self.write().await?;
            Ok(self.answers.lock().appended.clone())
        })
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Read(request));
            Ok(self.answers.lock().page.clone())
        })
    }

    fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> BoxFuture<'_, Result<CloseStreamResponse, CloseError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Close(request));
            self.write().await?;
            Ok(CloseStreamResponse {})
        })
    }

    fn describe_stream(
        &self,
        _: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        unimplemented!("A scripted Store only appends, reads and closes")
    }
}

pub(crate) fn task(started_event_id: i64, attempt: i32, seconds: i64) -> WorkflowTaskAttempt {
    WorkflowTaskAttempt {
        started_event_id,
        attempt,
        started_time: Some(Timestamp { seconds, nanos: 0 }),
    }
}

pub(crate) fn workflow(attempt: Option<WorkflowTaskAttempt>) -> Producer {
    Producer {
        kind: Some(Kind::Workflow(WorkflowProducer {
            namespace: "n".to_string(),
            workflow_id: "w".to_string(),
            run_id: "r".to_string(),
            attempt,
        })),
    }
}

pub(crate) fn client() -> Producer {
    Producer {
        kind: Some(Kind::Client(ClientProducer {
            namespace: "n".to_string(),
            id: "a".to_string(),
        })),
    }
}
