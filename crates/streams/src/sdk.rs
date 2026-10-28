//! The static calls, which need no runtime object and no Store. These are the Producer of an
//! Activity attempt, the scoped Stream id and the withdrawal rule. So every SDK applies one rule.

use crate::{
    Reason, StreamError, StreamResult,
    epoch::withdraws_producer,
    ids,
    proto::{
        ActivityContext, DeriveProducerRequest, DeriveProducerResponse, Producer, ProducerEpoch,
        ProducerPurpose, ScopedStreamIdRequest, ScopedStreamIdResponse, StreamProducerKind,
        WithdrawsRequest, WithdrawsResponse, WorkflowTaskEpoch,
    },
    runtime::decode,
};
use prost::Message;

/// Runs one static method on encoded input and returns the encoded output. A bridge exposes it as
/// a plain function.
pub fn call_static(method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
    match method {
        "DeriveProducer" => Ok(derive_producer(decode(input)?)?.encode_to_vec()),
        "Withdraws" => {
            let request = decode::<WithdrawsRequest>(input)?;
            let supersession = request
                .supersession
                .ok_or_else(|| StreamError::invalid("The request has no Superseded item"))?;
            Ok(WithdrawsResponse {
                withdraws: request
                    .producer
                    .is_some_and(|producer| withdraws_producer(&supersession, &producer)),
            }
            .encode_to_vec())
        }
        "ScopedStreamId" => {
            let request = decode::<ScopedStreamIdRequest>(input)?;
            Ok(ScopedStreamIdResponse {
                id: ids::scoped_stream_id(
                    &request.workflow_id,
                    &request.first_execution_run_id,
                    &request.name,
                ),
            }
            .encode_to_vec())
        }
        _ => Err(StreamError::new(
            Reason::Unsupported,
            format!("Unknown method {method}"),
        )),
    }
}

/// Whether [call_static] runs the method.
pub(crate) fn is_static(method: &str) -> bool {
    matches!(method, "DeriveProducer" | "ScopedStreamId" | "Withdraws")
}

/// The Producer of an Activity attempt, from the facts that lang copies from its Activity info. A
/// Workflow write keeps attempt 1, so the attempts in one Workflow Task resend one batch.
pub fn derive_producer(request: DeriveProducerRequest) -> StreamResult<DeriveProducerResponse> {
    let context = request
        .context
        .ok_or_else(|| StreamError::invalid("The request has no Activity context"))?;
    let purpose = ProducerPurpose::try_from(request.purpose).unwrap_or_default();
    if purpose == ProducerPurpose::Unspecified {
        return Err(StreamError::invalid("The request has no Producer purpose"));
    }
    if context.is_local {
        return local_activity(&context, purpose);
    }
    if purpose == ProducerPurpose::WorkflowWrite {
        return Err(StreamError::invalid(
            "Only a Local Activity writes for Workflow code",
        ));
    }
    let epoch = ProducerEpoch {
        attempt_scheduled_time: context.current_attempt_scheduled_time,
        attempt: context.attempt,
        ..Default::default()
    };
    let producer = if context.in_workflow {
        Producer {
            kind: StreamProducerKind::Activity as i32,
            id: ids::activity_producer_id(
                &context.workflow_run_id,
                &context.activity_id,
                &context.call_key,
            ),
            epoch: Some(epoch),
            run_id: context.workflow_run_id.clone(),
        }
    } else {
        Producer {
            kind: StreamProducerKind::StandaloneActivity as i32,
            id: ids::standalone_activity_producer_id(
                &context.activity_id,
                &context.activity_run_id,
            ),
            epoch: Some(epoch),
            run_id: String::new(),
        }
    };
    ids::check_producer(&producer)?;
    Ok(DeriveProducerResponse {
        producer: Some(producer),
        ..Default::default()
    })
}

fn local_activity(
    context: &ActivityContext,
    purpose: ProducerPurpose,
) -> StreamResult<DeriveProducerResponse> {
    // Without the task, a Local Activity that runs again in a later task has the same epoch.
    let task = context.workflow_task.as_ref().ok_or_else(|| {
        StreamError::invalid(
            "The Local Activity has no Workflow Task facts. The Worker's Core must send \
             Start.workflow_task",
        )
    })?;
    let attempt = match purpose {
        ProducerPurpose::WorkflowWrite => 1,
        _ => context.attempt,
    };
    let producer = Producer {
        kind: StreamProducerKind::LocalActivity as i32,
        id: ids::local_activity_producer_id(&context.workflow_run_id, &context.call_key),
        epoch: Some(ProducerEpoch {
            task_started_event_id: task.started_event_id,
            task_attempt: task.attempt as i32,
            task_started_time: task.started_time,
            attempt,
            ..Default::default()
        }),
        run_id: context.workflow_run_id.clone(),
    };
    ids::check_producer(&producer)?;
    Ok(DeriveProducerResponse {
        producer: Some(producer),
        // The Store withdraws the run's orphaned Local Activity Records only when this is set.
        previous_completed_task: Some(WorkflowTaskEpoch {
            started_event_id: task.previous_started_event_id,
            attempt: task.previous_attempt as i32,
            started_time: task.previous_started_time,
        }),
        failed_tasks: task.failed_tasks.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::WorkflowTaskAttempt;
    use prost_types::Timestamp;
    use temporalio_common::protos::coresdk::activity_task::WorkflowTaskAttempt as CoreTaskAttempt;

    fn at(seconds: i64, nanos: i32) -> Option<Timestamp> {
        Some(Timestamp { seconds, nanos })
    }

    fn local(purpose: ProducerPurpose) -> DeriveProducerRequest {
        DeriveProducerRequest {
            context: Some(ActivityContext {
                is_local: true,
                in_workflow: true,
                workflow_id: "wf".into(),
                workflow_run_id: "run-1".into(),
                activity_id: "3".into(),
                attempt: 4,
                call_key: "7".into(),
                workflow_task: Some(WorkflowTaskAttempt {
                    started_event_id: 15,
                    attempt: 2,
                    started_time: at(10, 5),
                    previous_started_event_id: 9,
                    failed_tasks: vec![12],
                    previous_attempt: 1,
                    previous_started_time: at(8, 0),
                }),
                ..Default::default()
            }),
            purpose: purpose as i32,
        }
    }

    #[test]
    fn a_workflow_write_keeps_attempt_one_and_the_task_facts() {
        let derived = derive_producer(local(ProducerPurpose::WorkflowWrite)).unwrap();
        let producer = derived.producer.unwrap();
        assert_eq!(producer.id, "la:run-1:7");
        assert_eq!(producer.run_id, "run-1");
        let epoch = producer.epoch.unwrap();
        assert_eq!(
            (
                epoch.task_started_event_id,
                epoch.task_attempt,
                epoch.attempt
            ),
            (15, 2, 1)
        );
        assert_eq!(epoch.task_started_time, at(10, 5));
        assert_eq!(
            derived.previous_completed_task,
            Some(WorkflowTaskEpoch {
                started_event_id: 9,
                attempt: 1,
                started_time: at(8, 0),
            })
        );
        assert_eq!(derived.failed_tasks, vec![12]);
    }

    #[test]
    fn a_local_activity_writer_takes_its_attempt() {
        let derived = derive_producer(local(ProducerPurpose::ActivityWriter)).unwrap();
        assert_eq!(derived.producer.unwrap().epoch.unwrap().attempt, 4);
    }

    #[test]
    fn the_first_task_of_a_run_still_sends_a_completed_task_of_zero() {
        let mut request = local(ProducerPurpose::WorkflowWrite);
        let task = request
            .context
            .as_mut()
            .unwrap()
            .workflow_task
            .as_mut()
            .unwrap();
        (task.previous_started_event_id, task.previous_attempt) = (0, 0);
        task.previous_started_time = None;
        let derived = derive_producer(request).unwrap();
        assert_eq!(
            derived.previous_completed_task,
            Some(WorkflowTaskEpoch::default())
        );
    }

    #[test]
    fn core_s_task_bytes_decode_as_the_task_facts() {
        let core = CoreTaskAttempt {
            started_event_id: 15,
            attempt: 2,
            started_time: at(10, 5),
            previous_started_event_id: 9,
            failed_tasks: vec![12, 13],
            previous_attempt: 3,
            previous_started_time: at(8, 1),
        };
        let decoded = WorkflowTaskAttempt::decode(core.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            decoded,
            WorkflowTaskAttempt {
                started_event_id: 15,
                attempt: 2,
                started_time: at(10, 5),
                previous_started_event_id: 9,
                failed_tasks: vec![12, 13],
                previous_attempt: 3,
                previous_started_time: at(8, 1),
            }
        );
    }

    #[test]
    fn an_activity_and_a_standalone_activity_take_their_ids_and_scheduled_time() {
        let mut request = local(ProducerPurpose::ActivityWriter);
        let context = request.context.as_mut().unwrap();
        context.is_local = false;
        context.workflow_task = None;
        context.current_attempt_scheduled_time = at(50, 0);
        let derived = derive_producer(request.clone()).unwrap();
        assert_eq!(derived.previous_completed_task, None);
        let producer = derived.producer.unwrap();
        assert_eq!(producer.id, "act:run-1:3:7");
        let epoch = producer.epoch.unwrap();
        assert_eq!(
            (epoch.attempt, epoch.attempt_scheduled_time),
            (4, at(50, 0))
        );
        assert_eq!(epoch.task_started_event_id, 0);
        let context = request.context.as_mut().unwrap();
        context.in_workflow = false;
        context.activity_run_id = "ar".into();
        let producer = derive_producer(request).unwrap().producer.unwrap();
        assert_eq!(producer.id, "sa:3:ar");
        assert_eq!(producer.kind, StreamProducerKind::StandaloneActivity as i32);
        assert!(producer.run_id.is_empty());
    }

    #[test]
    fn a_bad_context_is_refused() {
        let mut no_task = local(ProducerPurpose::WorkflowWrite);
        no_task.context.as_mut().unwrap().workflow_task = None;
        let mut no_purpose = local(ProducerPurpose::WorkflowWrite);
        no_purpose.purpose = 0;
        let mut regular_write = local(ProducerPurpose::WorkflowWrite);
        regular_write.context.as_mut().unwrap().is_local = false;
        let no_context = DeriveProducerRequest {
            purpose: ProducerPurpose::ActivityWriter as i32,
            ..Default::default()
        };
        for request in [no_task, no_purpose, regular_write, no_context] {
            assert_eq!(
                derive_producer(request).unwrap_err().reason,
                Reason::Invalid
            );
        }
    }

    #[test]
    fn the_static_call_derives_and_scopes_through_bytes() {
        let output = call_static(
            "DeriveProducer",
            &local(ProducerPurpose::WorkflowWrite).encode_to_vec(),
        )
        .unwrap();
        let derived = DeriveProducerResponse::decode(output.as_slice()).unwrap();
        assert_eq!(derived.producer.unwrap().id, "la:run-1:7");
        let request = ScopedStreamIdRequest {
            workflow_id: "a:b".into(),
            first_execution_run_id: "r".into(),
            name: "turn 1".into(),
        };
        let output = call_static("ScopedStreamId", &request.encode_to_vec()).unwrap();
        let scoped = ScopedStreamIdResponse::decode(output.as_slice()).unwrap();
        assert_eq!(scoped.id, "a%3Ab:r:turn%201");
        assert_eq!(
            call_static("Nope", &[]).unwrap_err().reason,
            Reason::Unsupported
        );
        assert_eq!(
            call_static("DeriveProducer", &[0xff]).unwrap_err().reason,
            Reason::Invalid
        );
    }
}
