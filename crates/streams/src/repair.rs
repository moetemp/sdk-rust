//! Settling stages a Worker left behind, as History decides them.
//!
//! A Worker that stops after a Workflow Task's commit but before the promotion leaves the stage
//! pending, and a run that finished is never replayed to promote it. A reader looks for such
//! stages and reads the staging run's History to promote or abort each one.

use temporalio_common::protos::{
    constants::EXTERNAL_STREAM_MARKER_NAME,
    coresdk::external_data::{ExternalOutputStreamManifest, extract_external_stream_marker_data},
    temporal::api::{
        enums::v1::EventType,
        history::v1::{HistoryEvent, history_event::Attributes},
    },
};

/// What History says about one stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// History holds the marker that names the stage, so its Workflow Task committed.
    Promote,
    /// The stage's Workflow Task never committed, and nothing can commit it now.
    Abort,
    /// History doesn't tell yet, for example while a Local Activity holds the task open.
    Unknown,
}

fn output(event: &HistoryEvent) -> Option<ExternalOutputStreamManifest> {
    let Some(Attributes::MarkerRecordedEventAttributes(marker)) = &event.attributes else {
        return None;
    };
    if marker.marker_name != EXTERNAL_STREAM_MARKER_NAME {
        return None;
    }
    extract_external_stream_marker_data(&marker.details)?.output
}

fn run_closed(event: &HistoryEvent) -> bool {
    matches!(
        event.event_type(),
        EventType::WorkflowExecutionCompleted
            | EventType::WorkflowExecutionFailed
            | EventType::WorkflowExecutionTimedOut
            | EventType::WorkflowExecutionCanceled
            | EventType::WorkflowExecutionTerminated
            | EventType::WorkflowExecutionContinuedAsNew
    )
}

/// What History says about the stage `token`.
///
/// `events` are the staging run's events after the stage's History floor, in order. A marker
/// that names the token proves the commit. Otherwise a closed run, or a failed or timed out
/// result for the Workflow Task after the floor, proves the completion was dropped. Anything
/// else is not known yet.
///
/// `survived_eviction` turns on the same-floor rule. Several commits of one task attempt share a
/// floor, but never straddle an eviction, and a failed attempt always ends in one, since Core
/// evicts a run whose Workflow Task failed. So a stage from before an eviction whose floor
/// another stage committed at belongs to a failed attempt, even a transient one History never
/// records. A Worker sets it for a stage held across an eviction, since its own attempt may still
/// be committing. A reader sets it always, since it never holds an attempt in flight.
pub fn decide_token(
    events: &[HistoryEvent],
    token: &str,
    history_floor_event_id: i64,
    survived_eviction: bool,
) -> Decision {
    let outputs: Vec<_> = events.iter().filter_map(output).collect();
    if outputs.iter().any(|output| output.stage_token == token) {
        return Decision::Promote;
    }
    // The run is over and never committed this stage, so nothing can.
    if events.iter().any(run_closed) {
        return Decision::Abort;
    }
    if survived_eviction
        && outputs
            .iter()
            .any(|output| output.history_floor_event_id == history_floor_event_id)
    {
        return Decision::Abort;
    }
    let result = events.iter().find(|event| {
        event.event_id > history_floor_event_id
            && matches!(
                event.event_type(),
                EventType::WorkflowTaskCompleted
                    | EventType::WorkflowTaskFailed
                    | EventType::WorkflowTaskTimedOut
            )
    });
    match result.map(HistoryEvent::event_type) {
        Some(EventType::WorkflowTaskFailed | EventType::WorkflowTaskTimedOut) => Decision::Abort,
        _ => Decision::Unknown,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use temporalio_common::protos::{
        coresdk::external_data::{ExternalStreamMarkerData, build_external_stream_marker_details},
        temporal::api::history::v1::MarkerRecordedEventAttributes,
    };

    pub(crate) fn event(event_id: i64, event_type: EventType) -> HistoryEvent {
        HistoryEvent {
            event_id,
            event_type: event_type as i32,
            ..Default::default()
        }
    }

    pub(crate) fn marker(event_id: i64, token: &str, floor: i64) -> HistoryEvent {
        let data = ExternalStreamMarkerData {
            schema_version: 1,
            output: Some(ExternalOutputStreamManifest {
                stage_token: token.to_string(),
                history_floor_event_id: floor,
                ..Default::default()
            }),
            ..Default::default()
        };
        HistoryEvent {
            event_id,
            event_type: EventType::MarkerRecorded as i32,
            attributes: Some(Attributes::MarkerRecordedEventAttributes(
                MarkerRecordedEventAttributes {
                    marker_name: EXTERNAL_STREAM_MARKER_NAME.to_string(),
                    details: build_external_stream_marker_details(&data),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    #[test]
    fn a_marker_naming_the_stage_proves_the_commit() {
        let events = [
            event(4, EventType::WorkflowTaskCompleted),
            marker(5, "t1", 3),
        ];
        assert_eq!(decide_token(&events, "t1", 3, false), Decision::Promote);
        assert_eq!(decide_token(&events, "t2", 3, false), Decision::Unknown);
    }

    #[test]
    fn a_marker_of_another_kind_proves_nothing() {
        let mut other = marker(5, "t1", 3);
        if let Some(Attributes::MarkerRecordedEventAttributes(attributes)) = &mut other.attributes {
            attributes.marker_name = "core_local_activity".to_string();
        }
        assert_eq!(decide_token(&[other], "t1", 3, false), Decision::Unknown);
    }

    #[test]
    fn a_closed_run_that_never_committed_the_stage_aborts_it() {
        for closed in [
            EventType::WorkflowExecutionCompleted,
            EventType::WorkflowExecutionFailed,
            EventType::WorkflowExecutionTimedOut,
            EventType::WorkflowExecutionCanceled,
            EventType::WorkflowExecutionTerminated,
            EventType::WorkflowExecutionContinuedAsNew,
        ] {
            let events = [event(9, closed)];
            assert_eq!(decide_token(&events, "t1", 3, false), Decision::Abort);
        }
    }

    #[test]
    fn the_task_result_after_the_floor_decides_a_dropped_completion() {
        for failed in [
            EventType::WorkflowTaskFailed,
            EventType::WorkflowTaskTimedOut,
        ] {
            let events = [event(4, EventType::WorkflowTaskStarted), event(5, failed)];
            assert_eq!(decide_token(&events, "t1", 3, false), Decision::Abort);
        }
        // A completion without the marker may still hold it in a later event, for example after
        // a Local Activity, so it decides nothing.
        let completed = [event(5, EventType::WorkflowTaskCompleted)];
        assert_eq!(decide_token(&completed, "t1", 3, false), Decision::Unknown);
        // A result at or before the floor belongs to an earlier task.
        let earlier = [event(3, EventType::WorkflowTaskFailed)];
        assert_eq!(decide_token(&earlier, "t1", 3, false), Decision::Unknown);
    }

    #[test]
    fn rule_24_1_a_commit_at_the_same_floor_aborts_a_stage_from_before_an_eviction() {
        let events = [
            event(5, EventType::WorkflowTaskCompleted),
            marker(6, "other", 3),
        ];
        assert_eq!(decide_token(&events, "t1", 3, true), Decision::Abort);
        // Without an eviction between them, both stages may belong to one attempt.
        assert_eq!(decide_token(&events, "t1", 3, false), Decision::Unknown);
        // Another floor says nothing about this stage.
        assert_eq!(decide_token(&events, "t1", 4, true), Decision::Unknown);
    }
}
