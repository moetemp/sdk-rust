//! The external stream marker machine.
//!
//! Modeled on [`super::local_activity_state_machine`], and for the same reason: a marker written
//! live must be *matched* by the `MarkerRecorded` event the server writes back.

use super::{
    EventInfo, HistEventData, NewMachineWithCommand, OnEventWrapper, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::nondeterminism;
use std::convert::TryFrom;
use temporalio_common::protos::{
    constants::EXTERNAL_STREAM_MARKER_NAME,
    coresdk::external_data::{
        ExternalOutputStreamManifest, ExternalStreamMarkerData,
        build_external_stream_marker_details, extract_external_stream_marker_data,
    },
    temporal::api::{
        command::v1::{Command as ProtoCommand, RecordMarkerCommandAttributes, command},
        enums::v1::{CommandType, EventType},
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name ExternalStreamMachine;
    command ExternalStreamCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Live path: the marker is created from the output commit lang sent.
    Created --(Emit, shared on_emit) --> MarkerCommandCreated;
    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;
    ResultNotified --(MarkerRecorded(ExternalStreamMarkerData), shared on_marker_recorded)
      --> MarkerCommandRecorded;
}

#[derive(Debug, Clone)]
pub(super) struct SharedState {
    /// The envelope this machine will write, or did write.
    ///
    /// Held rather than rebuilt so the command and the later reconciliation are provably the same
    /// data.
    data: ExternalStreamMarkerData,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ExternalStreamCommand {
    /// Write the marker to History.
    RecordMarker,
}

#[derive(Default, Clone)]
pub(super) struct Created {}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct ResultNotified {}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}

impl Created {
    pub(super) fn on_emit(
        self,
        _state: &mut SharedState,
    ) -> ExternalStreamMachineTransition<MarkerCommandCreated> {
        TransitionResult::commands(vec![ExternalStreamCommand::RecordMarker])
    }
}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(
        self,
    ) -> ExternalStreamMachineTransition<ResultNotified> {
        TransitionResult::default()
    }
}

impl ResultNotified {
    pub(super) fn on_marker_recorded(
        self,
        state: &mut SharedState,
        data: ExternalStreamMarkerData,
    ) -> ExternalStreamMachineTransition<MarkerCommandRecorded> {
        verify_marker_matches(state, &data)
    }
}

/// Rejects a marker whose output manifest differs from the one this machine expects.
///
/// Markers carry no sequence number and are paired with machines in History order, so comparing
/// the manifest is what turns a reordered or foreign marker into a nondeterminism error rather
/// than a silently different published batch. The stage token is left out. Core mints it fresh
/// for each attempt, so it says nothing about what the Workflow published.
fn verify_marker_matches(
    state: &mut SharedState,
    data: &ExternalStreamMarkerData,
) -> ExternalStreamMachineTransition<MarkerCommandRecorded> {
    if without_stage_token(&data.output) != without_stage_token(&state.data.output) {
        return TransitionResult::Err(WFMachinesError::Nondeterminism(
            "External stream marker in history carries a different external output manifest than \
             the machine expecting it"
                .to_string(),
        ));
    }
    TransitionResult::default()
}

fn without_stage_token(
    output: &Option<ExternalOutputStreamManifest>,
) -> Option<ExternalOutputStreamManifest> {
    output.clone().map(|mut manifest| {
        manifest.stage_token.clear();
        manifest
    })
}

impl ExternalStreamMachine {
    /// A machine that **writes** one output commit's marker.
    ///
    /// The live path only. A machine created here issues the `RecordMarker` command and then
    /// reconciles it against the `MarkerRecorded` event the server writes back.
    pub(super) fn record_marker(data: ExternalStreamMarkerData) -> NewMachineWithCommand {
        let mut machine = ExternalStreamMachine {
            state: Some(ExternalStreamMachineState::Created(Created {})),
            shared_state: SharedState { data: data.clone() },
        };
        OnEventWrapper::on_event_mut(&mut machine, ExternalStreamMachineEvents::Emit)
            .expect("Emit is always valid from the initial state");
        NewMachineWithCommand {
            command: marker_command(&data),
            machine: machine.into(),
        }
    }
}

fn marker_command(data: &ExternalStreamMarkerData) -> command::Attributes {
    command::Attributes::RecordMarkerCommandAttributes(RecordMarkerCommandAttributes {
        marker_name: EXTERNAL_STREAM_MARKER_NAME.to_string(),
        details: build_external_stream_marker_details(data),
        header: None,
        failure: None,
    })
}

impl TryFrom<CommandType> for ExternalStreamMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistEventData> for ExternalStreamMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        if e.event_type() != EventType::MarkerRecorded {
            return Err(nondeterminism!(
                "External stream machine cannot handle this event: {e}"
            ));
        }
        match extract_stream_marker(&e) {
            Some(data) => Ok(ExternalStreamMachineEvents::MarkerRecorded(data)),
            None => Err(nondeterminism!(
                "Marker recorded event {e} is not an external stream marker"
            )),
        }
    }
}

/// The envelope inside a `MarkerRecorded` event, if it is one of ours.
pub(super) fn extract_stream_marker(e: &HistoryEvent) -> Option<ExternalStreamMarkerData> {
    if e.event_type() != EventType::MarkerRecorded {
        return None;
    }
    match &e.attributes {
        Some(history_event::Attributes::MarkerRecordedEventAttributes(
            MarkerRecordedEventAttributes {
                marker_name,
                details,
                ..
            },
        )) if marker_name == EXTERNAL_STREAM_MARKER_NAME => {
            extract_external_stream_marker_data(details)
        }
        _ => None,
    }
}

impl WFMachinesAdapter for ExternalStreamMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            ExternalStreamCommand::RecordMarker => {
                vec![MachineResponse::IssueNewCommand(ProtoCommand {
                    ..marker_command(&self.shared_state.data).into()
                })]
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(stage_token: &str, history_floor_event_id: i64) -> ExternalStreamMarkerData {
        ExternalStreamMarkerData {
            schema_version: 1,
            output: Some(ExternalOutputStreamManifest {
                schema_version: 1,
                fingerprint_version: 2,
                stage_token: stage_token.to_string(),
                history_floor_event_id,
                run_id: "run-id".to_string(),
                provider_id: "provider".to_string(),
                provider_format_version: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_different_output_manifest_is_nondeterministic() {
        let expected = marker("token", 1);
        let mut state = SharedState {
            data: expected.clone(),
        };
        let actual = marker("token", 2);

        assert!(matches!(
            verify_marker_matches(&mut state, &actual),
            TransitionResult::Err(WFMachinesError::Nondeterminism(message))
                if message.contains("different external output manifest")
        ));
    }

    #[test]
    fn the_same_output_manifest_matches_whatever_its_stage_token() {
        let mut state = SharedState {
            data: marker("token", 1),
        };

        for recorded in [marker("token", 1), marker("other attempt's token", 1)] {
            assert!(matches!(
                verify_marker_matches(&mut state, &recorded),
                TransitionResult::Ok { .. }
            ));
        }
    }
}
