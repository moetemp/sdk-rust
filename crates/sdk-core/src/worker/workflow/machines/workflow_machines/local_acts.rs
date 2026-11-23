use super::super::local_activity_state_machine::ResolveDat;
use crate::{
    protosext::{CompleteLocalActivityData, ValidScheduleLA},
    worker::{ExecutingLAId, LocalActRequest, NewLocalAct, workflow::streams::CallKey},
};
use std::{
    collections::{HashSet, VecDeque},
    time::SystemTime,
};
use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

struct Preresolution {
    /// Older markers have no group, so replay must keep their existing batching behavior.
    activation_index: Option<u64>,
    result: PeekedResult,
}

/// A result of History that a replay holds for the activation of the first run.
enum PeekedResult {
    LocalActivity {
        seq: u32,
        resolution: ResolveDat,
    },
    /// The result of a Stream call. Its job stays with the calls of the run.
    Stream(CallKey),
}

/// The result of History that a replay gives next.
#[derive(Clone, Copy)]
pub(super) enum NextPeeked {
    LocalActivity(u32),
    Stream((u64, CallKey)),
}

impl Preresolution {
    fn is_local_activity(&self, of_seq: u32) -> bool {
        matches!(self.result, PeekedResult::LocalActivity { seq, .. } if seq == of_seq)
    }
}

#[derive(Default)]
pub(super) struct LocalActivityData {
    /// Queued local activity requests which need to be executed
    new_requests: Vec<ValidScheduleLA>,
    /// Queued cancels that need to be dispatched
    cancel_requests: Vec<ExecutingLAId>,
    /// Seq #s of local activities which we have sent to be executed but have not yet resolved
    executing: HashSet<u32>,
    /// Local activity resolutions in the order their markers were found while looking ahead at the
    /// next WFT, and the Stream results of that WFT, each after the resolutions of its activation.
    preresolutions: VecDeque<Preresolution>,
    /// Position of the next activation within the current WFT. Recorded on markers so replay can
    /// deliver each resolution in the same activation it was delivered in originally. Counting
    /// from the start of the WFT keeps activations that leave no trace in history, such as those
    /// for rejected updates, from shifting the positions of later ones.
    activation_index: u64,
    /// Set true if the workflow is terminating
    am_terminating: bool,
}

impl LocalActivityData {
    pub(super) fn enqueue(&mut self, act: ValidScheduleLA) {
        self.new_requests.push(act);
    }

    pub(super) fn enqueue_cancel(&mut self, cancel: ExecutingLAId) {
        self.cancel_requests.push(cancel);
    }

    pub(super) fn done_executing(&mut self, seq: u32) {
        // This seems nonsense, but can happen during abandonment
        self.new_requests.retain(|req| req.seq != seq);
        self.executing.remove(&seq);
    }

    /// Drain all requests to execute or cancel LAs. Additional info is passed in to be able to
    /// augment the data this struct has to form complete request data.
    pub(super) fn take_all_reqs(
        &mut self,
        wf_type: &str,
        wf_id: &str,
        run_id: &str,
    ) -> Vec<LocalActRequest> {
        if self.am_terminating {
            return vec![LocalActRequest::CancelAllInRun(run_id.to_string())];
        }

        self.cancel_requests
            .drain(..)
            .map(LocalActRequest::Cancel)
            .chain(self.new_requests.drain(..).map(|sa| {
                self.executing.insert(sa.seq);
                LocalActRequest::New(NewLocalAct {
                    schedule_time: SystemTime::now(),
                    // `ManagedRun` sets this when it dispatches the request.
                    workflow_task: None,
                    schedule_cmd: sa,
                    workflow_type: wf_type.to_string(),
                    workflow_exec_info: WorkflowExecution {
                        workflow_id: wf_id.to_string(),
                        run_id: run_id.to_string(),
                    },
                })
            }))
            .collect()
    }

    /// Returns all outstanding local activities, whether executing or requested and in the queue
    pub(super) fn outstanding_la_count(&self) -> usize {
        if self.am_terminating {
            return 0;
        }
        self.executing.len() + self.new_requests.len()
    }

    pub(super) fn insert_peeked_marker(&mut self, dat: CompleteLocalActivityData) {
        self.preresolutions.push_back(Preresolution {
            activation_index: dat.marker_dat.activation_index,
            result: PeekedResult::LocalActivity {
                seq: dat.marker_dat.seq,
                resolution: dat.into(),
            },
        });
    }

    /// Inserts the Stream results of a peeked marker, which come in the order of the first run.
    /// In one activation, the first run gave the results of Local Activities before them.
    pub(super) fn insert_peeked_stream_results(&mut self, results: Vec<(u64, CallKey)>) {
        for (activation_index, call) in results {
            let comes_before = |item: &Preresolution| {
                (item.activation_index).is_none_or(|index| index <= activation_index)
            };
            let at = self.preresolutions.iter().rposition(comes_before);
            let item = Preresolution {
                activation_index: Some(activation_index),
                result: PeekedResult::Stream(call),
            };
            self.preresolutions.insert(at.map_or(0, |at| at + 1), item);
        }
    }

    pub(super) fn take_preresolution(&mut self, seq: u32) -> Option<ResolveDat> {
        let idx = self
            .preresolutions
            .iter()
            .position(|item| item.is_local_activity(seq))?;
        let item = self
            .preresolutions
            .remove(idx)
            .expect("This index was just found to contain seq");
        match item.result {
            PeekedResult::LocalActivity { resolution, .. } => Some(resolution),
            PeekedResult::Stream(_) => None,
        }
    }

    /// Takes the Stream result that [`Self::peek_preresolution`] returned.
    pub(super) fn take_stream_preresolution(&mut self) {
        self.preresolutions.pop_front();
    }

    /// Returns the next peeked resolution, unless it was recorded for a later activation in this
    /// WFT and `include_held` is false.
    pub(super) fn peek_preresolution(&self, include_held: bool) -> Option<NextPeeked> {
        let item = self.preresolutions.front()?;
        if !include_held && self.is_held(item) {
            return None;
        }
        Some(match item.result {
            PeekedResult::LocalActivity { seq, .. } => NextPeeked::LocalActivity(seq),
            PeekedResult::Stream(call) => {
                let activation_index = item.activation_index.expect("A Stream result has one");
                NextPeeked::Stream((activation_index, call))
            }
        })
    }

    pub(super) fn has_held_preresolution(&self, seq: u32) -> bool {
        self.preresolutions
            .iter()
            .any(|item| item.is_local_activity(seq) && self.is_held(item))
    }

    fn is_held(&self, item: &Preresolution) -> bool {
        item.activation_index
            .is_some_and(|group| group > self.activation_index)
    }

    pub(super) fn wft_applied(&mut self) {
        self.activation_index = 0;
    }

    pub(super) fn activation_dispatched(&mut self) {
        self.activation_index += 1;
    }

    pub(super) fn advance_to_next_resolution_activation(&mut self) {
        if let Some(activation_index) = self
            .preresolutions
            .front()
            .and_then(|item| item.activation_index)
        {
            self.activation_index = self.activation_index.max(activation_index);
        }
    }

    pub(super) fn current_activation_index(&self) -> u64 {
        self.activation_index
    }

    pub(super) fn remove_from_queue(&mut self, seq: u32) -> Option<ValidScheduleLA> {
        self.new_requests
            .iter()
            .position(|req| req.seq == seq)
            .map(|i| self.new_requests.remove(i))
    }

    /// Store that this workflow is terminating, and thus no new LA requests need be processed,
    /// and any executing LAs should not prevent us from shutting down.
    pub(super) fn indicate_terminating(&mut self) {
        self.am_terminating = true;
    }
}
