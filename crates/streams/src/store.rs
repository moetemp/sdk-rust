use crate::{
    StreamResult,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, PendingStage, PromoteResult, StageRef,
        StagedBatch, StoreAppendRequest, StoreAppendResponse, StoreLatestRequest,
        StoreLatestResponse, StoreReadRequest, StoreReadResponse,
    },
};

/// The result a stream closes with when its owner gave none: a `binary/null` payload, which every
/// SDK's converter reads as no value. A payload with no encoding fails a caller that expects a
/// typed result, and its Workflow Task fails on every retry.
pub fn empty_close_result() -> crate::proto::Payload {
    crate::proto::Payload {
        metadata: [("encoding".to_string(), b"binary/null".to_vec())].into(),
        ..Default::default()
    }
}

/// What a closed stream's operations complete with.
// One is built per close, so the size of a failure doesn't matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum CloseOutcome {
    /// They complete successfully with this payload, after lang's codec.
    Result(crate::proto::Payload),
    /// They complete as failed with this failure, or as canceled when it has canceled info.
    Failure(temporalio_common::protos::temporal::api::failure::v1::Failure),
}

impl CloseOutcome {
    /// The result, for a store that keeps no failure.
    pub fn result(&self) -> Option<crate::proto::Payload> {
        match self {
            CloseOutcome::Result(result) => Some(result.clone()),
            CloseOutcome::Failure(_) => None,
        }
    }
}

/// A store that holds streams, keyed by their owner's run chain.
///
/// A store moves serialized `temporal.sdk.streams.v1.StreamRecord`s with encoded bodies and deals
/// in its own positions. Owner checks, cursors, supersession and repair sit above it, so every
/// store behaves the same where the contract says so, and a new store only implements this.
///
/// Every call is safe to repeat. A call that fails while the store may have acted reports
/// [crate::StreamError::outcome_unknown] for an append and a storage failure otherwise.
#[async_trait::async_trait]
pub trait StreamStore: Send + Sync {
    /// The short name minted into this store's cursors, such as `redis` or `memory`. A cursor
    /// from another store is refused.
    fn name(&self) -> &str;

    /// Appends one batch for one producer attempt, whole and in order.
    ///
    /// The store keeps the attempt's newest batch: its first sequence, its record count, its
    /// positions and its digest. A repeat of that batch with the same digest answers with its
    /// positions and writes nothing. The same first sequence with another digest fails as
    /// producer divergent, and a first sequence below the end of the newest batch as producer
    /// stale. A closed chain or topic refuses a new batch as closed, but still answers a repeat.
    /// The answer, a repeat's too, reports the topic's arm, so a lost notification goes again.
    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse>;

    /// Reads the records of a topic after a position, waiting up to the request's `wait` for one
    /// when none is there.
    ///
    /// Fails as expired when the position names a record retention dropped, or when retention
    /// dropped records the read had not delivered yet. Fails as not found when nothing is known
    /// about the topic and the position is not empty.
    ///
    /// A read that asks to arm and ends with no record on an open topic arms the topic in the
    /// same step that finds it empty. The next append or promotion then reports the arm, and
    /// the producer notifies the topic's callers.
    async fn read(&self, request: StoreReadRequest) -> StreamResult<StoreReadResponse>;

    /// The position of the newest record of a topic, or empty when it holds none.
    async fn latest(&self, request: StoreLatestRequest) -> StreamResult<StoreLatestResponse>;

    /// The serialized record at `position` on a topic, or `None` when the store no longer holds
    /// it. A read that resumes looks at the record it last delivered, to know that record's
    /// producer attempt.
    async fn record_at(
        &self,
        chain: &ChainId,
        topic: &str,
        position: &str,
    ) -> StreamResult<Option<Vec<u8>>>;

    /// The position of the newest record a topic ever took, even one retention dropped since,
    /// or empty when it took none. A read that has delivered nothing keeps it as its mark, since
    /// it has no position the store could check.
    async fn newest_ever(&self, _chain: &ChainId, _topic: &str) -> StreamResult<String> {
        Ok(String::new())
    }

    /// The position of the newest record retention dropped from a topic, when that record came
    /// after `mark`, the topic's [StreamStore::newest_ever] when a read started. Such a record
    /// was lost to the read. A store that never drops records keeps the default.
    async fn dropped_after(
        &self,
        _chain: &ChainId,
        _topic: &str,
        _mark: &str,
    ) -> StreamResult<Option<String>> {
        Ok(None)
    }

    /// Holds a Workflow Task's records, invisible to readers, until [StreamStore::promote] or
    /// [StreamStore::abort] names its token. A stage outlives the retention, since crash repair
    /// can come after it.
    async fn stage(&self, batch: StagedBatch) -> StreamResult<()>;

    /// Moves a stage's records into their topics in one step, so readers see all or none of
    /// them. Idempotent, and the records become visible once. Reports each promoted topic's arm.
    async fn promote(&self, stage: &StageRef) -> StreamResult<PromoteResult>;

    /// Drops a stage whose Workflow Task never committed. Idempotent.
    async fn abort(&self, stage: &StageRef) -> StreamResult<()>;

    /// Marks a chain closed, so its topics refuse new batches from producers. Promotions still
    /// land, since a Workflow's committed output is never refused.
    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()>;

    /// Closes a chain whose final Workflow Task its own Worker saw accepted, after that task's
    /// topic closes. A reader that finds the owner ended calls [StreamStore::close_chain]
    /// instead, since it can't know whether the Workflow's own closes landed yet.
    async fn end_chain(&self, chain: &ChainId) -> StreamResult<()> {
        self.close_chain(chain).await
    }

    /// Like [StreamStore::end_chain], for a chain whose owner closed with `outcome`. A store keeps
    /// no outcome, but a wrapper that completes the streams' operations needs it.
    async fn end_chain_with(&self, chain: &ChainId, outcome: CloseOutcome) -> StreamResult<()> {
        let _ = outcome;
        self.end_chain(chain).await
    }

    /// Marks one topic of a chain closed, like [StreamStore::close_chain] for that topic alone.
    ///
    /// `result` is what the operations that handed the topic out complete with, when the owner
    /// gave one. A store keeps no result itself, but a wrapper that notifies those operations
    /// needs it.
    async fn close_topic(
        &self,
        chain: &ChainId,
        topic: &str,
        result: Option<crate::proto::Payload>,
    ) -> StreamResult<()>;

    /// Like [StreamStore::close_topic], with a failure as well as a result for the operations that
    /// handed the topic out. A store keeps neither.
    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        outcome: CloseOutcome,
    ) -> StreamResult<()> {
        self.close_topic(chain, topic, outcome.result()).await
    }

    /// The chain's stages that no one has promoted or aborted yet, for crash repair.
    async fn pending_stages(&self, chain: &ChainId) -> StreamResult<Vec<PendingStage>>;

    /// The topics of a chain that the store knows and that [StreamStore::close_topic] hasn't
    /// closed, in no particular order. A staged topic shows as soon as it's staged. A store that
    /// can't list its topics answers none.
    async fn open_topics(&self, chain: &ChainId) -> StreamResult<Vec<String>> {
        let _ = chain;
        Ok(vec![])
    }

    /// Deletes every stream and stage of one owner, across all its chains.
    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse>;

    /// Closes one topic of a chain in the store, then tells the topic's notifier on the server,
    /// which completes every attached operation with `outcome`. The store closes first, so a
    /// reader the completion reaches always finds the stream's end. A store that sends no
    /// notifications only closes the topic. Fails as refused when the server will never take the
    /// close, and with another kind when trying again may help.
    async fn close_stream(
        &self,
        chain: &ChainId,
        topic: &str,
        outcome: CloseOutcome,
    ) -> StreamResult<()> {
        self.close_topic_with(chain, topic, outcome).await
    }

    /// Waits until the notifications this store sent are out. A store that sends none returns at
    /// once.
    async fn flush_notifications(&self) {}

    /// Clears the arm of a topic when it still holds `generation`, once the notification that
    /// generation asked for landed. A reader that armed again since holds a higher generation,
    /// and its arm stays for the next append. A store that can't arm keeps the default.
    async fn disarm(&self, chain: &ChainId, topic: &str, generation: u64) -> StreamResult<()> {
        let _ = (chain, topic, generation);
        Ok(())
    }

    /// The position of the newest record of a topic, or, when it holds none, an arm set in the
    /// same step that finds it empty, so the first append reports the arm. A store that can't
    /// arm keeps the default, which answers [NewestOrArm::Unarmed] for an empty topic.
    async fn newest_or_arm(&self, chain: &ChainId, topic: &str) -> StreamResult<NewestOrArm> {
        let position = self
            .latest(StoreLatestRequest {
                chain: Some(chain.clone()),
                topic: topic.to_string(),
            })
            .await?
            .position;
        Ok(if position.is_empty() {
            NewestOrArm::Unarmed
        } else {
            NewestOrArm::Newest(position)
        })
    }
}

/// What [StreamStore::newest_or_arm] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewestOrArm {
    /// The position of the topic's newest record.
    Newest(String),
    /// The topic held no record, and the store armed it with this generation.
    Armed(u64),
    /// The topic held no record, and the store can't arm.
    Unarmed,
}

#[cfg(test)]
mod tests {
    use super::StreamStore;
    use std::sync::Arc;

    #[test]
    fn a_store_can_be_shared_as_a_trait_object() {
        // Core holds the configured store behind one pointer for every Worker and client call.
        fn _shared(_: Arc<dyn StreamStore>) {}
    }
}
