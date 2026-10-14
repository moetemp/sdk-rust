use crate::{
    StreamResult,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse, StoredClose,
    },
};

/// The most bytes a close's outcome takes in the store, `closed_by` included. A store keeps it
/// with the stream as it keeps a record, so it gets the bound one record gets.
pub const MAX_CLOSE_BYTES: usize = 1536 * 1024;

/// The result a stream closes with when its owner gave none: a `binary/null` payload, which every
/// SDK's converter reads as no value. A payload with no encoding fails a caller that expects a
/// typed result.
pub fn empty_close_result() -> crate::proto::Payload {
    crate::proto::Payload {
        metadata: [("encoding".to_string(), b"binary/null".to_vec())].into(),
        ..Default::default()
    }
}

/// What a closed stream's waiters see: the readers and callbacks its notifier holds.
// One is built per close, so the size of a failure doesn't matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum CloseOutcome {
    /// The stream ended with this payload, after lang's codec.
    Result(crate::proto::Payload),
    /// The stream ended with this failure, or as canceled when it has canceled info.
    Failure(temporalio_common::protos::temporal::api::failure::v1::Failure),
}

impl CloseOutcome {
    /// The result, or `None` for a failure.
    pub fn result(&self) -> Option<crate::proto::Payload> {
        match self {
            CloseOutcome::Result(result) => Some(result.clone()),
            CloseOutcome::Failure(_) => None,
        }
    }

    /// What the waiters of a stream closed with `close` see. A close with no outcome gives them
    /// [empty_close_result].
    pub fn of(close: &StoredClose) -> Self {
        match (&close.failure, &close.result) {
            (Some(failure), _) => CloseOutcome::Failure(failure.clone()),
            (None, Some(result)) => CloseOutcome::Result(result.clone()),
            (None, None) => CloseOutcome::Result(empty_close_result()),
        }
    }
}

impl From<CloseOutcome> for StoredClose {
    fn from(outcome: CloseOutcome) -> Self {
        match outcome {
            CloseOutcome::Result(result) => StoredClose {
                result: Some(result),
                ..Default::default()
            },
            CloseOutcome::Failure(failure) => StoredClose {
                failure: Some(failure),
                ..Default::default()
            },
        }
    }
}

/// A store that holds streams, keyed by their owner's run chain.
///
/// A store moves serialized `temporal.sdk.streams.v1.StreamRecord`s with encoded bodies and deals
/// in its own positions. Owner checks, cursors and supersession sit above it, so every
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
    /// same step that finds it empty. The next append then reports the arm, and the producer
    /// notifies the topic's callers.
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

    /// Marks a chain closed, so its topics refuse new batches from producers.
    async fn close_chain(&self, chain: &ChainId) -> StreamResult<()>;

    /// Marks one topic of a chain closed, like [StreamStore::close_chain] for that topic alone.
    async fn close_topic(&self, chain: &ChainId, topic: &str) -> StreamResult<()>;

    /// Like [StreamStore::close_topic], with the close's outcome and who closed it. A store that
    /// keeps the outcome keeps the topic's first close only, and a read of the closed topic
    /// answers it. A store that keeps none keeps the default, but a wrapper that tells the
    /// stream's notifier still needs the outcome.
    async fn close_topic_with(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<()> {
        let _ = close;
        self.close_topic(chain, topic).await
    }

    /// Drops the newest-batch record [StreamStore::append] keeps for one producer attempt on a
    /// topic, and nothing else. A missing attempt or topic is no failure.
    ///
    /// Call it only once the attempt's last append returned. A retry of an earlier batch would
    /// then find no record to compare with and land a second time.
    async fn retire_producer(
        &self,
        chain: &ChainId,
        topic: &str,
        producer_id: &str,
        attempt: i64,
    ) -> StreamResult<()>;

    /// Deletes every stream of one owner, across all its chains.
    async fn delete_owner(&self, request: DeleteOwnerRequest) -> StreamResult<DeleteOwnerResponse>;

    /// Holds the records of a topic after `position` against retention for `holder`, a
    /// positions-only reader that must find them again on replay. The store keeps the lowest
    /// position a holder held, and renews the holder's lease. An empty position holds the whole
    /// topic. A store that can't hold refuses, so a positions-only read fails at once and not
    /// at replay.
    async fn hold_floor(
        &self,
        chain: &ChainId,
        topic: &str,
        holder: &str,
        position: &str,
    ) -> StreamResult<()> {
        let _ = (chain, topic, holder, position);
        Err(crate::StreamError::unsupported(
            "this store can't hold records for a positions-only reader",
        ))
    }

    /// Ends `holder`'s hold after the store's release grace, so a replay right after the reader
    /// ended still finds its records. A holder the store doesn't know is no failure.
    async fn release_floor(&self, chain: &ChainId, topic: &str, holder: &str) -> StreamResult<()> {
        let _ = (chain, topic, holder);
        Err(crate::StreamError::unsupported(
            "this store can't hold records for a positions-only reader",
        ))
    }

    /// Closes one topic of a chain in the store, then tells the topic's notifier on the server,
    /// which completes every attached callback with the outcome. The store closes first, so a
    /// reader the completion reaches always finds the stream's end. A store that sends no
    /// notifications only closes the topic. Fails as refused when the server will never take the
    /// close, and with another kind when trying again may help.
    async fn close_stream(
        &self,
        chain: &ChainId,
        topic: &str,
        close: StoredClose,
    ) -> StreamResult<()> {
        self.close_topic_with(chain, topic, close).await
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
