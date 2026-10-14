use crate::{
    StreamResult,
    proto::{
        ChainId, DeleteOwnerRequest, DeleteOwnerResponse, StoreAppendRequest, StoreAppendResponse,
        StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse,
    },
};

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
    async fn append(&self, request: StoreAppendRequest) -> StreamResult<StoreAppendResponse>;

    /// Reads the records of a topic after a position, waiting up to the request's `wait` for one
    /// when none is there.
    ///
    /// Fails as expired when the position names a record retention dropped, or when retention
    /// dropped records the read had not delivered yet. Fails as not found when nothing is known
    /// about the topic and the position is not empty.
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

    /// Waits until the notifications this store sent are out. A store that sends none returns at
    /// once.
    async fn flush_notifications(&self) {}
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
