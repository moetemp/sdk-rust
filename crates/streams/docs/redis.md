# The Redis stream store

The store on the application's Redis.

Records live in Redis and never pass through Temporal. The key layout, the stored values and
the Lua scripts are the ones the SDKs used before the store moved into Core, apart from the
append's topic check and the arm reports, so a stream one wrote stays readable and writable by
the other.

**Appends.** One script writes a whole batch, so a reader never sees part of one. The topic's
meta hash keeps one field per producer attempt: the newest batch's first sequence, its record
count, its first and last entry ids, and lang's digest of it in hex. The script refuses a
new batch once the chain or the topic is closed, in the same step as the write, so no batch
lands after a close returned. That topic check and the arm report differ from the SDKs' script.

**Arms.** A reader that sleeps until it hears of progress arms the topic when its read ends
with nothing. The topic's meta holds two more fields. `arm_seq` counts the arms the topic ever
took, and `arm` holds the newest one's generation while a reader waits. One script finds the
log empty after the read's position and sets `arm` to the next `arm_seq`, or answers the newer
record. The append script answers the topic's `arm` with its positions, so a producer
notifies only while a reader waits. Once its notification landed, the producer
deletes `arm` only when it still holds the generation that notification carried, so a reader
that armed again keeps its arm. An arm on a topic with no meta creates the meta with the
tombstone's expiry, and an arm on a meta leaves its expiry as the last write set it. The
append script's hash differs from the SDKs', so a server's script cache entry for an SDK's
provider no longer serves Core. The stored format only gains the two fields, and
an SDK's provider ignores them.

**Retired producers.** A field stays until a write's trim finds it older than the retention, so
a topic written by many short-lived producers holds one field per producer attempt of the last
retention window, about 235 bytes each. `RetireProducer` deletes one attempt's field with
`HDEL` and touches nothing else. A missing field or meta is no failure. Call it only once the
attempt's last append returned: a retry of an earlier batch would find no field to compare with
and land a second time. Readers lose nothing, since a store compares a batch with its own
attempt's field only and readers find a newer attempt from the records.

**Retention.** Every script that writes a log trims it with `XTRIM MINID` to the retention and
slides the expiry of the log and its meta, so a stream dies a retention after its last write
and nobody cleans up when the Workflow closes. The meta outlives its log by a 30-day grace, as
the tombstone that tells a log that expired from one that never existed.

**Deletes.** Deleting an owner's streams scans every primary for the Workflow id's keys. `SCAN`
makes no promise about keys that move to another node while it runs, so a delete during a
cluster reshard can miss some. Run it again once the reshard is done.

**Server.** Redis 7.0 or later is required, and the store refuses an older server when it
connects. At the same moment it reads `maxmemory-policy` on every primary and logs a warning
for anything but `noeviction`, since an evicting Redis drops whole stream keys under memory
pressure, losing records and their dedupe state.

**ACL.** Every key of a namespace's streams starts with `<prefix>:{<namespace>:`, each part
percent-encoded. A Redis ACL user for one namespace's applications, with the rules the
conformance suite runs as:

```text
ACL SETUSER streams-app on >secret resetkeys ~temporal-streams:{my-ns:* resetchannels -@all +evalsha +eval +script|load +multi +exec +xadd +xread +xrevrange +xrange +xtrim +xlen +hget +hset +hgetall +hincrby +hdel +hscan +rpush +lrange +exists +del +unlink +pexpire +pttl +time +info +config|get +ping +hello +client|setinfo +cluster|slots
```

A cluster client reads the topology with `CLUSTER SLOTS` first. An ACL user lives on one node,
so on a cluster the user must exist on every node. Deleting an owner's streams also needs
`+scan`. `+config|get` only reads
`maxmemory-policy`, and the store goes on when a server refuses it.

**Tests.** The Redis tests run when `STREAMS_REDIS_URL` names a standalone server and
`STREAMS_REDIS_CLUSTER_URL` a cluster seed. `scripts/redis-test-servers.sh` starts both. The
conformance suite runs on each, and once more as a user with exactly the ACL above. The
cross-implementation test also needs `STREAMS_BROOK_PY`, a Python SDK checkout whose provider
predates the move.
