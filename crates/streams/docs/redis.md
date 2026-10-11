# The Redis stream store

The store on the application's Redis.

Records live in Redis and never pass through Temporal. The key layout, the stored values and
the Lua scripts are the ones the SDKs used before the store moved into Core, apart from the
append's topic check, so a stream one wrote stays readable and writable by the other.

**Appends.** One script writes a whole batch, so a reader never sees part of one. The topic's
meta hash keeps one field per producer attempt: the newest batch's first sequence, its record
count, its first and last entry ids, and lang's digest of it in hex. The script refuses a
new batch once the chain or the topic is closed, in the same step as the write, so no batch
lands after a close returned. Only that topic check differs from the SDKs' script.

**Retention.** Every script that writes a log trims it with `XTRIM MINID` to the retention and
slides the expiry of the log and its meta, so a stream dies a retention after its last write
and nobody cleans up when the Workflow closes. The meta outlives its log by a 30-day grace, as
the tombstone that tells a log that expired from one that never existed.

**Stages.** A Workflow's own output is staged next to its logs and moved into them by one
script, so readers see a Workflow Task's records together or not at all.

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
ACL SETUSER streams-app on >secret resetkeys ~temporal-streams:{my-ns:* resetchannels -@all +evalsha +eval +script|load +multi +exec +xadd +xread +xrevrange +xrange +xtrim +xlen +hget +hset +hgetall +hincrby +hdel +hscan +sadd +smembers +rpush +lrange +exists +del +unlink +pexpire +pttl +time +info +config|get +ping +hello +client|setinfo +cluster|slots
```

A cluster client reads the topology with `CLUSTER SLOTS` first. An ACL user lives on one node,
so on a cluster the user must exist on every node. Deleting an owner's streams also needs
`+scan`. `+sadd` and `+smembers` keep and read each chain's topic set, which lists the chain's
topics when it ends, so a chain end can close every topic's notifier without a scan. Topics
written only by an SDK release from before the store moved into Core aren't in the set, so a
chain end doesn't close those notifiers. That gap covers only streams written before the upgrade.
`+config|get` only reads `maxmemory-policy`, and the store goes on when a server refuses it.

**Upgrading.** The ACL the Python SDK documented before the move has no `+sadd` or
`+smembers`. Appends and Workflow output keep working with it. The store skips the topic set and
logs a warning once. But a chain end then can't list the chain's topics, so callers of those
streams wait for the notifier's owner check. Widen the ACL before or with the upgrade.

**Tests.** The Redis tests run when `STREAMS_REDIS_URL` names a standalone server and
`STREAMS_REDIS_CLUSTER_URL` a cluster seed. `scripts/redis-test-servers.sh` starts both. The
conformance suite runs on each, and once more as a user with exactly the ACL above. The
cross-implementation test also needs `STREAMS_BROOK_PY`, a Python SDK checkout whose provider
predates the move.
