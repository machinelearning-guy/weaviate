# Shard Self-Recovery

When a node restarts and finds shard directories missing on disk that the
schema says it should be hosting, it pulls those shards from a healthy
peer using the same file-copy machinery as scale-out replication. The
hook fires only at startup; runtime shard creation (new collections,
empty-source replica adds) keeps today's "create empty dir" behavior.

Disabled by default. Operators opt in per the rollout discipline below.

## Enabling

Set on every node:

```
SELF_RECOVERY_ENABLED=true
SELF_RECOVERY_CONCURRENCY=10    # default; per-node parallelism
REPLICA_MOVEMENT_ENABLED=true                  # required for /replication/* observability
```

**Rollout discipline.** Mixed-version clusters with the flag on cause
RAFT FSM apply divergence (older nodes don't know the `SELF_RECOVERY`
transfer type and reject it). Same caveat as `REPLICA_MOVEMENT_ENABLED`.
**Upgrade every node first, then enable the flag.**

## Operator-visible state during recovery

| Surface | Signal |
|---|---|
| `GET /nodes?output=verbose` | shard reports `status: "RECOVERING"`, `loaded: false` |
| `GET /replication/replicate/list?targetNode=<self>` | in-flight `SELF_RECOVERY` op with state `REGISTERED`/`HYDRATING`/`FINALIZING`/`READY` |
| `/metrics` (Prometheus) | series listed below |
| Structured logs | `event=self_recovery.{started\|peer_probe\|op_registered\|completed\|failed\|empty_fallback\|accept_empty\|restart}` |

### Metrics

| Metric | Type | Labels | Question it answers |
|---|---|---|---|
| `weaviate_self_recovery_in_progress` | gauge | — | how many recoveries are running on this node? |
| `weaviate_self_recovery_started_total` | counter | `source_node` | how many were kicked off, by source peer? |
| `weaviate_self_recovery_completed_total` | counter | `result` (success\|failure\|cancelled) | terminal outcomes |
| `weaviate_self_recovery_duration_seconds` | histogram | `result` | end-to-end recovery time |
| `weaviate_self_recovery_no_data_empty_total` | counter | — | catastrophic-wipe occurrences (post-bootstrap; alert on this) |
| `weaviate_self_recovery_no_data_during_bootstrap_total` | counter | — | empty-fallback during the RAFT bootstrap window (likely a class added during this node's downtime — benign; informational) |
| `weaviate_self_recovery_unreachable_peer_total` | counter | `peer` | peer reachability problems |
| `weaviate_self_recovery_giveup_total` | counter | — | retries exhausted |
| `weaviate_self_recovery_accept_empty_total` | counter | — | operator escape-hatch invocations |

Per-(collection, shard) drill-down is available via `/replication/replicate/list`
and the structured logs.

### Suggested alerts

```promql
# catastrophic-wipe candidate — investigate immediately
rate(weaviate_self_recovery_no_data_empty_total[5m]) > 0

# recovery stuck for too long
weaviate_self_recovery_in_progress > 0  for: 1h

# retries exhausted — manual intervention required
rate(weaviate_self_recovery_giveup_total[15m]) > 0
```

## Operator escape hatches

| Endpoint | When to use |
|---|---|
| `POST /replication/replicate/{id}/cancel` | abandon one in-flight op (any transfer type) |
| `POST /debug/self-recovery/restart?collection=X&shard=Y` | abandon current SELF_RECOVERY attempt for the shard, erase partial `.recovering/` state, start fresh (probe re-randomises source peer selection) |
| `POST /debug/self-recovery/accept-empty?collection=X&shard=Y` | declare "no recoverable data exists, accept empty shard". Confirm via metrics/logs that all peers report no data first. |

## Runbook: `no_data_empty_total > 0` after a restart

This counter increments when **all probed peers definitively reported no
data** for a shard the orchestrator was trying to recover. Either:

1. **Catastrophic full-cluster wipe** (every replica of the shard lost data).
2. **Genuinely-new shard added while the node was offline** (e.g. an
   empty-source `AddReplicaToShard` applied during the node's catchup).
   Benign.

To distinguish, find the structured log line:

```
event=self_recovery.empty_fallback collection=... shard=... probed_peers=[...]
```

- If you recognise the (collection, shard) as one with real data, this
  is case 1 — restore from backup.
- If it's a recently-created or empty replica, case 2 — no action.

To restore from backup: shut down (or quiesce) the node, run the Backup
module's restore for the affected collection, restart.

## Limitations

**Recovery is only triggered when the rejoining node receives a RAFT
snapshot from a peer.** This is the common case in any cluster with
non-trivial activity: RAFT defaults take a snapshot every 120 seconds
once the log has accumulated 8192 entries (configurable via
`RAFT_SNAPSHOT_INTERVAL` and `RAFT_SNAPSHOT_THRESHOLD`).

If a wiped node rejoins a cluster that has *never* taken a snapshot
(very-low-traffic deployment, no schema changes since boot), the
leader will replay the log instead of installing a snapshot. In that
case the SELF_RECOVERY hook does not fire and the node creates empty
shards on disk just like today.

This trade-off keeps the new-class creation path on a healthy cluster
fast — the orchestrator never speculatively wraps a shard whose data
the cluster also lacks. The wiped-without-snapshot scenario is the
explicit cost.

**Mitigation if you operate a long-quiet cluster and want recovery to
be guaranteed:** lower `RAFT_SNAPSHOT_THRESHOLD` (e.g. to 16) so a
snapshot is taken after the first batch of schema changes; check
`weaviate_raft_last_snapshot_index` in Prometheus to confirm a
snapshot exists before trusting recovery on rejoin.

## Maintenance mode

When the node is in maintenance mode, `Submit` becomes a no-op — the
orchestrator does not start new recoveries. Already-running recoveries
run to completion. To pause an in-flight one, cancel via the endpoint
above.

## Downgrade safety

If a node is downgraded to a binary without SELF_RECOVERY support, any
leftover `<shard>.recovering/` directories sit unused on disk until
re-upgrade (which sweeps them at startup). To clean manually:

```
rm -rf <data_root>/<collection>/<shard>.recovering
```

(Only safe when the live `<shard>/` exists alongside; otherwise the dir
holds an in-flight recovery to resume.)
