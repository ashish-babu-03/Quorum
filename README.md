# Quorum

A distributed lock manager built on a self-implemented Raft-style consensus
core — leader election, log replication, majority-based commit, dynamic
cluster membership, and a conflict-safe client-facing lock API. Every
guarantee below has been verified live against a real 3+ node cluster,
not just asserted.

## Why this exists

Most backend portfolios show CRUD services. This shows something
different: a system where the hard part is agreeing on truth across
machines that can crash, restart, or go silent at any moment — the same
fundamental problem underneath etcd (which Kubernetes runs on) and
Kafka's controller election, built from scratch and proven correct under
real failure conditions, not just compiled successfully.

## What it does

- **Automatic leader election** with randomized timeouts and majority
  voting — no external coordinator, no manual failover
- **Log replication** with majority-based commit — the cluster keeps
  making progress even if a minority of nodes are slow or down
- **A real lock API** — `acquire` / `release` / `renew` / `status` —
  backed by the replicated log, so lock state survives a leader crash
  without loss
- **Lease expiry** — locks auto-release if a client dies without
  releasing, cleaned up via a leader-driven, safely-replicated sweep
- **Disk persistence** — term, vote history, and log survive a full
  process restart, not just an in-memory container restart
- **Dynamic cluster membership** — nodes can be added or removed at
  runtime, with a safety guard preventing two overlapping configuration
  changes from ever being in flight at once

## How to run

Requires Docker and Docker Compose, and internet access on your machine
to pull dependencies from Maven Central on first build.

```bash
docker compose up --build
```

Watch the logs for a leader election, then the HTTP API is available on
each node's `gRPC port + 1000` (e.g. node-1's gRPC is on `9091`, its
HTTP API is on `10091`).

### Try it

```bash
# Acquire a lock (redirects to the leader automatically if you hit a follower)
curl -X POST -d '{"clientId":"client-1","leaseDurationMs":30000}' \
  http://localhost:10091/locks/my-resource/acquire

# A second client trying the same resource gets a 409
curl -X POST -d '{"clientId":"client-2","leaseDurationMs":30000}' \
  http://localhost:10091/locks/my-resource/acquire

# Release it with the real token from the acquire response
curl -X POST -d '{"clientId":"client-1","lockToken":"<token>"}' \
  http://localhost:10091/locks/my-resource/release
```

### Watch a real failover

```bash
docker kill node-2   # or whichever node your logs show as leader
```

Within ~150-300ms the surviving nodes elect a new leader automatically —
watch it happen live in the logs, no manual intervention. Bring it back
with `docker start node-2` and it rejoins as a follower.

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/locks/{resourceId}/acquire` | Body: `{clientId, leaseDurationMs}`. `200` on success, `409` if held by someone else, `307` redirect if this node isn't leader. |
| `POST` | `/locks/{resourceId}/release` | Body: `{clientId, lockToken}`. `200` on success, `403` if token/client mismatch. |
| `POST` | `/locks/{resourceId}/renew` | Body: `{clientId, lockToken, expiresAt}`. Extends an existing lease. |
| `GET` | `/locks/{resourceId}/status` | Current holder and expiry, if any. |
| `POST` | `/cluster/add` | Body: `{nodeId, address}`. Adds a node to the cluster. `409` if another config change is already in flight. |
| `POST` | `/cluster/remove` | Body: `{nodeId}`. Removes a node from the cluster. Same in-flight guard applies. |

## Design decisions — the "why", not just the "what"

**Why randomized election timeouts, not a fixed value?**
If every follower used the same timeout, all of them would notice the
leader's silence at the same instant, all become candidates
simultaneously, and split every vote forever — a livelock. Randomizing
per node means one almost always times out first and wins cleanly.

**Why does a candidate need a strict majority, not just "the most votes"?**
Majority is what makes the whole system fault-tolerant. As long as more
than half the cluster is alive and reachable, progress can always
continue — and because any two majorities must overlap by at least one
node, two different nodes can never both win an election in the same
term.

**Why does the leader only need a majority ack to commit, not every node?**
This is what lets the cluster keep working even when a node is slow or
temporarily unreachable — proven live in this project's logs, where a
commit advanced on 2-of-3 acks without waiting on the third.

**Why is success/failure reported explicitly by the state machine,
instead of inferred from resulting state?**
Found the hard way, via manual testing: inferring "did my release
succeed" by checking whether the lock is now null or has a different
token is ambiguous — that same observable state also describes "someone
else already holds this lock, so my request was correctly rejected."
Both looked identical from the outside. The fix was making every command
application return an explicit typed result (`CommandResult.Success` /
`Failure`) that calling code checks directly, rather than guessing from
side effects. General lesson: never infer an outcome from state when the
operation itself can just report it.

**Why does lease expiry get replicated through the normal Raft log,
instead of each node independently deleting expired locks locally?**
If every node expired locks on its own local clock, nodes could disagree
about whether a lock is still held — especially under clock skew or a
network partition. Routing expiry through the same replicated,
majority-committed path as every other state change keeps the whole
cluster's view of lock state consistent, not just individually
plausible.

**Why does only the leader accept writes, with followers redirecting
instead of silently forwarding?**
Redirecting forces the client to know explicitly who the leader is,
avoiding a silent middle-man adding latency or masking which node
actually processed the request — an explicit contract is safer than an
invisible one.

**Why must only one cluster membership change be in flight at a time?**
Allowing two overlapping configuration changes could let two different,
non-overlapping sets of nodes each believe they're a valid majority at
the same time — which breaks the single most important guarantee in the
whole system. A `pendingConfigChangeIndex` guard, checked and set under
the same mutex as everything else, ensures a second change is rejected
with `409` until the first one has fully committed.

## Verified guarantees — proven live, not just asserted

- **Real 3-node failover**: killed the live leader mid-cluster,
  confirmed automatic re-election within milliseconds, confirmed the
  recovered node correctly rejoined and resynced.
- **Conflict-safe locking under real concurrency**: 50 simultaneous HTTP
  acquire requests fired at the exact same resource at the exact same
  time — exactly 1 succeeded, 49 correctly rejected with `409`.
- **The exact release-ambiguity bug, confirmed fixed**: a client
  attempting to release a lock it doesn't actually hold (using another
  client's valid token) is correctly rejected, verified against the
  precise scenario that originally exposed the bug.
- **Dynamic membership safety guard, confirmed live**: two `/cluster/add`
  requests fired back-to-back — one succeeded, the second was correctly
  rejected with `409` while the first was still in flight.
- **Disk persistence, confirmed (partly by accident)**: a cluster-wide
  `docker compose down`/`up` cycle reloaded prior term state from disk
  rather than resetting to zero — the exact behavior persistence is
  supposed to provide, observed directly while debugging an unrelated
  issue.

## Known limitations

Documenting these honestly rather than pretending they don't exist:

**No Pre-Vote phase.** While testing dynamic membership, a newly-added
node with an empty log repeatedly lost elections (correctly, since its
log wasn't up to date) but kept incrementing its term on every failed
attempt — each failed attempt still forced the real, stable leader to
step down on seeing a higher term, since term comparison alone can't
distinguish "a legitimate new leader" from "a node that can never
actually win." Watched this destabilize a healthy cluster in real time,
with terms climbing into the thousands before the disruptive node was
properly added through the safe membership-change path. This is exactly
the problem a Pre-Vote phase solves in the real Raft extension — a
candidate first asks "would you vote for me if I ran?" without
incrementing its real term, so a node that can't legitimately win can't
disrupt a stable leader. Not implemented here; documented as a real,
understood gap rather than silently worked around.

**Removed-node gRPC channels aren't explicitly torn down.** When a node
is removed via `/cluster/remove`, its entry is correctly cleared from
the active peer list — confirmed via a live sanity check showing normal
lock operations continuing correctly afterward — but the underlying gRPC
channel object isn't explicitly shut down, so it continues periodic
background reconnection attempts, visible as occasional harmless DNS
resolution warnings in the logs. Doesn't affect correctness. A complete
implementation would call `channel.shutdown()` in the removal handler.

## What I'd do differently

- Add the Pre-Vote phase described above — the single highest-value
  addition given what testing actually surfaced.
- Explicitly shut down gRPC channels on node removal.
- The current lazy + swept lease expiry (checked on-access, and cleaned
  proactively every ~5s by the leader) works well at this scale; a much
  larger cluster might want conflict-hint-based faster catch-up for
  followers that are far behind (the `conflictTerm`/`conflictIndex`
  optimization from the Raft paper, not implemented here — the simpler
  decrement-by-one baseline was used instead, deliberately, to keep the
  first working version provably correct before optimizing).