# Quorum

Quorum — a distributed lock manager built on a self-implemented Raft-style consensus
core (leader election + heartbeat-based liveness). Multiple nodes coordinate
over gRPC to agree on cluster leadership without any external coordinator —
if the leader dies, a new one is elected automatically within a few hundred
milliseconds, with no manual intervention.

**Status: Milestone 1 — leader election is complete and working.**
Log replication and the client-facing lock API (`/acquire`, `/release`) are
Milestone 2, not yet built. See "What's next" below.

## Why this exists

Most backend portfolios show CRUD services. This shows something different:
a system where the *hard part* is agreeing on truth across machines that can
crash or go silent at any moment — the same fundamental problem underneath
etcd (which Kubernetes runs on) and Kafka's controller election.

## How to run it

Requires Docker and Docker Compose. Requires internet access on your machine
to pull dependencies from Maven Central on first build (this project was
written in a sandboxed environment without that access, so this is the
first real compile — if you hit a build error, the error message is the
next thing to fix, not a sign anything is fundamentally wrong).

```bash
docker compose up --build
```

You'll see three containers start, each logging its own state transitions:

```
[node-2][term=1][role=CANDIDATE] election timeout -> became CANDIDATE for term 1
[node-2][term=1][role=CANDIDATE] vote granted by node-1 (2/2 needed), term 1
[node-2][term=1][role=CANDIDATE] vote granted by node-3 (3/2 needed), term 1
[node-2][term=1][role=LEADER] WON election for term 1 -> became LEADER
```

### Watch a real failover

Kill whichever node won the first election:

```bash
docker kill node-2   # or whichever node's log shows it became LEADER
```

Within ~150-300ms, the surviving two nodes will notice heartbeats stopped,
one of them will time out first, and you'll see a fresh election in the
logs — with **zero manual intervention**. Bring the dead node back:

```bash
docker start node-2
```

It rejoins as a FOLLOWER and recognizes the new leader from its heartbeats.

## Design decisions (the "why", not just the "what")

**Why randomized election timeouts (150-300ms), not a fixed value?**
If every follower used the same timeout, all of them would notice the
leader's silence at the exact same instant, all become candidates
simultaneously, and split every vote forever — a livelock. Randomizing per
node means one almost always times out first and wins cleanly before a
competitor even starts.

**Why does a candidate need a strict majority, not just "the most votes"?**
Majority is what makes the whole system fault-tolerant. As long as more
than half the cluster is alive and reachable, an election can always
succeed — and because any two majorities must overlap by at least one node,
it's mathematically impossible for two different nodes to both win
election as leader in the same term.

**Why two separate timeouts (election timeout vs. RPC timeout)?**
The election timeout (`ElectionTimer`) is a silence detector — "how long
since I last heard from a leader." The RPC timeout (`PeerClient`,
100ms default) is a normal per-call network timeout — "how long do I wait
for one peer to answer one request." Conflating them would mean one slow
peer could block an entire election; keeping them separate means a
candidate simply treats a timed-out peer as "no vote from them" and moves
on with whatever responses it already has.

**Why does a node immediately step down on seeing a higher term?**
The term number is a logical clock. Any message carrying a higher term than
what a node currently knows means "there has been an election more recent
than what I'm aware of" — the node's own view of the world (including
believing itself to be leader) is stale and must be discarded immediately,
not negotiated.

**What happens if two followers time out within milliseconds of each
other (a split vote)?** Both become candidates for the same term, both
request votes, but each other node can only vote once per term — so the
five (or however many) available votes get divided between the two
candidates, and neither reaches majority. Both candidates' *next* election
timeout is freshly randomized, so this is very unlikely to repeat on the
next attempt — the system just tries again with a new, higher term.

## What's next (Milestone 2+, not yet built)

- **Real log replication**: currently `AppendEntries` is heartbeat-only.
  Milestone 2 adds actual log entries, `prevLogIndex`/`prevLogTerm`
  matching, and commit-on-majority-ack logic.
- **The actual lock API**: `POST /locks/{resourceId}/acquire`,
  `/release`, `/renew` — these will be client-facing HTTP endpoints on
  whichever node is currently leader, backed by the replicated log so
  lock state survives a leader crash without loss (this is what makes it
  "self-healing" — an accurate description once this lands, not a
  bolted-on buzzword).
- **Disk persistence**: `currentTerm`/`votedFor`/`log` need to survive a
  process restart, not just live in memory.
- **Load test**: concurrent lock-acquire attempts from multiple clients on
  the same resource, proving exactly one ever wins.

## What I'd do differently

Worth being honest about in any write-up or interview: this uses gRPC's
plaintext channel (no TLS) since it's designed for an internal cluster
network, not public exposure — a production version would add mTLS between
nodes. The 75ms heartbeat / 150-300ms election timeout ratio was chosen to
match typical Raft reference implementations but hasn't been load-tested
under real network jitter yet — that's part of the Milestone 2+ work above.
