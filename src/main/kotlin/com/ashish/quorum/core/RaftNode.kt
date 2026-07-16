package com.ashish.quorum.core

import com.ashish.quorum.rpc.PeerClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * A single Raft node. Owns its own state and drives its own election timer
 * and (when leader) per-peer replication loops. All state mutation goes through
 * [mutex] because gRPC request handlers and the internal timer/replication
 * coroutines run concurrently and touch the same fields.
 */
class RaftNode(
    val nodeId: String,
    private val peers: Map<String, String>, // peerId -> "host:port"
    private val heartbeatIntervalMillis: Long = 75
) {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val mutex = Mutex()
    private val state = RaftState()

    // The application state machine. Sits above the Raft log — only committed
    // entries (those replicated to a majority) are ever applied here.
    private val stateMachine = StateMachine()

    private val peerClients: Map<String, PeerClient> =
        peers.mapValues { (_, address) -> PeerClient(address) }

    private lateinit var electionTimer: ElectionTimer
    private var heartbeatJob: Job? = null
    private var dummyJob: Job? = null

    // Cluster size = every peer + self. Majority is the smallest number
    // that is more than half — this is what lets the cluster tolerate
    // node failures: as long as a majority are alive and reachable, an
    // election (or a commit) can succeed.
    private val majority: Int
        get() = (peers.size + 1) / 2 + 1

    suspend fun start() {
        electionTimer = ElectionTimer(scope) { onElectionTimeout() }
        electionTimer.reset()
        log("started as FOLLOWER, cluster size=${peers.size + 1}, majority needed=$majority")
    }

    // ---- Election timeout: this node has heard nothing from a leader ----

    private suspend fun onElectionTimeout() {
        val termForThisElection: Long
        mutex.withLock {
            if (state.role == NodeRole.LEADER) return // leaders never self-time-out

            state.currentTerm += 1
            state.role = NodeRole.CANDIDATE
            state.votedFor = nodeId
            state.currentLeader = null
            termForThisElection = state.currentTerm
            log("election timeout -> became CANDIDATE for term $termForThisElection")

            // Reset again now: if this election also fails to reach majority
            // (e.g. a split vote), we need a fresh randomized timeout so we
            // (or someone else) tries again rather than hanging forever.
            electionTimer.reset()
        }

        runElection(termForThisElection)
    }

    private suspend fun runElection(termForThisElection: Long) {
        val lastLogIndex: Long
        val lastLogTerm: Long
        mutex.withLock {
            lastLogIndex = state.lastLogIndex()
            lastLogTerm = state.lastLogTerm()
        }

        var votesReceived = 1 // counts our own self-vote from onElectionTimeout

        // Fire all RequestVote calls concurrently — we must not let one slow
        // or dead peer block the others. Each PeerClient enforces its own
        // short RPC timeout independently of this election's timeout.
        val pending = peerClients.map { (peerId, client) ->
            scope.async {
                peerId to runCatching {
                    client.requestVote(
                        term = termForThisElection,
                        candidateId = nodeId,
                        lastLogIndex = lastLogIndex,
                        lastLogTerm = lastLogTerm
                    )
                }.getOrNull()
            }
        }

        for (deferred in pending) {
            val (peerId, response) = deferred.await()
            if (response == null) {
                log("no response from $peerId for term $termForThisElection (timeout or unreachable)")
                continue
            }

            mutex.withLock {
                if (response.term > state.currentTerm) {
                    // A peer is on a higher term than us — our election is
                    // stale no matter what else happens. Step down.
                    stepDownTo(response.term)
                    return@withLock
                }
                if (state.role != NodeRole.CANDIDATE || state.currentTerm != termForThisElection) {
                    // This election was already decided (we won, lost, or a
                    // newer election has started) — ignore late replies.
                    return@withLock
                }
                if (response.voteGranted) {
                    votesReceived += 1
                    log("vote granted by $peerId ($votesReceived/$majority needed), term $termForThisElection")
                    if (votesReceived >= majority) {
                        becomeLeader(termForThisElection)
                    }
                }
            }
        }
    }

    /** Caller must already hold [mutex]. */
    private fun becomeLeader(termForThisElection: Long) {
        if (state.role != NodeRole.CANDIDATE || state.currentTerm != termForThisElection) return
        state.role = NodeRole.LEADER
        state.currentLeader = nodeId
        electionTimer.cancel() // leaders don't run an election timer

        // Initialize per-peer volatile leader state (re-initialized on EVERY
        // becomeLeader — these are never persisted).
        //
        // nextIndex[peer] = lastLogIndex + 1: optimistic guess — "I assume you're
        //   fully caught up; I'll start by sending you anything after this point."
        //   If the peer is behind, its AppendEntries reply will be success=false,
        //   and we'll decrement nextIndex until we find where the logs agree.
        //
        // matchIndex[peer] = -1: conservative — "I haven't confirmed anything about
        //   your log yet." matchIndex only advances on a SUCCESS response. The commit
        //   rule uses matchIndex (not nextIndex), so we never commit based on what
        //   we optimistically hope a peer has.
        val initialNextIndex = state.lastLogIndex() + 1
        for (peerId in peers.keys) {
            state.nextIndex[peerId] = initialNextIndex
            state.matchIndex[peerId] = -1L
        }

        // Append a no-op entry in the new leader's own term as the very first act.
        //
        // WHY: A newly elected leader may have uncommitted entries from a PREVIOUS
        // leader's term already in its log. The Raft paper (§5.4.2, "Figure 8")
        // proves it is UNSAFE to commit those old entries by counting replicas in
        // the current term — because a majority storing an old-term entry doesn't
        // guarantee a later leader couldn't have overwritten it on a different majority.
        //
        // The fix: the new leader appends a NOOP in ITS OWN TERM. Once the NOOP is
        // committed (replicated to majority in currentTerm, which IS safe to count),
        // the log-matching invariant guarantees that any node storing the NOOP also
        // stores all prior entries. So the NOOP's commit implicitly commits all prior
        // entries — without having to count replicas for each old-term entry separately.
        //
        // After this add, nextIndex[peer] correctly points to the no-op's index,
        // so the very first replication round will send it to all followers.
        val noopIndex = state.lastLogIndex() + 1
        state.log.add(LogEntry(term = termForThisElection, index = noopIndex, command = "NOOP"))

        log("WON election for term $termForThisElection -> became LEADER, appended NOOP at index=$noopIndex")
        startReplication()
        startDummyCommandLoop()
    }

    // ---- Per-peer replication loops ----

    private fun startDummyCommandLoop() {
        dummyJob?.cancel()
        dummyJob = scope.launch {
            var count = 1
            while (true) {
                delay(3000)
                try {
                    // Temporarily added for Milestone 2 visibility
                    // TODO: Remove this once Milestone 3 adds real HTTP client API
                    submitCommand("test-command-${count++}")
                } catch (e: Exception) {
                    // expected if we step down mid-loop
                }
            }
        }
    }

    // One dedicated coroutine per peer, replacing the old single shared heartbeat.
    //
    // WHY per-peer instead of one shared 75ms tick?
    // nextIndex and matchIndex are inherently per-follower. A follower that's far
    // behind (e.g. crashed and restarted) may need many rapid retries to catch up,
    // while another peer is already current. Coupling them to a shared clock would
    // artificially throttle catch-up to one attempt per 75ms tick. Per-peer loops
    // also mean one unreachable peer never delays progress for the others, and the
    // retry logic (decrement nextIndex, retry immediately) is local to each loop.
    //
    // All per-peer coroutines are children of heartbeatJob, so cancelling heartbeatJob
    // (which stepDownTo() does) cancels all of them atomically via structured concurrency.
    private fun startReplication() {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            for ((peerId, client) in peerClients) {
                launch { replicateToPeer(peerId, client) }
            }
        }
    }

    private suspend fun replicateToPeer(peerId: String, client: PeerClient) {
        while (true) {
            // --- Snapshot all needed state under the lock ---
            //
            // We MUST NOT hold the mutex during the RPC itself. The gRPC call may
            // take up to rpcTimeoutMillis (100ms), and holding the lock that long
            // would block every incoming RPC handler (RequestVote, AppendEntries)
            // on this node for the entire network round-trip — effectively freezing
            // the node. Pattern: read what we need, release lock, do I/O, re-acquire
            // lock to process the result.
            val term: Long
            val prevLogIndex: Long
            val prevLogTerm: Long
            val entries: List<LogEntry>
            val leaderCommit: Long
            val hasNewEntries: Boolean

            mutex.withLock {
                if (state.role != NodeRole.LEADER) return // no longer leader: exit loop

                val nextIdx = state.nextIndex[peerId] ?: return
                prevLogIndex = nextIdx - 1

                // prevLogIndex=-1 means "send me everything from the very beginning."
                // In that case prevLogTerm is irrelevant (there's no prior entry to match),
                // and the follower accepts unconditionally if its own log is also empty up to that point.
                prevLogTerm = if (prevLogIndex >= 0) {
                    state.log.getOrNull(prevLogIndex.toInt())?.term ?: 0L
                } else 0L

                // Batch at most 50 entries per RPC. Avoids sending one huge message
                // to a very stale follower — keeps individual RPCs bounded in size
                // and lets the follower apply + ack in increments.
                entries = state.log.drop(nextIdx.toInt()).take(50)
                hasNewEntries = entries.isNotEmpty()
                leaderCommit = state.commitIndex
                term = state.currentTerm
            }

            // --- RPC call (lock released) ---
            val response = runCatching {
                client.appendEntries(
                    term = term,
                    leaderId = nodeId,
                    prevLogIndex = prevLogIndex,
                    prevLogTerm = prevLogTerm,
                    entries = entries,
                    leaderCommit = leaderCommit
                )
            }.getOrNull()

            if (response == null) {
                // Peer is unreachable or timed out. Back off — don't spin on a dead
                // peer. The peer's own election timer handles leader detection; we'll
                // retry on the next heartbeat interval.
                log("no response from $peerId — retrying after ${heartbeatIntervalMillis}ms")
                delay(heartbeatIntervalMillis)
                continue
            }

            // --- Handle response under lock ---
            var retryImmediately = false
            mutex.withLock {
                // Guard: we may have stepped down between the RPC and now. A response
                // arriving after we've moved to a new term could advance matchIndex or
                // commitIndex based on stale information — completely ignore it.
                if (state.role != NodeRole.LEADER || state.currentTerm != term) return@withLock

                if (response.term > state.currentTerm) {
                    // Peer knows about a more recent term — we're a stale leader.
                    // A newer election happened without us; step down immediately.
                    stepDownTo(response.term)
                    return@withLock
                }

                if (response.success) {
                    // Peer accepted all entries we sent. Update its tracking:
                    //   matchIndex = prevLogIndex + count(entries sent)
                    //             = highest index now confirmed on that peer
                    //   nextIndex  = matchIndex + 1
                    //
                    // We take max() with the existing matchIndex. Although out-of-order
                    // responses are rare (gRPC is ordered per-connection), being defensive
                    // ensures matchIndex never regresses — regression would cause incorrect
                    // commit decisions.
                    val newMatchIndex = prevLogIndex + entries.size
                    if (newMatchIndex > (state.matchIndex[peerId] ?: -1L)) {
                        state.matchIndex[peerId] = newMatchIndex
                        state.nextIndex[peerId] = newMatchIndex + 1
                        log("$peerId acked up to index=$newMatchIndex (nextIndex=${newMatchIndex + 1})")
                        tryAdvanceCommitIndex()
                    }
                    // If we sent entries AND there are more entries still waiting,
                    // loop back immediately rather than sleeping the full heartbeat
                    // interval — don't throttle active replication bursts.
                    retryImmediately = hasNewEntries &&
                        (state.nextIndex[peerId] ?: 0L) <= state.lastLogIndex()
                } else {
                    // Peer rejected: log inconsistency at prevLogIndex/prevLogTerm.
                    // Back nextIndex up by 1 (Option A — the Raft paper's baseline).
                    //
                    // On the next iteration we'll send prevLogIndex one step earlier,
                    // repeating until the follower's log agrees with ours at prevLogIndex.
                    // This is O(divergence-length) round-trips in the worst case, which is
                    // acceptable for a 3-node cluster. Option B (follower-accelerated rollback
                    // via conflictTerm/conflictIndex hints) reduces this to O(1) round-trips
                    // and is the standard optimization for multi-hundred-entry divergences.
                    val currentNext = state.nextIndex[peerId] ?: 0L
                    state.nextIndex[peerId] = maxOf(0L, currentNext - 1)
                    log("$peerId rejected (conflict): nextIndex backed up to ${state.nextIndex[peerId]}")
                    retryImmediately = true // catch-up: don't wait for heartbeat interval
                }
            }

            if (!retryImmediately) {
                delay(heartbeatIntervalMillis)
            }
            // retryImmediately=true: loop back at once with updated nextIndex
        }
    }

    // ---- Commit index advancement ----

    /**
     * Scans for the highest log index N where ALL of the following hold:
     *   1. N > commitIndex (we're actually advancing)
     *   2. log[N].term == currentTerm   (the §5.4.2 safety rule — see below)
     *   3. A majority of nodes have confirmed N: self (always) + peers with matchIndex >= N
     *
     * If such an N exists, advances commitIndex to N and applies all newly committed
     * entries to the state machine.
     *
     * Caller must already hold [mutex].
     *
     * WHY must log[N].term == currentTerm (condition 2)?
     * This is the subtlest invariant in Raft (§5.4.2, "Figure 8 scenario").
     *
     * Imagine a new leader (term=4) has an entry at index 5 from term=2 (carried
     * over from an old leader). Even if a current majority stores that entry, it
     * is UNSAFE to commit it by counting replicas — a different node elected in
     * term=4 with a more recent log could legally overwrite it on those same nodes
     * during a concurrent partition scenario.
     *
     * The safe rule: a leader ONLY directly commits entries from its own current term.
     * Older entries are committed INDIRECTLY: once the current-term entry at index 6
     * is committed, log-matching guarantees every node that has index 6 also has
     * index 5 — so index 5 is effectively committed too, just not by the counting
     * rule alone. The no-op appended in becomeLeader() is specifically designed to
     * trigger this indirect commit of any leftover old-term entries.
     */
    private fun tryAdvanceCommitIndex() {
        val lastIdx = state.lastLogIndex()
        // Scan highest-to-lowest: stop at the first N that satisfies all conditions.
        // This gives us the highest possible new commitIndex in one pass.
        for (n in lastIdx downTo (state.commitIndex + 1)) {
            val entryTerm = state.log.getOrNull(n.toInt())?.term ?: continue
            if (entryTerm != state.currentTerm) continue // safety: skip old-term entries

            // Count replicas: self (leader always has it) + peers that have confirmed >= N
            val replicaCount = 1 + state.matchIndex.values.count { it >= n }
            if (replicaCount >= majority) {
                log("commitIndex ${state.commitIndex} -> $n " +
                    "(replicated on $replicaCount/${peers.size + 1} nodes)")
                state.commitIndex = n
                applyCommittedEntries()
                break // highest N found; entries below are committed implicitly
            }
        }
    }

    // ---- State machine application ----

    /**
     * Applies all log entries from [lastApplied + 1] through [commitIndex] to
     * the state machine, advancing [lastApplied] after each one.
     *
     * Caller must already hold [mutex].
     *
     * WHY sequential and in strict index order?
     * The state machine is a deterministic function of the command sequence.
     * Every node applying the same commands in the same order will arrive at
     * the same state — this is Raft's core consistency guarantee. Skipping an
     * index or applying out of order would cause permanent state divergence
     * across nodes, which is exactly the problem Raft is designed to prevent.
     */
    private fun applyCommittedEntries() {
        while (state.lastApplied < state.commitIndex) {
            state.lastApplied += 1
            val entry = state.log.getOrNull(state.lastApplied.toInt()) ?: break
            stateMachine.apply(entry.index, entry.command)
        }
    }

    // ---- Incoming RPC handlers, called by RaftGrpcService on the server side ----

    suspend fun handleRequestVote(
        term: Long,
        candidateId: String,
        lastLogIndex: Long,
        lastLogTerm: Long
    ): Pair<Long, Boolean> = mutex.withLock {
        if (term > state.currentTerm) {
            stepDownTo(term)
        }

        if (term < state.currentTerm) {
            log("rejected vote for $candidateId: their term $term < our term ${state.currentTerm}")
            return@withLock state.currentTerm to false
        }

        val alreadyVotedForSomeoneElse = state.votedFor != null && state.votedFor != candidateId
        val candidateLogIsAtLeastAsUpToDate =
            lastLogTerm > state.lastLogTerm() ||
                (lastLogTerm == state.lastLogTerm() && lastLogIndex >= state.lastLogIndex())

        val grant = !alreadyVotedForSomeoneElse && candidateLogIsAtLeastAsUpToDate
        if (grant) {
            state.votedFor = candidateId
            electionTimer.reset() // heard from a legitimate candidate this term
            log("granted vote to $candidateId for term $term")
        } else {
            log("rejected vote for $candidateId: alreadyVotedFor=${state.votedFor}, logUpToDate=$candidateLogIsAtLeastAsUpToDate")
        }
        state.currentTerm to grant
    }

    suspend fun handleAppendEntries(
        term: Long,
        leaderId: String,
        prevLogIndex: Long,
        prevLogTerm: Long,
        entries: List<LogEntry>,
        leaderCommit: Long
    ): Pair<Long, Boolean> = mutex.withLock {

        // Step 1: Stale leader — reject and tell it our term so it steps down.
        if (term < state.currentTerm) {
            return@withLock state.currentTerm to false
        }

        // Step 2: Valid message from current or newer leader. Update role/term as needed.
        if (term > state.currentTerm || state.role != NodeRole.FOLLOWER) {
            stepDownTo(term)
        }
        state.currentLeader = leaderId
        electionTimer.reset() // valid heartbeat/append from the current leader — suppress election

        // Step 3: Log consistency check — the log-matching invariant (§5.3).
        //
        // prevLogIndex and prevLogTerm describe the entry IMMEDIATELY BEFORE the
        // first new entry the leader wants us to append. We verify our log contains
        // that exact entry (same index AND same term) before accepting anything new.
        //
        // WHY is one-entry verification sufficient?
        // The log-matching invariant states: if two logs agree at index N (same term),
        // they agree on ALL entries up to N. This is proven inductively — AppendEntries
        // always checks the entry before the new batch, so every successful append
        // extends agreement one step. Verifying just prevLogIndex is enough to guarantee
        // our entire log up to prevLogIndex matches the leader's.
        //
        // If the check fails, we reject. The leader will decrement nextIndex[us] and
        // retry from one entry earlier, repeating until it finds a prevLogIndex where
        // our logs agree — then it'll send us all the entries from there onwards.
        if (prevLogIndex >= 0) {
            val entryAtPrev = state.log.getOrNull(prevLogIndex.toInt())
            if (entryAtPrev == null || entryAtPrev.term != prevLogTerm) {
                log("rejected AppendEntries from $leaderId: mismatch at prevLogIndex=$prevLogIndex " +
                    "(expected term=$prevLogTerm, have=${entryAtPrev?.term ?: "nothing"})")
                return@withLock state.currentTerm to false
            }
        }

        // Step 4: Merge incoming entries, resolving any conflicts.
        //
        // Three cases for each incoming entry:
        //   a. We already have the same entry (index + term match) — skip.
        //      AppendEntries is idempotent by design; a retried RPC must not
        //      corrupt the log by double-appending.
        //   b. We have a DIFFERENT entry at that index (same index, different term) —
        //      truncate from that index onwards and append the leader's version.
        //      Any entry with a different term is wrong (the leader is the authority).
        //      And because of log-matching, EVERYTHING after the conflict is also
        //      suspect — so we delete the entire suffix, not just the one entry.
        //   c. We have nothing at that index — append directly.
        for (entry in entries) {
            val idx = entry.index.toInt()
            val existing = state.log.getOrNull(idx)
            when {
                existing == null -> {
                    // Gap-free append guaranteed by Raft: entries always arrive
                    // in consecutive order after a successful prevLogIndex check.
                    state.log.add(entry)
                }
                existing.term != entry.term -> {
                    log("conflict at index=${entry.index}: truncating " +
                        "(our term=${existing.term}, leader's term=${entry.term})")
                    while (state.log.size > idx) state.log.removeAt(state.log.size - 1)
                    state.log.add(entry)
                }
                // existing.term == entry.term: identical entry already present — skip
            }
        }

        // Step 5: Advance our commitIndex from the leader's leaderCommit field.
        //
        // leaderCommit tells us the highest index the leader has committed. We can
        // safely advance our own commitIndex to min(leaderCommit, lastLogIndex):
        //   - The min() guards against a leaderCommit that's ahead of what this
        //     specific AppendEntries batch actually delivered. We can only commit
        //     what we've actually stored.
        //   - Once committed, we apply those entries to the state machine in order.
        if (leaderCommit > state.commitIndex) {
            state.commitIndex = minOf(leaderCommit, state.lastLogIndex())
            applyCommittedEntries()
        }

        state.currentTerm to true
    }

    // ---- Command submission ----

    /**
     * Appends a new command to the leader's log and returns the log index at
     * which it was stored.
     *
     * This method only writes to the local log. The per-peer replication loops
     * running in [startReplication] pick up new entries automatically on their
     * next iteration — no explicit "wake up and replicate" signal is needed,
     * because each loop checks state.log from nextIndex onwards on every pass.
     *
     * The returned index can be used with [isCommitted] to determine when the
     * entry has been replicated to a majority and applied to the state machine.
     * In Milestone 3, this will be replaced by a proper future/channel so the
     * HTTP handler can wait for commit before responding to the client.
     *
     * Throws [IllegalStateException] if this node is not the leader — callers
     * that receive this should redirect to [state.currentLeader].
     */
    suspend fun submitCommand(command: String): Long = mutex.withLock {
        check(state.role == NodeRole.LEADER) {
            "submitCommand rejected: not the leader (current leader=${state.currentLeader})"
        }
        val index = state.lastLogIndex() + 1
        state.log.add(LogEntry(term = state.currentTerm, index = index, command = command))
        log("submitCommand: appended '$command' at index=$index term=${state.currentTerm}")
        index
    }

    /**
     * Returns true if the entry at [logIndex] has been committed (replicated to
     * a majority). Applied to the state machine immediately after commit.
     */
    suspend fun isCommitted(logIndex: Long): Boolean = mutex.withLock {
        state.commitIndex >= logIndex
    }

    /**
     * Returns a point-in-time snapshot of the state machine contents.
     * Useful for testing and for Milestone 3's read endpoints.
     */
    suspend fun getStateMachineSnapshot(): Map<String, String> = mutex.withLock {
        stateMachine.snapshot()
    }

    /** Caller must already hold [mutex]. */
    private fun stepDownTo(newTerm: Long) {
        val wasLeader = state.role == NodeRole.LEADER
        state.currentTerm = newTerm
        state.role = NodeRole.FOLLOWER
        state.votedFor = null
        if (wasLeader) {
            // Cancels all per-peer replication coroutines via structured concurrency.
            heartbeatJob?.cancel()
            dummyJob?.cancel()
            log("stepping down from LEADER — saw higher term $newTerm")
        }
        electionTimer.reset()
    }

    private fun log(message: String) {
        println("[$nodeId][term=${state.currentTerm}][role=${state.role}] $message")
    }
}
