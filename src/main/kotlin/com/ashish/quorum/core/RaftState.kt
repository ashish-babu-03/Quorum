package com.ashish.quorum.core

data class LogEntry(
    val term: Long,
    val index: Long,
    val command: String
)

/**
 * Holds a single node's Raft state. Never mutate this directly from outside
 * RaftNode — every field here is only safe to touch while holding RaftNode's
 * mutex, since gRPC handlers and internal timers run on different coroutines.
 *
 * In a production system, currentTerm/votedFor/log would be fssynced to disk
 * before a vote or heartbeat response is ever sent — a node that crashes and
 * restarts must not forget who it voted for this term, or safety breaks.
 * Kept in-memory here for Milestone 1 clarity; disk persistence is a
 * Milestone 3 addition.
 */
class RaftState {
    var currentTerm: Long = 0
    var votedFor: String? = null
    val log: MutableList<LogEntry> = mutableListOf()

    var role: NodeRole = NodeRole.FOLLOWER
    var currentLeader: String? = null

    // commitIndex: the highest log index known to be committed (replicated to a
    // majority). Persistent in production; in-memory here. Initialized to -1
    // (sentinel for "nothing committed yet").
    var commitIndex: Long = -1

    // lastApplied: the highest log index applied to the state machine.
    // Always <= commitIndex. Separate from commitIndex because applying entries
    // is sequential and asynchronous — we commit (majority ack) the instant a
    // majority stores the entry, then apply entries to the state machine one at
    // a time in order, catching up from lastApplied+1 to commitIndex.
    var lastApplied: Long = -1

    // Tracks the log index of an uncommitted configuration change (ADD_NODE or REMOVE_NODE).
    // The Raft paper (§6) requires that only ONE configuration change can be in-flight
    // at a time. If two overlapping membership changes are allowed simultaneously,
    // they could create two different valid majorities at the exact same time, breaking
    // the system's core safety guarantee (a split-brain where two leaders can be elected).
    var pendingConfigChangeIndex: Long? = null

    // ---- Volatile leader state — re-initialized on EVERY becomeLeader() ----
    // These are NOT persistent: if the leader crashes, it rediscovers peer
    // progress by probing with AppendEntries on the next term.

    // nextIndex[peer]: the index of the next log entry to send to that peer.
    // Initialized optimistically to lastLogIndex+1 on election win — "I assume
    // you're caught up; I'll send you whatever comes next."
    // Decremented on each rejection (Option A) until we find where logs agree.
    val nextIndex: MutableMap<String, Long> = mutableMapOf()

    // matchIndex[peer]: the highest log index CONFIRMED to be stored on that peer.
    // Initialized conservatively to -1 — "I've confirmed nothing about your log yet."
    // Only advances on a SUCCESS response. The commit rule uses matchIndex (not
    // nextIndex) so we never commit based on what we *optimistically hope* a peer has.
    val matchIndex: MutableMap<String, Long> = mutableMapOf()

    fun lastLogIndex(): Long = log.lastOrNull()?.index ?: -1
    fun lastLogTerm(): Long = log.lastOrNull()?.term ?: 0
}
