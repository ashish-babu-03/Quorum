package com.ashish.quorum.core

/**
 * Simple in-memory key-value store that serves as the state machine for the
 * Raft cluster. Milestone 3's lock API (ACQUIRE/RELEASE) will sit on top of this.
 *
 * WHY is this separate from RaftState?
 * RaftState is the CONSENSUS layer (term, log, votes). StateMachine is the
 * APPLICATION layer (the result of applying all committed operations in order).
 * Keeping them separate means:
 *   1. If you add log compaction (snapshotting) later, you snapshot the state
 *      machine independently from the Raft log — the snapshot IS the state
 *      machine at a given log index.
 *   2. The state machine never sees uncommitted entries — only entries that
 *      have already been confirmed by a majority are applied here.
 *
 * NOT thread-safe on its own. All calls must be made while holding RaftNode's
 * mutex — which is already true since applyCommittedEntries() holds the lock.
 */
class StateMachine {
    private val store: MutableMap<String, String> = mutableMapOf()

    /**
     * Apply one committed log entry to the state machine.
     *
     * Supported command formats:
     *   "SET <key> <value>"   — sets store[key] = value
     *   "DELETE <key>"        — removes key from store
     *   "NOOP"                — no-op (used by new leaders to commit prior entries
     *                          indirectly; has no effect on stored state)
     *
     * WHY must apply() never throw?
     * If an exception propagated out of apply(), the caller (applyCommittedEntries)
     * would leave lastApplied and commitIndex in an inconsistent state — fewer
     * entries applied than committed. On a follower this would mean the state
     * machine falls behind permanently. Unknown/malformed commands are logged and
     * skipped instead, which is safe: a skipped command is idempotent on replay.
     */
    fun apply(index: Long, command: String) {
        val parts = command.trim().split(" ", limit = 3)
        when (parts.getOrNull(0)?.uppercase()) {
            "SET" -> {
                val key = parts.getOrNull(1) ?: return logUnknown(index, command)
                val value = parts.getOrNull(2) ?: return logUnknown(index, command)
                store[key] = value
                println("[StateMachine] index=$index SET $key=$value")
            }
            "DELETE" -> {
                val key = parts.getOrNull(1) ?: return logUnknown(index, command)
                store.remove(key)
                println("[StateMachine] index=$index DELETE $key")
            }
            "NOOP" -> {
                // Leader no-op: commits prior entries indirectly, no state change.
                println("[StateMachine] index=$index NOOP (leader commit sentinel)")
            }
            else -> logUnknown(index, command)
        }
    }

    fun get(key: String): String? = store[key]

    /**
     * Returns a point-in-time copy of the entire store. Safe to return outside
     * the mutex because the copy is immutable once taken.
     */
    fun snapshot(): Map<String, String> = store.toMap()

    private fun logUnknown(index: Long, command: String) {
        println("[StateMachine] WARNING: unknown command at index=$index: '$command' — skipped")
    }
}
