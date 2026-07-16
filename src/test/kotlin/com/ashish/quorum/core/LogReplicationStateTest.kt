package com.ashish.quorum.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for RaftState log operations and the replication-related state.
 * These test the data model in isolation — no RaftNode, no gRPC, no coroutines.
 *
 * The goal is to verify the invariants that RaftNode relies on when making
 * commit and log-conflict decisions.
 */
class LogReplicationStateTest {

    // ---- lastLogIndex / lastLogTerm ----

    @Test
    fun `empty log returns sentinel index and term`() {
        val state = RaftState()
        assertEquals(-1L, state.lastLogIndex())
        assertEquals(0L, state.lastLogTerm())
    }

    @Test
    fun `lastLogIndex and lastLogTerm reflect the tail entry`() {
        val state = RaftState()
        state.log.add(LogEntry(term = 1, index = 0, command = "SET a 1"))
        state.log.add(LogEntry(term = 2, index = 1, command = "SET b 2"))
        assertEquals(1L, state.lastLogIndex())
       assertEquals(2L, state.lastLogTerm())
    }

    // ---- nextIndex / matchIndex initialization ----

    @Test
    fun `nextIndex and matchIndex start empty`() {
        val state = RaftState()
        assertTrue(state.nextIndex.isEmpty())
        assertTrue(state.matchIndex.isEmpty())
    }

    @Test
    fun `nextIndex and matchIndex can be independently set per peer`() {
        val state = RaftState()
        state.nextIndex["node-2"] = 5L
        state.nextIndex["node-3"] = 3L
        state.matchIndex["node-2"] = 4L
        state.matchIndex["node-3"] = 2L

        assertEquals(5L, state.nextIndex["node-2"])
        assertEquals(3L, state.nextIndex["node-3"])
        assertEquals(4L, state.matchIndex["node-2"])
        assertEquals(2L, state.matchIndex["node-3"])
    }

    // ---- log conflict resolution logic (simulated inline) ----

    @Test
    fun `conflict truncation removes suffix from conflict point`() {
        // Simulate the follower truncation logic from handleAppendEntries.
        // Follower log: [t1@0, t1@1, t1@2]  (entries at indices 0,1,2 from term 1)
        // Leader sends: entry at index=1 with term=2 (conflict!)
        // Expected result: follower truncates from index 1 onwards, appends leader's entry.
        val log = mutableListOf(
            LogEntry(term = 1, index = 0, command = "A"),
            LogEntry(term = 1, index = 1, command = "B"), // will be overwritten
            LogEntry(term = 1, index = 2, command = "C")  // will be deleted
        )
        val conflictEntry = LogEntry(term = 2, index = 1, command = "B-leader")

        val idx = conflictEntry.index.toInt()
        val existing = log.getOrNull(idx)
        if (existing != null && existing.term != conflictEntry.term) {
            while (log.size > idx) log.removeAt(log.size - 1)
            log.add(conflictEntry)
        }

        assertEquals(2, log.size) // indices 0 and 1 only
        assertEquals(LogEntry(term = 1, index = 0, command = "A"), log[0])
        assertEquals(LogEntry(term = 2, index = 1, command = "B-leader"), log[1])
    }

    @Test
    fun `identical entry at same index is skipped (idempotent)`() {
        val log = mutableListOf(
            LogEntry(term = 1, index = 0, command = "A"),
            LogEntry(term = 1, index = 1, command = "B")
        )
        val sameEntry = LogEntry(term = 1, index = 1, command = "B") // identical

        val idx = sameEntry.index.toInt()
        val existing = log.getOrNull(idx)
        val sizeBeforeAttempt = log.size

        if (existing != null && existing.term != sameEntry.term) {
            while (log.size > idx) log.removeAt(log.size - 1)
            log.add(sameEntry)
        }
        // Identical entry: no change
        assertEquals(sizeBeforeAttempt, log.size)
        assertEquals(LogEntry(term = 1, index = 1, command = "B"), log[1])
    }

    // ---- commitIndex / majority commit simulation ----

    @Test
    fun `majority of 3-node cluster is 2`() {
        // peers.size = 2 (node-2, node-3); self = 1 total = 3
        val clusterSize = 3
        val peersSize = clusterSize - 1
        val majority = (peersSize + 1) / 2 + 1
        assertEquals(2, majority)
    }

    @Test
    fun `tryAdvanceCommitIndex logic only advances when majority confirmed`() {
        // Simulate: log has entries at 0, 1, 2 all in term=1.
        // matchIndex: node-2=1, node-3=-1 (only node-2 has confirmed up to 1)
        // Self (leader) always has all entries.
        // Majority = 2. At index=1: count = 1(self) + 1(node-2) = 2 >= 2. Commit!
        // At index=2: count = 1(self) + 0 = 1 < 2. Don't commit.
        val matchIndex = mapOf("node-2" to 1L, "node-3" to -1L)
        val majority = 2
        val currentTerm = 1L
        val log = listOf(
            LogEntry(term = 1, index = 0, command = "A"),
            LogEntry(term = 1, index = 1, command = "B"),
            LogEntry(term = 1, index = 2, command = "C")
        )
        val commitIndex = -1L

        var newCommitIndex = commitIndex
        for (n in log.lastIndex.toLong() downTo (commitIndex + 1)) {
            val entryTerm = log.getOrNull(n.toInt())?.term ?: continue
            if (entryTerm != currentTerm) continue
            val replicaCount = 1 + matchIndex.values.count { it >= n }
            if (replicaCount >= majority) {
                newCommitIndex = n
                break
            }
        }

        assertEquals(1L, newCommitIndex) // committed up to index 1
    }

    @Test
    fun `tryAdvanceCommitIndex does not commit old-term entries directly`() {
        // §5.4.2: leader (term=2) must NOT directly commit a term=1 entry even if
        // a majority has it. Only entries from currentTerm=2 can be directly committed.
        val matchIndex = mapOf("node-2" to 0L, "node-3" to 0L) // both have index 0
        val majority = 2
        val currentTerm = 2L // leader is in term 2
        val log = listOf(
            LogEntry(term = 1, index = 0, command = "OLD") // term=1 entry, majority stored it
        )
        val commitIndex = -1L

        var newCommitIndex = commitIndex
        for (n in log.lastIndex.toLong() downTo (commitIndex + 1)) {
            val entryTerm = log.getOrNull(n.toInt())?.term ?: continue
            if (entryTerm != currentTerm) continue // skip: old-term entry, not directly committable
            val replicaCount = 1 + matchIndex.values.count { it >= n }
            if (replicaCount >= majority) {
                newCommitIndex = n
                break
            }
        }

        // commitIndex must NOT advance — the old-term entry is not directly committable
        assertEquals(-1L, newCommitIndex)
    }

    // ---- lastApplied tracking ----

    @Test
    fun `lastApplied starts at -1 and commitIndex starts at -1`() {
        val state = RaftState()
        assertEquals(-1L, state.lastApplied)
        assertEquals(-1L, state.commitIndex)
    }

    @Test
    fun `lastApplied is always less than or equal to commitIndex after sequential application`() {
        // Simulate applyCommittedEntries: drive lastApplied from -1 to commitIndex
        val log = mutableListOf(
            LogEntry(term = 1, index = 0, command = "ACQUIRE_LOCK a client-1 t1 5000000000000 0"),
            LogEntry(term = 1, index = 1, command = "ACQUIRE_LOCK b client-1 t2 5000000000000 0"),
            LogEntry(term = 1, index = 2, command = "ACQUIRE_LOCK c client-1 t3 5000000000000 0")
        )
        var lastApplied = -1L
        val commitIndex = 2L
        val sm = StateMachine()

        while (lastApplied < commitIndex) {
            lastApplied += 1
            val entry = log.getOrNull(lastApplied.toInt()) ?: break
            sm.apply(entry.index, entry.command)
        }

        assertEquals(commitIndex, lastApplied)
        assertEquals("client-1", sm.getLock("a")?.clientId)
        assertEquals("client-1", sm.getLock("b")?.clientId)
        assertEquals("client-1", sm.getLock("c")?.clientId)
    }
}
