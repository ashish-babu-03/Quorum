package com.ashish.raftlock.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Unit tests for StateMachine in isolation. These test the application layer
 * independently of the Raft consensus machinery — no coroutines, no gRPC, no mutex.
 */
class StateMachineTest {

    @Test
    fun `SET stores value and GET retrieves it`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "SET mykey myvalue")
        assertEquals("myvalue", sm.get("mykey"))
    }

    @Test
    fun `SET overwrites an existing key`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "SET k v1")
        sm.apply(index = 1, command = "SET k v2")
        assertEquals("v2", sm.get("k"))
    }

    @Test
    fun `DELETE removes a key`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "SET k v")
        sm.apply(index = 1, command = "DELETE k")
        assertNull(sm.get("k"))
    }

    @Test
    fun `DELETE on non-existent key does not throw`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "DELETE missing") // must not throw
        assertNull(sm.get("missing"))
    }

    @Test
    fun `NOOP does not affect state`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "SET k v")
        sm.apply(index = 1, command = "NOOP")
        assertEquals("v", sm.get("k"))
    }

    @Test
    fun `unknown command is skipped without throwing`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "SET k v")
        sm.apply(index = 1, command = "ACQUIRE lock1") // unknown for now — must not crash
        assertEquals("v", sm.get("k")) // previous state intact
    }

    @Test
    fun `snapshot returns a copy not a live reference`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "SET a 1")
        val snap = sm.snapshot()
        sm.apply(index = 1, command = "SET a 2") // mutate after snapshot
        // Snapshot must not reflect post-snapshot mutations
        assertEquals("1", snap["a"])
        assertEquals("2", sm.get("a"))
    }

    @Test
    fun `commands applied in order produce correct final state`() {
        val sm = StateMachine()
        sm.apply(index = 0, command = "SET lock:res1 node-1")
        sm.apply(index = 1, command = "SET lock:res2 node-2")
        sm.apply(index = 2, command = "DELETE lock:res1")
        sm.apply(index = 3, command = "SET lock:res1 node-3")

        assertNull(sm.get("nonexistent"))
        assertEquals("node-3", sm.get("lock:res1"))
        assertEquals("node-2", sm.get("lock:res2"))
    }
}
