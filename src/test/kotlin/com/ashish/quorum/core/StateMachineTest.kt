package com.ashish.quorum.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class StateMachineTest {

    @Test
    fun `ACQUIRE_LOCK creates a new lock`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        val lock = sm.getLock("res-1")
        assertEquals("client-A", lock?.clientId)
        assertEquals("token-1", lock?.lockToken)
        assertEquals(expiresAt, lock?.expiresAt)
    }

    @Test
    fun `ACQUIRE_LOCK fails if already held and not expired`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        // Try to acquire again with a new request timestamp that is BEFORE expiration
        sm.apply(index = 1, command = "ACQUIRE_LOCK res-1 client-B token-2 $expiresAt ${now + 1000}")
        
        val lock = sm.getLock("res-1")
        assertEquals("client-A", lock?.clientId) // Lock remains with A
    }

    @Test
    fun `ACQUIRE_LOCK succeeds if previous lock expired`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 1000
        
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        
        // Try to acquire again with a request timestamp that is AFTER the first expiration
        val newNow = now + 2000
        val newExpiresAt = newNow + 5000
        sm.apply(index = 1, command = "ACQUIRE_LOCK res-1 client-B token-2 $newExpiresAt $newNow")
        
        val lock = sm.getLock("res-1")
        assertEquals("client-B", lock?.clientId) // B gets the lock because A's expired
    }

    @Test
    fun `RELEASE_LOCK removes a held lock`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        sm.apply(index = 1, command = "RELEASE_LOCK res-1 client-A token-1")
        
        assertNull(sm.getLock("res-1"))
    }

    @Test
    fun `RELEASE_LOCK fails with incorrect token`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        sm.apply(index = 1, command = "RELEASE_LOCK res-1 client-A wrong-token")
        
        val lock = sm.getLock("res-1")
        assertEquals("client-A", lock?.clientId) // Still held by A
    }

    @Test
    fun `RENEW_LOCK extends the expiration`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        
        val newExpiresAt = now + 10000
        sm.apply(index = 1, command = "RENEW_LOCK res-1 client-A token-1 $newExpiresAt")
        
        val lock = sm.getLock("res-1")
        assertEquals(newExpiresAt, lock?.expiresAt)
    }

    @Test
    fun `NOOP does not affect state`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        sm.apply(index = 1, command = "NOOP")
        assertEquals("client-A", sm.getLock("res-1")?.clientId)
    }

    @Test
    fun `unknown command is skipped without throwing`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        sm.apply(index = 0, command = "ACQUIRE_LOCK res-1 client-A token-1 $expiresAt $now")
        sm.apply(index = 1, command = "BOGUS_COMMAND") 
        assertEquals("client-A", sm.getLock("res-1")?.clientId) 
    }

    @Test
    fun `snapshot returns a copy not a live reference`() {
        val sm = StateMachine()
        val now = System.currentTimeMillis()
        val expiresAt = now + 5000
        sm.apply(index = 0, command = "ACQUIRE_LOCK a client-A t1 $expiresAt $now")
        val snap = sm.snapshot()
        sm.apply(index = 1, command = "ACQUIRE_LOCK b client-B t2 $expiresAt $now") 
        
        assertEquals(1, snap.size)
        assertEquals(2, sm.snapshot().size)
    }
}
