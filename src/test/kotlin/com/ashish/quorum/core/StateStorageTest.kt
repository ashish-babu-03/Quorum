package com.ashish.quorum.core

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class StateStorageTest {

    @Test
    fun `save and load state correctly`() {
        val file = File.createTempFile("raft-state", ".json")
        file.deleteOnExit()

        val log = listOf(
            LogEntry(1, 0, "NOOP"),
            LogEntry(1, 1, "ACQUIRE_LOCK res client token 5000 12345"),
            LogEntry(2, 2, "SOME \"QUOTED\" STRING\nNEWLINE")
        )

        StateStorage.save(file, 2, "node-1", log)

        val loaded = StateStorage.load(file)
        
        assertEquals(2L, loaded?.first)
        assertEquals("node-1", loaded?.second)
        assertEquals(3, loaded?.third?.size)
        
        assertEquals(1L, loaded?.third?.get(0)?.term)
        assertEquals(0L, loaded?.third?.get(0)?.index)
        assertEquals("NOOP", loaded?.third?.get(0)?.command)

        assertEquals("ACQUIRE_LOCK res client token 5000 12345", loaded?.third?.get(1)?.command)
        assertEquals("SOME \"QUOTED\" STRING\nNEWLINE", loaded?.third?.get(2)?.command)
    }

    @Test
    fun `load from empty or missing file returns null`() {
        val file = File("does-not-exist.json")
        assertNull(StateStorage.load(file))
        
        val emptyFile = File.createTempFile("empty", ".json")
        emptyFile.deleteOnExit()
        assertNull(StateStorage.load(emptyFile))
    }
}
