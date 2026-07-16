package com.ashish.raftlock.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.random.Random

/**
 * Fires [onTimeout] if [reset] is not called again within a randomized
 * window (150-300ms by default).
 *
 * This is NOT a response deadline for any single RPC — it is a silence
 * detector. Every time a valid heartbeat/AppendEntries arrives from the
 * current leader, [reset] cancels the pending timeout and starts a fresh
 * one. The callback only ever fires when the leader has gone quiet for
 * the full randomized window.
 *
 * The randomization is the whole trick that keeps elections from
 * deadlocking: if every node used the same fixed timeout, all followers
 * would notice the leader's silence at the same instant, all become
 * candidates simultaneously, and split every vote forever. Randomizing
 * per-node means one node almost always times out a little before the
 * others, starts its election first, and usually wins cleanly before a
 * competing candidate even appears.
 */
class ElectionTimer(
    private val scope: CoroutineScope,
    private val minMillis: Long = 150,
    private val maxMillis: Long = 300,
    private val onTimeout: suspend () -> Unit
) {
    private var job: Job? = null

    fun reset() {
        job?.cancel()
        val delayMillis = Random.nextLong(minMillis, maxMillis)
        job = scope.launch {
            delay(delayMillis)
            // Launched as a SEPARATE child coroutine deliberately. onTimeout()
            // (onElectionTimeout in RaftNode) calls reset() on this same timer
            // as one of its first steps. If onTimeout() ran directly in this
            // coroutine, that reset() call would cancel `job` — which would be
            // this very coroutine, aborting onTimeout() mid-execution before
            // it ever gets to count votes. Running it as an independent child
            // means reset() only ever cancels the *next* pending delay, never
            // the callback that's currently in flight.
            scope.launch { onTimeout() }
        }
    }

    fun cancel() {
        job?.cancel()
        job = null
    }
}
