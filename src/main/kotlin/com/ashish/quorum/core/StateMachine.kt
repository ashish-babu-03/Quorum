package com.ashish.quorum.core

data class LockRecord(
    val resourceId: String,
    val clientId: String,
    val lockToken: String,
    val expiresAt: Long
)

sealed class CommandResult {
    data class Success(val message: String) : CommandResult()
    data class Failure(val reason: String) : CommandResult()
}

/**
 * The application layer State Machine for Quorum.
 * Stores lock records and processes deterministic commands from the Raft log.
 */
class StateMachine {
    private val locks = mutableMapOf<String, LockRecord>()
    private val commandResults = mutableMapOf<Long, CommandResult>()

    /**
     * Applies a committed command.
     * 
     * WHY does the command include a 'requestTimestamp'?
     * State machine application MUST be deterministic. If a follower crashes and
     * replays its log an hour later, it must reconstruct the exact same state. 
     * If apply() called System.currentTimeMillis() to check if a lock had expired,
     * an ACQUIRE that failed originally might succeed during replay. By encoding 
     * the leader's timestamp in the command, replay behavior is perfectly identical 
     * across all nodes and all times.
     */
    fun apply(index: Long, command: String): CommandResult {
        val parts = command.trim().split(" ")
        val result: CommandResult = when (parts.getOrNull(0)?.uppercase()) {
            "ACQUIRE_LOCK" -> {
                // Format: ACQUIRE_LOCK <resourceId> <clientId> <lockToken> <expiresAt> <requestTimestamp>
                val resourceId = parts.getOrNull(1) ?: return logUnknown(index, command)
                val clientId = parts.getOrNull(2) ?: return logUnknown(index, command)
                val lockToken = parts.getOrNull(3) ?: return logUnknown(index, command)
                val expiresAt = parts.getOrNull(4)?.toLongOrNull() ?: return logUnknown(index, command)
                val requestTimestamp = parts.getOrNull(5)?.toLongOrNull() ?: return logUnknown(index, command)
                
                val current = locks[resourceId]
                
                // We can acquire if it's completely free, OR if the existing lock had expired 
                // BEFORE this request was made (as determined by the leader's requestTimestamp).
                if (current == null || current.expiresAt <= requestTimestamp) {
                    locks[resourceId] = LockRecord(resourceId, clientId, lockToken, expiresAt)
                    println("[StateMachine] index=$index ACQUIRE_LOCK success: $resourceId by $clientId")
                    CommandResult.Success("Lock acquired successfully")
                } else {
                    println("[StateMachine] index=$index ACQUIRE_LOCK failed (already held): $resourceId")
                    CommandResult.Failure("Conflict. Lock already held by someone else.")
                }
            }
            "RELEASE_LOCK" -> {
                // Format: RELEASE_LOCK <resourceId> <clientId> <lockToken>
                val resourceId = parts.getOrNull(1) ?: return logUnknown(index, command)
                val clientId = parts.getOrNull(2) ?: return logUnknown(index, command)
                val lockToken = parts.getOrNull(3) ?: return logUnknown(index, command)
                
                val current = locks[resourceId]
                if (current != null && current.clientId == clientId && current.lockToken == lockToken) {
                    locks.remove(resourceId)
                    println("[StateMachine] index=$index RELEASE_LOCK success: $resourceId")
                    CommandResult.Success("Lock released successfully")
                } else {
                    println("[StateMachine] index=$index RELEASE_LOCK failed (mismatch or not held): $resourceId")
                    CommandResult.Failure("Failed to release. Token mismatch or lock belongs to someone else.")
                }
            }
            "RENEW_LOCK" -> {
                // Format: RENEW_LOCK <resourceId> <clientId> <lockToken> <expiresAt>
                val resourceId = parts.getOrNull(1) ?: return logUnknown(index, command)
                val clientId = parts.getOrNull(2) ?: return logUnknown(index, command)
                val lockToken = parts.getOrNull(3) ?: return logUnknown(index, command)
                val expiresAt = parts.getOrNull(4)?.toLongOrNull() ?: return logUnknown(index, command)
                
                val current = locks[resourceId]
                if (current != null && current.clientId == clientId && current.lockToken == lockToken) {
                    locks[resourceId] = current.copy(expiresAt = expiresAt)
                    println("[StateMachine] index=$index RENEW_LOCK success: $resourceId extended to $expiresAt")
                    CommandResult.Success("Lock renewed successfully")
                } else {
                    println("[StateMachine] index=$index RENEW_LOCK failed (mismatch or not held): $resourceId")
                    CommandResult.Failure("Failed to renew. Lock not held or token mismatch.")
                }
            }
            "NOOP" -> {
                println("[StateMachine] index=$index NOOP (leader commit sentinel)")
                CommandResult.Success("NOOP applied")
            }
            else -> logUnknown(index, command)
        }
        commandResults[index] = result
        return result
    }

    /**
     * Retrieves the definitive result of a command previously applied at [index].
     */
    fun getCommandResult(index: Long): CommandResult? = commandResults[index]

    /**
     * Reads the current lock state. 
     * NOTE: We still check local System.currentTimeMillis() here for READS because 
     * this is not mutating state. If a client queries status and the lock is expired 
     * according to the local clock, we report it as expired.
     */
    fun getLock(resourceId: String): LockRecord? {
        val lock = locks[resourceId]
        if (lock != null && lock.expiresAt > System.currentTimeMillis()) {
            return lock
        }
        return null // return null if it's physically gone or logically expired
    }

    fun snapshot(): Map<String, LockRecord> = locks.toMap()

    private fun logUnknown(index: Long, command: String): CommandResult.Failure {
        println("[StateMachine] WARNING: unknown command at index=$index: '$command' — skipped")
        return CommandResult.Failure("Unknown or malformed command")
    }
}
