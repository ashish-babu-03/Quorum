package com.ashish.quorum.core

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * Handles lightweight disk persistence for Raft state (currentTerm, votedFor, log).
 * Avoids heavyweight dependencies like Jackson/Gson by using a custom, flat JSON parser
 * optimized specifically for our LogEntry format.
 */
object StateStorage {

    fun load(file: File): Triple<Long, String?, List<LogEntry>>? {
        if (!file.exists()) return null
        
        val content = file.readText()
        if (content.isBlank()) return null
        
        val termMatch = "\"currentTerm\":\\s*(\\d+)".toRegex().find(content)
        val term = termMatch?.groupValues?.get(1)?.toLong() ?: return null

        val votedForMatch = "\"votedFor\":\\s*(null|\"[^\"]+\")".toRegex().find(content)
        val votedForStr = votedForMatch?.groupValues?.get(1)
        val votedFor = if (votedForStr == null || votedForStr == "null") null else votedForStr.trim('"')

        val log = mutableListOf<LogEntry>()
        val entryRegex = "\\{\"term\":\\s*(\\d+),\\s*\"index\":\\s*(\\d+),\\s*\"command\":\\s*\"(.*?)\"\\}".toRegex()
        entryRegex.findAll(content).forEach { match ->
            val eTerm = match.groupValues[1].toLong()
            val eIndex = match.groupValues[2].toLong()
            val eCmd = match.groupValues[3].replace("\\\"", "\"").replace("\\n", "\n")
            log.add(LogEntry(term = eTerm, index = eIndex, command = eCmd))
        }

        return Triple(term, votedFor, log)
    }

    /**
     * Atomically saves the state to disk. Writes to a temporary file first,
     * then renames it over the target file to prevent corruption if the process
     * crashes mid-write.
     */
    fun save(file: File, term: Long, votedFor: String?, log: List<LogEntry>) {
        val temp = File(file.parentFile, file.name + ".tmp")
        val sb = StringBuilder()
        sb.append("{\n")
        sb.append("  \"currentTerm\": $term,\n")
        sb.append("  \"votedFor\": ${if (votedFor == null) "null" else "\"$votedFor\""},\n")
        sb.append("  \"log\": [\n")
        for (i in log.indices) {
            val entry = log[i]
            val escapedCmd = entry.command.replace("\"", "\\\"").replace("\n", "\\n")
            sb.append("    {\"term\": ${entry.term}, \"index\": ${entry.index}, \"command\": \"$escapedCmd\"}")
            if (i < log.size - 1) sb.append(",")
            sb.append("\n")
        }
        sb.append("  ]\n")
        sb.append("}\n")

        temp.writeText(sb.toString())
        Files.move(temp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }
}
