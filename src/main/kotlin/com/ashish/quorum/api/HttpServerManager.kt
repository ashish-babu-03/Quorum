package com.ashish.quorum.api

import com.ashish.quorum.core.CommandResult
import com.ashish.quorum.core.NodeRole
import com.ashish.quorum.core.RaftNode
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

class HttpServerManager(
    private val raftNode: RaftNode,
    private val port: Int,
    private val scope: CoroutineScope
) {
    private var server: HttpServer? = null

    // This offset assumes a fixed port-offset convention in our Docker Compose setup.
    // A real production deployment would use explicit service discovery (e.g., Consul/DNS)
    // to map node IDs to HTTP endpoints, rather than this simple calculation.
    private val HTTP_PORT_OFFSET = 1000

    fun start() {
        server = HttpServer.create(InetSocketAddress(port), 0).apply {
            createContext("/locks/", ::handleLocks)
            createContext("/cluster/", ::handleLocks)
            executor = null // Creates a default executor
            start()
        }
        println("[HttpServerManager] Started on port $port")
    }

    fun stop() {
        server?.stop(0)
    }

    private fun handleLocks(exchange: HttpExchange) {
        // Dispatch to coroutine so we can use suspend functions (awaitCommit)
        scope.launch {
            try {
                processRequest(exchange)
            } catch (e: Exception) {
                e.printStackTrace()
                sendResponse(exchange, 500, "{\"error\": \"Internal Server Error\"}")
            }
        }
    }

    private suspend fun processRequest(exchange: HttpExchange) {
        val path = exchange.requestURI.path // e.g., /locks/my-resource/acquire
        val method = exchange.requestMethod

        if (path.startsWith("/cluster/")) {
            val action = path.removePrefix("/cluster/")
            handleCluster(exchange, action)
            return
        }

        val parts = path.removePrefix("/locks/").split("/")
        if (parts.size < 2) {
            sendResponse(exchange, 400, "{\"error\": \"Invalid path format. Expected /locks/{resourceId}/{action}\"}")
            return
        }

        val resourceId = parts[0]
        val action = parts[1]

        if (method == "GET" && action == "status") {
            handleStatus(exchange, resourceId)
            return
        }

        if (method != "POST") {
            sendResponse(exchange, 405, "{\"error\": \"Method not allowed\"}")
            return
        }

        val (role, currentLeader) = raftNode.getLeadershipInfo()
        if (role != NodeRole.LEADER) {
            if (currentLeader == null) {
                sendResponse(exchange, 503, "{\"error\": \"Cluster has no leader currently. Retry later.\"}")
                return
            }
            // Redirect to leader. In our setup, leader's HTTP port = GRPC port + HTTP_PORT_OFFSET
            // Since `HttpServerManager` just uses `port`, and all peers have the same `port` inside their container,
            // we can route by `http://$currentLeader:$port/...`
            val redirectUrl = "http://$currentLeader:$port$path"
            exchange.responseHeaders.set("Location", redirectUrl)
            exchange.sendResponseHeaders(307, -1)
            exchange.close()
            return
        }

        // We are the LEADER. Parse body.
        val requestBody = String(exchange.requestBody.readAllBytes(), StandardCharsets.UTF_8)
        val parsedJson = parseFlatJson(requestBody)

        val clientId = parsedJson["clientId"]
        if (clientId == null) {
            sendResponse(exchange, 400, "{\"error\": \"Missing clientId\"}")
            return
        }

        when (action) {
            "acquire" -> {
                val leaseDurationMs = parsedJson["leaseDurationMs"]?.toLongOrNull() ?: 5000L
                val lockToken = java.util.UUID.randomUUID().toString()
                
                val now = System.currentTimeMillis()
                val expiresAt = now + leaseDurationMs
                
                // Submit to Raft: ACQUIRE_LOCK <resourceId> <clientId> <lockToken> <expiresAt> <requestTimestamp>
                val command = "ACQUIRE_LOCK $resourceId $clientId $lockToken $expiresAt $now"
                val logIndex = raftNode.submitCommand(command)
                
                val committed = raftNode.awaitCommit(logIndex, timeoutMs = 2000)
                if (!committed) {
                    sendResponse(exchange, 503, "{\"error\": \"Timed out waiting for consensus. Cluster may be mid-election.\"}")
                    return
                }
                
                // WHY use getCommandResult instead of inferring from state?
                // In earlier versions, success was inferred by checking if the lock state matched
                // the requested token. This is ambiguous (e.g. failing to release because someone else
                // holds it looks identical to successfully releasing it, as both result in the lock
                // missing or having a different token). Explicit result reporting fixes this entirely.
                when (val result = raftNode.getCommandResult(logIndex)) {
                    is CommandResult.Success -> {
                        sendResponse(exchange, 200, "{\"status\": \"acquired\", \"token\": \"$lockToken\", \"expiresAt\": $expiresAt}")
                    }
                    is CommandResult.Failure -> {
                        sendResponse(exchange, 409, "{\"error\": \"${result.reason}\"}")
                    }
                    null -> {
                        sendResponse(exchange, 500, "{\"error\": \"Internal server error. Missing command result.\"}")
                    }
                }
            }
            "release" -> {
                val lockToken = parsedJson["lockToken"]
                if (lockToken == null) {
                    sendResponse(exchange, 400, "{\"error\": \"Missing lockToken\"}")
                    return
                }

                val command = "RELEASE_LOCK $resourceId $clientId $lockToken"
                val logIndex = raftNode.submitCommand(command)
                
                val committed = raftNode.awaitCommit(logIndex, timeoutMs = 2000)
                if (!committed) {
                    sendResponse(exchange, 503, "{\"error\": \"Timed out waiting for consensus. Cluster may be mid-election.\"}")
                    return
                }

                when (val result = raftNode.getCommandResult(logIndex)) {
                    is CommandResult.Success -> {
                        sendResponse(exchange, 200, "{\"status\": \"released\"}") 
                    }
                    is CommandResult.Failure -> {
                        sendResponse(exchange, 403, "{\"error\": \"${result.reason}\"}")
                    }
                    null -> {
                        sendResponse(exchange, 500, "{\"error\": \"Internal server error. Missing command result.\"}")
                    }
                }
            }
            "renew" -> {
                val lockToken = parsedJson["lockToken"]
                if (lockToken == null) {
                    sendResponse(exchange, 400, "{\"error\": \"Missing lockToken\"}")
                    return
                }
                val leaseDurationMs = parsedJson["leaseDurationMs"]?.toLongOrNull() ?: 5000L
                val now = System.currentTimeMillis()
                val newExpiresAt = now + leaseDurationMs

                val command = "RENEW_LOCK $resourceId $clientId $lockToken $newExpiresAt"
                val logIndex = raftNode.submitCommand(command)

                val committed = raftNode.awaitCommit(logIndex, timeoutMs = 2000)
                if (!committed) {
                    sendResponse(exchange, 503, "{\"error\": \"Timed out waiting for consensus. Cluster may be mid-election.\"}")
                    return
                }

                when (val result = raftNode.getCommandResult(logIndex)) {
                    is CommandResult.Success -> {
                        sendResponse(exchange, 200, "{\"status\": \"renewed\", \"expiresAt\": $newExpiresAt}")
                    }
                    is CommandResult.Failure -> {
                        sendResponse(exchange, 403, "{\"error\": \"${result.reason}\"}")
                    }
                    null -> {
                        sendResponse(exchange, 500, "{\"error\": \"Internal server error. Missing command result.\"}")
                    }
                }
            }
            else -> {
                sendResponse(exchange, 404, "{\"error\": \"Unknown action: $action\"}")
            }
        }
    }

    private suspend fun handleStatus(exchange: HttpExchange, resourceId: String) {
        val lock = raftNode.getLock(resourceId)
        if (lock == null) {
            sendResponse(exchange, 404, "{\"status\": \"free\"}")
        } else {
            sendResponse(exchange, 200, "{\"status\": \"locked\", \"clientId\": \"${lock.clientId}\", \"expiresAt\": ${lock.expiresAt}}")
        }
    }

    private suspend fun handleCluster(exchange: HttpExchange, action: String) {
        if (exchange.requestMethod != "POST") {
            sendResponse(exchange, 405, "{\"error\": \"Method not allowed\"}")
            return
        }
        
        val (role, currentLeader) = raftNode.getLeadershipInfo()
        if (role != NodeRole.LEADER) {
            if (currentLeader == null) {
                sendResponse(exchange, 503, "{\"error\": \"Cluster has no leader currently. Retry later.\"}")
                return
            }
            val redirectUrl = "http://$currentLeader:$port${exchange.requestURI.path}"
            exchange.responseHeaders.set("Location", redirectUrl)
            exchange.sendResponseHeaders(307, -1)
            exchange.close()
            return
        }

        val requestBody = String(exchange.requestBody.readAllBytes(), StandardCharsets.UTF_8)
        val parsedJson = parseFlatJson(requestBody)
        val nodeId = parsedJson["nodeId"]
        
        if (nodeId == null) {
            sendResponse(exchange, 400, "{\"error\": \"Missing nodeId\"}")
            return
        }

        when (action) {
            "add" -> {
                val address = parsedJson["address"]
                if (address == null) {
                    sendResponse(exchange, 400, "{\"error\": \"Missing address\"}")
                    return
                }
                
                val logIndex = raftNode.trySubmitConfigCommand("ADD_NODE $nodeId $address")
                if (logIndex == null) {
                    sendResponse(exchange, 409, "{\"error\": \"A configuration change is already in progress, try again once it completes.\"}")
                    return
                }
                
                val committed = raftNode.awaitCommit(logIndex, timeoutMs = 5000)
                if (!committed) {
                    sendResponse(exchange, 503, "{\"error\": \"Timed out waiting for consensus.\"}")
                    return
                }
                sendResponse(exchange, 200, "{\"status\": \"added\", \"nodeId\": \"$nodeId\"}")
            }
            "remove" -> {
                val logIndex = raftNode.trySubmitConfigCommand("REMOVE_NODE $nodeId")
                if (logIndex == null) {
                    sendResponse(exchange, 409, "{\"error\": \"A configuration change is already in progress, try again once it completes.\"}")
                    return
                }
                
                val committed = raftNode.awaitCommit(logIndex, timeoutMs = 5000)
                if (!committed) {
                    sendResponse(exchange, 503, "{\"error\": \"Timed out waiting for consensus.\"}")
                    return
                }
                sendResponse(exchange, 200, "{\"status\": \"removed\", \"nodeId\": \"$nodeId\"}")
            }
            else -> {
                sendResponse(exchange, 404, "{\"error\": \"Unknown cluster action: $action\"}")
            }
        }
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, json: String) {
        val bytes = json.toByteArray(StandardCharsets.UTF_8)
        exchange.responseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.write(bytes)
        exchange.close()
    }

    /**
     * A simple, hand-written flat JSON parser.
     * Intentionally does not support nested JSON objects, arrays, or complex types,
     * because this API only receives simple flat { "key": "value", "key2": 123 } bodies.
     * Safer than Regex for basic string extraction.
     */
    private fun parseFlatJson(json: String): Map<String, String> {
        val result = mutableMapOf<String, String>()
        val content = json.trim().removePrefix("{").removeSuffix("}").trim()
        if (content.isEmpty()) return result

        var i = 0
        while (i < content.length) {
            // Find key
            while (i < content.length && content[i] != '"') i++
            if (i >= content.length) break
            i++ // skip quote
            val keyStart = i
            while (i < content.length && content[i] != '"') i++
            val key = content.substring(keyStart, i)
            i++ // skip quote

            // Find colon
            while (i < content.length && content[i] != ':') i++
            i++ // skip colon

            // Find value (could be string or number)
            while (i < content.length && content[i].isWhitespace()) i++
            if (i >= content.length) break

            if (content[i] == '"') {
                // String value
                i++ // skip quote
                val valStart = i
                while (i < content.length && content[i] != '"') i++
                val value = content.substring(valStart, i)
                i++ // skip quote
                result[key] = value
            } else {
                // Number or boolean value
                val valStart = i
                while (i < content.length && content[i] != ',' && !content[i].isWhitespace() && content[i] != '}') i++
                val value = content.substring(valStart, i).trim()
                result[key] = value
            }

            // Find comma for next
            while (i < content.length && content[i] != ',' && content[i] != '}') i++
        }
        return result
    }
}
