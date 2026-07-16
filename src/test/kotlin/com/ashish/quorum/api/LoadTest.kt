package com.ashish.quorum.api

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LoadTest {

    /**
     * IMPORTANT: This test assumes the Quorum Docker Compose cluster is ALREADY RUNNING
     * and that a leader is reachable at one of the mapped ports.
     */
    @Test
    fun `test concurrent lock acquisition`() = runBlocking(Dispatchers.IO) {
        val httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build()

        val numClients = 50
        val resourceId = "load-test-resource-${System.currentTimeMillis()}"
        
        println("Firing $numClients concurrent acquire requests for resource: $resourceId")

        // Fire 50 concurrent requests exactly at the same time
        val deferredResponses = (1..numClients).map { clientId ->
            async {
                val requestBody = """{"clientId":"client-$clientId", "leaseDurationMs":"60000"}"""
                val request = HttpRequest.newBuilder()
                    // We are pointing to 10093 because from your last logs, node-3 is the current leader.
                    // If a follower is hit instead, they would all just return 307 redirects.
                    .uri(URI.create("http://localhost:10093/locks/$resourceId/acquire"))
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .header("Content-Type", "application/json")
                    .build()

                runCatching {
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString())
                }.getOrNull()
            }
        }

        val responses = deferredResponses.awaitAll().filterNotNull()
        
        println("Received ${responses.size} responses.")
        
        val successCount = responses.count { it.statusCode() == 200 }
        val conflictCount = responses.count { it.statusCode() == 409 }
        val redirectCount = responses.count { it.statusCode() == 307 }

        println("Success (200): $successCount")
        println("Conflict (409): $conflictCount")
        println("Redirect (307): $redirectCount")

        // If we hit the leader directly, we expect exactly 1 success and N-1 conflicts.
        // If we hit a follower, we expect N redirects.
        assertTrue(
            (successCount == 1 && conflictCount == numClients - 1) || redirectCount == numClients,
            "Expected exactly 1 success and 49 conflicts, OR 50 redirects if we hit a follower."
        )
    }
}
