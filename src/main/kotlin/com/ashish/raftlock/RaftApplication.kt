package com.ashish.raftlock

import com.ashish.raftlock.core.RaftNode
import com.ashish.raftlock.rpc.RaftGrpcService
import io.grpc.ServerBuilder
import kotlinx.coroutines.runBlocking

/**
 * Boots one Raft node.
 *
 * Configuration comes entirely from environment variables so the exact same
 * container image can run as node-1, node-2, or node-3 — see docker-compose.yml.
 *
 *   NODE_ID   - this node's own id, e.g. "node-1"
 *   GRPC_PORT - port this node listens on, e.g. 9090
 *   PEERS     - comma-separated "peerId@host:port" list of every OTHER node,
 *               e.g. "node-2@node-2:9090,node-3@node-3:9090"
 */
fun main() {
    val nodeId = System.getenv("NODE_ID") ?: "node-1"
    val grpcPort = (System.getenv("GRPC_PORT") ?: "9090").toInt()
    val peersEnv = System.getenv("PEERS") ?: ""

    val peers: Map<String, String> = peersEnv
        .split(",")
        .filter { it.isNotBlank() }
        .associate { entry ->
            val (id, address) = entry.split("@")
            id to address
        }

    val raftNode = RaftNode(nodeId = nodeId, peers = peers)

    val grpcServer = ServerBuilder
        .forPort(grpcPort)
        .addService(RaftGrpcService(raftNode))
        .build()

    grpcServer.start()
    println("[$nodeId] gRPC server listening on port $grpcPort, peers=$peers")

    runBlocking {
        raftNode.start()
    }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            println("[$nodeId] shutting down")
            grpcServer.shutdownNow()
        }
    )

    grpcServer.awaitTermination()
}
