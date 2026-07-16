package com.ashish.quorum.rpc

import com.ashish.quorum.core.LogEntry
import com.ashish.quorum.grpc.AppendEntriesRequest
import com.ashish.quorum.grpc.AppendEntriesResponse
import com.ashish.quorum.grpc.LogEntryProto
import com.ashish.quorum.grpc.RaftServiceGrpcKt
import com.ashish.quorum.grpc.VoteRequest
import com.ashish.quorum.grpc.VoteResponse
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.withTimeout

/**
 * Thin wrapper around a gRPC stub to exactly one peer.
 *
 * Every call here has its own short timeout ([rpcTimeoutMillis]) — this is
 * the "RPC timeout", a completely separate clock from the election timeout
 * in ElectionTimer. A slow or unreachable peer must never be allowed to
 * block an election or a heartbeat round; we just treat a timed-out call
 * as "no response from this peer" and move on.
 */
class PeerClient(address: String, private val rpcTimeoutMillis: Long = 100) {
    private val host = address.substringBefore(":")
    private val port = address.substringAfter(":").toInt()

    private val channel = ManagedChannelBuilder
        .forAddress(host, port)
        .usePlaintext() // fine for an internal cluster network; add TLS for anything public
        .build()

    private val stub = RaftServiceGrpcKt.RaftServiceCoroutineStub(channel)

    suspend fun requestVote(
        term: Long,
        candidateId: String,
        lastLogIndex: Long,
        lastLogTerm: Long
    ): VoteResponse = withTimeout(rpcTimeoutMillis) {
        stub.requestVote(
            VoteRequest.newBuilder()
                .setTerm(term)
                .setCandidateId(candidateId)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build()
        )
    }

    suspend fun appendEntries(
        term: Long,
        leaderId: String,
        prevLogIndex: Long,
        prevLogTerm: Long,
        entries: List<LogEntry>,
        leaderCommit: Long
    ): AppendEntriesResponse = withTimeout(rpcTimeoutMillis) {
        stub.appendEntries(
            AppendEntriesRequest.newBuilder()
                .setTerm(term)
                .setLeaderId(leaderId)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommit(leaderCommit)
                .addAllEntries(
                    entries.map {
                        LogEntryProto.newBuilder()
                            .setTerm(it.term)
                            .setIndex(it.index)
                            .setCommand(it.command)
                            .build()
                    }
                )
                .build()
        )
    }
}
