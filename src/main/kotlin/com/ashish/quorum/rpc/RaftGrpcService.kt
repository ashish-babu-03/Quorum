package com.ashish.quorum.rpc

import com.ashish.quorum.core.LogEntry
import com.ashish.quorum.core.RaftNode
import com.ashish.quorum.grpc.AppendEntriesRequest
import com.ashish.quorum.grpc.AppendEntriesResponse
import com.ashish.quorum.grpc.RaftServiceGrpcKt
import com.ashish.quorum.grpc.VoteRequest
import com.ashish.quorum.grpc.VoteResponse

/**
 * Server-side gRPC endpoint. Deliberately thin — all real decision-making
 * (vote grant/deny, term handling, stepping down) lives in RaftNode. This
 * class only translates between wire messages and RaftNode's method calls.
 */
class RaftGrpcService(
    private val raftNode: RaftNode
) : RaftServiceGrpcKt.RaftServiceCoroutineImplBase() {

    override suspend fun requestVote(request: VoteRequest): VoteResponse {
        val (term, granted) = raftNode.handleRequestVote(
            term = request.term,
            candidateId = request.candidateId,
            lastLogIndex = request.lastLogIndex,
            lastLogTerm = request.lastLogTerm
        )
        return VoteResponse.newBuilder()
            .setTerm(term)
            .setVoteGranted(granted)
            .build()
    }

    override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
        val entries = request.entriesList.map {
            LogEntry(term = it.term, index = it.index, command = it.command)
        }
        val (term, success) = raftNode.handleAppendEntries(
            term = request.term,
            leaderId = request.leaderId,
            prevLogIndex = request.prevLogIndex,
            prevLogTerm = request.prevLogTerm,
            entries = entries,
            leaderCommit = request.leaderCommit
        )
        return AppendEntriesResponse.newBuilder()
            .setTerm(term)
            .setSuccess(success)
            .build()
    }
}
