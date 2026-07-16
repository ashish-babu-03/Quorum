package com.ashish.quorum.core

/**
 * Every node is in exactly one of these states at any moment.
 * There is no fixed "candidate list" — any FOLLOWER can become a
 * CANDIDATE on its own, purely because its election timer ran out first.
 */
enum class NodeRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
