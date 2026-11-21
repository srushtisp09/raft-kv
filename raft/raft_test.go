package raft

import (
	"testing"
	"time"
)

// Creates a Raft server with a fake RequestVote function.
func newTestRaft(id int, peers []string, fakeVote func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool) *Raft {
	rf := NewRaft(id, peers)

	rf.mu.Lock()
	rf.sendRequestVote = fakeVote
	rf.mu.Unlock()

	return rf
}

// Waits until the Raft server reaches the expected role.
func waitForRole(t *testing.T, rf *Raft, expected Role) {
	t.Helper()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		rf.mu.Lock()
		currentRole := rf.role
		rf.mu.Unlock()

		if currentRole == expected {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("server did not reach expected role: %v", expected)
}

// Test 1: Candidate receives majority votes and becomes Leader.
func TestElectionWins(t *testing.T) {
	peers := []string{"server0", "server1", "server2", "server3", "server4"}

	rf := newTestRaft(0, peers, func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
		reply.Term = args.Term
		reply.VoteGranted = true
		return true
	})
	defer rf.Kill()

	rf.mu.Lock()
	rf.startElection()
	rf.mu.Unlock()

	waitForRole(t, rf, Leader)
}

// Test 2: Candidate receives a higher term and becomes Follower.
func TestHigherTermMakesFollower(t *testing.T) {
	peers := []string{"server0", "server1", "server2", "server3", "server4"}

	rf := newTestRaft(0, peers, func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
		reply.Term = args.Term + 1
		reply.VoteGranted = false
		return true
	})
	defer rf.Kill()

	rf.mu.Lock()
	rf.startElection()
	rf.mu.Unlock()

	waitForRole(t, rf, Follower)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != 2 {
		t.Fatalf("expected term 2, got %d", rf.currentTerm)
	}
}

// Test 3: Candidate does not receive a majority and stays Candidate.
func TestElectionNoMajority(t *testing.T) {
	peers := []string{"server0", "server1", "server2", "server3", "server4"}

	rf := newTestRaft(0, peers, func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
		reply.Term = args.Term
		reply.VoteGranted = (peer == 1)
		return true
	})
	defer rf.Kill()

	rf.mu.Lock()
	rf.startElection()
	rf.mu.Unlock()

	// Wait for the vote RPC goroutines to finish.
	time.Sleep(100 * time.Millisecond)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate {
		t.Fatalf("expected Candidate, got %v", rf.role)
	}
}
