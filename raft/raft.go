package raft

import (
	"math/rand"
	"sync"
	"time"
)

// LogEntry represents a single command along with the term it was created in.
type LogEntry struct {
	Term    int
	Command string
}

// Role defines the current state of a Raft server.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// Raft represents a single node in the consensus cluster.
type Raft struct {
	mu sync.Mutex

	id    int
	peers []string

	// Persistent state (must be saved to stable storage before responding to RPCs)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Leader-only state (reinitialized after election)
	nextIndex  []int
	matchIndex []int

	role Role

	// Election timer state
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	// Closed by Kill() to stop the ticker goroutine
	stopCh   chan struct{}
	killOnce sync.Once
}

// NewRaft creates a node, starts it as a follower and launches its ticker.
func NewRaft(id int, peers []string) *Raft {
	rf := &Raft{
		id:       id,
		peers:    peers,
		votedFor: -1,                    // -1 means "no vote yet"
		log:      []LogEntry{{Term: 0}}, // dummy entry at index 0
		role:     Follower,
		stopCh:   make(chan struct{}),
	}

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	go rf.ticker()
	return rf
}

// Kill stops the background ticker. Safe to call more than once.
func (rf *Raft) Kill() {
	rf.killOnce.Do(func() {
		close(rf.stopCh)
	})
}

// resetElectionTimer must be called with rf.mu held.
// It records "now" as the last time we heard from a leader/candidate
// and picks a fresh random timeout (300-500 ms).
func (rf *Raft) resetElectionTimer() {
	timeout := 300 + rand.Intn(200)
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(timeout) * time.Millisecond
}

// ticker periodically checks whether the election timeout has elapsed.
func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-time.After(10 * time.Millisecond):
		}

		rf.mu.Lock()
		if rf.role != Leader &&
			time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection must be called with rf.mu held.
func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id
	rf.resetElectionTimer() // restart timer so we retry if the election splits

	// TODO: send RequestVote RPCs to all peers (in goroutines, WITHOUT
	// holding rf.mu during the network call) and count votes.
}

// becomeFollower must be called with rf.mu held.
// The vote is cleared only when the term actually increases, so a node
// can never vote twice in the same term.
func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

// ---------------------------------------------------------------------------
// RPC types
// ---------------------------------------------------------------------------

// RequestVoteArgs is sent by candidates to gather votes during elections.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the response sent back to a candidate.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is used by the leader to replicate log entries and act as heartbeats.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the response sent back to the leader after log replication.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// ---------------------------------------------------------------------------
// RPC handlers
// ---------------------------------------------------------------------------

// AppendEntries handles heartbeats and log replication from the leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Stale leader: reject and do NOT reset the election timer.
	if args.Term < rf.currentTerm {
		return
	}

	// Newer (or equal) term from a legitimate leader: step down if needed.
	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	// Valid heartbeat from the current leader: reset the timer.
	rf.resetElectionTimer()

	// TODO: log consistency check (PrevLogIndex/PrevLogTerm), append
	// Entries, and advance commitIndex using LeaderCommit.
	reply.Success = true
}

// RequestVote handles vote requests from candidates.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	// Candidate's log must be at least as up-to-date as ours.
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer() // only reset when we actually grant a vote
	}
}
