package raft

import "sync"

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

	// Persistent state (Must be saved to stable storage before responding to RPCs)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Leader-only state (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	role Role
}

func NewRaft(id int, peers []string) *Raft {
	rf := &Raft{
		id:       id,
		peers:    peers,
		votedFor: -1,                    // -1 means "no vote yet"
		log:      []LogEntry{{Term: 0}}, // dummy entry at index 0
		role:     Follower,
	}
	return rf
}

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
