package raft

import (
	"math/rand"
	"sync"
	"time"
)

// LogEntry represents a command and its term.
type LogEntry struct {
	Term    int
	Command string
}

// Role defines the state of a Raft server.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

// Raft represents a single node.
type Raft struct {
	mu sync.Mutex

	id    int
	peers []string

	// Swappable transport (fake in tests, real net/rpc later).
	sendRequestVote   func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	sendAppendEntries func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool

	// Persistent state.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state.
	commitIndex int
	lastApplied int

	// Leader-only state.
	nextIndex  []int
	matchIndex []int

	role Role

	// Election timer.
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	stopCh   chan struct{}
	killOnce sync.Once
}

// NewRaft creates a follower and starts its ticker.
func NewRaft(id int, peers []string) *Raft {
	rf := &Raft{
		id:       id,
		peers:    peers,
		votedFor: -1,
		log:      []LogEntry{{Term: 0}},
		role:     Follower,
		stopCh:   make(chan struct{}),
	}

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	go rf.ticker()

	return rf
}

// setSendRequestVote swaps the transport safely while the ticker is running.
func (rf *Raft) setSendRequestVote(f func(int, *RequestVoteArgs, *RequestVoteReply) bool) {
	rf.mu.Lock()
	rf.sendRequestVote = f
	rf.mu.Unlock()
}

// setSendAppendEntries swaps the transport safely while the ticker is running.
func (rf *Raft) setSendAppendEntries(f func(int, *AppendEntriesArgs, *AppendEntriesReply) bool) {
	rf.mu.Lock()
	rf.sendAppendEntries = f
	rf.mu.Unlock()
}

// Kill stops all background goroutines. Safe to call more than once.
func (rf *Raft) Kill() {
	rf.killOnce.Do(func() {
		close(rf.stopCh)
	})
}

// resetElectionTimer must be called with rf.mu held.
func (rf *Raft) resetElectionTimer() {
	timeout := 300 + rand.Intn(200)

	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(timeout) * time.Millisecond
}

// ticker checks election timeout periodically.
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
	// 1. Start a new election.
	rf.currentTerm++
	electionTerm := rf.currentTerm

	rf.role = Candidate
	rf.votedFor = rf.id
	rf.resetElectionTimer()

	// 2. Prepare vote request.
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	args := RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// 3. Vote for ourselves.
	votes := 1

	// 4. Copy RPC function while lock is held.
	sendVote := rf.sendRequestVote

	// A single-node cluster needs no RPCs.
	if votes > len(rf.peers)/2 {
		rf.becomeLeader()
		return
	}

	// 5. Ask all other peers concurrently.
	for peer := range rf.peers {
		if peer == rf.id {
			continue
		}

		go func(peer int) {
			var reply RequestVoteReply

			// Never hold rf.mu during RPC.
			if sendVote == nil || !sendVote(peer, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Check A: higher term, step down.
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			// Check B: ignore outdated election responses.
			if rf.role != Candidate || rf.currentTerm != electionTerm {
				return
			}

			// Check C: count granted votes, check majority.
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader must be called with rf.mu held.
func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.heartbeatLoop(rf.currentTerm)
}

// heartbeatLoop sends empty AppendEntries every 100 ms while we are the
// leader of the given term.
func (rf *Raft) heartbeatLoop(term int) {
	for {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		last := len(rf.log) - 1
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.id,
			PrevLogIndex: last,
			PrevLogTerm:  rf.log[last].Term,
			LeaderCommit: rf.commitIndex,
		}
		send := rf.sendAppendEntries
		rf.mu.Unlock()

		if send != nil {
			for peer := range rf.peers {
				if peer == rf.id {
					continue
				}
				go func(peer int) {
					var reply AppendEntriesReply
					if !send(peer, &args, &reply) {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					}
				}(peer)
			}
		}

		select {
		case <-rf.stopCh:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// becomeFollower must be called with rf.mu held.
// The vote is cleared only when the term increases.
func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

// RPC types.

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries handles heartbeats and log replication.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	rf.resetElectionTimer()

	// TODO: Log consistency and replication.
	reply.Success = true
}

// RequestVote handles incoming vote requests.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	// 1. Reject stale term.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// 2. Update newer term.
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// 3. Check candidate log.
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	// 4. Grant vote if allowed.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
}
