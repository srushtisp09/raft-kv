package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

// LogEntry represents a command and its term.
type LogEntry struct {
	Term    int
	Command string
}

// ApplyMsg is delivered to the KV layer when an entry is committed.
type ApplyMsg struct {
	Index   int
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

	// Swappable transport (fake in tests, real net/rpc in production).
	sendRequestVote   func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	sendAppendEntries func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool

	// Where persistent state is saved (may be nil: nothing is saved).
	persister Persister

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

	// Committed entries are delivered here, in order.
	applyCh chan ApplyMsg

	// Signaled whenever commitIndex advances, so the applier wakes immediately.
	applyNotify chan struct{}

	stopCh   chan struct{}
	killOnce sync.Once
}

// NewRaft creates a follower, restores any saved state, and starts its
// ticker and applier.
func NewRaft(id int, peers []string, applyCh chan ApplyMsg, persister Persister) *Raft {
	rf := &Raft{
		id:          id,
		peers:       peers,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}}, // dummy entry at index 0
		role:        Follower,
		applyCh:     applyCh,
		applyNotify: make(chan struct{}, 1),
		persister:   persister,
		stopCh:      make(chan struct{}),
	}

	// No goroutines are running yet, so no lock is needed.
	rf.readPersist()

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	go rf.ticker()
	go rf.applier()

	return rf
}

// persist saves currentTerm, votedFor and log. Must be called with rf.mu held,
// and BEFORE replying to an RPC or sending a message that depends on the change.
func (rf *Raft) persist() {
	if rf.persister == nil {
		return
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(rf.currentTerm); err != nil {
		panic(err)
	}
	if err := enc.Encode(rf.votedFor); err != nil {
		panic(err)
	}
	if err := enc.Encode(rf.log); err != nil {
		panic(err)
	}
	// If the disk write fails we must not carry on as if the vote or entry
	// was saved, so crashing is the safe choice.
	if err := rf.persister.Save(buf.Bytes()); err != nil {
		panic(err)
	}
}

// readPersist restores saved state. Called once from NewRaft.
func (rf *Raft) readPersist() {
	if rf.persister == nil {
		return
	}
	data, err := rf.persister.Load()
	if err != nil {
		panic(err)
	}
	if len(data) == 0 {
		return // brand-new node, nothing saved
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	var term, votedFor int
	var entries []LogEntry
	if err := dec.Decode(&term); err != nil {
		panic(err)
	}
	if err := dec.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := dec.Decode(&entries); err != nil {
		panic(err)
	}
	if len(entries) == 0 {
		panic("persisted log is missing its dummy entry")
	}

	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = entries
}

// GetState returns the current term and whether this node is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// SetTransport plugs in the functions used to reach other nodes.
func (rf *Raft) SetTransport(
	rv func(int, *RequestVoteArgs, *RequestVoteReply) bool,
	ae func(int, *AppendEntriesArgs, *AppendEntriesReply) bool,
) {
	rf.mu.Lock()
	rf.sendRequestVote = rv
	rf.sendAppendEntries = ae
	rf.mu.Unlock()
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

// notifyApplier wakes the applier without blocking (the buffer of 1 means
// repeated signals collapse into one).
func (rf *Raft) notifyApplier() {
	select {
	case rf.applyNotify <- struct{}{}:
	default:
	}
}

// Start is called on the leader when a client sends a command.
// It returns (index, term, isLeader).
func (rf *Raft) Start(command string) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index := len(rf.log) - 1

	rf.advanceCommitIndex() // lets a single-node cluster commit right away
	go rf.broadcastAppendEntries(rf.currentTerm)

	return index, rf.currentTerm, true
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

// applier delivers committed entries to applyCh, in order. It wakes as soon
// as commitIndex advances; the timeout is only a safety net.
func (rf *Raft) applier() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.applyNotify:
		case <-time.After(50 * time.Millisecond): // safety net
		}

		rf.mu.Lock()
		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{
				Index:   rf.lastApplied,
				Command: rf.log[rf.lastApplied].Command,
			})
		}
		rf.mu.Unlock()

		// Send with no lock held, so a full channel can't freeze the node.
		for _, m := range msgs {
			select {
			case rf.applyCh <- m:
			case <-rf.stopCh:
				return
			}
		}
	}
}

// startElection must be called with rf.mu held.
func (rf *Raft) startElection() {
	// 1. Start a new election.
	rf.currentTerm++
	electionTerm := rf.currentTerm

	rf.role = Candidate
	rf.votedFor = rf.id
	rf.persist() // new term + self-vote must be on disk before asking for votes
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

// heartbeatLoop sends AppendEntries to every peer every 100 ms
// while we are the leader of the given term.
func (rf *Raft) heartbeatLoop(term int) {
	for {
		rf.broadcastAppendEntries(term)

		select {
		case <-rf.stopCh:
			return
		case <-time.After(100 * time.Millisecond):
		}

		rf.mu.Lock()
		stillLeader := rf.role == Leader && rf.currentTerm == term
		rf.mu.Unlock()
		if !stillLeader {
			return
		}
	}
}

// broadcastAppendEntries sends each peer the entries it is missing
// (an empty list is a heartbeat).
func (rf *Raft) broadcastAppendEntries(term int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term || rf.sendAppendEntries == nil {
		rf.mu.Unlock()
		return
	}
	send := rf.sendAppendEntries

	// Build each peer's args under the lock.
	argsList := make([]AppendEntriesArgs, len(rf.peers))
	for peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		prev := rf.nextIndex[peer] - 1
		entries := make([]LogEntry, len(rf.log)-rf.nextIndex[peer])
		copy(entries, rf.log[rf.nextIndex[peer]:]) // copy: the log may change after we unlock
		argsList[peer] = AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.id,
			PrevLogIndex: prev,
			PrevLogTerm:  rf.log[prev].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
	}
	rf.mu.Unlock()

	// Send with no lock held.
	for peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		go func(peer int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			if !send(peer, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.role != Leader || rf.currentTerm != term {
				return // stale reply
			}

			if reply.Success {
				// Use what we SENT, not len(rf.log) now: the log may have grown.
				match := args.PrevLogIndex + len(args.Entries)
				if match > rf.matchIndex[peer] {
					rf.matchIndex[peer] = match
				}
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.advanceCommitIndex()
			} else if args.PrevLogIndex < rf.nextIndex[peer] {
				// Walk back to the point we just tried (never below 1).
				rf.nextIndex[peer] = max(1, args.PrevLogIndex)
			}
		}(peer, argsList[peer])
	}
}

// advanceCommitIndex must be called with rf.mu held.
func (rf *Raft) advanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// Figure 8 rule: only commit entries from the CURRENT term by
		// counting replicas. An old-term entry can be on a majority and
		// still be overwritten by a later leader, so it only becomes
		// committed indirectly, when a current-term entry after it commits.
		if rf.log[n].Term != rf.currentTerm {
			break
		}
		count := 1 // our own log
		for peer := range rf.peers {
			if peer != rf.id && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.notifyApplier() // wake the applier right away
			break
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
		rf.persist()
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

	// Rule 1: our log is too short to contain PrevLogIndex.
	if args.PrevLogIndex >= len(rf.log) {
		return
	}
	// Rule 2: we have that index, but the term doesn't match.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// Rule 3: append new entries. Truncate ONLY on a real conflict,
	// otherwise a delayed duplicate RPC would delete good entries.
	logChanged := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = append(rf.log[:idx], args.Entries[i:]...)
				logChanged = true
				break
			}
			// same term at same index: already have it, keep going
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
	}
	if logChanged {
		rf.persist() // before we reply Success
	}

	// Rule 4: advance commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNew)
		rf.notifyApplier() // wake the applier right away
	}

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
		rf.persist() // vote must be on disk before we reply "granted"
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
}
