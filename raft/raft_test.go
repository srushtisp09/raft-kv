package raft

import (
	"sync"
	"testing"
	"time"
)

// Creates a Raft server with a fake RequestVote function.
func newTestRaft(id int, peers []string, fakeVote func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool) *Raft {
	rf := NewRaft(id, peers, make(chan ApplyMsg, 100), &MemPersister{})
	rf.setSendRequestVote(fakeVote)
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

// ---------------------------------------------------------------------------
// Cluster tests: nodes talk to each other through fake in-memory RPC.
// ---------------------------------------------------------------------------

type cluster struct {
	peers      []string
	nodes      []*Raft
	applyChs   []chan ApplyMsg
	persisters []Persister
	mu         sync.Mutex
	down       []bool // true = node is cut off from the network
}

func (c *cluster) isDown(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.down[i]
}

func (c *cluster) setDown(i int, v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.down[i] = v
}

// node returns the current Raft instance for index i (it can be replaced by restart).
func (c *cluster) node(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

// wire connects node i's transport to the other nodes through fake RPC.
func (c *cluster) wire(i int) {
	from := i
	rf := c.node(i)
	rf.setSendRequestVote(func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
		if c.isDown(from) || c.isDown(peer) {
			return false
		}
		c.node(peer).RequestVote(args, reply)
		return true
	})
	rf.setSendAppendEntries(func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
		if c.isDown(from) || c.isDown(peer) {
			return false
		}
		c.node(peer).AppendEntries(args, reply)
		return true
	})
}

func makeCluster(n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = "server" + string(rune('0'+i))
	}

	c := &cluster{
		peers:      peers,
		nodes:      make([]*Raft, n),
		applyChs:   make([]chan ApplyMsg, n),
		persisters: make([]Persister, n),
		down:       make([]bool, n),
	}
	for i := 0; i < n; i++ {
		c.persisters[i] = &MemPersister{}
		c.applyChs[i] = make(chan ApplyMsg, 100)
		c.nodes[i] = NewRaft(i, peers, c.applyChs[i], c.persisters[i])
	}
	for i := 0; i < n; i++ {
		c.wire(i)
	}
	return c
}

// restart simulates a crash and reboot: the old node is killed and a new one
// is built from the SAME persister, with a fresh apply channel.
func (c *cluster) restart(i int) {
	c.nodes[i].Kill()

	ch := make(chan ApplyMsg, 100)
	rf := NewRaft(i, c.peers, ch, c.persisters[i])

	c.mu.Lock()
	c.nodes[i] = rf
	c.applyChs[i] = ch
	c.mu.Unlock()

	c.wire(i)
}

func (c *cluster) killAll() {
	for i := range c.nodes {
		c.node(i).Kill()
	}
}

// leaders returns the IDs of connected nodes that currently think they are leader.
func (c *cluster) leaders() []int {
	var ids []int
	for i := range c.nodes {
		if c.isDown(i) {
			continue
		}
		rf := c.node(i)
		rf.mu.Lock()
		if rf.role == Leader {
			ids = append(ids, i)
		}
		rf.mu.Unlock()
	}
	return ids
}

// waitForOneLeader waits until exactly one connected node is leader.
func waitForOneLeader(t *testing.T, c *cluster) int {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ids := c.leaders(); len(ids) == 1 {
			return ids[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within 3s, leaders: %v", c.leaders())
	return -1
}

// expectApplied reads one message from ch and checks it.
func expectApplied(t *testing.T, node int, ch chan ApplyMsg, index int, command string) {
	t.Helper()
	select {
	case msg := <-ch:
		if msg.Index != index || msg.Command != command {
			t.Fatalf("node %d applied %+v, expected index %d command %q", node, msg, index, command)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("node %d never applied index %d (%q)", node, index, command)
	}
}

// Test 4: a 3-node cluster elects exactly one leader and keeps it stable.
func TestClusterElectsOneLeader(t *testing.T) {
	c := makeCluster(3)
	defer c.killAll()

	leader := waitForOneLeader(t, c)

	// Heartbeats should keep the same leader for a while.
	time.Sleep(1500 * time.Millisecond)

	ids := c.leaders()
	if len(ids) != 1 || ids[0] != leader {
		t.Fatalf("leadership changed: was %d, now %v", leader, ids)
	}
}

// Test 5: when the leader dies, another node takes over.
func TestClusterReelectsAfterLeaderFailure(t *testing.T) {
	c := makeCluster(3)
	defer c.killAll()

	old := waitForOneLeader(t, c)

	// Cut the leader off and stop its goroutines.
	c.setDown(old, true)
	c.nodes[old].Kill()

	newLeader := waitForOneLeader(t, c)
	if newLeader == old {
		t.Fatalf("expected a new leader, but %d is still leader", old)
	}
}

// ---------------------------------------------------------------------------
// Log replication tests
// ---------------------------------------------------------------------------

// Test 6: a command replicates to every node and is applied everywhere.
func TestReplicatesCommand(t *testing.T) {
	c := makeCluster(3)
	defer c.killAll()

	leader := waitForOneLeader(t, c)
	idx, _, ok := c.nodes[leader].Start("SET name Nobita")
	if !ok {
		t.Fatal("leader rejected command")
	}

	for i, ch := range c.applyChs {
		expectApplied(t, i, ch, idx, "SET name Nobita")
	}
}

// Test 7: a command committed before the leader dies is not lost.
func TestCommittedEntrySurvivesLeaderFailure(t *testing.T) {
	c := makeCluster(3)
	defer c.killAll()

	old := waitForOneLeader(t, c)
	c.nodes[old].Start("A")
	time.Sleep(500 * time.Millisecond) // let it commit and replicate

	c.setDown(old, true)
	c.nodes[old].Kill()

	newLeader := waitForOneLeader(t, c)
	nl := c.nodes[newLeader]
	nl.mu.Lock()
	defer nl.mu.Unlock()
	if len(nl.log) < 2 || nl.log[1].Command != "A" {
		t.Fatalf("new leader lost the committed entry: %+v", nl.log)
	}
}

// ---------------------------------------------------------------------------
// Persistence tests
// ---------------------------------------------------------------------------

// Test 8: term, vote and log come back after a restart.
func TestPersistRestoresState(t *testing.T) {
	p := &MemPersister{}
	peers := []string{"server0", "server1", "server2"}
	rf := NewRaft(0, peers, make(chan ApplyMsg, 100), p)

	var vr RequestVoteReply
	rf.RequestVote(&RequestVoteArgs{Term: 3, CandidateId: 1}, &vr)
	if !vr.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	var ar AppendEntriesReply
	rf.AppendEntries(&AppendEntriesArgs{
		Term:         3,
		LeaderId:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 3, Command: "A"}, {Term: 3, Command: "B"}},
	}, &ar)
	if !ar.Success {
		t.Fatal("expected AppendEntries to succeed")
	}
	rf.Kill()

	// "Reboot" with the same persister.
	rf2 := NewRaft(0, peers, make(chan ApplyMsg, 100), p)
	defer rf2.Kill()

	rf2.mu.Lock()
	defer rf2.mu.Unlock()
	if rf2.currentTerm != 3 {
		t.Fatalf("term not restored: got %d", rf2.currentTerm)
	}
	if rf2.votedFor != 1 {
		t.Fatalf("votedFor not restored: got %d", rf2.votedFor)
	}
	if len(rf2.log) != 3 || rf2.log[1].Command != "A" || rf2.log[2].Command != "B" {
		t.Fatalf("log not restored: %+v", rf2.log)
	}
}

// Test 9: a node that voted in term 5 cannot vote for someone else in term 5
// after a crash and restart.
func TestNoDoubleVoteAfterRestart(t *testing.T) {
	p := &MemPersister{}
	peers := []string{"server0", "server1", "server2"}
	rf := NewRaft(0, peers, make(chan ApplyMsg, 100), p)

	var r1 RequestVoteReply
	rf.RequestVote(&RequestVoteArgs{Term: 5, CandidateId: 1}, &r1)
	if !r1.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}
	rf.Kill()

	rf2 := NewRaft(0, peers, make(chan ApplyMsg, 100), p)
	defer rf2.Kill()

	var r2 RequestVoteReply
	rf2.RequestVote(&RequestVoteArgs{Term: 5, CandidateId: 2}, &r2)
	if r2.VoteGranted {
		t.Fatal("node voted twice in term 5 after restart")
	}

	// Asking again from the same candidate is fine (idempotent).
	var r3 RequestVoteReply
	rf2.RequestVote(&RequestVoteArgs{Term: 5, CandidateId: 1}, &r3)
	if !r3.VoteGranted {
		t.Fatal("expected repeat vote for the same candidate to be granted")
	}
}

// Test 10: a crashed follower comes back, keeps its log, and catches up.
func TestFollowerRestartCatchesUp(t *testing.T) {
	c := makeCluster(3)
	defer c.killAll()

	leader := waitForOneLeader(t, c)
	if _, _, ok := c.nodes[leader].Start("A"); !ok {
		t.Fatal("leader rejected A")
	}
	for i, ch := range c.applyChs {
		expectApplied(t, i, ch, 1, "A")
	}

	// Crash a follower.
	follower := (leader + 1) % 3
	other := (leader + 2) % 3
	c.setDown(follower, true)
	c.nodes[follower].Kill()

	// The remaining 2 of 3 nodes still form a majority and commit B.
	if _, _, ok := c.nodes[leader].Start("B"); !ok {
		t.Fatal("leader rejected B")
	}
	expectApplied(t, leader, c.applyChs[leader], 2, "B")
	expectApplied(t, other, c.applyChs[other], 2, "B")

	// Reboot the follower from its persister and reconnect it.
	c.restart(follower)
	c.setDown(follower, false)

	// It rebuilds from its saved log (A) and catches up on B from the leader.
	expectApplied(t, follower, c.applyChs[follower], 1, "A")
	expectApplied(t, follower, c.applyChs[follower], 2, "B")
}
