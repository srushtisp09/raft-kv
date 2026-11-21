package raft

import (
	"sync"
	"testing"
	"time"
)

// Creates a Raft server with a fake RequestVote function.
func newTestRaft(id int, peers []string, fakeVote func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool) *Raft {
	rf := NewRaft(id, peers, make(chan ApplyMsg, 100))
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
	nodes    []*Raft
	applyChs []chan ApplyMsg
	mu       sync.Mutex
	down     []bool // true = node is cut off from the network
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

func makeCluster(n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = "server" + string(rune('0'+i))
	}

	c := &cluster{
		nodes:    make([]*Raft, n),
		applyChs: make([]chan ApplyMsg, n),
		down:     make([]bool, n),
	}
	for i := 0; i < n; i++ {
		c.applyChs[i] = make(chan ApplyMsg, 100)
		c.nodes[i] = NewRaft(i, peers, c.applyChs[i])
	}

	for i := 0; i < n; i++ {
		from := i
		c.nodes[i].setSendRequestVote(func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
			if c.isDown(from) || c.isDown(peer) {
				return false
			}
			c.nodes[peer].RequestVote(args, reply)
			return true
		})
		c.nodes[i].setSendAppendEntries(func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
			if c.isDown(from) || c.isDown(peer) {
				return false
			}
			c.nodes[peer].AppendEntries(args, reply)
			return true
		})
	}
	return c
}

func (c *cluster) killAll() {
	for _, rf := range c.nodes {
		rf.Kill()
	}
}

// leaders returns the IDs of connected nodes that currently think they are leader.
func (c *cluster) leaders() []int {
	var ids []int
	for i, rf := range c.nodes {
		if c.isDown(i) {
			continue
		}
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
		select {
		case msg := <-ch:
			if msg.Index != idx || msg.Command != "SET name Nobita" {
				t.Fatalf("node %d applied wrong entry: %+v", i, msg)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("node %d never applied the command", i)
		}
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
	c.nodes[newLeader].mu.Lock()
	defer c.nodes[newLeader].mu.Unlock()
	if len(c.nodes[newLeader].log) < 2 || c.nodes[newLeader].log[1].Command != "A" {
		t.Fatalf("new leader lost the committed entry: %+v", c.nodes[newLeader].log)
	}
}
