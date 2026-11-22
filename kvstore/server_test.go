package kvstore

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/srushtisp09/raft-kv/raft"
)

func newSingleNodeKV(t *testing.T) *KVServer {
	applyCh := make(chan raft.ApplyMsg, 100)

	rf := raft.NewRaft(
		0,
		[]string{"server0"},
		applyCh,
		nil,
	)

	kv := NewKVServer(rf, applyCh)

	t.Cleanup(func() {
		kv.Kill()
		rf.Kill()
	})

	return kv
}

func waitForLeader(t *testing.T, kv *KVServer) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		err := kv.Put("_ready", "yes")

		if err == nil {
			return
		}

		if !errors.Is(err, ErrWrongLeader) {
			t.Fatalf("Put failed while waiting for leader: %v", err)
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("server did not become leader in time")
}

func TestPutGetDelete(t *testing.T) {
	kv := newSingleNodeKV(t)
	waitForLeader(t, kv)

	err := kv.Put("name", "Alice")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err := kv.Get("name")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if value != "Alice" {
		t.Fatalf("expected Alice, got %q", value)
	}

	err = kv.Delete("name")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	value, err = kv.Get("name")
	if err != nil {
		t.Fatalf("Get after Delete failed: %v", err)
	}

	if value != "" {
		t.Fatalf("expected empty value, got %q", value)
	}
}

func TestClerkPutGetDelete(t *testing.T) {
	kv := newSingleNodeKV(t)
	waitForLeader(t, kv)

	ck := MakeClerk([]*KVServer{kv})

	ck.Put("name", "Alice")

	value := ck.Get("name")
	if value != "Alice" {
		t.Fatalf("expected Alice, got %q", value)
	}

	ck.Delete("name")

	value = ck.Get("name")
	if value != "" {
		t.Fatalf("expected empty value after Delete, got %q", value)
	}
}
func TestDuplicateRequestDetection(t *testing.T) {
	kv := newSingleNodeKV(t)
	waitForLeader(t, kv)

	op1 := Op{
		Type:     "Put",
		Key:      "name",
		Value:    "Alice",
		ClientID: 100,
		SeqNum:   1,
	}

	if err := kv.PutWithOp(op1); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	op2 := Op{
		Type:     "Put",
		Key:      "name",
		Value:    "Bob",
		ClientID: 100,
		SeqNum:   1,
	}

	if err := kv.PutWithOp(op2); err != nil {
		t.Fatalf("duplicate Put failed: %v", err)
	}

	kv.mu.Lock()
	value := kv.data["name"]
	kv.mu.Unlock()

	if value != "Alice" {
		t.Fatalf("expected Alice, got %q", value)
	}
}
func TestClerkWorksAfterLeaderCrash(t *testing.T) {
	const nodeCount = 3

	peers := []string{"server0", "server1", "server2"}

	nodes := make([]*raft.Raft, nodeCount)
	applyChs := make([]chan raft.ApplyMsg, nodeCount)
	kvs := make([]*KVServer, nodeCount)

	// Tracks which servers are unavailable.
	var networkMu sync.Mutex
	down := make([]bool, nodeCount)

	// Create all Raft nodes first.
	for i := 0; i < nodeCount; i++ {
		applyChs[i] = make(chan raft.ApplyMsg, 100)

		nodes[i] = raft.NewRaft(
			i,
			peers,
			applyChs[i],
			nil,
		)
	}

	// Connect every Raft node to every other node.
	for i := 0; i < nodeCount; i++ {
		from := i

		nodes[i].SetTransport(
			func(peer int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
				networkMu.Lock()
				unavailable := down[from] || down[peer]
				networkMu.Unlock()

				if unavailable {
					return false
				}

				nodes[peer].RequestVote(args, reply)
				return true
			},
			func(peer int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
				networkMu.Lock()
				unavailable := down[from] || down[peer]
				networkMu.Unlock()

				if unavailable {
					return false
				}

				nodes[peer].AppendEntries(args, reply)
				return true
			},
		)
	}

	// Create a KV server on top of each Raft node.
	for i := 0; i < nodeCount; i++ {
		kvs[i] = NewKVServer(nodes[i], applyChs[i])
	}

	// Clean up all servers when the test finishes.
	t.Cleanup(func() {
		for _, kv := range kvs {
			kv.Kill()
		}

		for _, node := range nodes {
			node.Kill()
		}
	})

	// Wait until one of the nodes becomes leader.
	waitForClusterLeader := func() int {
		deadline := time.Now().Add(5 * time.Second)

		for time.Now().Before(deadline) {
			for i, node := range nodes {
				networkMu.Lock()
				unavailable := down[i]
				networkMu.Unlock()

				if unavailable {
					continue
				}

				_, isLeader := node.GetState()
				if isLeader {
					return i
				}
			}

			time.Sleep(20 * time.Millisecond)
		}

		t.Fatal("no leader elected in time")
		return -1
	}

	// Create a Clerk that can contact all three servers.
	ck := MakeClerk(kvs)

	// Find the initial leader.
	oldLeader := waitForClusterLeader()
	t.Logf("Initial leader: server %d", oldLeader)

	// Write data while the cluster has a leader.
	ck.Put("language", "Go")

	// Verify that the value was stored.
	value := ck.Get("language")
	if value != "Go" {
		t.Fatalf("expected Go before crash, got %q", value)
	}

	// Mark the current leader as unavailable.
	networkMu.Lock()
	down[oldLeader] = true
	networkMu.Unlock()

	// Crash the old leader.
	kvs[oldLeader].Kill()
	nodes[oldLeader].Kill()

	t.Logf("Crashed leader: server %d", oldLeader)

	// The remaining two nodes should elect a new leader.
	newLeader := waitForClusterLeader()

	if newLeader == oldLeader {
		t.Fatal("the crashed server was incorrectly reported as leader")
	}

	t.Logf("New leader: server %d", newLeader)

	// The Clerk should retry and retrieve the previously stored value.
	resultCh := make(chan string, 1)

	go func() {
		resultCh <- ck.Get("language")
	}()

	select {
	case value := <-resultCh:
		if value != "Go" {
			t.Fatalf("expected Go after leader crash, got %q", value)
		}

	case <-time.After(8 * time.Second):
		t.Fatal("Clerk could not retrieve the value after leader crash")
	}
}
