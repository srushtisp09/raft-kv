package raft

import (
	"net"
	"testing"
	"time"
)

func TestThreeNodeTCPCluster(t *testing.T) {
	const nodeCount = 3

	peers := []string{
		"server0",
		"server1",
		"server2",
	}

	nodes := make([]*Raft, nodeCount)
	applyChs := make([]chan ApplyMsg, nodeCount)
	listeners := make([]net.Listener, nodeCount)
	addrs := make([]string, nodeCount)

	// Create three Raft nodes.
	for i := 0; i < nodeCount; i++ {
		applyChs[i] = make(chan ApplyMsg, 100)

		nodes[i] = NewRaft(
			i,
			peers,
			applyChs[i],
			nil,
		)
	}

	// Start a TCP RPC server for every Raft node.
	for i := 0; i < nodeCount; i++ {
		listener, err := Serve(nodes[i], "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to start server %d: %v", i, err)
		}

		listeners[i] = listener
		addrs[i] = listener.Addr().String()
	}

	// Configure every node to communicate over TCP.
	for i := 0; i < nodeCount; i++ {
		sendVote, sendAppend := NewRPCTransport(i, addrs)
		nodes[i].SetTransport(sendVote, sendAppend)
	}

	// Clean up nodes and TCP listeners after the test.
	t.Cleanup(func() {
		for _, node := range nodes {
			node.Kill()
		}

		for _, listener := range listeners {
			if listener != nil {
				listener.Close()
			}
		}
	})

	// Wait for a leader to be elected.
	leader := -1
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		for i, node := range nodes {
			_, isLeader := node.GetState()

			if isLeader {
				leader = i
				break
			}
		}

		if leader != -1 {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if leader == -1 {
		t.Fatal("no leader elected in TCP cluster")
	}

	t.Logf("TCP cluster elected server %d as leader", leader)

	// Submit a command through the elected leader.
	command := "tcp-test-entry"

	_, _, isLeader := nodes[leader].Start(command)
	if !isLeader {
		t.Fatal("selected server is no longer leader")
	}

	// Verify that every node applies the replicated command.
	for i := 0; i < nodeCount; i++ {
		select {
		case msg := <-applyChs[i]:
			if msg.Command != command {
				t.Fatalf(
					"server %d: expected command %q, got %q",
					i,
					command,
					msg.Command,
				)
			}

			t.Logf("Server %d applied command: %s", i, msg.Command)

		case <-time.After(5 * time.Second):
			t.Fatalf("server %d did not apply the command in time", i)
		}
	}
}
func TestTCPClusterReelectsAfterLeaderFailure(t *testing.T) {
	const nodeCount = 3

	peers := []string{
		"server0",
		"server1",
		"server2",
	}

	nodes := make([]*Raft, nodeCount)
	applyChs := make([]chan ApplyMsg, nodeCount)
	listeners := make([]net.Listener, nodeCount)
	addrs := make([]string, nodeCount)

	// Create three Raft nodes.
	for i := 0; i < nodeCount; i++ {
		applyChs[i] = make(chan ApplyMsg, 100)

		nodes[i] = NewRaft(
			i,
			peers,
			applyChs[i],
			nil,
		)
	}

	// Start a TCP server for every node.
	for i := 0; i < nodeCount; i++ {
		listener, err := Serve(nodes[i], "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to start server %d: %v", i, err)
		}

		listeners[i] = listener
		addrs[i] = listener.Addr().String()
	}

	// Configure TCP communication.
	for i := 0; i < nodeCount; i++ {
		sendVote, sendAppend := NewRPCTransport(i, addrs)
		nodes[i].SetTransport(sendVote, sendAppend)
	}

	// Clean up everything after the test.
	t.Cleanup(func() {
		for _, node := range nodes {
			node.Kill()
		}

		for _, listener := range listeners {
			if listener != nil {
				listener.Close()
			}
		}
	})

	// Find the initial leader.
	leader := -1
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		for i, node := range nodes {
			_, isLeader := node.GetState()

			if isLeader {
				leader = i
				break
			}
		}

		if leader != -1 {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if leader == -1 {
		t.Fatal("no initial leader elected")
	}

	t.Logf("Initial TCP leader: server %d", leader)

	// Simulate the leader crashing.
	nodes[leader].Kill()
	listeners[leader].Close()

	t.Logf("Crashed TCP leader: server %d", leader)

	// Find a new leader among the two surviving nodes.
	newLeader := -1
	deadline = time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if i == leader {
				continue
			}

			_, isLeader := node.GetState()

			if isLeader {
				newLeader = i
				break
			}
		}

		if newLeader != -1 {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if newLeader == -1 {
		t.Fatal("surviving TCP nodes did not elect a new leader")
	}

	t.Logf("New TCP leader: server %d", newLeader)

	// Submit a command through the new leader.
	command := "after-tcp-leader-crash"

	_, _, isLeader := nodes[newLeader].Start(command)
	if !isLeader {
		t.Fatal("new leader lost leadership before Start")
	}

	// Verify that both surviving nodes apply the command.
	for i := 0; i < nodeCount; i++ {
		if i == leader {
			continue
		}

		select {
		case msg := <-applyChs[i]:
			if msg.Command != command {
				t.Fatalf(
					"server %d: expected %q, got %q",
					i,
					command,
					msg.Command,
				)
			}

			t.Logf("Server %d applied command: %s", i, msg.Command)

		case <-time.After(5 * time.Second):
			t.Fatalf(
				"server %d did not apply command in time",
				i,
			)
		}
	}
}
