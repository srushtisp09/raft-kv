package kvstore

import (
	"errors"
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
