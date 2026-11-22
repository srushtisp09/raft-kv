package kvstore

import (
	"sync"
	"time"

	"github.com/srushtisp09/raft-kv/raft"
)

// localClientID marks ops issued directly on the server (not by a Clerk).
// They are exempt from duplicate detection. Clerks must never use this ID.
const localClientID int64 = -1

const requestTimeout = 2 * time.Second

// result is what applyLoop hands back to the waiting request handler.
// Value is captured at the moment the op is applied, so a Get returns the
// state at its exact log position.
type result struct {
	Op    Op
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	data        map[string]string
	waitCh      map[int]chan result // log index -> waiting handler
	lastApplied map[int64]int64     // clientID -> highest SeqNum applied
	localSeq    int64

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewKVServer(rf *raft.Raft, applyCh chan raft.ApplyMsg) *KVServer {
	kv := &KVServer{
		rf:          rf,
		applyCh:     applyCh,
		data:        make(map[string]string),
		waitCh:      make(map[int]chan result),
		lastApplied: make(map[int64]int64),
		stopCh:      make(chan struct{}),
	}

	go kv.applyLoop()
	return kv
}

// applyLoop applies committed ops in order and wakes the matching handler.
func (kv *KVServer) applyLoop() {
	for {
		select {
		case <-kv.stopCh:
			return

		case msg, ok := <-kv.applyCh:
			if !ok {
				return
			}

			op, err := decodeOp(msg.Command)
			if err != nil {
				continue // not a KV command; ignore
			}

			kv.mu.Lock()

			// Local ops are never deduplicated. Client ops are applied only
			// if their SeqNum is newer than the last one from that client.
			lastSeq, seen := kv.lastApplied[op.ClientID]
			isNew := op.ClientID == localClientID || !seen || op.SeqNum > lastSeq

			if isNew {
				switch op.Type {
				case "Put":
					kv.data[op.Key] = op.Value
				case "Delete":
					delete(kv.data, op.Key)
				}
				if op.ClientID != localClientID {
					kv.lastApplied[op.ClientID] = op.SeqNum
				}
			}

			res := result{Op: op}
			if op.Type == "Get" {
				res.Value = kv.data[op.Key] // state at this log position
			}

			// Channel is buffered (size 1) and removed right after the send,
			// so this never blocks while holding kv.mu.
			if ch, ok := kv.waitCh[msg.Index]; ok {
				ch <- res
				delete(kv.waitCh, msg.Index)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) newLocalOp(opType, key, value string) Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.localSeq++
	return Op{
		Type:     opType,
		Key:      key,
		Value:    value,
		ClientID: localClientID,
		SeqNum:   kv.localSeq,
	}
}

// Local helpers (used by tests and simple callers).

func (kv *KVServer) Put(key, value string) error {
	_, err := kv.submit(kv.newLocalOp("Put", key, value))
	return err
}

func (kv *KVServer) Get(key string) (string, error) {
	return kv.submit(kv.newLocalOp("Get", key, ""))
}

func (kv *KVServer) Delete(key string) error {
	_, err := kv.submit(kv.newLocalOp("Delete", key, ""))
	return err
}

// Methods used by the Clerk (the Clerk supplies ClientID and SeqNum).

func (kv *KVServer) PutWithOp(op Op) error {
	_, err := kv.submit(op)
	return err
}

func (kv *KVServer) GetWithOp(op Op) (string, error) {
	return kv.submit(op)
}

func (kv *KVServer) DeleteWithOp(op Op) error {
	_, err := kv.submit(op)
	return err
}

// submit appends op to the Raft log and waits until it is applied.
// It returns the value for Gets.
func (kv *KVServer) submit(op Op) (string, error) {
	command, err := encodeOp(op)
	if err != nil {
		return "", err
	}

	ch := make(chan result, 1)

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return "", ErrWrongLeader
	}
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	// Remove our channel on any exit path, but only if it is still ours.
	cleanup := func() {
		kv.mu.Lock()
		if kv.waitCh[index] == ch {
			delete(kv.waitCh, index)
		}
		kv.mu.Unlock()
	}

	timer := time.NewTimer(requestTimeout)
	defer timer.Stop()

	select {
	case res := <-ch:
		// A different op at our index means we lost leadership and the
		// slot was overwritten. The client must retry.
		if res.Op != op {
			return "", ErrWrongLeader
		}
		return res.Value, nil

	case <-timer.C:
		cleanup()
		return "", ErrTimeout

	case <-kv.stopCh:
		cleanup()
		return "", ErrTimeout
	}
}

// Kill stops the apply loop. Safe to call more than once.
func (kv *KVServer) Kill() {
	kv.stopOnce.Do(func() {
		close(kv.stopCh)
	})
}
