package kvstore

import (
	"sync"
	"time"

	"github.com/srushtisp09/raft-kv/raft"
)

type KVServer struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	data        map[string]string
	waitCh      map[int]chan Op
	lastApplied map[int64]int64

	localSeq int64

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewKVServer(rf *raft.Raft, applyCh chan raft.ApplyMsg) *KVServer {
	kv := &KVServer{
		rf:          rf,
		applyCh:     applyCh,
		data:        make(map[string]string),
		waitCh:      make(map[int]chan Op),
		lastApplied: make(map[int64]int64),
		stopCh:      make(chan struct{}),
	}

	go kv.applyLoop()
	return kv
}

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
				continue
			}

			kv.mu.Lock()

			lastSeq, seen := kv.lastApplied[op.ClientID]
			isNewRequest := !seen || op.SeqNum > lastSeq

			if isNewRequest {
				switch op.Type {
				case "Put":
					kv.data[op.Key] = op.Value
				case "Delete":
					delete(kv.data, op.Key)
				case "Get":
					// Get does not modify the database.
				}

				kv.lastApplied[op.ClientID] = op.SeqNum
			}

			if ch := kv.waitCh[msg.Index]; ch != nil {
				ch <- op
				delete(kv.waitCh, msg.Index)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) newLocalOp(opType, key, value string) Op {
	kv.mu.Lock()
	kv.localSeq++
	seq := kv.localSeq
	kv.mu.Unlock()

	return Op{
		Type:     opType,
		Key:      key,
		Value:    value,
		ClientID: -1,
		SeqNum:   seq,
	}
}

func (kv *KVServer) Put(key, value string) error {
	op := kv.newLocalOp("Put", key, value)
	return kv.submit(op)
}

func (kv *KVServer) Get(key string) (string, error) {
	op := kv.newLocalOp("Get", key, "")
	if err := kv.submit(op); err != nil {
		return "", err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.data[key], nil
}

func (kv *KVServer) Delete(key string) error {
	op := kv.newLocalOp("Delete", key, "")
	return kv.submit(op)
}

func (kv *KVServer) submit(op Op) error {
	command, err := encodeOp(op)
	if err != nil {
		return err
	}

	ch := make(chan Op, 1)

	kv.mu.Lock()

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	kv.waitCh[index] = ch
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp != op {
			return ErrWrongLeader
		}
		return nil

	case <-time.After(2 * time.Second):
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		return ErrTimeout

	case <-kv.stopCh:
		return ErrTimeout
	}
}

func (kv *KVServer) Kill() {
	kv.stopOnce.Do(func() {
		close(kv.stopCh)
	})
}
