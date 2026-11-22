package kvstore

import (
	"sync"
	"sync/atomic"
	"time"
)

var nextClientID int64

type Clerk struct {
	mu sync.Mutex

	servers []KVService

	leader   int
	clientID int64
	seqNum   int64
}

// MakeClerk creates a new client.
func MakeClerk(servers []KVService) *Clerk {
	return &Clerk{
		servers:  servers,
		leader:   0,
		clientID: atomic.AddInt64(&nextClientID, 1),
	}
}

// Get reads a value from the KV store.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqNum++

	op := Op{
		Type:     "Get",
		Key:      key,
		ClientID: ck.clientID,
		SeqNum:   ck.seqNum,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			serverIndex := (ck.leader + i) % len(ck.servers)

			value, err := ck.servers[serverIndex].GetWithOp(op)

			if err == nil {
				ck.leader = serverIndex
				return value
			}

			if err == ErrWrongLeader || err == ErrTimeout {
				continue
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// Put writes a value into the KV store.
func (ck *Clerk) Put(key, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqNum++

	op := Op{
		Type:     "Put",
		Key:      key,
		Value:    value,
		ClientID: ck.clientID,
		SeqNum:   ck.seqNum,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			serverIndex := (ck.leader + i) % len(ck.servers)

			err := ck.servers[serverIndex].PutWithOp(op)

			if err == nil {
				ck.leader = serverIndex
				return
			}

			if err == ErrWrongLeader || err == ErrTimeout {
				continue
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// Delete removes a key from the KV store.
func (ck *Clerk) Delete(key string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqNum++

	op := Op{
		Type:     "Delete",
		Key:      key,
		ClientID: ck.clientID,
		SeqNum:   ck.seqNum,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			serverIndex := (ck.leader + i) % len(ck.servers)

			err := ck.servers[serverIndex].DeleteWithOp(op)

			if err == nil {
				ck.leader = serverIndex
				return
			}

			if err == ErrWrongLeader || err == ErrTimeout {
				continue
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}
