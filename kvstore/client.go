package kvstore

import (
	"math/rand"
	"sync"
	"time"
)

type Clerk struct {
	mu sync.Mutex

	servers []KVService

	leader   int
	clientID int64
	seqNum   int64
}

// MakeClerk creates a new client. The ID is random and positive, so separate
// client processes don't collide and it can never equal the reserved -1.
func MakeClerk(servers []KVService) *Clerk {
	return &Clerk{
		servers:  servers,
		leader:   0,
		clientID: rand.Int63n(1<<62) + 1,
	}
}

// Get reads a value from the KV store.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqNum++

	// Built once: every retry reuses the same SeqNum (exactly-once).
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

			if err := ck.servers[serverIndex].PutWithOp(op); err == nil {
				ck.leader = serverIndex
				return
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

			if err := ck.servers[serverIndex].DeleteWithOp(op); err == nil {
				ck.leader = serverIndex
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// tryOp is like Get/Put/Delete but gives up after timeout.
// ok=false means "unknown": the operation may or may not have taken effect.
func (ck *Clerk) tryOp(typ, key, value string, timeout time.Duration) (string, bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqNum++
	op := Op{Type: typ, Key: key, Value: value, ClientID: ck.clientID, SeqNum: ck.seqNum}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.leader + i) % len(ck.servers)

			var v string
			var err error
			switch typ {
			case "Get":
				v, err = ck.servers[idx].GetWithOp(op)
			case "Put":
				err = ck.servers[idx].PutWithOp(op)
			case "Delete":
				err = ck.servers[idx].DeleteWithOp(op)
			}
			if err == nil {
				ck.leader = idx
				return v, true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", false
}

func (ck *Clerk) TryGet(key string, timeout time.Duration) (string, bool) {
	return ck.tryOp("Get", key, "", timeout)
}

func (ck *Clerk) TryPut(key, value string, timeout time.Duration) bool {
	_, ok := ck.tryOp("Put", key, value, timeout)
	return ok
}

func (ck *Clerk) TryDelete(key string, timeout time.Duration) bool {
	_, ok := ck.tryOp("Delete", key, "", timeout)
	return ok
}
