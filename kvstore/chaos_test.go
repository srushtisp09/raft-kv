package kvstore

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/srushtisp09/raft-kv/raft"
)

// ---- fault-injecting network (same idea as raft/chaos_test.go) ----

type kvNet struct {
	mu        sync.Mutex
	dropRate  float64
	maxDelay  time.Duration
	down      map[int]bool
	partition map[int]int
}

func newKVNet() *kvNet {
	return &kvNet{down: map[int]bool{}, partition: map[int]int{}}
}

func (f *kvNet) allow(from, to int) (bool, time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[from] || f.down[to] || f.partition[from] != f.partition[to] {
		return false, 0
	}
	if f.dropRate > 0 && rand.Float64() < f.dropRate {
		return false, 0
	}
	var d time.Duration
	if f.maxDelay > 0 {
		d = time.Duration(rand.Int63n(int64(f.maxDelay)))
	}
	return true, d
}

func (f *kvNet) isDown(i int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.down[i]
}

func (f *kvNet) setDown(i int, v bool) {
	f.mu.Lock()
	f.down[i] = v
	f.mu.Unlock()
}

func (f *kvNet) setPartition(g map[int]int) {
	f.mu.Lock()
	f.partition = g
	f.mu.Unlock()
}

func (f *kvNet) setFaults(drop float64, delay time.Duration) {
	f.mu.Lock()
	f.dropRate, f.maxDelay = drop, delay
	f.mu.Unlock()
}

// ---- cluster of Raft + KVServer nodes ----

type kvNode struct {
	rf *raft.Raft
	kv *KVServer
}

type kvChaos struct {
	net   *kvNet
	peers []string
	pers  []raft.Persister
	mu    sync.Mutex
	nodes []*kvNode
}

func (c *kvChaos) node(i int) *kvNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

// start (re)builds node i from its persister; the KV map is rebuilt by log replay.
func (c *kvChaos) start(i int) {
	ch := make(chan raft.ApplyMsg, 1000)
	rf := raft.NewRaft(i, c.peers, ch, c.pers[i])
	kv := NewKVServer(rf, ch)

	from := i
	rf.SetTransport(
		func(peer int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
			ok, d := c.net.allow(from, peer)
			if !ok {
				return false
			}
			time.Sleep(d)
			c.node(peer).rf.RequestVote(args, reply)
			return true
		},
		func(peer int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
			ok, d := c.net.allow(from, peer)
			if !ok {
				return false
			}
			time.Sleep(d)
			c.node(peer).rf.AppendEntries(args, reply)
			return true
		},
	)

	c.mu.Lock()
	c.nodes[i] = &kvNode{rf: rf, kv: kv}
	c.mu.Unlock()
}

func (c *kvChaos) crash(i int) {
	c.net.setDown(i, true)
	nd := c.node(i)
	nd.kv.Kill()
	nd.rf.Kill()
}

func (c *kvChaos) restart(i int) {
	c.start(i)
	c.net.setDown(i, false)
}

func (c *kvChaos) killAll() {
	for i := range c.nodes {
		nd := c.node(i)
		nd.kv.Kill()
		nd.rf.Kill()
	}
}

// kvSlot is a stable KVService handle for node i that survives restarts.
type kvSlot struct {
	c *kvChaos
	i int
}

func (s *kvSlot) GetWithOp(op Op) (string, error) {
	if s.c.net.isDown(s.i) {
		return "", ErrTimeout
	}
	return s.c.node(s.i).kv.GetWithOp(op)
}

func (s *kvSlot) PutWithOp(op Op) error {
	if s.c.net.isDown(s.i) {
		return ErrTimeout
	}
	return s.c.node(s.i).kv.PutWithOp(op)
}

func (s *kvSlot) DeleteWithOp(op Op) error {
	if s.c.net.isDown(s.i) {
		return ErrTimeout
	}
	return s.c.node(s.i).kv.DeleteWithOp(op)
}

// ---- porcupine model: one register per key ----

type kvInput struct {
	Op    int // 0 = Get, 1 = Put, 2 = Delete
	Key   string
	Value string
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		var parts [][]porcupine.Operation
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(kvInput)
		s := state.(string)
		switch in.Op {
		case 0: // Get must return the current value
			return output.(string) == s, s
		case 1: // Put
			return true, in.Value
		default: // Delete
			return true, ""
		}
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

func TestKVChaosLinearizable(t *testing.T) {
	const n = 5
	const numClients = 4
	keys := []string{"k0", "k1", "k2"}

	c := &kvChaos{
		net:   newKVNet(),
		peers: make([]string, n),
		pers:  make([]raft.Persister, n),
		nodes: make([]*kvNode, n),
	}
	for i := 0; i < n; i++ {
		c.pers[i] = &raft.MemPersister{}
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	defer c.killAll()

	c.net.setFaults(0.05, 15*time.Millisecond)

	begin := time.Now()
	now := func() int64 { return time.Since(begin).Nanoseconds() }

	var histMu sync.Mutex
	var history []porcupine.Operation
	add := func(op porcupine.Operation) {
		histMu.Lock()
		history = append(history, op)
		histMu.Unlock()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for id := 0; id < numClients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			services := make([]KVService, n)
			for i := range services {
				services[i] = &kvSlot{c: c, i: i}
			}
			ck := MakeClerk(services)

			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}

				key := keys[rand.Intn(len(keys))]
				call := now()

				switch r := rand.Intn(10); {
				case r < 4: // Put
					val := fmt.Sprintf("c%d-%d", id, seq)
					ok := ck.TryPut(key, val, 3*time.Second)
					ret := now()
					if !ok {
						ret = math.MaxInt64 // unknown: may take effect any time later
					}
					add(porcupine.Operation{ClientId: id, Input: kvInput{Op: 1, Key: key, Value: val}, Call: call, Output: "", Return: ret})

				case r < 5: // Delete
					ok := ck.TryDelete(key, 3*time.Second)
					ret := now()
					if !ok {
						ret = math.MaxInt64
					}
					add(porcupine.Operation{ClientId: id, Input: kvInput{Op: 2, Key: key}, Call: call, Output: "", Return: ret})

				default: // Get
					v, ok := ck.TryGet(key, 3*time.Second)
					ret := now()
					if ok { // a failed Get changes nothing, so it is dropped
						add(porcupine.Operation{ClientId: id, Input: kvInput{Op: 0, Key: key}, Call: call, Output: v, Return: ret})
					}
				}
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
		}(id)
	}

	// Chaos: never more than 2 of 5 nodes down.
	downNow := map[int]bool{}
	end := time.Now().Add(8 * time.Second)
	for time.Now().Before(end) {
		switch rand.Intn(4) {
		case 0:
			if len(downNow) < 2 {
				i := rand.Intn(n)
				if !downNow[i] {
					c.crash(i)
					downNow[i] = true
				}
			}
		case 1:
			for i := range downNow {
				c.restart(i)
				delete(downNow, i)
				break
			}
		case 2:
			g := map[int]int{}
			for i := 0; i < n; i++ {
				g[i] = rand.Intn(2)
			}
			c.net.setPartition(g)
		case 3:
			c.net.setPartition(map[int]int{})
		}
		time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
	}

	// Heal, then let clients run a bit more so pending ops finish.
	c.net.setPartition(map[int]int{})
	for i := range downNow {
		c.restart(i)
	}
	c.net.setFaults(0, 0)
	time.Sleep(3 * time.Second)

	close(stop)
	wg.Wait()

	t.Logf("recorded %d operations", len(history))
	if len(history) == 0 {
		t.Fatal("no operations recorded")
	}

	switch porcupine.CheckOperationsTimeout(kvModel, history, 60*time.Second) {
	case porcupine.Illegal:
		t.Fatal("history is NOT linearizable")
	case porcupine.Unknown:
		t.Log("checker timed out: result unknown (try fewer keys or a shorter run)")
	default:
		t.Log("history is linearizable")
	}
}
