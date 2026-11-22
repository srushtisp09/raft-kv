package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// faultyNet decides, per RPC, whether it is dropped, delayed or blocked.
type faultyNet struct {
	mu        sync.Mutex
	dropRate  float64 // 0.0 to 1.0
	maxDelay  time.Duration
	down      map[int]bool // crashed nodes
	partition map[int]int  // node -> group; nodes in different groups can't talk
}

func newFaultyNet() *faultyNet {
	return &faultyNet{down: map[int]bool{}, partition: map[int]int{}}
}

// allow reports whether a call from -> to goes through, and how long to wait first.
func (f *faultyNet) allow(from, to int) (bool, time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down[from] || f.down[to] {
		return false, 0
	}
	if f.partition[from] != f.partition[to] {
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

func (f *faultyNet) setDown(i int, v bool) {
	f.mu.Lock()
	f.down[i] = v
	f.mu.Unlock()
}

func (f *faultyNet) setPartition(groups map[int]int) {
	f.mu.Lock()
	f.partition = groups
	f.mu.Unlock()
}

func (f *faultyNet) heal() {
	f.mu.Lock()
	f.partition = map[int]int{}
	f.mu.Unlock()
}

// chaosCluster is a 5-node cluster whose network is a faultyNet.
type chaosCluster struct {
	net      *faultyNet
	peers    []string
	nodes    []*Raft
	applyChs []chan ApplyMsg
	pers     []Persister
	mu       sync.Mutex
}

func (c *chaosCluster) node(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *chaosCluster) wire(i int) {
	from := i
	c.node(i).SetTransport(
		func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
			ok, d := c.net.allow(from, peer)
			if !ok {
				return false
			}
			time.Sleep(d)
			c.node(peer).RequestVote(args, reply)
			return true
		},
		func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
			ok, d := c.net.allow(from, peer)
			if !ok {
				return false
			}
			time.Sleep(d)
			c.node(peer).AppendEntries(args, reply)
			return true
		},
	)
}

func makeChaosCluster(n int) *chaosCluster {
	c := &chaosCluster{
		net:      newFaultyNet(),
		peers:    make([]string, n),
		nodes:    make([]*Raft, n),
		applyChs: make([]chan ApplyMsg, n),
		pers:     make([]Persister, n),
	}
	for i := 0; i < n; i++ {
		c.pers[i] = &MemPersister{}
		c.applyChs[i] = make(chan ApplyMsg, 1000)
		c.nodes[i] = NewRaft(i, c.peers, c.applyChs[i], c.pers[i])
	}
	for i := 0; i < n; i++ {
		c.wire(i)
	}
	return c
}

// crash kills node i and cuts it off the network.
func (c *chaosCluster) crash(i int) {
	c.net.setDown(i, true)
	c.node(i).Kill()
}

// restart rebuilds node i from its persister and reconnects it.
func (c *chaosCluster) restart(i int) {
	ch := make(chan ApplyMsg, 1000)
	rf := NewRaft(i, c.peers, ch, c.pers[i])
	c.mu.Lock()
	c.nodes[i] = rf
	c.applyChs[i] = ch
	c.mu.Unlock()
	c.wire(i)
	c.net.setDown(i, false)
}

func (c *chaosCluster) killAll() {
	for i := range c.nodes {
		c.node(i).Kill()
	}
}

// Sanity test: with 10% drops and up to 20ms delay, a leader still emerges
// and a command still commits on every node.
func TestChaosLossyNetworkStillCommits(t *testing.T) {
	c := makeChaosCluster(5)
	defer c.killAll()

	c.net.mu.Lock()
	c.net.dropRate = 0.10
	c.net.maxDelay = 20 * time.Millisecond
	c.net.mu.Unlock()

	var leader = -1
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && leader == -1 {
		for i := range c.nodes {
			if _, isLeader := c.node(i).GetState(); isLeader {
				leader = i
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == -1 {
		t.Fatal("no leader under lossy network")
	}

	// The leader may change under drops, so retry Start until it sticks.
	var idx int
	committed := false
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end) && !committed; {
		for i := range c.nodes {
			if ix, _, ok := c.node(i).Start("chaos-1"); ok {
				idx = ix
				committed = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !committed {
		t.Fatal("no node accepted the command")
	}
	_ = idx

	// Every node should eventually apply "chaos-1" (at some index).
	for i := range c.nodes {
		got := false
		timeout := time.After(8 * time.Second)
		for !got {
			select {
			case msg := <-c.applyChs[i]:
				if msg.Command == "chaos-1" {
					got = true
				}
			case <-timeout:
				t.Fatalf("node %d never applied chaos-1", i)
			}
		}
	}
}

// collector records every ApplyMsg from a node's applyCh, across restarts.
type collector struct {
	mu      sync.Mutex
	applied map[int]string // index -> command
	bad     string         // first violation found
}

func newCollector() *collector {
	return &collector{applied: map[int]string{}}
}

func (col *collector) record(m ApplyMsg) {
	col.mu.Lock()
	defer col.mu.Unlock()
	if prev, ok := col.applied[m.Index]; ok && prev != m.Command && col.bad == "" {
		col.bad = fmt.Sprintf("index %d applied %q then %q on the same node", m.Index, prev, m.Command)
	}
	col.applied[m.Index] = m.Command
}

func TestChaosRandomCrashesAndPartitions(t *testing.T) {
	const n = 5
	c := makeChaosCluster(n)
	defer c.killAll()

	c.net.mu.Lock()
	c.net.dropRate = 0.05
	c.net.maxDelay = 15 * time.Millisecond
	c.net.mu.Unlock()

	cols := make([]*collector, n)
	for i := range cols {
		cols[i] = newCollector()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// One drainer per node; it follows restarts by re-reading the channel.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				c.mu.Lock()
				ch := c.applyChs[i]
				c.mu.Unlock()
				select {
				case m := <-ch:
					cols[i].record(m)
				case <-stop:
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
		}(i)
	}

	// Client: keep proposing unique commands to whoever is leader.
	var seq int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			seq++
			cmd := fmt.Sprintf("cmd-%d", seq)
			for i := 0; i < n; i++ {
				if _, _, ok := c.node(i).Start(cmd); ok {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	// Chaos: random crashes, restarts, partitions. Never more than 2 nodes
	// down at once, so a majority can still exist.
	downNow := map[int]bool{}
	end := time.Now().Add(8 * time.Second)
	for time.Now().Before(end) {
		switch rand.Intn(4) {
		case 0: // crash a node
			if len(downNow) < 2 {
				i := rand.Intn(n)
				if !downNow[i] {
					c.crash(i)
					downNow[i] = true
				}
			}
		case 1: // restart a node
			for i := range downNow {
				c.restart(i)
				delete(downNow, i)
				break
			}
		case 2: // random partition into two groups
			g := map[int]int{}
			for i := 0; i < n; i++ {
				g[i] = rand.Intn(2)
			}
			c.net.setPartition(g)
		case 3:
			c.net.heal()
		}
		time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
	}

	// Heal everything and let the cluster settle.
	c.net.heal()
	for i := range downNow {
		c.restart(i)
	}
	c.net.mu.Lock()
	c.net.dropRate = 0
	c.net.maxDelay = 0
	c.net.mu.Unlock()
	time.Sleep(3 * time.Second)

	close(stop)
	wg.Wait()

	// Check 1: no node ever applied two different commands at one index.
	for i, col := range cols {
		if col.bad != "" {
			t.Fatalf("node %d: %s", i, col.bad)
		}
	}
	// Check 2: nodes agree with each other at every index they share.
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			for idx, a := range cols[i].applied {
				if b, ok := cols[j].applied[idx]; ok && a != b {
					t.Fatalf("nodes %d and %d disagree at index %d: %q vs %q", i, j, idx, a, b)
				}
			}
		}
	}
	committed := 0
	for _, col := range cols {
		if len(col.applied) > committed {
			committed = len(col.applied)
		}
	}
	t.Logf("highest number of entries applied on one node: %d", committed)
	if committed == 0 {
		t.Fatal("nothing was committed during the whole run")
	}
}
