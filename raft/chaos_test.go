package raft

import (
	"math/rand"
	"sync"
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
