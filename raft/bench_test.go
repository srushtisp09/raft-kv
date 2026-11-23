package raft

import (
	"sort"
	"testing"
	"time"
)

func waitOneLeader(c *cluster, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if len(c.leaders()) == 1 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// Measures how long a 3-node cluster takes to elect a new leader after the
// current one crashes.
func TestBenchElectionTime(t *testing.T) {
	const trials = 15
	var times []time.Duration

	for k := 0; k < trials; k++ {
		c := makeCluster(3)
		if !waitOneLeader(c, 5*time.Second) {
			c.killAll()
			t.Fatal("no initial leader")
		}
		old := c.leaders()[0]

		start := time.Now()
		c.setDown(old, true)
		c.nodes[old].Kill()
		ok := waitOneLeader(c, 5*time.Second)
		elapsed := time.Since(start)
		c.killAll()

		if !ok {
			t.Fatal("no new leader after crash")
		}
		times = append(times, elapsed)
	}

	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	t.Logf("re-election over %d trials: min=%v median=%v max=%v",
		trials, times[0], times[len(times)/2], times[len(times)-1])
}
