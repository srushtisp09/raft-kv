package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srushtisp09/raft-kv/raft"
)

// newBenchCluster builds an n-node cluster. With useFile it persists to real
// files in dir; otherwise it persists in memory.
func newBenchCluster(n int, useFile bool, dir string) *kvChaos {
	c := &kvChaos{
		net:   newKVNet(),
		peers: make([]string, n),
		pers:  make([]raft.Persister, n),
		nodes: make([]*kvNode, n),
	}
	for i := 0; i < n; i++ {
		if useFile {
			c.pers[i] = raft.NewFilePersister(filepath.Join(dir, fmt.Sprintf("node%d", i)))
		} else {
			c.pers[i] = &raft.MemPersister{}
		}
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	return c
}

func runThroughput(c *kvChaos, n, clients int, dur time.Duration) float64 {
	newClerk := func() *Clerk {
		services := make([]KVService, n)
		for i := range services {
			services[i] = &kvSlot{c: c, i: i}
		}
		return MakeClerk(services)
	}

	newClerk().Put("warmup", "x") // waits until a leader exists

	var ops int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)
	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ck := newClerk()
			for j := 0; time.Now().Before(deadline); j++ {
				ck.Put(fmt.Sprintf("k%d-%d", id, j%10), "v")
				atomic.AddInt64(&ops, 1)
			}
		}(id)
	}
	wg.Wait()
	return float64(atomic.LoadInt64(&ops)) / dur.Seconds()
}

func TestBenchThroughput(t *testing.T) {
	var dirs []string
	// Delete the temp folders only at the very end, after everything stopped.
	defer func() {
		time.Sleep(500 * time.Millisecond)
		for _, d := range dirs {
			os.RemoveAll(d)
		}
	}()

	for _, useFile := range []bool{false, true} {
		for _, clients := range []int{1, 4, 16} {
			dir, err := os.MkdirTemp("", "raftbench")
			if err != nil {
				t.Fatal(err)
			}
			dirs = append(dirs, dir)

			c := newBenchCluster(3, useFile, dir)
			rate := runThroughput(c, 3, clients, 5*time.Second)
			t.Logf("fsync-to-disk=%-5v clients=%-2d  %.0f puts/sec", useFile, clients, rate)

			c.killAll()
			time.Sleep(300 * time.Millisecond)
		}
	}
}
