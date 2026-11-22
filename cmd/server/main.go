package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/srushtisp09/raft-kv/kvstore"
	"github.com/srushtisp09/raft-kv/raft"
)

func main() {
	// 1. Read command-line flags.
	id := flag.Int("id", 0, "Raft node ID")
	peersFlag := flag.String("peers", "", "Comma-separated Raft peer addresses")
	kvAddr := flag.String("kv-addr", "", "Address where KV clients connect")
	dataDir := flag.String("data-dir", "", "Directory for persistent Raft state")

	flag.Parse()

	// 2. Validate peer addresses and node ID.
	if *peersFlag == "" {
		log.Fatal("please provide -peers")
	}

	peers := strings.Split(*peersFlag, ",")
	for i := range peers {
		peers[i] = strings.TrimSpace(peers[i])
	}

	if *id < 0 || *id >= len(peers) {
		log.Fatalf("invalid node ID %d: must be between 0 and %d", *id, len(peers)-1)
	}

	if *kvAddr == "" {
		log.Fatal("please provide -kv-addr")
	}

	// 3. Create this node's persistent data directory.
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/node%d", *id)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data directory: %v", err)
	}

	p := raft.NewFilePersister(filepath.Join(*dataDir, "raft-state"))

	// 4. Create the Raft node and KV server.
	applyCh := make(chan raft.ApplyMsg, 100)

	rf := raft.NewRaft(*id, peers, applyCh, p)
	kv := kvstore.NewKVServer(rf, applyCh)

	// 5. Start the Raft TCP server.
	raftLn, err := raft.Serve(rf, peers[*id])
	if err != nil {
		kv.Kill()
		rf.Kill()
		log.Fatalf("start Raft RPC server: %v", err)
	}

	// Configure Raft's outgoing RPC connections.
	sendVote, sendAppend := raft.NewRPCTransport(*id, peers)
	rf.SetTransport(sendVote, sendAppend)

	// Start the KV TCP server.
	kvLn, err := kvstore.ServeKV(kv, *kvAddr)
	if err != nil {
		raftLn.Close()
		kv.Kill()
		rf.Kill()
		log.Fatalf("start KV RPC server: %v", err)
	}

	log.Printf("Node %d started", *id)
	log.Printf("Raft address: %s", peers[*id])
	log.Printf("KV address: %s", *kvAddr)
	log.Printf("Data directory: %s", *dataDir)

	// 6. Wait for an interrupt or termination signal.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	log.Printf("Node %d shutting down", *id)

	signal.Stop(ch)
	kvLn.Close()
	raftLn.Close()
	kv.Kill()
	rf.Kill()

	log.Printf("Node %d stopped", *id)
}
