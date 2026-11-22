package raft

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// RaftRPC exposes Raft methods through Go's net/rpc package.
type RaftRPC struct {
	rf *Raft
}

// RequestVote handles a remote RequestVote RPC.
func (s *RaftRPC) RequestVote(
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) error {
	s.rf.RequestVote(args, reply)
	return nil
}

// AppendEntries handles a remote AppendEntries RPC.
func (s *RaftRPC) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) error {
	s.rf.AppendEntries(args, reply)
	return nil
}

// Serve starts an RPC server for this Raft node.
func Serve(rf *Raft, addr string) (net.Listener, error) {
	server := rpc.NewServer()

	if err := server.RegisterName("RaftRPC", &RaftRPC{rf: rf}); err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go server.Accept(listener)

	return listener, nil
}

// RPCTransport maintains reusable RPC clients for Raft peers.
type RPCTransport struct {
	self  int
	addrs []string

	mu      sync.Mutex
	clients map[int]*rpc.Client
}

// NewRPCTransport creates transport functions for RequestVote
// and AppendEntries RPCs.
func NewRPCTransport(
	self int,
	addrs []string,
) (
	func(int, *RequestVoteArgs, *RequestVoteReply) bool,
	func(int, *AppendEntriesArgs, *AppendEntriesReply) bool,
) {
	transport := &RPCTransport{
		self:    self,
		addrs:   append([]string(nil), addrs...),
		clients: make(map[int]*rpc.Client),
	}

	sendRequestVote := func(
		peer int,
		args *RequestVoteArgs,
		reply *RequestVoteReply,
	) bool {
		return transport.call(peer, "RaftRPC.RequestVote", args, reply)
	}

	sendAppendEntries := func(
		peer int,
		args *AppendEntriesArgs,
		reply *AppendEntriesReply,
	) bool {
		return transport.call(peer, "RaftRPC.AppendEntries", args, reply)
	}

	return sendRequestVote, sendAppendEntries
}

// getClient returns the cached client for a peer, dialing when necessary.
func (t *RPCTransport) getClient(peer int) (*rpc.Client, error) {
	if peer < 0 || peer >= len(t.addrs) || peer == t.self {
		return nil, errors.New("invalid Raft peer")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if client := t.clients[peer]; client != nil {
		return client, nil
	}

	conn, err := net.DialTimeout("tcp", t.addrs[peer], 100*time.Millisecond)
	if err != nil {
		return nil, err
	}

	client := rpc.NewClient(conn)
	t.clients[peer] = client

	return client, nil
}

// discardClient removes and closes a failed cached connection.
func (t *RPCTransport) discardClient(peer int, client *rpc.Client) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Don't remove a newer connection installed by another call.
	if t.clients[peer] == client {
		delete(t.clients, peer)
	}

	client.Close()
}

// call sends an RPC with a bounded wait time.
func (t *RPCTransport) call(
	peer int,
	method string,
	args interface{},
	reply interface{},
) bool {
	client, err := t.getClient(peer)
	if err != nil {
		return false
	}

	done := make(chan *rpc.Call, 1)

	client.Go(method, args, reply, done)

	select {
	case result := <-done:
		if result.Error != nil {
			t.discardClient(peer, client)
			return false
		}
		return true

	case <-time.After(200 * time.Millisecond):
		t.discardClient(peer, client)
		return false
	}
}
