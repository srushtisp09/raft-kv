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
func (s *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.rf.RequestVote(args, reply)
	return nil
}

// AppendEntries handles a remote AppendEntries RPC.
func (s *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.rf.AppendEntries(args, reply)
	return nil
}

// trackedListener remembers every accepted connection, so Close() can cut
// them all. This simulates a real crash: a dead process drops its open
// connections, it doesn't keep answering on them.
type trackedListener struct {
	net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

type trackedConn struct {
	net.Conn
	l *trackedListener
}

func (c *trackedConn) Close() error {
	c.l.mu.Lock()
	delete(c.l.conns, c.Conn)
	c.l.mu.Unlock()
	return c.Conn.Close()
}

func (l *trackedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		conn.Close()
		return nil, net.ErrClosed
	}
	l.conns[conn] = struct{}{}
	l.mu.Unlock()

	return &trackedConn{Conn: conn, l: l}, nil
}

// Close stops listening and closes every open connection.
func (l *trackedListener) Close() error {
	l.mu.Lock()
	l.closed = true
	open := make([]net.Conn, 0, len(l.conns))
	for c := range l.conns {
		open = append(open, c)
	}
	l.conns = make(map[net.Conn]struct{})
	l.mu.Unlock()

	err := l.Listener.Close()
	for _, c := range open {
		c.Close()
	}
	return err
}

// Serve starts an RPC server for this Raft node. Closing the returned
// listener also drops all of its open connections.
func Serve(rf *Raft, addr string) (net.Listener, error) {
	server := rpc.NewServer()

	if err := server.RegisterName("RaftRPC", &RaftRPC{rf: rf}); err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	listener := &trackedListener{
		Listener: ln,
		conns:    make(map[net.Conn]struct{}),
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
	t := &RPCTransport{
		self:    self,
		addrs:   append([]string(nil), addrs...),
		clients: make(map[int]*rpc.Client),
	}

	sendRequestVote := func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
		return t.call(peer, "RaftRPC.RequestVote", args, reply)
	}

	sendAppendEntries := func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
		return t.call(peer, "RaftRPC.AppendEntries", args, reply)
	}

	return sendRequestVote, sendAppendEntries
}

// getClient returns the cached client for a peer, dialing when necessary.
// It dials WITHOUT holding the lock, so a dead peer can't block calls to
// healthy ones.
func (t *RPCTransport) getClient(peer int) (*rpc.Client, error) {
	if peer < 0 || peer >= len(t.addrs) || peer == t.self {
		return nil, errors.New("invalid Raft peer")
	}

	t.mu.Lock()
	client := t.clients[peer]
	t.mu.Unlock()
	if client != nil {
		return client, nil
	}

	conn, err := net.DialTimeout("tcp", t.addrs[peer], 100*time.Millisecond)
	if err != nil {
		return nil, err
	}
	newClient := rpc.NewClient(conn)

	t.mu.Lock()
	if existing := t.clients[peer]; existing != nil {
		// Another goroutine dialed first; use theirs.
		t.mu.Unlock()
		newClient.Close()
		return existing, nil
	}
	t.clients[peer] = newClient
	t.mu.Unlock()

	return newClient, nil
}

// discardClient removes and closes a failed cached connection.
func (t *RPCTransport) discardClient(peer int, client *rpc.Client) {
	t.mu.Lock()
	// Don't remove a newer connection installed by another call.
	if t.clients[peer] == client {
		delete(t.clients, peer)
	}
	t.mu.Unlock()

	client.Close()
}

// call sends an RPC with a bounded wait time.
func (t *RPCTransport) call(peer int, method string, args, reply interface{}) bool {
	client, err := t.getClient(peer)
	if err != nil {
		return false
	}

	// Buffered, so the rpc goroutine never blocks if we time out first.
	done := make(chan *rpc.Call, 1)
	client.Go(method, args, reply, done)

	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case result := <-done:
		if result.Error != nil {
			t.discardClient(peer, client)
			return false
		}
		return true

	case <-timer.C:
		t.discardClient(peer, client)
		return false
	}
}
