package kvstore

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	dialTimeout = 500 * time.Millisecond
	// Must be longer than the server's requestTimeout (2s), otherwise the
	// client gives up on requests the server is still working on.
	callTimeout = 3 * time.Second
)

type KVClient struct {
	addr string

	mu     sync.Mutex
	client *rpc.Client
	closed bool
}

// NewKVClient connects to a remote KV server. Later calls redial
// automatically if the connection breaks.
func NewKVClient(addr string) (*KVClient, error) {
	c := &KVClient{addr: addr}
	if _, err := c.getClient(); err != nil {
		return nil, err
	}
	return c, nil
}

// getClient returns the cached connection, dialing if needed.
// Dialing happens without holding the lock.
func (c *KVClient) getClient() (*rpc.Client, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("KV RPC client is closed")
	}
	cl := c.client
	c.mu.Unlock()
	if cl != nil {
		return cl, nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	fresh := rpc.NewClient(conn)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		fresh.Close()
		return nil, errors.New("KV RPC client is closed")
	}
	if c.client != nil { // someone else dialed first
		fresh.Close()
		return c.client, nil
	}
	c.client = fresh
	return fresh, nil
}

// discard drops a broken connection (only if it is still the cached one).
func (c *KVClient) discard(cl *rpc.Client) {
	c.mu.Lock()
	if c.client == cl {
		c.client = nil
	}
	c.mu.Unlock()
	cl.Close()
}

// call sends an RPC. Any transport failure becomes ErrTimeout, so the
// Clerk treats the server as unavailable and tries the next one.
func (c *KVClient) call(method string, args, reply interface{}) error {
	cl, err := c.getClient()
	if err != nil {
		return ErrTimeout
	}

	done := make(chan *rpc.Call, 1)
	cl.Go(method, args, reply, done)

	timer := time.NewTimer(callTimeout)
	defer timer.Stop()

	select {
	case result := <-done:
		if result.Error != nil {
			c.discard(cl)
			return ErrTimeout
		}
		return nil
	case <-timer.C:
		c.discard(cl)
		return ErrTimeout
	}
}

// rpcError converts the server's error string back into a sentinel error.
func rpcError(message string) error {
	switch message {
	case "":
		return nil
	case ErrWrongLeader.Error():
		return ErrWrongLeader
	case ErrTimeout.Error():
		return ErrTimeout
	default:
		return errors.New(message)
	}
}

func (c *KVClient) GetWithOp(op Op) (string, error) {
	var reply GetWithOpReply
	if err := c.call("KVService.GetWithOp", &op, &reply); err != nil {
		return "", err
	}
	if err := rpcError(reply.Error); err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (c *KVClient) PutWithOp(op Op) error {
	var reply PutWithOpReply
	if err := c.call("KVService.PutWithOp", &op, &reply); err != nil {
		return err
	}
	return rpcError(reply.Error)
}

func (c *KVClient) DeleteWithOp(op Op) error {
	var reply DeleteWithOpReply
	if err := c.call("KVService.DeleteWithOp", &op, &reply); err != nil {
		return err
	}
	return rpcError(reply.Error)
}

// Close closes the connection and stops future redials.
func (c *KVClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.client == nil {
		return nil
	}
	err := c.client.Close()
	c.client = nil
	return err
}

// Compile-time check: KVClient implements KVService.
var _ KVService = (*KVClient)(nil)
