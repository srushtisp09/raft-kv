package kvstore

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type KVClient struct {
	mu     sync.Mutex
	client *rpc.Client
}

// NewKVClient connects to a remote KV server.
func NewKVClient(addr string) (*KVClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}

	return &KVClient{
		client: rpc.NewClient(conn),
	}, nil
}

// call sends an RPC and waits for its response.
func (c *KVClient) call(method string, args, reply interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return errors.New("KV RPC client is closed")
	}

	done := make(chan *rpc.Call, 1)
	c.client.Go(method, args, reply, done)

	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case result := <-done:
		if result.Error != nil {
			c.client.Close()
			c.client = nil
			return result.Error
		}
		return nil

	case <-timer.C:
		c.client.Close()
		c.client = nil
		return errors.New("KV RPC request timed out")
	}
}

// rpcError converts the server's error string back into a local error.
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

	err := c.call("KVService.GetWithOp", &op, &reply)
	if err != nil {
		return "", err
	}

	return reply.Value, rpcError(reply.Error)
}

func (c *KVClient) PutWithOp(op Op) error {
	var reply PutWithOpReply

	err := c.call("KVService.PutWithOp", &op, &reply)
	if err != nil {
		return err
	}

	return rpcError(reply.Error)
}

func (c *KVClient) DeleteWithOp(op Op) error {
	var reply DeleteWithOpReply

	err := c.call("KVService.DeleteWithOp", &op, &reply)
	if err != nil {
		return err
	}

	return rpcError(reply.Error)
}

// Close closes the connection to the remote server.
func (c *KVClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil
	}

	err := c.client.Close()
	c.client = nil
	return err
}

// Compile-time check: KVClient implements KVService.
var _ KVService = (*KVClient)(nil)
