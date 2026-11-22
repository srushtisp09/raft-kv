package kvstore

import (
	"net"
	"net/rpc"
)

type KVServiceRPC struct {
	service KVService
}

type GetWithOpReply struct {
	Value string
	Error string
}

type PutWithOpReply struct {
	Error string
}

type DeleteWithOpReply struct {
	Error string
}

func (s *KVServiceRPC) GetWithOp(args *Op, reply *GetWithOpReply) error {
	value, err := s.service.GetWithOp(*args)
	reply.Value = value

	if err != nil {
		reply.Error = err.Error()
	}

	return nil
}

func (s *KVServiceRPC) PutWithOp(args *Op, reply *PutWithOpReply) error {
	err := s.service.PutWithOp(*args)

	if err != nil {
		reply.Error = err.Error()
	}

	return nil
}

func (s *KVServiceRPC) DeleteWithOp(args *Op, reply *DeleteWithOpReply) error {
	err := s.service.DeleteWithOp(*args)

	if err != nil {
		reply.Error = err.Error()
	}

	return nil
}

func ServeKV(service KVService, addr string) (net.Listener, error) {
	server := rpc.NewServer()

	err := server.RegisterName("KVService", &KVServiceRPC{
		service: service,
	})
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go server.Accept(listener)

	return listener, nil
}
