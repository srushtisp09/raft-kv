package kvstore

import "errors"

var (
	ErrWrongLeader = errors.New("not the leader")
	ErrTimeout     = errors.New("request timed out")
)
