package kvstore

type KVService interface {
	GetWithOp(op Op) (string, error)
	PutWithOp(op Op) error
	DeleteWithOp(op Op) error
}
