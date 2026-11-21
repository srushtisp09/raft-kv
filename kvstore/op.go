package kvstore

import (
	"encoding/json"
)

// Op represents a database operation replicated through Raft.
type Op struct {
	Type     string
	Key      string
	Value    string
	ClientID int64
	SeqNum   int64
}

// encodeOp converts an Op into a JSON string.
func encodeOp(op Op) (string, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// decodeOp converts a JSON string back into an Op.
func decodeOp(command string) (Op, error) {
	var op Op

	err := json.Unmarshal([]byte(command), &op)
	if err != nil {
		return Op{}, err
	}

	return op, nil
}
