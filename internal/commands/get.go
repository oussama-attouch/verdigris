package commands

import (
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// GetHandler returns a Handler that reads a value by key.
//
//	GET key -> BulkString(value) when the key exists
//	GET key -> Null{}           when it does not
//
// Arity must be exactly 2.
func GetHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) != 2 {
			return wrongArityReply("get")
		}

		key, ok := args[1].(protocol.BulkString)
		if !ok {
			return invalidArgumentReply("get")
		}

		// Get returns a copy the handler owns, so returning it directly is
		// safe even though the store may be mutated concurrently afterwards.
		value, found := s.Get(key)
		if !found {
			// A missing key is not an error in Redis. The null bulk string is
			// the reply a client checks for, and it is also what
			// redis-cli renders as (nil).
			return protocol.Null{}, nil
		}

		return protocol.BulkString(value), nil
	}
}
