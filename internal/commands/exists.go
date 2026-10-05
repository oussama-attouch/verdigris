package commands

import (
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// ExistsHandler returns a Handler that counts how many of the given keys
// exist.
//
//	EXISTS key [key ...] -> Integer(count)
//
// Arity must be at least 2.
//
// The count is over arguments, not distinct keys: EXISTS k k answers 2 when k
// exists. That looks like a bug and is not — Redis documents it as counting
// the arguments that exist, and real clients rely on it for the idiom
// `EXISTS a b c` as a "how many of these are present" probe. This is the
// opposite of DEL, where a repeated key is removed once.
func ExistsHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) < 2 {
			return wrongArityReply("exists")
		}

		var count int64
		for _, arg := range args[1:] {
			key, ok := arg.(protocol.BulkString)
			if !ok {
				return invalidArgumentReply("exists")
			}
			if s.Exists(key) {
				count++
			}
		}

		return protocol.Integer(count), nil
	}
}
