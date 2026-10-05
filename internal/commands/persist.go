package commands

import (
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// PersistHandler returns a Handler for the PERSIST command.
//
//	PERSIST key -> Integer(1) if a TTL was removed, Integer(0) otherwise
//
// Arity must be exactly 2.
//
// The zero reply covers two different situations — the key does not exist, and
// the key exists but has no expiry — because Redis counts expiries removed
// rather than keys touched. A caller that needs to tell those apart should ask
// EXISTS first, which is the only way to know anyway: between the two calls
// the answer can change.
func PersistHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) != 2 {
			return wrongArityReply("persist")
		}

		key, ok := args[1].(protocol.BulkString)
		if !ok {
			return invalidArgumentReply("persist")
		}

		if s.Persist(key) {
			return protocol.Integer(1), nil
		}
		return protocol.Integer(0), nil
	}
}
