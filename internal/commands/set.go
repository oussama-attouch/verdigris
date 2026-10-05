package commands

import (
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// SetHandler returns a Handler that stores a key-value pair. The handler
// captures the store; it is constructed once at registry build time.
//
//	SET key value -> SimpleString("OK")
//
// Arity must be exactly 3. The EX and PX variants arrive with expiration in
// milestone 5; until then any other arity is a syntax error, which is what
// Redis reports for a SET form it does not understand.
func SetHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) != 3 {
			// Redis distinguishes "wrong number of arguments" from "syntax
			// error", and a SET with extra tokens is the latter: the shape
			// looks like a valid SET with options we do not support yet.
			if len(args) > 3 {
				return protocol.Error("ERR syntax error"), nil
			}
			return wrongArityReply("set")
		}

		key, ok := args[1].(protocol.BulkString)
		if !ok {
			return invalidArgumentReply("set")
		}
		value, ok := args[2].(protocol.BulkString)
		if !ok {
			return invalidArgumentReply("set")
		}

		// No copy here, on purpose. The store copies on entry, so passing the
		// slices that alias the connection buffer is safe and avoids a second
		// allocation for the same bytes.
		s.Set(key, value)

		return protocol.SimpleString("OK"), nil
	}
}
