package commands

import (
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// DelHandler returns a Handler that removes one or more keys.
//
//	DEL key [key ...] -> Integer(number of keys that existed)
//
// Arity must be at least 2: DEL with no keys is a syntax error rather than a
// no-op, matching Redis. Deleting a key that does not exist is not an error
// and does not count towards the result.
func DelHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) < 2 {
			return wrongArityReply("del")
		}

		// Every argument is validated before any deletion happens. Doing both
		// in one pass would let a malformed key three arguments in leave the
		// first two already deleted, so a command that answered with an error
		// would still have changed the keyspace — and the client would have no
		// way to learn what was removed.
		//
		// The cost is a second iteration over a handful of arguments. The
		// alternative, deleting as we go and trying to undo on failure, would
		// be more code and could not be made correct without the store
		// supporting transactions it does not have.
		keys := make([]protocol.BulkString, 0, len(args)-1)
		for _, arg := range args[1:] {
			key, ok := arg.(protocol.BulkString)
			if !ok {
				return invalidArgumentReply("del")
			}
			keys = append(keys, key)
		}

		// Deleting the same key twice in one command counts it once, because
		// the second Delete reports false. That is Redis's behaviour too: the
		// reply counts keys actually removed, not arguments naming keys.
		// (EXISTS is the opposite; see exists.go.)
		var removed int64
		for _, key := range keys {
			if s.Delete(key) {
				removed++
			}
		}

		return protocol.Integer(removed), nil
	}
}
