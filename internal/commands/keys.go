package commands

import (
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// KeysHandler returns the KEYS command. Only `*` is accepted.
//
// Redis's KEYS takes a glob pattern, and this accepts exactly one of them. The
// restriction is deliberate and is recorded in docs/DECISIONS.md: a matcher
// that handles `*`, `?` and `[...]` is a second language with its own edge
// cases — escapes, nested brackets, a trailing backslash — and a partial
// implementation would answer `KEYS user:*` with a wrong result rather than a
// refusal. Refusing is the honest option, and the error names the one pattern
// that works so a client is not left guessing.
//
// The reply is an array of bulk strings, one per live key, and an empty array
// when there are none. Redis answers an empty array here too, not a null, which
// matters because a client iterating the reply branches on the difference.
func KeysHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		// Arity is checked before the argument's type, so `KEYS` with no
		// pattern reports the arity problem rather than complaining about an
		// argument that was never supplied.
		if len(args) != 2 {
			return protocol.Error("ERR wrong number of arguments for 'keys' command"), nil
		}

		pattern, ok := args[1].(protocol.BulkString)
		if !ok {
			// A pattern must arrive as a bulk string. An integer or an array
			// here is a malformed request rather than an unsupported pattern,
			// and saying which it is tells the client what to fix.
			return protocol.Error("ERR invalid argument for 'keys' command"), nil
		}

		// Compared without trimming or case folding: a pattern is data, not a
		// keyword, and `" *"` is a different pattern from `"*"` in every glob
		// implementation. Accepting a padded version would be this server
		// inventing syntax that Redis does not have.
		if string(pattern) != "*" {
			return protocol.Error("ERR KEYS: only * is supported"), nil
		}

		keys := s.Keys()

		reply := make(protocol.Array, 0, len(keys))
		for _, k := range keys {
			reply = append(reply, protocol.BulkString(k))
		}
		return reply, nil
	}
}
