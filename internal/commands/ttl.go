package commands

import (
	"math"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// TTLHandler returns a Handler for the TTL command.
//
//	TTL key -> Integer(seconds remaining)
//	         | Integer(-1) when the key exists with no expiry
//	         | Integer(-2) when the key does not exist
//
// Arity must be exactly 2.
//
// The reply is rounded to the nearest second, matching Redis, so a key with
// 1500 ms left reads as 2 rather than 1.
//
// A live key never reads as 0, and that floor is not cosmetic. Rounding alone
// would report 0 for anything with under half a second left — including the
// `SET k v PX 500` case the milestone's own demo exercises, because by the
// time the reply is built a few hundred microseconds have passed and 499.9 ms
// rounds to 0. Zero is a lie about a key that exists: a client checking
// `TTL k > 0` to decide whether the key is present would conclude it is gone
// while it is still readable, and would then be surprised by the next GET.
// Redis reports at least 1 second for any key that is still alive, and the
// useful property — nothing that round-trips through this integer ever
// contradicts a key's existence — is worth more than exact rounding at the
// sub-second boundary.
func TTLHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) != 2 {
			return wrongArityReply("ttl")
		}

		key, ok := args[1].(protocol.BulkString)
		if !ok {
			return invalidArgumentReply("ttl")
		}

		remaining := s.TTL(key)

		// The sentinels are compared explicitly rather than converted
		// arithmetically. Dividing a time.Duration by time.Second is integer
		// division on nanoseconds, which happens to give the right answer for
		// these constants and would not for any magnitude below a second — so
		// the switch states the intent instead of relying on that coincidence.
		switch remaining {
		case store.TTLMissing:
			return protocol.Integer(-2), nil
		case store.TTLNoExpiry:
			return protocol.Integer(-1), nil
		}

		// A live TTL is always strictly positive, so this is the only path
		// that reaches the rounding.
		seconds := int64(math.Round(remaining.Seconds()))
		if seconds < 1 {
			seconds = 1
		}
		return protocol.Integer(seconds), nil
	}
}
