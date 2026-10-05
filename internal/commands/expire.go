package commands

import (
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// ExpireHandler returns a Handler for the EXPIRE command.
//
//	EXPIRE key seconds -> Integer(1) if a TTL was set, Integer(0) otherwise
//
// Arity must be exactly 3. The seconds argument must be a RESP Integer, not a
// bulk string: a real client sends `EXPIRE k 10` as `*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n:10\r\n`,
// and a bulk string in that position means the client built the request
// wrong.
//
// A zero or negative value deletes the key, which is Redis's behaviour, and
// that deletion still counts as success.
func ExpireHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) != 3 {
			return wrongArityReply("expire")
		}

		key, ok := args[1].(protocol.BulkString)
		if !ok {
			return invalidArgumentReply("expire")
		}

		seconds, ok := parseIntegerArg(args[2])
		if !ok {
			// The timeout must be an integer, in either of the two forms a
			// client may legitimately send it. See parseIntegerArg for why
			// both are accepted; redis-cli uses the bulk-string form.
			return notAnIntegerReply()
		}

		// The multiplication is guarded against overflow rather than trusted.
		// int64 seconds times one second overflows for arguments above about
		// 292 years, and a wrapped duration would turn a client asking for a
		// very long TTL into one asking for a very short or negative one —
		// which deletes the key. Clamping keeps the intent: "as long as
		// possible" is not "immediately".
		ttl := clampSecondsToDuration(seconds)

		if s.Expire(key, ttl) {
			return protocol.Integer(1), nil
		}
		return protocol.Integer(0), nil
	}
}

// maxExpireSeconds is the largest number of seconds that survives conversion
// to a time.Duration without overflowing: math.MaxInt64 nanoseconds is about
// 292 years.
const maxExpireSeconds = int64(1<<63-1) / int64(time.Second)

// clampSecondsToDuration converts seconds to a duration, saturating rather
// than wrapping.
func clampSecondsToDuration(seconds int64) time.Duration {
	if seconds > maxExpireSeconds {
		return time.Duration(1<<63 - 1)
	}
	if seconds < -maxExpireSeconds {
		return time.Duration(-1 << 63)
	}
	return time.Duration(seconds) * time.Second
}
