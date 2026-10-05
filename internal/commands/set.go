package commands

import (
	"strings"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// SetHandler returns a Handler that stores a key-value pair. The handler
// captures the store; it is constructed once at registry build time.
//
//	SET key value                 -> SimpleString("OK")
//	SET key value EX <seconds>    -> SimpleString("OK"), expires in seconds
//	SET key value PX <milliseconds> -> SimpleString("OK"), expires in ms
//
// A SET with no expiry option clears any existing TTL. That is Redis's
// behaviour, and clients rely on it to turn a temporary key permanent.
//
// Options this milestone does not support — NX, XX, GET, KEEPTTL — produce a
// syntax error rather than being ignored, because silently accepting an
// option and not honouring it is worse than refusing: a client using
// `SET k v NX` would believe it had a conditional write.
func SetHandler(s *store.Store) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		switch len(args) {
		case 3:
			key, value, ok := keyValueArgs(args)
			if !ok {
				return invalidArgumentReply("set")
			}
			s.Set(key, value)
			return protocol.SimpleString("OK"), nil

		case 5:
			key, value, ok := keyValueArgs(args)
			if !ok {
				return invalidArgumentReply("set")
			}

			option, ok := args[3].(protocol.BulkString)
			if !ok {
				return protocol.Error("ERR syntax error"), nil
			}

			// Option names are case-insensitive, like command names.
			var unit time.Duration
			switch strings.ToUpper(string(option)) {
			case "EX":
				unit = time.Second
			case "PX":
				unit = time.Millisecond
			default:
				// NX, XX, GET, KEEPTTL and anything else land here. They are
				// real Redis options this server does not implement yet.
				return protocol.Error("ERR syntax error"), nil
			}

			amount, ok := parseIntegerArg(args[4])
			if !ok {
				return notAnIntegerReply()
			}

			// Clamped rather than multiplied directly: see expire.go for why
			// an overflowing duration would invert the client's intent.
			ttl := clampAmountToDuration(amount, unit)

			s.SetWithTTL(key, value, ttl)
			return protocol.SimpleString("OK"), nil

		default:
			// Redis distinguishes "wrong number of arguments" from "syntax
			// error". A SET with a plausible but incomplete option tail — four
			// arguments, or six — is the latter; a SET with one argument is
			// simply short.
			if len(args) > 3 {
				return protocol.Error("ERR syntax error"), nil
			}
			return wrongArityReply("set")
		}
	}
}

// keyValueArgs extracts and type-checks args[1] and args[2], which every SET
// form shares.
func keyValueArgs(args []protocol.Value) (key, value protocol.BulkString, ok bool) {
	key, keyOK := args[1].(protocol.BulkString)
	value, valueOK := args[2].(protocol.BulkString)
	return key, value, keyOK && valueOK
}

// clampAmountToDuration converts a count of unit-sized intervals into a
// duration, saturating instead of wrapping.
func clampAmountToDuration(amount int64, unit time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)

	if amount > 0 && amount > int64(maxDuration/unit) {
		return maxDuration
	}
	if amount < 0 && amount < int64(-maxDuration/unit) {
		return -maxDuration
	}
	return time.Duration(amount) * unit
}
