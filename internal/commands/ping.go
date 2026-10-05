package commands

import (
	"fmt"
	"strconv"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// Ping implements the PING command:
//
//	PING          -> SimpleString("PONG")
//	PING message  -> BulkString(message)
//
// The second form is real Redis behavior: PING with an argument echoes that
// argument back. It exists so a client can measure a round trip without
// asking for any state, and so a health check can prove it reached the right
// server rather than merely something listening on the port.
//
// Arity is 1 or 2. Anything more is an error.
func Ping(args []protocol.Value) (protocol.Value, error) {
	switch len(args) {
	case 1:
		return protocol.SimpleString("PONG"), nil

	case 2:
		// The reply is a bulk string, not a simple string: the argument is
		// arbitrary client data and may contain CRLF, which a simple string
		// cannot carry.
		msg, ok := args[1].(protocol.BulkString)
		if !ok || msg == nil {
			// A null argument has no bytes to echo. Redis treats that, and a
			// non-bulk argument, as a protocol violation rather than
			// inventing an empty reply.
			return protocol.Error("ERR invalid argument for 'ping' command"), nil
		}
		return protocol.BulkString(msg), nil

	default:
		return wrongArityReply("ping")
	}
}

// wrongArity builds the reply Redis sends when a command is given the wrong
// number of arguments. It is shared so every command words that error the
// same way, which matters because clients and test suites match on it.
func wrongArity(name string) protocol.Value {
	return protocol.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
}

// wrongArityReply pairs the wrong-arity reply with a nil error, which is the
// combination almost every call site wants. It exists so a handler's arity
// guard reads as one line instead of two.
func wrongArityReply(name string) (protocol.Value, error) {
	return wrongArity(name), nil
}

// invalidArgumentReply builds the reply for an argument of the wrong type,
// which is a malformed request rather than a wrong-arity one.
func invalidArgumentReply(name string) (protocol.Value, error) {
	return protocol.Error(fmt.Sprintf("ERR invalid argument for '%s' command", name)), nil
}

// notAnIntegerReply builds the reply Redis sends when a numeric argument
// cannot be parsed as an integer, or is outside the 64-bit range.
//
// Several commands need this — EXPIRE's timeout, SET's EX and PX counts — and
// they must word it identically, because clients match on the text. It is
// deliberately the same message for "not a number" and "out of range": Redis
// does not distinguish them either, and the client's remedy is the same.
func notAnIntegerReply() (protocol.Value, error) {
	return protocol.Error("ERR value is not an integer or out of range"), nil
}

// parseIntegerArg reads an integral argument from either of the two forms a
// real client may send.
//
// This accepts both because a faithful client sends both, and the obvious
// implementation accepts only one:
//
//   - redis-cli sends every command-line token as a bulk string, so
//     `EXPIRE k 100` arrives as `$3\r\n100\r\n` and `SET k v EX 5` as
//     `$1\r\n5\r\n`. This was verified by capturing the bytes off the wire,
//     not inferred.
//   - A client library that knows the argument is numeric may send a RESP
//     integer (`:100\r\n`), which is equally valid RESP and what the protocol
//     specification's examples imply.
//
// Redis itself accepts both for these arguments. A server that accepts only
// the integer form works with the second kind of client and fails with
// redis-cli — and the failure looks like a server bug rather than a parsing
// gap, because the client is sending something entirely reasonable.
//
// The bulk-string form is parsed strictly: no leading plus, no whitespace,
// no trailing bytes. strconv.ParseInt is used rather than a hand-rolled scan
// because it reports range errors, which is what turns a client asking for a
// 300-year TTL into a diagnosable reply instead of a wrapped duration.
func parseIntegerArg(v protocol.Value) (int64, bool) {
	switch v := v.(type) {
	case protocol.Integer:
		return int64(v), true
	case protocol.BulkString:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}
