package commands

import (
	"fmt"

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
