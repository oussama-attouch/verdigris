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
func Ping(args []protocol.Value) protocol.Value {
	switch len(args) {
	case 1:
		return protocol.SimpleString("PONG")

	case 2:
		// The reply is a bulk string, not a simple string: the argument is
		// arbitrary client data and may contain CRLF, which a simple string
		// cannot carry.
		msg, ok := args[1].(protocol.BulkString)
		if !ok {
			return protocol.Error("ERR invalid argument for 'ping' command")
		}
		if msg == nil {
			// A null argument has no bytes to echo. Redis treats this as a
			// protocol violation rather than inventing an empty reply.
			return protocol.Error("ERR invalid argument for 'ping' command")
		}
		return protocol.BulkString(msg)

	default:
		return protocol.Error("ERR wrong number of arguments for 'ping' command")
	}
}

// wrongArity builds the reply Redis sends when a command is given the wrong
// number of arguments. It is shared so every command words that error the
// same way, which matters because clients and test suites match on it.
func wrongArity(name string) protocol.Value {
	return protocol.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
}
