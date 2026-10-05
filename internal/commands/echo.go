package commands

import "github.com/Oussama-Att/verdigris/internal/protocol"

// Echo implements the ECHO command:
//
//	ECHO message  -> BulkString(message)
//
// Arity must be exactly 2. The reply is always a bulk string, never a simple
// string, because the message is arbitrary client data: it may contain CRLF,
// invalid UTF-8, or NUL bytes, and only a bulk string can carry those
// unchanged.
func Echo(args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArity("echo")
	}

	msg, ok := args[1].(protocol.BulkString)
	if !ok {
		return protocol.Error("ERR invalid argument for 'echo' command")
	}
	if msg == nil {
		return protocol.Error("ERR invalid argument for 'echo' command")
	}

	// The bulk string aliases the connection's read buffer. The server
	// encodes the reply before reading more bytes, so the alias is still
	// valid here; see handleConnection for that ordering guarantee.
	return protocol.BulkString(msg)
}
