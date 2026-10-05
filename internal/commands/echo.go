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
func Echo(args []protocol.Value) (protocol.Value, error) {
	if len(args) != 2 {
		return wrongArityReply("echo")
	}

	msg, ok := args[1].(protocol.BulkString)
	if !ok || msg == nil {
		return invalidArgumentReply("echo")
	}

	// The bulk string aliases the connection's read buffer. The server
	// encodes and writes the reply before reading more bytes, so the alias is
	// still valid here; see readCommands for that ordering. This handler does
	// not retain the argument, so no copy is needed.
	return protocol.BulkString(msg), nil
}
