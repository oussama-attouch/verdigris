package commands

import "github.com/Oussama-Att/verdigris/internal/protocol"

// Handler executes one command. It receives the full argument list — the
// command name as args[0] — and returns the reply and an internal fault.
//
// The Value is always sent to the client. For domain errors (wrong arity,
// bad type, unknown key), return a protocol.Error value and nil error. For
// internal faults (a bug in the handler), return nil and a non-nil error;
// the caller logs the error and replies with a generic message. Never leak
// an internal error message to the client.
//
// A handler never touches a socket, never encodes bytes, and never knows
// which client sent the request. Arguments may alias the connection's read
// buffer; a handler that retains them must copy, and the store does.
type Handler func(args []protocol.Value) (protocol.Value, error)

// HandlerError builds the reply for a domain error — something the client did
// wrong, such as a bad arity or a bad argument type.
//
// It returns a Value, not an error, because a domain error is part of the
// protocol: the client is meant to read it. The internal-fault channel is the
// Handler's error return, and the two must not be confused. Malformed input
// is not a server bug, and a server bug is not something to describe to a
// client.
//
// Callers write `return HandlerError(msg), nil`. The nil is the point: no
// internal fault occurred.
func HandlerError(msg string) protocol.Value {
	return protocol.Error(msg)
}
