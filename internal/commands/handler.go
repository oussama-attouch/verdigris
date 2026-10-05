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
