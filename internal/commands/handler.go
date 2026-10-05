package commands

import "github.com/Oussama-Att/verdigris/internal/protocol"

// Handler executes one command. It receives the full argument list — the
// command name as args[0] — and returns the reply. A handler never touches a
// socket, never encodes bytes, and never knows which client sent the request.
type Handler func(args []protocol.Value) protocol.Value

// HandlerError is returned to the client as a RESP Error frame. Handlers
// use it for both protocol violations (wrong arity, wrong type) and domain
// errors (unknown command). The distinction matters only to the caller.
func HandlerError(msg string) protocol.Value {
	return protocol.Error(msg)
}
