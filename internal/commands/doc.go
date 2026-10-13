// Package commands implements the command layer: the registry that maps a
// command name to its handler, the validation that runs before a handler is
// called, and the handlers themselves.
//
// This package is responsible for command semantics — what PING, ECHO, SET,
// GET, DEL, EXISTS, EXPIRE, and TTL mean, and what reply each produces. It
// is NOT responsible for reading or writing sockets (that is the server's
// job), for encoding RESP frames (that is the protocol package's job), or
// for the storage invariants of the keyspace (that is the store's job). A
// handler receives already-decoded arguments and returns a reply value; it
// never touches a connection directly. Keeping it that way is what makes
// every command unit-testable without a socket.
//
// Milestone 6 brings the registry to twelve commands: PING, ECHO, SET, GET,
// DEL, EXISTS, EXPIRE, TTL, PERSIST, SAVE, plus CLIENT and COMMAND, which
// exist so that modern Redis clients and probing tools connect quietly rather
// than seeing errors they cannot act on.
//
// SAVE is the first command that needs anything from outside the store, and it
// gets it through a SaveFunc parameter on DefaultRegistry rather than by
// importing the server. The handler calls the function and reports its error
// as an internal fault; it does not know what a file is.
//
// A handler returns (Value, error). The Value is the reply and is always sent
// to the client; the error is an internal fault, which the server logs and
// replaces with a generic message rather than forwarding. Domain errors —
// wrong arity, a bad argument type, an unknown command — are protocol.Error
// values with a nil error, because the client is meant to read them.
//
// Integral arguments are read through parseIntegerArg rather than by a type
// assertion, because real clients send them in both RESP forms: redis-cli
// transmits `EXPIRE k 100` with the timeout as a bulk string, while a client
// library may send a RESP integer. Accepting only one form works with half the
// clients and looks like a server bug to the other half.
package commands
