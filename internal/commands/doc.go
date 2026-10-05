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
// Milestone 4 brings the registry to eight commands: PING, ECHO, SET, GET,
// DEL, EXISTS, plus CLIENT and COMMAND, which exist so that modern Redis
// clients and probing tools connect quietly rather than seeing errors they
// cannot act on. EXPIRE and TTL arrive with expiration in milestone 5.
//
// A handler returns (Value, error). The Value is the reply and is always sent
// to the client; the error is an internal fault, which the server logs and
// replaces with a generic message rather than forwarding. Domain errors —
// wrong arity, a bad argument type, an unknown command — are protocol.Error
// values with a nil error, because the client is meant to read them.
package commands
