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
// Milestone 3 registers the first two handlers, PING and ECHO. SET, GET,
// DEL, EXISTS, EXPIRE and TTL arrive with the store in milestones 4 and 5.
package commands
