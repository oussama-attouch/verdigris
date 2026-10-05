// Package commands implements the command layer: the registry that maps a
// command name to its handler, the arity and argument validation that runs
// before a handler is called, and the handlers themselves.
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
// Milestone 3 will register the first handlers (PING and ECHO). Until then,
// the package contains no exported symbols.
package commands
