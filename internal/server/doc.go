// Package server owns the network layer: the TCP listener, the accept loop,
// per-client connection state, and the event loop that dispatches readable
// connections to the protocol and command layers.
//
// This package is responsible for sockets, buffering, connection lifecycle,
// and the single-threaded event loop that mirrors Redis's own design. It is
// NOT responsible for parsing RESP frames (see package protocol), for
// holding key-value data or expiry (see package store), or for deciding what
// any command means (see package commands). The server moves bytes and
// coordinates; it does not interpret them.
//
// Milestone 1 will implement the TCP echo server. Until then, the package
// contains no exported symbols.
package server
