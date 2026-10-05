// Package server owns the network layer: the TCP listener, the accept loop,
// per-client connection lifecycle, and graceful shutdown.
//
// This package is responsible for sockets, buffering, connection lifecycle,
// and running one handler goroutine per accepted connection. It is NOT
// responsible for parsing RESP frames (see package protocol), for holding
// key-value data or expiry (see package store), or for deciding what any
// command means (see package commands). The server moves bytes and
// coordinates; it does not interpret them.
//
// Milestone 1 implements the echo server: every byte read from a connection
// is written straight back. That is a placeholder, not the design. From
// milestone 2 onward, handler will hand its buffer to the protocol package
// and write whatever the command layer produces. Nothing else in this package
// needs to change to make that swap — which is the point of keeping the
// handler's body to a single, obvious function call.
package server
