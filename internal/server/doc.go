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
// The connection handler is a loop with four steps: read bytes, ask the
// protocol package whether they hold a complete frame, hand that frame to the
// command registry, and write back whatever the registry returned. It never
// inspects a command name and never builds a reply of its own beyond the
// protocol-error frame it sends before closing a desynchronized connection.
//
// Buffering is the part worth understanding. A per-connection byte slice
// accumulates whatever has arrived, and Decode is called on it repeatedly
// until it reports ErrIncomplete, at which point the loop reads more. That is
// what makes pipelining work — several commands in one write are each decoded
// and answered in order — and it is what makes binary-safe bulk strings work,
// since a frame may contain CRLF and its length is only knowable after the
// header is parsed. A line-oriented reader cannot do either.
package server
