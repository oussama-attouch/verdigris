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
// The connection handler is a loop with five steps: read bytes, ask the
// protocol package whether they hold a complete frame, hand that frame to the
// command registry, append it to the log if it is a write, and write back
// whatever the registry returned. It never inspects a command name and never
// builds a reply of its own beyond the protocol-error frame it sends before
// closing a desynchronized connection. Whether a command is a write is the
// registry's answer, not the server's — see commands.Registry.IsWrite.
//
// That fourth step is where the durability contract lives. The log entry is
// written after the command has run and before the reply leaves, so a client
// can never receive OK for a write that was not logged. A logging failure does
// not change the reply: the command has already taken effect in the keyspace
// every other client reads, so reporting an error would tell the client the
// write did not happen when it did.
//
// # Startup
//
// Exactly one persistence source is applied before the listener opens, and the
// order is: append-only log, then snapshot, then neither. A non-empty
// -aof-path makes the log the source of truth and the snapshot is not read at
// all; SAVE still writes it. With only -rdb-path, the milestone 6 behavior is
// unchanged. With neither, the server starts empty and writes nothing.
//
// The log is replayed through the registry rather than by reimplementing each
// command, so replay and live traffic cannot drift apart, and the replay does
// not append — logging the commands it is reading would double the file on
// every start. The write handle is opened after the replay finishes, so this
// run's commands can never interleave with a previous run's unread ones.
//
// Buffering is the part worth understanding. A per-connection byte slice
// accumulates whatever has arrived, and Decode is called on it repeatedly
// until it reports ErrIncomplete, at which point the loop reads more. That is
// what makes pipelining work — several commands in one write are each decoded
// and answered in order — and it is what makes binary-safe bulk strings work,
// since a frame may contain CRLF and its length is only knowable after the
// header is parsed. A line-oriented reader cannot do either.
package server
