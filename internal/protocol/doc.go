// Package protocol implements the RESP (REdis Serialization Protocol) as
// specified at https://redis.io/docs/reference/protocol-spec/.
//
// This package is responsible only for encoding and decoding RESP frames.
// It has no knowledge of the data store, the network layer, or command
// semantics. A parser that needs a socket to work is a parser that cannot
// be unit tested in isolation.
//
// Milestone 2 will implement the decoder and encoder. Until then, the
// package contains no exported symbols.
package protocol
