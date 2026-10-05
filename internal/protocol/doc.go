// Package protocol implements the RESP (REdis Serialization Protocol) as
// specified at https://redis.io/docs/reference/protocol-spec/.
//
// The package decodes RESP frames from a byte buffer. Decode takes the bytes
// the caller has received so far and returns one frame, the number of bytes
// that frame consumed, and an error. It is a pure function: it does not read
// from a socket, does not hold state between calls, and imports nothing from
// the rest of the project. A parser that needed a connection to work would be
// a parser that could not be unit tested, and this one is tested exhaustively
// against every frame type, every malformed case, and every possible point at
// which a frame can be split.
//
// Decoding is a three-way outcome, and callers must handle all three. A nil
// error means a complete frame was found. ErrIncomplete means the buffer does
// not yet hold a whole frame, in which case nothing was consumed and the
// caller should read more bytes and call Decode again. A *ProtocolError means
// the bytes violate the RESP grammar and cannot become valid by waiting; it
// carries the byte offset where the problem was found.
//
// The caller owns the buffering. Decode never reads from the network and
// never asks for more data; it reports what the bytes it was given contain.
// When a frame arrives split across TCP segments, the caller accumulates
// bytes and calls Decode repeatedly until it stops returning ErrIncomplete.
// Decode does not copy: a BulkString aliases the buffer it was decoded from,
// so a caller that keeps a value past the next read must copy it.
//
// The encoder is not yet implemented. That is milestone 3, alongside the
// first command handlers; until then this package can read RESP but not write
// it, which is why the server still echoes bytes rather than answering
// commands.
package protocol
