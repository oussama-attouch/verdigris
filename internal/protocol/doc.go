// Package protocol implements the RESP (REdis Serialization Protocol) as
// specified at https://redis.io/docs/reference/protocol-spec/.
//
// The package both decodes and encodes RESP frames, and it is the only place
// that knows the wire format. Decode takes the bytes the caller has received
// so far and returns one frame, the number of bytes that frame consumed, and
// an error. Encode is its inverse: it takes a Value and returns the bytes
// that represent it. Both are pure functions: they do not read from a socket,
// do not hold state between calls, and import nothing from the rest of the
// project. A parser that needed a connection to work would be a parser that
// could not be unit tested, and this one is tested exhaustively against every
// frame type, every malformed case, and every possible point at which a frame
// can be split.
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
// Encode keeps its output inside the language Decode accepts, which is a
// stronger promise than "produces plausible bytes". Two values are therefore
// refused rather than emitted: a SimpleString or Error that is empty or
// contains CRLF, because either would produce a frame Decode rejects or a
// second frame injected into the stream. Both are answered with the error
// frame `-ERR invalid simple string`. Callers cannot reach those cases through
// Decode, which never produces them; they exist for a caller that constructs a
// Value from untrusted input directly.
//
// The two functions are not perfectly inverse for every Value, and the
// exceptions are properties of RESP2 rather than defects: a nil BulkString and
// Null share one wire form and cannot be told apart on the way back, as do a
// nil and an empty Array. Everything else round-trips exactly, and the
// property is checked by a fuzz target as well as a fixed table.
package protocol
