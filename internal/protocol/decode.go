package protocol

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrIncomplete reports that buf does not yet hold a complete frame. It is not
// a failure: the caller should read more bytes from the connection and call
// Decode again with the accumulated buffer. Decode consumes nothing when it
// returns ErrIncomplete, so retrying with a larger buffer is always safe.
var ErrIncomplete = errors.New("protocol: incomplete frame")

// ProtocolError is returned when buf contains bytes that violate the RESP
// grammar. Offset is the byte position within the input where the problem
// was detected.
//
// Unlike ErrIncomplete, this is terminal: the bytes on the wire cannot become
// valid by waiting for more of them, so the caller's only correct response is
// to report the error to the peer and close the connection.
type ProtocolError struct {
	Offset int
	Reason string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol: %s at offset %d", e.Reason, e.Offset)
}

// cursor is a position marker over an immutable input buffer.
//
// The alternative was free functions taking (buf, pos) and returning a new
// position. A cursor won because RESP arrays are recursive and the recursion
// is the hard part: with a cursor, a child decode is just another method call
// that advances the shared position, and every error can report the offset
// without any plumbing. With free functions every call site has to thread the
// position back out, including on the error paths — which is exactly where
// offsets get lost.
type cursor struct {
	buf []byte
	pos int
}

// len reports how many bytes remain unread.
func (c *cursor) len() int { return len(c.buf) - c.pos }

// line returns the bytes between the current position and the next CRLF, and
// advances past that CRLF. The returned slice excludes the terminator and
// aliases the input buffer; callers must not retain it past the lifetime of
// the buffer.
//
// A bare LF is not a terminator. Requiring the CR is what distinguishes a
// real RESP client from a hostile or broken one, and it costs one comparison.
// (Redis itself accepts a bare LF here for the length lines. We do not, by
// decision: accepting two forms means two code paths to test and a second
// frame boundary for an attacker to probe.)
func (c *cursor) line() ([]byte, error) {
	start := c.pos
	for i := start; i < len(c.buf); i++ {
		switch c.buf[i] {
		case '\r':
			// A CR must be followed by LF. If LF is not the next byte, the
			// input is malformed rather than incomplete: more bytes cannot
			// repair a CR that is already followed by something else.
			if i+1 >= len(c.buf) {
				return nil, ErrIncomplete
			}
			if c.buf[i+1] != '\n' {
				return nil, c.errAt(i, "bare CR not followed by LF")
			}
			line := c.buf[start:i]
			c.pos = i + 2
			return line, nil
		case '\n':
			return nil, c.errAt(i, "bare LF is not a valid terminator")
		}
	}
	// No terminator anywhere in the remaining bytes.
	return nil, ErrIncomplete
}

// take returns the next n bytes and advances past them.
//
// It re-checks the bound rather than trusting callers. Every caller already
// validates, so this is defence in depth against exactly the class of bug
// that a wrapped length comparison produces: a negative index reaching a
// slice expression and panicking in a goroutine that serves a connection.
func (c *cursor) take(n int) ([]byte, error) {
	if n < 0 || n > c.len() {
		return nil, ErrIncomplete
	}
	start := c.pos
	out := c.buf[start : start+n]
	c.pos = start + n
	return out, nil
}

// errAt builds a ProtocolError at an absolute offset in the input.
func (c *cursor) errAt(offset int, reason string) *ProtocolError {
	return &ProtocolError{Offset: offset, Reason: reason}
}

// Decode parses one RESP frame from the front of buf. On success it returns
// the value and the number of bytes consumed. If buf does not contain a
// complete frame, it returns ErrIncomplete with 0 bytes consumed, and the
// caller should read more bytes and try again. If buf contains a frame that
// violates the RESP grammar, it returns a typed error with the number of
// bytes inspected so far.
//
// Decode does not allocate a copy of buf's contents beyond what the decoded
// value requires; a BulkString aliases into buf rather than copying. Callers
// that intend to retain the value beyond the lifetime of buf must copy it.
func Decode(buf []byte) (Value, int, error) {
	c := &cursor{buf: buf}
	v, err := decodeValue(c)
	if err != nil {
		// An incomplete frame consumes nothing. Reporting a partial count
		// would invite a caller to advance its read position past bytes that
		// belong to a frame it has not parsed yet, which corrupts the stream
		// silently — the worst possible failure for a protocol decoder.
		if errors.Is(err, ErrIncomplete) {
			return nil, 0, err
		}
		// A protocol violation is terminal for the connection, but the caller
		// still benefits from knowing how much was understood before the
		// problem, so the consumed count is reported rather than zeroed.
		return nil, c.pos, err
	}
	return v, c.pos, nil
}

// decodeValue reads one frame from the front of c, advancing it past that
// frame. It is the recursive core: arrays call it once per element.
func decodeValue(c *cursor) (Value, error) {
	if c.len() == 0 {
		return nil, ErrIncomplete
	}

	marker := c.buf[c.pos]
	c.pos++ // consume exactly one marker byte; the payload starts after it

	switch marker {
	case '+':
		line, err := c.line()
		if err != nil {
			return nil, err
		}
		// A simple string is "simple" because it cannot contain CRLF. The
		// terminator guarantees that, but an empty payload is still a
		// violation: the wire form is "+<text>\r\n" with text non-empty.
		if len(line) == 0 {
			return nil, c.errAt(c.pos-3, "simple string must contain at least one byte")
		}
		return SimpleString(line), nil

	case '-':
		line, err := c.line()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			return nil, c.errAt(c.pos-3, "error must contain at least one byte")
		}
		return Error(line), nil

	case ':':
		line, err := c.line()
		if err != nil {
			return nil, err
		}
		return decodeInteger(c, line)

	case '$':
		return decodeBulkString(c)

	case '*':
		return decodeArray(c)

	default:
		// The marker was already consumed, so the offending byte sits one
		// position back. Pointing at it is what makes a ProtocolError
		// debuggable: the caller can index the input and see the bad byte.
		return nil, c.errAt(c.pos-1, fmt.Sprintf("invalid type marker %q", marker))
	}
}

// decodeInteger parses the body of an integer frame. The CRLF has already
// been consumed by the caller.
func decodeInteger(c *cursor, line []byte) (Value, error) {
	// The offset of the first body byte: c.pos is just past the CRLF, so the
	// body starts at c.pos-2-len(line) for a body of this length.
	bodyStart := c.pos - 2 - len(line)

	// ParseInt, not Atoi: it reports overflow instead of wrapping. RESP
	// integers are defined as exactly 64-bit signed, so a value that does not
	// fit is a malformed frame, not a number to be truncated.
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return nil, c.errAt(bodyStart, "integer out of 64-bit signed range")
		}
		return nil, c.errAt(bodyStart, fmt.Sprintf("invalid integer %q", line))
	}
	return Integer(n), nil
}

// decodeBulkString parses a bulk string body: a signed length, then CRLF, then
// exactly that many bytes, then CRLF.
func decodeBulkString(c *cursor) (Value, error) {
	lengthStart := c.pos

	line, err := c.line()
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return nil, c.errAt(lengthStart, "bulk length out of 64-bit signed range")
		}
		return nil, c.errAt(lengthStart, fmt.Sprintf("invalid bulk length %q", line))
	}

	// -1 is the null bulk string. Any other negative length is meaningless
	// and therefore malformed.
	if n == -1 {
		return Null{}, nil
	}
	if n < 0 {
		return nil, c.errAt(lengthStart, fmt.Sprintf("negative bulk length %d", n))
	}

	// Bounds are checked before any allocation or slicing. A declared length
	// of 2^63-1 with a 10-byte buffer must return ErrIncomplete, so this
	// comparison has to happen before make or take is reached — and it does,
	// because nothing is allocated until after this point.
	//
	// The remaining bytes are moved to the left of the comparison rather than
	// adding 2 to n. Computing n+2 first would wrap to a negative number for
	// a length near MaxInt64, the guard would compare as satisfied, and the
	// subsequent slice would panic — a remote crash from a 21-byte packet.
	// Subtracting from the (small, non-negative) remaining count cannot
	// overflow.
	//
	// n+2 is the payload plus the trailing CRLF.
	if n > int64(c.len())-2 {
		return nil, ErrIncomplete
	}

	payload, err := c.take(int(n))
	if err != nil {
		// Unreachable while the bounds check above is correct, and left in
		// place on purpose: take re-checks the same bound, so if a future edit
		// weakens the check above, this becomes the line that turns a bad
		// length into ErrIncomplete instead of a negative slice index and a
		// panic. It is the second of two guards, not dead weight.
		return nil, err
	}

	// The declared length was satisfied, so the terminator must be present
	// and correct. Two bytes are guaranteed to remain by the bounds check.
	if c.buf[c.pos] != '\r' || c.buf[c.pos+1] != '\n' {
		return nil, c.errAt(c.pos, "bulk string not terminated by CRLF")
	}
	c.pos += 2

	return BulkString(payload), nil
}

// decodeArray parses an array body: a signed element count followed by that
// many complete frames.
func decodeArray(c *cursor) (Value, error) {
	countStart := c.pos

	line, err := c.line()
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return nil, c.errAt(countStart, "array count out of 64-bit signed range")
		}
		return nil, c.errAt(countStart, fmt.Sprintf("invalid array count %q", line))
	}

	// -1 is the null array, indistinguishable from the null bulk string at
	// the semantic level in RESP2.
	if n == -1 {
		return Null{}, nil
	}
	if n < 0 {
		return nil, c.errAt(countStart, fmt.Sprintf("negative array count %d", n))
	}

	// No preallocation from n. The count is attacker-controlled and a frame
	// may declare millions of elements while supplying none, so trusting it
	// would turn a nine-byte input into a large allocation. Appending as
	// elements actually arrive makes memory proportional to bytes received,
	// which is the only bound worth having on untrusted input.
	var out Array
	for i := int64(0); i < n; i++ {
		// The child's error offset is already in this frame's coordinate
		// space, because children share the parent's cursor and the cursor
		// records absolute positions in the original input. No adjustment is
		// needed — which is one of the concrete wins of the cursor strategy.
		child, err := decodeValue(c)
		if err != nil {
			return nil, err
		}
		out = append(out, child)
	}

	return out, nil
}
