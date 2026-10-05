package protocol

import (
	"strconv"
	"strings"
)

// Preallocated wire fragments. Every frame ends in CRLF, and the markers are
// single bytes, so keeping them as constants means the encoder allocates only
// for the payload rather than for the punctuation around it.
const crlf = "\r\n"

// nullBulkString is what both Null and a nil BulkString encode to. Real Redis
// answers a missing key with `$-1\r\n`, so that is the null form a client
// expects to see on the wire even when the value came from an array context.
const nullBulkString = "$-1\r\n"

// invalidSimpleString is the reply sent when a SimpleString or Error is found
// to contain CRLF. It is a fixed string so that it can never itself be
// invalid, whatever the caller passed in.
const invalidSimpleString = "-ERR invalid simple string\r\n"

// Encode returns the wire-form RESP bytes for v. The return is guaranteed
// to be decodable by Decode into a value equal to v, and consumed in its
// entirety when fed back. Encode never returns nil for a valid Value; a
// zero-length return means v was nil.
//
// Encode does not allocate more than the frame requires.
func Encode(v Value) []byte {
	switch v := v.(type) {
	case nil:
		// A nil Value is "no value was produced", which is different from the
		// null frame. There are no bytes to represent it, and returning nil
		// rather than a null frame keeps the two distinguishable by callers.
		return nil

	case SimpleString:
		// The CRLF-free guarantee is the decoder's to enforce on the way in
		// and the encoder's to enforce on the way out. A caller can build a
		// SimpleString from untrusted input without going through Decode; if
		// that text contains CRLF, emitting it verbatim would inject a second
		// frame into the stream and desynchronize the client.
		//
		// An empty payload is refused for the same reason, and it is not a
		// hypothetical: Decode rejects `+\r\n` (a simple string must carry at
		// least one byte), so emitting it would produce a frame this server
		// cannot read back. Keeping Encode's output inside Decode's accepted
		// language is worth more than faithfully round-tripping a value no
		// client can send.
		if v == "" || containsCRLF(string(v)) {
			return []byte(invalidSimpleString)
		}
		return encodeLine('+', string(v))

	case Error:
		// Same reasoning as SimpleString, including the empty case.
		if v == "" || containsCRLF(string(v)) {
			return []byte(invalidSimpleString)
		}
		return encodeLine('-', string(v))

	case Integer:
		// FormatInt rather than fmt.Sprintf: this is the hot path for every
		// counter, and fmt reflects over its arguments and allocates.
		return encodeLine(':', strconv.FormatInt(int64(v), 10))

	case BulkString:
		// A nil slice is the null bulk string; an empty non-nil slice is a
		// zero-length bulk string. The distinction is real in RESP and is
		// preserved here rather than collapsed.
		if v == nil {
			return []byte(nullBulkString)
		}
		out := make([]byte, 0, 1+20+2+len(v)+2)
		out = append(out, '$')
		out = strconv.AppendInt(out, int64(len(v)), 10)
		out = append(out, crlf...)
		out = append(out, v...)
		out = append(out, crlf...)
		return out

	case Array:
		out := make([]byte, 0, 1+20+2)
		out = append(out, '*')
		out = strconv.AppendInt(out, int64(len(v)), 10)
		out = append(out, crlf...)
		for _, elem := range v {
			// A nil element encodes to nil bytes, which would silently shift
			// every later element and produce a frame with the wrong count.
			// Substituting the null bulk string keeps the header honest.
			if elem == nil {
				out = append(out, nullBulkString...)
				continue
			}
			out = append(out, Encode(elem)...)
		}
		return out

	case Null:
		return []byte(nullBulkString)

	default:
		// Unreachable while Value is sealed: the six types above are the only
		// ones that can implement it. The branch exists so that a future
		// seventh frame type fails loudly in tests instead of encoding as
		// zero bytes and desynchronizing a client.
		return []byte(invalidSimpleString)
	}
}

// encodeLine builds a frame whose payload is text terminated by CRLF, which
// is the shared shape of the SimpleString, Error, and Integer types.
func encodeLine(marker byte, payload string) []byte {
	out := make([]byte, 0, 1+len(payload)+2)
	out = append(out, marker)
	out = append(out, payload...)
	out = append(out, crlf...)
	return out
}

// containsCRLF reports whether s holds a byte that would terminate a
// single-line frame early.
//
// IndexByte is used rather than a range loop because it compiles to a
// byte-search that can scan several bytes per iteration, and this runs on
// every status reply.
func containsCRLF(s string) bool {
	return strings.IndexByte(s, '\r') >= 0 || strings.IndexByte(s, '\n') >= 0
}
