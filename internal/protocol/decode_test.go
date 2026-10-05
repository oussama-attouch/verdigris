package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

// decodeCase is one row of the main decode table.
type decodeCase struct {
	name string
	in   string

	// want is the expected value. Only comparable Value types may be used
	// here; nested arrays get their own dedicated test, because a slice of
	// slices has no equality operator.
	want Value

	// wantBytes is the expected payload for bulk strings. BulkString is a
	// []byte and Go slices are not comparable with ==, so those rows set this
	// field instead of want and are checked with bytes.Equal.
	wantBytes []byte

	// wantN is the expected bytes consumed. Asserting it on every row is
	// deliberate: a decoder that returns the right value with the wrong
	// consumed count desynchronizes the stream on the very next frame, and
	// pipelining is a documented RESP behaviour, not a corner case.
	wantN int

	incomplete bool // expect ErrIncomplete
	bad        bool // expect a *ProtocolError
}

// binarySafeFrame is the frame the protocol spec uses to prove bulk strings
// are not text: the payload contains a NUL, a byte that is not valid UTF-8,
// and an embedded CRLF.
//
// It is assembled from explicit bytes rather than written as one escaped
// string literal. That literal is genuinely hard to get right on paper, and
// getting it wrong produces a test that fails for the wrong reason: the
// declared length must equal exactly the number of payload bytes, and the
// terminator must then be a separate CRLF after them.
//
// Header "$6\r\n" is 4 bytes. The payload is 00 ff 0d 0a fe 0d, which is six
// bytes. The terminator is a further 0d 0a. Total 12.
func binarySafeFrame() []byte {
	header := []byte("$6\r\n")
	payload := []byte{0x00, 0xff, 0x0d, 0x0a, 0xfe, 0x0d}

	frame := make([]byte, 0, len(header)+len(payload)+2)
	frame = append(frame, header...)
	frame = append(frame, payload...)
	frame = append(frame, '\r', '\n')
	return frame
}

// TestDecode covers every frame type, every null and empty form, and every
// malformed case the protocol defines.
func TestDecode(t *testing.T) {
	tests := []decodeCase{
		// --- Simple strings ---
		{name: "simple string", in: "+OK\r\n", want: SimpleString("OK"), wantN: 5},
		{name: "simple string with spaces", in: "+Hello world\r\n", want: SimpleString("Hello world"), wantN: 14},
		{name: "simple string bare LF", in: "+OK\n", bad: true},
		{name: "simple string bare CR", in: "+OK\rX", bad: true},
		{name: "simple string empty", in: "+\r\n", bad: true},
		{name: "simple string marker only", in: "+", incomplete: true},

		// --- Errors ---
		{name: "error", in: "-ERR unknown command\r\n", want: Error("ERR unknown command"), wantN: 22},
		{name: "error empty", in: "-\r\n", bad: true},
		{name: "error bare LF", in: "-ERR\n", bad: true},

		// --- Integers ---
		{name: "integer positive", in: ":1000\r\n", want: Integer(1000), wantN: 7},
		{name: "integer zero", in: ":0\r\n", want: Integer(0), wantN: 4},
		{name: "integer negative", in: ":-42\r\n", want: Integer(-42), wantN: 6},
		{name: "integer max", in: ":9223372036854775807\r\n", want: Integer(math.MaxInt64), wantN: 22},
		{name: "integer min", in: ":-9223372036854775808\r\n", want: Integer(math.MinInt64), wantN: 23},
		{name: "integer overflow", in: ":9223372036854775808\r\n", bad: true},
		{name: "integer underflow", in: ":-9223372036854775809\r\n", bad: true},
		{name: "integer non-numeric", in: ":abc\r\n", bad: true},
		{name: "integer bare LF", in: ":12\n", bad: true},
		{name: "integer marker only", in: ":", incomplete: true},

		// --- Bulk strings ---
		{name: "bulk string", in: "$5\r\nhello\r\n", wantBytes: []byte("hello"), wantN: 11},
		{name: "bulk string empty", in: "$0\r\n\r\n", wantBytes: []byte{}, wantN: 6},
		{name: "bulk string binary safe", in: string(binarySafeFrame()), wantBytes: []byte{0x00, 0xff, 0x0d, 0x0a, 0xfe, 0x0d}, wantN: 12},
		{name: "bulk string embedded CRLF", in: "$4\r\n\r\n\r\n\r\n", wantBytes: []byte{0x0d, 0x0a, 0x0d, 0x0a}, wantN: 10},
		{name: "bulk string invalid UTF-8", in: "$3\r\n\xff\xfe\xfd\r\n", wantBytes: []byte{0xff, 0xfe, 0xfd}, wantN: 9},
		{name: "null bulk string", in: "$-1\r\n", want: Null{}, wantN: 5},
		{name: "negative non-null bulk length", in: "$-2\r\n", bad: true},
		{name: "negative large bulk length", in: "$-9999999999\r\n", bad: true},
		{name: "bulk string truncated payload", in: "$5\r\nhel", incomplete: true},
		{name: "bulk string missing terminator", in: "$5\r\nhelloXX", bad: true},
		{name: "bulk string half terminator", in: "$5\r\nhello\rX", bad: true},
		{name: "bulk string empty without payload terminator", in: "$0\r\nX\r\n", bad: true},
		{name: "bulk length non-numeric", in: "$abc\r\nhello\r\n", bad: true},
		{name: "bulk length overflow", in: "$9223372036854775808\r\n", bad: true},
		{name: "bulk length bare LF", in: "$5\nhello\r\n", bad: true},
		{name: "bulk string marker only", in: "$", incomplete: true},

		// --- Arrays ---
		// An empty array is covered by TestDecodeEmptyArray rather than here:
		// Array is a slice, and comparing one against a table value uses ==,
		// which panics. Any Array row must be tested structurally.
		{name: "null array", in: "*-1\r\n", want: Null{}, wantN: 5},
		{name: "negative non-null array count", in: "*-2\r\n", bad: true},
		{name: "array truncated mid child", in: "*2\r\n$4\r\nECHO\r\n$5\r\nhel", incomplete: true},
		{name: "array declared more children than present", in: "*3\r\n:1\r\n:2\r\n", incomplete: true},
		{name: "array count non-numeric", in: "*abc\r\n", bad: true},
		{name: "array count overflow", in: "*9223372036854775808\r\n", bad: true},
		{name: "array marker only", in: "*", incomplete: true},
		{name: "array with malformed child", in: "*2\r\n:1\r\n?bad\r\n", bad: true},

		// --- Structural ---
		{name: "invalid marker", in: "?foo\r\n", bad: true},
		{name: "empty buffer", in: "", incomplete: true},
		{name: "lone LF", in: "\n", bad: true},
		// A lone CR is taken as a marker byte, so it is an invalid marker
		// rather than an incomplete frame. Nothing about it becomes valid by
		// reading more bytes.
		{name: "lone CR", in: "\r", bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, n, err := Decode([]byte(tt.in))

			switch {
			case tt.incomplete:
				if !errors.Is(err, ErrIncomplete) {
					t.Fatalf("Decode(%q) error = %v, want ErrIncomplete", tt.in, err)
				}
				if n != 0 {
					t.Fatalf("Decode(%q) consumed %d bytes on an incomplete frame, want 0", tt.in, n)
				}
				if got != nil {
					t.Fatalf("Decode(%q) returned a value alongside ErrIncomplete: %#v", tt.in, got)
				}
				return

			case tt.bad:
				var perr *ProtocolError
				if !errors.As(err, &perr) {
					t.Fatalf("Decode(%q) error = %v (%T), want *ProtocolError", tt.in, err, err)
				}
				if perr.Offset < 0 || perr.Offset > len(tt.in) {
					t.Fatalf("Decode(%q) ProtocolError offset %d is outside the input", tt.in, perr.Offset)
				}
				if got != nil {
					t.Fatalf("Decode(%q) returned a value alongside an error: %#v", tt.in, got)
				}
				return
			}

			if err != nil {
				t.Fatalf("Decode(%q) unexpected error: %v", tt.in, err)
			}

			// Bulk strings carry []byte payloads, which have no == operator,
			// so rows for them set wantBytes and are compared byte-wise.
			if tt.wantBytes != nil {
				bs, ok := got.(BulkString)
				if !ok {
					t.Fatalf("Decode(%q) value is %T, want BulkString", tt.in, got)
				}
				if !bytes.Equal(bs, tt.wantBytes) {
					t.Fatalf("Decode(%q) value = %q, want %q", tt.in, bs, tt.wantBytes)
				}
			} else if got != tt.want {
				t.Fatalf("Decode(%q) value = %#v, want %#v", tt.in, got, tt.want)
			}

			if n != tt.wantN {
				t.Fatalf("Decode(%q) consumed %d bytes, want %d", tt.in, n, tt.wantN)
			}
		})
	}
}

// valueEqual reports whether two decoded Values are the same frame.
//
// It exists because Go slices have no == operator, and two of the six RESP
// types are slices: BulkString and Array. reflect.DeepEqual is the standard
// library's answer for exactly this.
func valueEqual(a, b Value) bool {
	return reflect.DeepEqual(a, b)
}

// TestDecodeEmptyArray covers `*0\r\n`, the zero-element array. It is
// separate from the main table because Array is a slice and slices are not
// comparable with ==.
func TestDecodeEmptyArray(t *testing.T) {
	const in = "*0\r\n"

	got, n, err := Decode([]byte(in))
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", in, err)
	}
	if n != 4 {
		t.Fatalf("consumed %d bytes, want 4", n)
	}

	arr, ok := got.(Array)
	if !ok {
		t.Fatalf("value is %T, want Array", got)
	}
	if len(arr) != 0 {
		t.Fatalf("array has %d elements, want 0", len(arr))
	}
}

// TestDecodeArrayOfBulkStrings is the canonical command frame: a Redis client
// sending `ECHO hello` puts exactly these bytes on the wire. It lives outside
// the main table because an Array containing BulkStrings is a slice of
// slices, which cannot be compared with ==.
func TestDecodeArrayOfBulkStrings(t *testing.T) {
	const in = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"

	got, n, err := Decode([]byte(in))
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", in, err)
	}
	if n != 25 {
		t.Fatalf("consumed %d bytes, want 25", n)
	}

	arr, ok := got.(Array)
	if !ok {
		t.Fatalf("value is %T, want Array", got)
	}
	if len(arr) != 2 {
		t.Fatalf("array has %d elements, want 2", len(arr))
	}
	if bs, ok := arr[0].(BulkString); !ok || !bytes.Equal(bs, []byte("ECHO")) {
		t.Fatalf("element 0 = %#v, want BulkString(\"ECHO\")", arr[0])
	}
	if bs, ok := arr[1].(BulkString); !ok || !bytes.Equal(bs, []byte("hello")) {
		t.Fatalf("element 1 = %#v, want BulkString(\"hello\")", arr[1])
	}
}

// TestDecodeArrayMixedTypes covers an array whose elements are different frame
// types, which is what a Redis multi-bulk reply mixing an integer, a status
// and a payload looks like.
func TestDecodeArrayMixedTypes(t *testing.T) {
	const in = "*3\r\n:1\r\n+OK\r\n$2\r\nhi\r\n"

	got, n, err := Decode([]byte(in))
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", in, err)
	}
	if n != 21 {
		t.Fatalf("consumed %d bytes, want 21", n)
	}

	arr, ok := got.(Array)
	if !ok {
		t.Fatalf("value is %T, want Array", got)
	}
	if len(arr) != 3 {
		t.Fatalf("array has %d elements, want 3", len(arr))
	}
	if arr[0] != Integer(1) {
		t.Fatalf("element 0 = %#v, want Integer(1)", arr[0])
	}
	if arr[1] != SimpleString("OK") {
		t.Fatalf("element 1 = %#v, want SimpleString(\"OK\")", arr[1])
	}
	if bs, ok := arr[2].(BulkString); !ok || !bytes.Equal(bs, []byte("hi")) {
		t.Fatalf("element 2 = %#v, want BulkString(\"hi\")", arr[2])
	}
}

// TestDecodeNestedArray covers arrays containing arrays, which needs its own
// test because Array is a slice and therefore not comparable in the table.
func TestDecodeNestedArray(t *testing.T) {
	const in = "*2\r\n*2\r\n:1\r\n:2\r\n:3\r\n"

	got, n, err := Decode([]byte(in))
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", in, err)
	}
	if n != 20 {
		t.Fatalf("consumed %d bytes, want 20", n)
	}

	arr, ok := got.(Array)
	if !ok {
		t.Fatalf("value is %T, want Array", got)
	}
	if len(arr) != 2 {
		t.Fatalf("top-level array has %d elements, want 2", len(arr))
	}

	inner, ok := arr[0].(Array)
	if !ok {
		t.Fatalf("element 0 is %T, want Array", arr[0])
	}
	if len(inner) != 2 || inner[0] != Integer(1) || inner[1] != Integer(2) {
		t.Fatalf("inner array = %#v, want [1 2]", inner)
	}
	if arr[1] != Integer(3) {
		t.Fatalf("element 1 = %#v, want Integer(3)", arr[1])
	}
}

// TestDecodeArrayContainingNull covers a null inside an array, which is how a
// real Redis reply signals a missing value in a multi-key response.
func TestDecodeArrayContainingNull(t *testing.T) {
	const in = "*3\r\n:1\r\n$-1\r\n:3\r\n"

	got, n, err := Decode([]byte(in))
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", in, err)
	}
	if n != 17 {
		t.Fatalf("consumed %d bytes, want 17", n)
	}

	arr, ok := got.(Array)
	if !ok {
		t.Fatalf("value is %T, want Array", got)
	}
	want := Array{Integer(1), Null{}, Integer(3)}
	if len(arr) != len(want) {
		t.Fatalf("array = %#v, want %#v", arr, want)
	}
	for i := range want {
		if arr[i] != want[i] {
			t.Fatalf("element %d = %#v, want %#v", i, arr[i], want[i])
		}
	}
}

// TestDecodePipelined is the reason the consumed count is part of the
// contract. RESP clients send several commands back to back without waiting
// for replies; the caller decodes one frame, slices its buffer by the
// returned count, and decodes again.
func TestDecodePipelined(t *testing.T) {
	buf := []byte("+OK\r\n:42\r\n$3\r\nfoo\r\n*1\r\n+PONG\r\n")

	want := []struct {
		value Value
		n     int
	}{
		{SimpleString("OK"), 5},
		{Integer(42), 5},
		{BulkString([]byte("foo")), 9},
		{Array{SimpleString("PONG")}, 11},
	}

	for i, w := range want {
		got, n, err := Decode(buf)
		if err != nil {
			t.Fatalf("frame %d: Decode error: %v", i, err)
		}
		if n != w.n {
			t.Fatalf("frame %d: consumed %d bytes, want %d", i, n, w.n)
		}
		if !valueEqual(got, w.value) {
			t.Fatalf("frame %d: value = %#v, want %#v", i, got, w.value)
		}
		buf = buf[n:]
	}

	if len(buf) != 0 {
		t.Fatalf("%d bytes left over after decoding every frame: %q", len(buf), buf)
	}
}

// splitFrames returns the inputs used by the split-point tests. Each must be a
// single complete, valid frame.
func splitFrames() [][]byte {
	return [][]byte{
		[]byte("+OK\r\n"),
		[]byte("+Hello world\r\n"),
		[]byte("-ERR unknown command\r\n"),
		[]byte(":1000\r\n"),
		[]byte(":-9223372036854775808\r\n"),
		[]byte("$5\r\nhello\r\n"),
		[]byte("$0\r\n\r\n"),
		binarySafeFrame(),
		[]byte("$-1\r\n"),
		[]byte("*0\r\n"),
		[]byte("*-1\r\n"),
		[]byte("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"),
		[]byte("*3\r\n:1\r\n+OK\r\n$2\r\nhi\r\n"),
		[]byte("*2\r\n*2\r\n:1\r\n:2\r\n:3\r\n"),
		[]byte("*3\r\n:1\r\n$-1\r\n:3\r\n"),
	}
}

// TestDecodeEverySplitPoint is the most valuable test in this file.
//
// For every complete frame it feeds all len-1 proper prefixes and requires
// ErrIncomplete from each, then requires the whole frame to decode cleanly.
// This is what proves the three-way outcome contract holds at every byte
// boundary, and it is the property the network layer will depend on: when a
// frame arrives split across TCP segments, at a point chosen by the network
// and not by us, the decoder must ask for more bytes rather than guessing or
// failing.
//
// A handful of hand-written splits would pass while leaving boundaries
// untested; enumerating all of them is the difference between nominal
// coverage and a real guarantee.
func TestDecodeEverySplitPoint(t *testing.T) {
	for _, frame := range splitFrames() {
		t.Run(fmt.Sprintf("%q", frame), func(t *testing.T) {
			for split := 0; split < len(frame); split++ {
				prefix := frame[:split]

				got, n, err := Decode(prefix)
				if !errors.Is(err, ErrIncomplete) {
					t.Fatalf("Decode(%q), a %d/%d-byte prefix, error = %v; want ErrIncomplete",
						prefix, split, len(frame), err)
				}
				if n != 0 {
					t.Fatalf("Decode(%q), a %d/%d-byte prefix, consumed %d bytes; want 0",
						prefix, split, len(frame), n)
				}
				if got != nil {
					t.Fatalf("Decode(%q) returned a value for a truncated frame: %#v", prefix, got)
				}
			}

			// The complete frame must still decode, and consume everything.
			_, n, err := Decode(frame)
			if err != nil {
				t.Fatalf("Decode(%q) of the complete frame failed: %v", frame, err)
			}
			if n != len(frame) {
				t.Fatalf("Decode(%q) consumed %d bytes, want %d", frame, n, len(frame))
			}
		})
	}
}

// TestDecodeByteAtATime feeds a frame one byte per call, the way a real
// connection delivers data when the client writes slowly or the network
// fragments aggressively. It is the split-point test applied cumulatively, and
// it exercises the caller's retry loop rather than just the decoder.
func TestDecodeByteAtATime(t *testing.T) {
	frame := []byte("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")

	var acc []byte
	for i := range frame {
		acc = append(acc, frame[i])

		_, n, err := Decode(acc)
		if i < len(frame)-1 {
			if !errors.Is(err, ErrIncomplete) {
				t.Fatalf("after %d bytes (%q) error = %v, want ErrIncomplete", i+1, acc, err)
			}
			if n != 0 {
				t.Fatalf("after %d bytes consumed %d, want 0", i+1, n)
			}
			continue
		}

		if err != nil {
			t.Fatalf("full frame failed to decode: %v", err)
		}
		if n != len(frame) {
			t.Fatalf("consumed %d bytes, want %d", n, len(frame))
		}
	}
}

// TestDecodeHugeLengthsDoNotAllocate guards the resource bound. A 21-byte
// frame can declare a nine-exabyte payload or a billion-element array; the
// decoder must reject both as incomplete without attempting to honour the
// declaration. A decoder that allocated from a declared length, or that added
// to one before comparing it, would be a one-packet memory exhaustion or a
// remote panic.
func TestDecodeHugeLengthsDoNotAllocate(t *testing.T) {
	for _, in := range []string{
		"$9223372036854775807\r\n",
		"$1000000000\r\nhello",
		"*9223372036854775807\r\n",
		"*1000000\r\n:1\r\n",
	} {
		got, n, err := Decode([]byte(in))
		if !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Decode(%q) error = %v, want ErrIncomplete", in, err)
		}
		if n != 0 {
			t.Fatalf("Decode(%q) consumed %d bytes, want 0", in, n)
		}
		if got != nil {
			t.Fatalf("Decode(%q) returned %#v, want nil", in, got)
		}
	}
}

// TestDecodeArrayHeaderOnly checks that a zero-count array consumes exactly
// its header and leaves trailing bytes for the next frame: the difference
// between "an empty array" and "an empty array followed by more input".
func TestDecodeArrayHeaderOnly(t *testing.T) {
	in := []byte("*0\r\n:1\r\n")

	got, n, err := Decode(in)
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", in, err)
	}
	if n != 4 {
		t.Fatalf("consumed %d bytes, want 4 (the header only)", n)
	}
	arr, ok := got.(Array)
	if !ok {
		t.Fatalf("value is %T, want Array", got)
	}
	if len(arr) != 0 {
		t.Fatalf("array = %#v, want empty", arr)
	}

	// The remainder must still decode, proving the counts compose.
	rest, n2, err := Decode(in[n:])
	if err != nil {
		t.Fatalf("Decode of the remainder failed: %v", err)
	}
	if rest != Integer(1) || n2 != 4 {
		t.Fatalf("remainder decoded to %#v consuming %d bytes, want Integer(1) and 4", rest, n2)
	}
}

// TestDecodeArrayPropagatesIncomplete checks that an incomplete child makes
// the whole array incomplete rather than a protocol error. The distinction
// matters: the caller must wait for more bytes, not close the connection.
func TestDecodeArrayPropagatesIncomplete(t *testing.T) {
	for _, in := range []string{
		"*2\r\n:1\r\n",              // second child entirely absent
		"*2\r\n:1\r\n$3\r\nab",      // second child present but truncated
		"*1\r\n*1\r\n",              // nested array missing its own child
		"*1\r\n*1\r\n$2\r\nhi",      // nested child truncated
		"*4\r\n:1\r\n:2\r\n:3\r\n$", // fourth child is only a marker byte
	} {
		_, n, err := Decode([]byte(in))
		if !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Decode(%q) error = %v, want ErrIncomplete", in, err)
		}
		if n != 0 {
			t.Fatalf("Decode(%q) consumed %d bytes, want 0", in, n)
		}
	}
}

// TestDecodeOffsetsPointAtRealBytes pins the diagnostic contract: every
// ProtocolError offset must index a byte that actually exists, and that byte
// must be the offending one. A decoder that reports an offset past the end of
// the input, or at a harmless position, sends the reader hunting in the wrong
// place, and an off-by-one in an offset is exactly the kind of thing that
// survives a value-only test suite.
func TestDecodeOffsetsPointAtRealBytes(t *testing.T) {
	tests := []struct {
		name         string
		in           string
		wantOffset   int
		wantByteHere string // substring that must begin at wantOffset
	}{
		{"invalid marker", "?foo\r\n", 0, "?foo"},
		{"bare LF in simple string", "+OK\n", 3, "\n"},
		{"bare CR not followed by LF", "+OK\rX", 3, "\rX"},
		{"invalid integer body", ":abc\r\n", 1, "abc"},
		{"integer overflow body", ":9223372036854775808\r\n", 1, "9223372036854775808"},
		{"negative bulk length", "$-2\r\n", 1, "-2"},
		{"bulk length non-numeric", "$abc\r\nhello\r\n", 1, "abc"},
		{"bulk missing terminator", "$5\r\nhelloXX", 8, "XX"},
		{"malformed child in array", "*2\r\n:1\r\n?bad\r\n", 7, "?bad"},
		{"negative array count", "*-2\r\n", 1, "-2"},
		{"array count non-numeric", "*abc\r\n", 1, "abc"},
		{"empty simple string", "+\r\n", 0, "+\r\n"},
		{"empty error", "-\r\n", 0, "-\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, n, err := Decode([]byte(tt.in))

			var perr *ProtocolError
			if !errors.As(err, &perr) {
				t.Fatalf("Decode(%q) error = %v (%T), want *ProtocolError", tt.in, err, err)
			}

			if perr.Offset < 0 || perr.Offset >= len(tt.in) {
				t.Fatalf("Decode(%q) offset %d does not index a real byte of a %d-byte input",
					tt.in, perr.Offset, len(tt.in))
			}
			if perr.Reason == "" {
				t.Fatalf("Decode(%q) ProtocolError has an empty Reason", tt.in)
			}

			rest := tt.in[perr.Offset:]
			if !strings.HasPrefix(rest, tt.wantByteHere) {
				t.Fatalf("Decode(%q) offset %d points at %q, want it to point at %q",
					tt.in, perr.Offset, rest, tt.wantByteHere)
			}

			if n < 0 || n > len(tt.in) {
				t.Fatalf("Decode(%q) reported %d bytes inspected of %d", tt.in, n, len(tt.in))
			}
		})
	}
}

// TestDecodeErrorContract pins the two error shapes a caller switches on:
// ErrIncomplete for "read more", *ProtocolError for "give up".
func TestDecodeErrorContract(t *testing.T) {
	t.Run("incomplete frames report ErrIncomplete", func(t *testing.T) {
		for _, in := range []string{"", "+", "+OK\r", "$5\r\nhel", "*2\r\n:1\r\n"} {
			_, n, err := Decode([]byte(in))
			if !errors.Is(err, ErrIncomplete) {
				t.Fatalf("Decode(%q) error = %v, want it to satisfy errors.Is(err, ErrIncomplete)", in, err)
			}
			if n != 0 {
				t.Fatalf("Decode(%q) consumed %d, want 0", in, n)
			}

			// ErrIncomplete must not be a ProtocolError; the caller's switch
			// would otherwise take the wrong branch.
			var perr *ProtocolError
			if errors.As(err, &perr) {
				t.Fatalf("Decode(%q) returned a ProtocolError for an incomplete frame: %v", in, perr)
			}
		}
	})

	t.Run("violations report ProtocolError", func(t *testing.T) {
		for _, in := range []string{"?", ":abc\r\n", "$5\r\nhelloXX", "*-2\r\n"} {
			_, _, err := Decode([]byte(in))

			var perr *ProtocolError
			if !errors.As(err, &perr) {
				t.Fatalf("Decode(%q) error = %v, want it to satisfy errors.As(err, &ProtocolError)", in, err)
			}
			if errors.Is(err, ErrIncomplete) {
				t.Fatalf("Decode(%q) returned both shapes; ProtocolError must not match ErrIncomplete", in)
			}
		}
	})

	t.Run("sentinel is comparable and stable", func(t *testing.T) {
		if !errors.Is(ErrIncomplete, ErrIncomplete) {
			t.Fatal("ErrIncomplete does not match itself")
		}
		if ErrIncomplete.Error() == "" {
			t.Fatal("ErrIncomplete has an empty message")
		}
	})
}

// TestDecodeBulkStringAliasesInput documents the no-copy guarantee: a
// BulkString points into the caller's buffer rather than duplicating it. The
// test proves it by mutating the input after decoding and observing the change
// through the value, which is also a warning to callers that intend to retain
// a value across buffer reuse.
func TestDecodeBulkStringAliasesInput(t *testing.T) {
	buf := []byte("$5\r\nhello\r\n")

	got, _, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	bs, ok := got.(BulkString)
	if !ok {
		t.Fatalf("value is %T, want BulkString", got)
	}
	if !bytes.Equal(bs, []byte("hello")) {
		t.Fatalf("value = %q, want %q", bs, "hello")
	}

	// Overwrite the payload in the source buffer.
	copy(buf[4:9], "HELLO")

	if !bytes.Equal(bs, []byte("HELLO")) {
		t.Fatalf("BulkString = %q after mutating the input; it does not alias the buffer", bs)
	}
}

// TestProtocolErrorMessage pins the diagnostic string and, incidentally,
// covers the Error method: every other test inspects err through errors.As,
// which never formats it, so a broken message would go unnoticed until an
// operator read it during an incident.
func TestProtocolErrorMessage(t *testing.T) {
	err := &ProtocolError{Offset: 3, Reason: "bare LF is not a valid terminator"}

	const want = "protocol: bare LF is not a valid terminator at offset 3"
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	// The same message must reach the caller through a real decode failure,
	// not only when the struct is built by hand.
	_, _, decodeErr := Decode([]byte("+OK\n"))
	if decodeErr == nil {
		t.Fatal("Decode accepted a bare LF")
	}
	if msg := decodeErr.Error(); !strings.HasPrefix(msg, "protocol: ") || !strings.Contains(msg, "offset ") {
		t.Fatalf("Decode error message = %q, want it to name the problem and an offset", msg)
	}
}

// TestCursorTakeRejectsBadCounts covers take's defensive bound directly.
//
// No path through Decode can reach it: every caller validates its length
// against the remaining bytes first. That is the point. This guard exists so
// that a future refactor which forgets to validate cannot turn a bad length
// into a negative slice index and a panic in a connection-serving goroutine.
// A guard that cannot be reached from outside is still worth having, and
// therefore worth testing.
func TestCursorTakeRejectsBadCounts(t *testing.T) {
	tests := []struct {
		name string
		take int
	}{
		{"negative", -1},
		{"far negative", math.MinInt64},
		{"longer than the buffer", 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cursor{buf: []byte("hello")}

			got, err := c.take(tt.take)
			if !errors.Is(err, ErrIncomplete) {
				t.Fatalf("take(%d) error = %v, want ErrIncomplete", tt.take, err)
			}
			if got != nil {
				t.Fatalf("take(%d) returned %v, want nil", tt.take, got)
			}
			if c.pos != 0 {
				t.Fatalf("take(%d) advanced the cursor to %d, want 0", tt.take, c.pos)
			}
		})
	}

	// The in-range cases must still work, including the whole buffer and zero.
	c := &cursor{buf: []byte("hello")}
	if got, err := c.take(5); err != nil || string(got) != "hello" {
		t.Fatalf("take(5) = %q, %v; want \"hello\", nil", got, err)
	}
	if got, err := c.take(0); err != nil || len(got) != 0 {
		t.Fatalf("take(0) = %q, %v; want empty, nil", got, err)
	}
}

// TestDecodeNegativeBulkLengths walks the signed range around -1, where the
// only meaningful negative value is -1 itself, the null bulk string. The
// distinction is worth a test because treating -1 as an error would break
// every GET of a missing key, and treating -2 as null would invent data.
func TestDecodeNegativeBulkLengths(t *testing.T) {
	tests := []struct {
		in      string
		wantBad bool
	}{
		{"$-1\r\n", false},
		{"$-2\r\n", true},
		{"$-3\r\n", true},
		{"$-9999999999\r\n", true},
		{"$-9223372036854775808\r\n", true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, n, err := Decode([]byte(tt.in))

			if tt.wantBad {
				var perr *ProtocolError
				if !errors.As(err, &perr) {
					t.Fatalf("Decode(%q) error = %v, want *ProtocolError", tt.in, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Decode(%q) error: %v", tt.in, err)
			}
			if got != (Null{}) {
				t.Fatalf("Decode(%q) = %#v, want Null{}", tt.in, got)
			}
			if n != len(tt.in) {
				t.Fatalf("Decode(%q) consumed %d, want %d", tt.in, n, len(tt.in))
			}
		})
	}
}
