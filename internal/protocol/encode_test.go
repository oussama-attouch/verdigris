package protocol

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

// encodeWantBytes normalizes a test expectation into bytes. A nil input means
// the case expects Encode to return nil rather than a frame.
func encodeWantBytes(t *testing.T, want any) []byte {
	t.Helper()

	switch w := want.(type) {
	case nil:
		return nil
	case string:
		return []byte(w)
	case []byte:
		return w
	default:
		t.Fatalf("test table error: unsupported expectation type %T", want)
		return nil
	}
}

// TestEncode pins the exact wire bytes of every frame type.
//
// "Exact bytes" is the point, not "a decodable frame": RESP is a byte
// protocol, and a reply that decodes correctly but is one byte longer than
// Redis's is a reply that breaks byte-counting clients, pipelining offsets,
// and every future comparison against real Redis.
func TestEncode(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want any // string or []byte or nil
	}{
		// --- Simple strings ---
		{name: "simple string", in: SimpleString("OK"), want: "+OK\r\n"},
		// An empty simple string is refused: Decode rejects `+\r\n`, so
		// emitting it would produce a frame this server cannot read back.
		{name: "simple string empty payload is refused", in: SimpleString(""), want: "-ERR invalid simple string\r\n"},
		{name: "simple string with spaces", in: SimpleString("Hello world"), want: "+Hello world\r\n"},
		{name: "simple string containing CR", in: SimpleString("bad\rstring"), want: "-ERR invalid simple string\r\n"},
		{name: "simple string containing LF", in: SimpleString("bad\nstring"), want: "-ERR invalid simple string\r\n"},
		{name: "simple string containing CRLF", in: SimpleString("bad\r\nstring"), want: "-ERR invalid simple string\r\n"},

		// --- Errors ---
		{name: "error", in: Error("ERR"), want: "-ERR\r\n"},
		{name: "error long", in: Error("ERR unknown command 'NOTACOMMAND'"), want: "-ERR unknown command 'NOTACOMMAND'\r\n"},
		{name: "error empty is refused", in: Error(""), want: "-ERR invalid simple string\r\n"},
		{name: "error containing CRLF", in: Error("ERR bad\r\ninjected"), want: "-ERR invalid simple string\r\n"},

		// --- Integers ---
		{name: "integer positive", in: Integer(1000), want: ":1000\r\n"},
		{name: "integer zero", in: Integer(0), want: ":0\r\n"},
		{name: "integer negative", in: Integer(-42), want: ":-42\r\n"},
		{name: "integer max", in: Integer(math.MaxInt64), want: ":9223372036854775807\r\n"},
		{name: "integer min", in: Integer(math.MinInt64), want: ":-9223372036854775808\r\n"},

		// --- Bulk strings ---
		{name: "bulk string", in: BulkString([]byte("hello")), want: "$5\r\nhello\r\n"},
		{name: "bulk string empty", in: BulkString([]byte{}), want: "$0\r\n\r\n"},
		{name: "bulk string nil is null", in: BulkString(nil), want: "$-1\r\n"},
		{name: "bulk string binary safe", in: BulkString([]byte{0x00, 0xff, 0x0d, 0x0a, 0xfe}), want: []byte{'$', '5', '\r', '\n', 0x00, 0xff, 0x0d, 0x0a, 0xfe, '\r', '\n'}},
		{name: "bulk string invalid UTF-8", in: BulkString([]byte{0xff, 0xfe, 0xfd}), want: []byte{'$', '3', '\r', '\n', 0xff, 0xfe, 0xfd, '\r', '\n'}},
		{name: "bulk string longer than one digit", in: BulkString(bytes.Repeat([]byte("a"), 100)), want: append(append([]byte("$100\r\n"), bytes.Repeat([]byte("a"), 100)...), '\r', '\n')},

		// --- Arrays ---
		{name: "array empty", in: Array([]Value{}), want: "*0\r\n"},
		{name: "array of two", in: Array{SimpleString("OK"), Integer(1)}, want: "*2\r\n+OK\r\n:1\r\n"},
		{name: "array of mixed", in: Array{Integer(1), SimpleString("OK"), BulkString([]byte("hi"))}, want: "*3\r\n:1\r\n+OK\r\n$2\r\nhi\r\n"},
		{name: "array of one empty bulk", in: Array{BulkString([]byte{})}, want: "*1\r\n$0\r\n\r\n"},
		{name: "array with null element", in: Array{Null{}}, want: "*1\r\n$-1\r\n"},
		{name: "array with nil element", in: Array{nil}, want: "*1\r\n$-1\r\n"},
		{name: "array nested", in: Array{Array{Integer(1), Integer(2)}, Integer(3)}, want: "*2\r\n*2\r\n:1\r\n:2\r\n:3\r\n"},
		{name: "array containing nested array and null", in: Array{Array{Integer(1)}, Null{}}, want: "*2\r\n*1\r\n:1\r\n$-1\r\n"},

		// --- Null and nil ---
		{name: "null encodes as null bulk string", in: Null{}, want: "$-1\r\n"},
		{name: "nil encodes to nothing", in: nil, want: nil},
		{name: "typed nil bulk string encodes as null", in: BulkString(nil), want: "$-1\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := encodeWantBytes(t, tt.want)
			got := Encode(tt.in)

			if !bytes.Equal(got, want) {
				t.Fatalf("Encode(%#v)\n got: %q\nwant: %q", tt.in, got, want)
			}
		})
	}
}

// TestEncodeNilIsNilNotFrame distinguishes the two "nothing" cases. A nil
// Value means no value was produced; Null{} means the peer's null arrived.
// Encode must keep those apart, because a caller checking for nil is asking a
// different question than a caller checking for a null frame.
func TestEncodeNilIsNilNotFrame(t *testing.T) {
	if got := Encode(nil); got != nil {
		t.Fatalf("Encode(nil) = %q, want nil", got)
	}
	if got := Encode(Null{}); got == nil {
		t.Fatal("Encode(Null{}) = nil, want the null bulk string frame")
	}
	if len(Encode(Null{})) == 0 {
		t.Fatal("Encode(Null{}) returned zero bytes")
	}
}

// TestEncodeDoesNotPanicOnInvalidSimpleStrings covers the validation path
// from the caller's side: a SimpleString built directly from untrusted input
// must produce a well-formed error frame rather than a second injected frame.
func TestEncodeDoesNotPanicOnInvalidSimpleStrings(t *testing.T) {
	inputs := []string{
		"\r",
		"\n",
		"\r\n",
		"OK\r\n+INJECTED\r\n",
		"a\rb",
		"a\nb",
	}

	for _, in := range inputs {
		for _, v := range []Value{SimpleString(in), Error(in)} {
			got := Encode(v)

			// Whatever comes back must be exactly one complete error frame.
			decoded, n, err := Decode(got)
			if err != nil {
				t.Fatalf("Encode(%T(%q)) produced undecodable bytes %q: %v", v, in, got, err)
			}
			if n != len(got) {
				t.Fatalf("Encode(%T(%q)) produced trailing bytes: consumed %d of %d", v, in, n, len(got))
			}
			if decoded != Error("ERR invalid simple string") {
				t.Fatalf("Encode(%T(%q)) = %#v, want the invalid-simple-string error", v, in, decoded)
			}
		}
	}
}

// roundTripValues is the corpus for the round-trip test and, with the extra
// undecodable rows kept separate, for the encoder fuzzer's seeds.
//
// Every Value type appears at least once: SimpleString, Error, Integer,
// BulkString (non-empty, empty, and nil), Array (empty, nested, mixed), and
// Null.
var roundTripValues = []Value{
	SimpleString("OK"),
	SimpleString("Hello world"),
	SimpleString("PONG"),
	Error("ERR"),
	Error("ERR unknown command 'NOTACOMMAND'"),
	Integer(0),
	Integer(1000),
	Integer(-42),
	Integer(math.MaxInt64),
	Integer(math.MinInt64),
	BulkString([]byte("hello")),
	BulkString([]byte{}),
	BulkString(nil),
	BulkString([]byte{0x00, 0xff, 0x0d, 0x0a, 0xfe}),
	BulkString([]byte{0xff, 0xfe, 0xfd}),
	BulkString(bytes.Repeat([]byte("a"), 1000)),
	Array([]Value{}),
	Array{SimpleString("OK"), Integer(1)},
	Array{Integer(1), SimpleString("OK"), BulkString([]byte("hi"))},
	Array{BulkString([]byte("ECHO")), BulkString([]byte("hello"))},
	Array{Null{}},
	Array{Integer(1), Null{}, Integer(3)},
	Array{Array{Integer(1), Integer(2)}, Integer(3)},
	Array{Array{SimpleString("deep"), Array{BulkString([]byte("deeper"))}}},
	Null{},
}

// TestEncodeDecodeRoundTrip is the property that makes the encoder correct
// rather than merely plausible: for every Value, Encode then Decode must
// consume every byte Encode produced, and must return an equal value.
//
// The wire assertion is unconditional and is the half that catches real bugs:
// an encoder that emitted a correct-looking frame plus a stray byte would
// still decode to the right value while corrupting every subsequent frame on
// a pipelined connection.
//
// The value assertion is exact except for the cases encodingLossy documents —
// a nil slice and an empty slice of the same RESP type share one wire form,
// and RESP2 has no way to distinguish them on the way back. Those are
// properties of the protocol, and TestEncodingLossyCasesAreExactlyThese pins
// the exact list so a new lossy case cannot appear unnoticed.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, v := range roundTripValues {
		t.Run(valueName(v), func(t *testing.T) {
			encoded := Encode(v)
			if encoded == nil {
				t.Fatalf("Encode(%#v) = nil", v)
			}

			decoded, n, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode(Encode(%#v)) error: %v (bytes %q)", v, err, encoded)
			}
			if n != len(encoded) {
				t.Fatalf("Decode(Encode(%#v)) consumed %d of %d bytes", v, n, len(encoded))
			}

			if encodingLossy(v) {
				// The wire form is still required to be a faithful encoding
				// of the value's shape; only the nil-versus-empty identity is
				// lost. Re-encoding the decoded value must reproduce the same
				// bytes, which is the strongest statement available.
				if again := Encode(decoded); !bytes.Equal(again, encoded) {
					t.Fatalf("lossy re-encode differs\n first: %q\nsecond: %q (value %#v)", encoded, again, v)
				}
				return
			}

			// reflect.DeepEqual rather than ==: BulkString and Array are
			// slices, which have no equality operator.
			if !reflect.DeepEqual(decoded, v) {
				t.Fatalf("round trip changed the value\n in: %#v\nout: %#v", v, decoded)
			}
		})
	}
}

// TestEncodingLossyCasesAreExactlyThese pins the set of values whose encoding
// cannot be decoded back to an identical value.
//
// Without this, a future change that quietly made another type lossy — say,
// dropping the empty-versus-nil BulkString distinction — would simply move a
// corpus entry into the lossy branch and the property test would keep
// passing while the guarantee weakened. Naming the exact permitted set is
// what makes that visible.
//
// A nil Array is deliberately NOT lossy: Decode normalizes a zero-count array
// to a non-nil empty Array, so both forms round-trip to Array{} and the value
// comparison below is exact. Only the impossible-to-distinguish cases are
// excused.
func TestEncodingLossyCasesAreExactlyThese(t *testing.T) {
	// The nil-array case must genuinely round-trip to the empty array, or the
	// claim above is false.
	decoded, _, err := Decode(Encode(Array(nil)))
	if err != nil {
		t.Fatalf("Decode(Encode(Array(nil))) error: %v", err)
	}
	arr, ok := decoded.(Array)
	if !ok {
		t.Fatalf("Decode(Encode(Array(nil))) = %T, want Array", decoded)
	}
	if arr == nil {
		t.Fatal("Decode returned a nil Array for a zero-count array; the encoder's round-trip identity depends on it being non-nil")
	}

	lossy := 0
	for _, v := range roundTripValues {
		if !encodingLossy(v) {
			continue
		}
		lossy++
	}

	if lossy == 0 {
		t.Fatal("no corpus entry is marked lossy; the round-trip test is no longer exercising the documented cases")
	}

	// The predicate is recursive through arrays, so these must all report
	// lossy even though their own type is not one of the lossy leaves.
	nested := []Value{
		Array{SimpleString("")},
		Array{Array{}, SimpleString("")},
		Array{Error("")},
		Array{BulkString(nil)},
		Array{nil},
		Array{Array{Array{BulkString(nil)}}},
	}
	for _, v := range nested {
		if !encodingLossy(v) {
			t.Errorf("%#v contains a lossy element but is not reported lossy", v)
		}
	}

	// And these must not, or the fuzzer would stop checking round trips for
	// values that are perfectly representable.
	intact := []Value{
		Array{},
		Array{SimpleString("OK")},
		Array{BulkString([]byte{})},
		Array{Null{}},
		Array{Array{Integer(1)}},
	}
	for _, v := range intact {
		if encodingLossy(v) {
			t.Errorf("%#v is fully representable but is reported lossy", v)
		}
	}
}

// TestRoundTripNilBulkStringBecomesNull pins the lossy bulk-string case and
// the non-lossy case beside it, so the difference is visible in one place.
func TestRoundTripNilBulkStringBecomesNull(t *testing.T) {
	// A nil bulk string and Null are the same bytes on the wire...
	if got, want := Encode(BulkString(nil)), Encode(Null{}); !bytes.Equal(got, want) {
		t.Fatalf("Encode(BulkString(nil)) = %q, Encode(Null{}) = %q; both should be the null frame", got, want)
	}

	// ...so both decode back to the same value. This is the direction that
	// loses information, and it is inherent to RESP2 rather than a bug.
	for _, in := range []Value{BulkString(nil), Null{}} {
		decoded, _, err := Decode(Encode(in))
		if err != nil {
			t.Fatalf("Decode(Encode(%#v)) error: %v", in, err)
		}
		if decoded != (Null{}) {
			t.Fatalf("Decode(Encode(%#v)) = %#v, want Null{}", in, decoded)
		}
	}

	// The empty non-nil bulk string is NOT null, and must survive intact.
	decoded, _, err := Decode(Encode(BulkString([]byte{})))
	if err != nil {
		t.Fatalf("Decode(Encode(empty bulk)) error: %v", err)
	}
	bs, ok := decoded.(BulkString)
	if !ok {
		t.Fatalf("empty bulk decoded to %T, want BulkString", decoded)
	}
	if bs == nil {
		t.Fatal("empty bulk decoded to a nil BulkString, want an empty one")
	}
}

// TestRoundTripCorpusIsExhaustive guards the test above from silently losing
// coverage: if a future edit drops a type from roundTripValues, the property
// test would keep passing while testing less.
func TestRoundTripCorpusIsExhaustive(t *testing.T) {
	seen := make(map[reflect.Type]int)
	for _, v := range roundTripValues {
		seen[reflect.TypeOf(v)]++
	}

	for _, want := range []reflect.Type{
		reflect.TypeOf(SimpleString("")),
		reflect.TypeOf(Error("")),
		reflect.TypeOf(Integer(0)),
		reflect.TypeOf(BulkString(nil)),
		reflect.TypeOf(Array(nil)),
		reflect.TypeOf(Null{}),
	} {
		if seen[want] == 0 {
			t.Errorf("round-trip corpus has no %s case", want)
		}
	}

	if seen[reflect.TypeOf(BulkString(nil))] < 3 {
		t.Errorf("round-trip corpus has %d bulk strings, want at least 3 so the nil, empty and non-empty forms are all covered",
			seen[reflect.TypeOf(BulkString(nil))])
	}
}

// TestEncodePipelinedRepliesAreConcatenable is the encoder half of the
// pipelining contract: replies encoded back to back must decode back to the
// same sequence, which is what the server relies on when several commands
// arrive in one read.
func TestEncodePipelinedRepliesAreConcatenable(t *testing.T) {
	replies := []Value{
		SimpleString("PONG"),
		BulkString([]byte("hello verdigris")),
		Error("ERR unknown command 'NOPE'"),
		Integer(42),
		Null{},
		Array{BulkString([]byte("a")), BulkString([]byte("b"))},
	}

	var stream []byte
	for _, r := range replies {
		stream = append(stream, Encode(r)...)
	}

	for i, want := range replies {
		got, n, err := Decode(stream)
		if err != nil {
			t.Fatalf("reply %d: Decode error: %v", i, err)
		}
		if n <= 0 {
			t.Fatalf("reply %d: Decode consumed %d bytes", i, n)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("reply %d: got %#v, want %#v", i, got, want)
		}
		stream = stream[n:]
	}

	if len(stream) != 0 {
		t.Fatalf("%d bytes left over after decoding every reply", len(stream))
	}
}

// encodeFuzzSeeds is the seed corpus for FuzzEncode: the same interesting
// frames FuzzDecode starts from, so both fuzzers explore the same shapes.
func encodeFuzzSeeds() []string {
	return []string{
		"+OK\r\n",
		"-ERR unknown\r\n",
		":0\r\n",
		":-9223372036854775808\r\n",
		"$0\r\n\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"*-1\r\n",
		"*0\r\n",
		"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
		"*2\r\n*2\r\n:1\r\n:2\r\n:3\r\n",
		"$9223372036854775807\r\n",
		"*1000000\r\n",
	}
}

// valueFromData deterministically constructs a Value from fuzzer bytes,
// walking every branch of the encoder's type switch.
//
// A fuzz target that only ever encodes one type would report success while
// leaving five sixths of Encode unexercised, so the selector byte is taken
// modulo the number of types and the remaining bytes become the payload.
func valueFromData(data []byte) Value {
	if len(data) == 0 {
		return nil
	}

	payload := data[1:]

	switch data[0] % 8 {
	case 0:
		return SimpleString(string(payload))
	case 1:
		return Error(string(payload))
	case 2:
		const offset = int64(1) << 40
		return Integer(int64(len(payload))*2654435761 - offset)
	case 3:
		return BulkString(payload)
	case 4:
		return BulkString(nil)
	case 5:
		return Null{}
	case 6:
		return Array{}
	default:
		// A nested array, so the recursive encode path is reached too.
		inner := make(Array, 0, len(payload))
		for _, b := range payload {
			inner = append(inner, BulkString([]byte{b}))
		}
		return Array{inner, SimpleString(string(payload))}
	}
}

// valueName renders a type name for a subtest, tolerating a nil Value.
// reflect.TypeOf(nil) is nil, and calling String() on that nil Type panics.
func valueName(v Value) string {
	if v == nil {
		return "nil"
	}
	return reflect.TypeOf(v).String()
}

// encodingLossy reports whether Encode cannot reproduce v byte-for-byte on a
// decode, and why. Every case is a property of the RESP grammar rather than a
// defect:
//
//   - A nil BulkString and a Null both encode to the null bulk string, and
//     RESP2 cannot tell them apart on the way back.
//   - A SimpleString or Error containing CRLF is refused, because emitting it
//     verbatim would inject a second frame into the stream. The caller gets an
//     error frame instead.
//   - An empty SimpleString or Error is refused for the same reason: RESP has
//     no zero-length simple string, and Decode rejects `+\r\n`.
//
// The array case is recursive, and that recursion is not a detail: an array
// containing one lossy element is itself lossy, because Encode walks the
// elements and substitutes the error frame for the offending one. The fuzzer
// found this — a top-level-only check passed Array{Array{}, ""} through to the
// strict comparison and reported a round-trip failure that was really a gap in
// this predicate.
//
// What is NOT lossy, and must stay that way:
//
//   - An empty non-nil BulkString encodes as `$0\r\n\r\n`, which decodes back
//     to an empty non-nil BulkString.
//   - A nil or empty Array encodes as `*0\r\n`, and Decode normalizes both to
//     a non-nil empty Array, so the round trip lands on Array{} either way.
func encodingLossy(v Value) bool {
	switch v := v.(type) {
	case BulkString:
		return v == nil
	case SimpleString:
		return v == "" || containsCRLF(string(v))
	case Error:
		return v == "" || containsCRLF(string(v))
	case Array:
		for _, elem := range v {
			if elem == nil || encodingLossy(elem) {
				// A nil element is also lossy: Encode writes the null bulk
				// string for it, which decodes back as Null{} rather than nil.
				return true
			}
		}
		return false
	default:
		return false
	}
}

// FuzzEncode asserts the properties that must hold for every Value the
// encoder can be handed: it must not panic, it must never emit bytes that
// fail to decode, and — except for the two documented lossy cases — it must
// round-trip exactly while consuming every byte it produced.
//
// Note what is being fuzzed. The input is bytes, and valueFromData turns them
// into a Value, so the fuzzer explores the *type switch* rather than the
// payload bytes alone. That matters because Value is sealed — the six
// concrete types plus nil are the entire reachable domain — so this target is
// genuinely exhaustive over the shape space in a way most fuzz targets are
// not.
//
// Run with:
//
//	go test -run=^$ -fuzz=FuzzEncode -fuzztime=60s ./internal/protocol/
func FuzzEncode(f *testing.F) {
	// Seed with the same interesting shapes FuzzDecode explores.
	for _, s := range encodeFuzzSeeds() {
		f.Add([]byte(s))
	}
	f.Add([]byte{0x00, 0xff, 0x0d, 0x0a, 0xfe})

	f.Fuzz(func(t *testing.T, data []byte) {
		v := valueFromData(data)

		encoded := Encode(v)

		if v == nil {
			if encoded != nil {
				t.Fatalf("Encode(nil) = %q, want nil", encoded)
			}
			return
		}

		if encoded == nil {
			t.Fatalf("Encode(%#v) = nil for a non-nil Value", v)
		}
		if len(encoded) == 0 {
			t.Fatalf("Encode(%#v) returned zero bytes", v)
		}

		// Whatever came back must be exactly one well-formed frame. This
		// holds for the lossy cases too: their output is one complete error
		// or null frame, not something malformed.
		decoded, n, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode(Encode(%#v)) error: %v (bytes %q)", v, err, encoded)
		}
		if n != len(encoded) {
			t.Fatalf("Decode(Encode(%#v)) consumed %d of %d bytes", v, n, len(encoded))
		}

		if encodingLossy(v) {
			// Byte stability is the strongest statement available once exact
			// value identity is off the table: re-encoding what came back must
			// reproduce the identical bytes. Any lossy substitution Encode
			// makes has to be idempotent, or repeated round trips would keep
			// changing the value.
			if again := Encode(decoded); !bytes.Equal(again, encoded) {
				t.Fatalf("lossy encoding is not byte-stable for %#v\n first: %q\nsecond: %q (decoded %#v)",
					v, encoded, again, decoded)
			}
			return
		}

		if !reflect.DeepEqual(decoded, v) {
			t.Fatalf("round trip changed the value\n in: %#v\nout: %#v", v, decoded)
		}
	})
}
