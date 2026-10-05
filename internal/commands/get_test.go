package commands

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

func TestGetExistingKey(t *testing.T) {
	s := store.New()
	s.Set([]byte("foo"), []byte("bar"))

	got, fault := GetHandler(s)(args("GET", "foo"))
	if fault != nil {
		t.Fatalf("GET reported an internal fault: %v", fault)
	}

	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET = %#v (%T), want BulkString", got, got)
	}
	if !bytes.Equal(bs, []byte("bar")) {
		t.Fatalf("GET = %q, want %q", bs, "bar")
	}
}

// TestGetMissingKeyReturnsNull pins the shape of a miss. Null{}, not a nil
// Value and not an error: a missing key is a normal answer in Redis, and the
// null bulk string is what redis-cli renders as (nil).
func TestGetMissingKeyReturnsNull(t *testing.T) {
	s := store.New()

	got, fault := GetHandler(s)(args("GET", "nothere"))
	if fault != nil {
		t.Fatalf("GET reported an internal fault: %v", fault)
	}

	if got != (protocol.Null{}) {
		t.Fatalf("GET on a missing key = %#v (%T), want Null{}", got, got)
	}

	// And it must encode as the null bulk string, which is what a client
	// checks for.
	if wire := string(protocol.Encode(got)); wire != "$-1\r\n" {
		t.Fatalf("encoded missing-key reply = %q, want %q", wire, "$-1\r\n")
	}
}

func TestGetWrongArity(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
	}{
		{"no key", args("GET")},
		{"two keys", args("GET", "a", "b")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := store.New()

			got, fault := GetHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("GET reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("GET(%v) = %#v, want protocol.Error", tt.args, got)
			}
			const want = "ERR wrong number of arguments for 'get' command"
			if string(errValue) != want {
				t.Fatalf("GET(%v) = %q, want %q", tt.args, errValue, want)
			}
		})
	}
}

func TestGetRejectsNonBulkKey(t *testing.T) {
	inputs := []protocol.Value{
		protocol.Integer(1),
		protocol.SimpleString("foo"),
		protocol.Null{},
		protocol.Array{protocol.BulkString([]byte("foo"))},
	}

	const want = "ERR invalid argument for 'get' command"

	for _, in := range inputs {
		t.Run(commandsTypeName(in), func(t *testing.T) {
			s := store.New()
			s.Set([]byte("foo"), []byte("bar"))

			got, fault := GetHandler(s)([]protocol.Value{protocol.BulkString([]byte("GET")), in})
			if fault != nil {
				t.Fatalf("GET reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("GET with %T key = %#v, want protocol.Error", in, got)
			}
			if string(errValue) != want {
				t.Fatalf("GET with %T key = %q, want %q", in, errValue, want)
			}
		})
	}
}

// TestGetDoesNotLeakStoreMemory is the handler-level half of the copy-on-exit
// invariant: mutating what GET returned must not be able to reach the stored
// bytes, because the store handed over a copy.
func TestGetDoesNotLeakStoreMemory(t *testing.T) {
	s := store.New()
	s.Set([]byte("k"), []byte("original"))
	h := GetHandler(s)

	first, _ := h(args("GET", "k"))
	firstBytes, ok := first.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET = %T, want BulkString", first)
	}

	copy(firstBytes, "MUTATED!")

	second, _ := h(args("GET", "k"))
	secondBytes, ok := second.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET = %T, want BulkString", second)
	}
	if !bytes.Equal(secondBytes, []byte("original")) {
		t.Fatalf("second GET = %q after mutating the first, want %q", secondBytes, "original")
	}
}

// TestGetIsBinarySafe checks a value with NUL, invalid UTF-8 and CRLF survives
// the round trip through the store and the handler.
func TestGetIsBinarySafe(t *testing.T) {
	s := store.New()
	value := []byte{0x00, 0xff, '\r', '\n', 0xfe}
	s.Set([]byte("k"), value)

	got, fault := GetHandler(s)(args("GET", "k"))
	if fault != nil {
		t.Fatalf("GET reported an internal fault: %v", fault)
	}

	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET = %T, want BulkString", got)
	}
	if !bytes.Equal(bs, value) {
		t.Fatalf("GET = % x, want % x", bs, value)
	}
}

// TestGetValueWithEmbeddedCommandBytes checks that a stored value which looks
// like RESP traffic comes back untouched, which is the case a naive
// implementation that re-encoded or parsed values would corrupt.
func TestGetValueWithEmbeddedCommandBytes(t *testing.T) {
	s := store.New()
	value := []byte("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
	s.Set([]byte("k"), value)

	got, _ := GetHandler(s)(args("GET", "k"))

	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET = %T, want BulkString", got)
	}
	if !reflect.DeepEqual(bs, protocol.BulkString(value)) {
		t.Fatalf("GET = %q, want %q", bs, value)
	}
}

// TestGetEmptyValueIsNotMissing distinguishes an empty value from an absent
// key. The reply for the first is `$0\r\n\r\n` and for the second `$-1\r\n`;
// collapsing them would break every client that checks for nil.
func TestGetEmptyValueIsNotMissing(t *testing.T) {
	s := store.New()
	s.Set([]byte("empty"), []byte{})

	got, fault := GetHandler(s)(args("GET", "empty"))
	if fault != nil {
		t.Fatalf("GET reported an internal fault: %v", fault)
	}

	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET of an empty value = %#v (%T), want BulkString", got, got)
	}
	if bs == nil {
		t.Fatal("GET of an empty value returned a nil BulkString, want an empty one")
	}
	if len(bs) != 0 {
		t.Fatalf("GET of an empty value = %q, want empty", bs)
	}

	if wire := string(protocol.Encode(got)); wire != "$0\r\n\r\n" {
		t.Fatalf("encoded empty-value reply = %q, want %q", wire, "$0\r\n\r\n")
	}
}
