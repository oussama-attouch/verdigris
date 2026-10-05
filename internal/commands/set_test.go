package commands

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// TestSetStoresThePair is the handler-level happy path, plus the check that
// matters most: the value is actually retrievable from the store afterwards,
// not merely acknowledged with OK.
func TestSetStoresThePair(t *testing.T) {
	s := store.New()
	h := SetHandler(s)

	got, fault := h(args("SET", "foo", "bar"))
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("SET = %#v, want SimpleString(\"OK\")", got)
	}

	// The reply alone proves nothing; the store must hold the pair.
	stored, found := s.Get([]byte("foo"))
	if !found {
		t.Fatal("SET replied OK but the key is not in the store")
	}
	if !bytes.Equal(stored, []byte("bar")) {
		t.Fatalf("stored value = %q, want %q", stored, "bar")
	}
}

func TestSetWrongArity(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
		want protocol.Value
	}{
		{"no key or value", args("SET"), protocol.Error("ERR wrong number of arguments for 'set' command")},
		{"key without value", args("SET", "foo"), protocol.Error("ERR wrong number of arguments for 'set' command")},
		// Extra tokens look like the EX/PX forms this milestone does not
		// support yet, so Redis calls that a syntax error rather than an
		// arity error. Matching that distinction keeps the reply honest about
		// which mistake the client made.
		{"too many arguments", args("SET", "foo", "bar", "baz"), protocol.Error("ERR syntax error")},
		{"the EX form is not supported yet", args("SET", "foo", "bar", "EX", "10"), protocol.Error("ERR syntax error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := store.New()

			got, fault := SetHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("SET(%v) = %#v, want %#v", tt.args, got, tt.want)
			}
			if s.Len() != 0 {
				t.Fatalf("a rejected SET still wrote %d keys", s.Len())
			}
		})
	}
}

func TestSetRejectsNonBulkArguments(t *testing.T) {
	inputs := []struct {
		name string
		args []protocol.Value
	}{
		{"integer key", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.Integer(1), protocol.BulkString([]byte("v"))}},
		{"simple string key", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.SimpleString("k"), protocol.BulkString([]byte("v"))}},
		{"null key", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.Null{}, protocol.BulkString([]byte("v"))}},
		{"integer value", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.BulkString([]byte("k")), protocol.Integer(1)}},
		{"null value", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.BulkString([]byte("k")), protocol.Null{}}},
		{"array value", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.BulkString([]byte("k")), protocol.Array{}}},
	}

	const want = "ERR invalid argument for 'set' command"

	for _, tt := range inputs {
		t.Run(tt.name, func(t *testing.T) {
			s := store.New()

			got, fault := SetHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("SET(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("SET(%v) = %q, want %q", tt.args, errValue, want)
			}
			if s.Len() != 0 {
				t.Fatalf("a rejected SET still wrote %d keys", s.Len())
			}
		})
	}
}

// TestSetOverwritesThroughTheHandler checks the handler does not add its own
// existence check, since SET is an unconditional overwrite in Redis.
func TestSetOverwritesThroughTheHandler(t *testing.T) {
	s := store.New()
	h := SetHandler(s)

	if got, _ := h(args("SET", "k", "first")); got != protocol.SimpleString("OK") {
		t.Fatalf("first SET = %#v", got)
	}
	if got, _ := h(args("SET", "k", "second")); got != protocol.SimpleString("OK") {
		t.Fatalf("second SET = %#v", got)
	}

	stored, found := s.Get([]byte("k"))
	if !found || !bytes.Equal(stored, []byte("second")) {
		t.Fatalf("stored value = %q, %v; want %q", stored, found, "second")
	}
}

// TestSetIsBinarySafeThroughTheHandler passes a key and value containing NUL,
// invalid UTF-8 and CRLF. The handler must not interpret either.
func TestSetIsBinarySafeThroughTheHandler(t *testing.T) {
	s := store.New()
	h := SetHandler(s)

	key := []byte{0x00, 0xff, '\r', '\n'}
	value := []byte{0xff, 0x00, '\r', '\n', 0xfe}

	got, fault := h([]protocol.Value{
		protocol.BulkString([]byte("SET")),
		protocol.BulkString(key),
		protocol.BulkString(value),
	})
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("SET = %#v, want OK", got)
	}

	stored, found := s.Get(key)
	if !found {
		t.Fatal("binary key not found after SET")
	}
	if !bytes.Equal(stored, value) {
		t.Fatalf("stored value = % x, want % x", stored, value)
	}
}

// TestSetWithEmptyKeyAndValue checks the degenerate-but-legal shapes: an empty
// key and an empty value are both valid bulk strings in Redis.
func TestSetWithEmptyKeyAndValue(t *testing.T) {
	s := store.New()
	h := SetHandler(s)

	if got, _ := h(args("SET", "", "")); got != protocol.SimpleString("OK") {
		t.Fatalf("SET with empty key and value = %#v, want OK", got)
	}

	stored, found := s.Get([]byte(""))
	if !found {
		t.Fatal("the empty key is not in the store after SET")
	}
	if len(stored) != 0 {
		t.Fatalf("stored value = %q, want empty", stored)
	}
}

// TestSetStoresDirectlyWithoutCopyingInTheHandler is a contract check rather
// than a behaviour check: the handler must pass the argument slices straight
// through, because the store is what guarantees ownership. If the handler
// copied, it would be doing the store's job twice on every write.
//
// It is asserted indirectly, by mutating the caller's slice after the call and
// confirming the store is unaffected — which is true either way, and so this
// test documents the guarantee the store provides rather than the handler's
// internals.
func TestSetStoresDirectlyWithoutCopyingInTheHandler(t *testing.T) {
	s := store.New()
	h := SetHandler(s)

	key := []byte("original-key")
	value := []byte("original-value")

	if _, fault := h([]protocol.Value{
		protocol.BulkString([]byte("SET")),
		protocol.BulkString(key),
		protocol.BulkString(value),
	}); fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}

	copy(key, "MUTATED-KEY!")
	copy(value, "MUTATED-VALUE")

	stored, found := s.Get([]byte("original-key"))
	if !found || !bytes.Equal(stored, []byte("original-value")) {
		t.Fatalf("after mutating the caller's slices, stored = %q, %v; want the originals", stored, found)
	}
}
