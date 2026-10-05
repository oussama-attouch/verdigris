package commands

import (
	"reflect"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

func TestDelSingleKey(t *testing.T) {
	s := store.New()
	s.Set([]byte("k"), []byte("v"))

	got, fault := DelHandler(s)(args("DEL", "k"))
	if fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("DEL = %#v, want Integer(1)", got)
	}
	if s.Exists([]byte("k")) {
		t.Fatal("DEL replied 1 but the key is still present")
	}
}

// TestDelMultipleKeys is the count semantics: the reply is how many keys were
// actually removed, which is neither the argument count nor a boolean.
func TestDelMultipleKeys(t *testing.T) {
	s := store.New()
	s.Set([]byte("k1"), []byte("v1"))
	s.Set([]byte("k2"), []byte("v2"))

	got, fault := DelHandler(s)(args("DEL", "k1", "k2"))
	if fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(2) {
		t.Fatalf("DEL = %#v, want Integer(2)", got)
	}
	if s.Len() != 0 {
		t.Fatalf("Len() = %d after deleting both keys, want 0", s.Len())
	}
}

// TestDelCountsOnlyKeysThatExisted covers the mixed case, which is what the
// milestone's redis-cli demo exercises: DEL k1 k2 k3 with k3 absent answers 2.
func TestDelCountsOnlyKeysThatExisted(t *testing.T) {
	s := store.New()
	s.Set([]byte("k1"), []byte("v1"))
	s.Set([]byte("k2"), []byte("v2"))

	got, fault := DelHandler(s)(args("DEL", "k1", "k2", "k3"))
	if fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(2) {
		t.Fatalf("DEL k1 k2 k3 = %#v, want Integer(2)", got)
	}
	if s.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", s.Len())
	}
}

// TestDelMissingKeyIsNotAnError pins that deleting nothing is a success
// reporting zero, not an error frame.
func TestDelMissingKeyIsNotAnError(t *testing.T) {
	s := store.New()

	got, fault := DelHandler(s)(args("DEL", "nothere"))
	if fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(0) {
		t.Fatalf("DEL of a missing key = %#v, want Integer(0)", got)
	}
	if _, isErr := got.(protocol.Error); isErr {
		t.Fatal("DEL of a missing key returned an error")
	}
}

// TestDelRepeatedKeyCountsOnce is the deliberate asymmetry with EXISTS: DEL
// counts keys removed, so naming the same key twice yields 1, while
// EXISTS k k yields 2. Both match Redis, and putting the tests side by side is
// what stops a later refactor from "fixing" one of them into the other.
func TestDelRepeatedKeyCountsOnce(t *testing.T) {
	s := store.New()
	s.Set([]byte("k"), []byte("v"))

	got, fault := DelHandler(s)(args("DEL", "k", "k"))
	if fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("DEL k k = %#v, want Integer(1): the second delete removes nothing", got)
	}
}

func TestDelWrongArity(t *testing.T) {
	s := store.New()

	got, fault := DelHandler(s)(args("DEL"))
	if fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("DEL with no keys = %#v, want protocol.Error", got)
	}
	const want = "ERR wrong number of arguments for 'del' command"
	if string(errValue) != want {
		t.Fatalf("DEL with no keys = %q, want %q", errValue, want)
	}
}

func TestDelRejectsNonBulkKey(t *testing.T) {
	inputs := []protocol.Value{
		protocol.Integer(1),
		protocol.SimpleString("k"),
		protocol.Null{},
		protocol.Array{},
	}

	const want = "ERR invalid argument for 'del' command"

	for _, in := range inputs {
		t.Run(commandsTypeName(in), func(t *testing.T) {
			s := store.New()
			s.Set([]byte("k"), []byte("v"))

			got, fault := DelHandler(s)([]protocol.Value{protocol.BulkString([]byte("DEL")), in})
			if fault != nil {
				t.Fatalf("DEL reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("DEL with %T key = %#v, want protocol.Error", in, got)
			}
			if string(errValue) != want {
				t.Fatalf("DEL with %T key = %q, want %q", in, errValue, want)
			}
		})
	}
}

// TestDelStopsAtTheFirstBadArgument checks that a malformed key aborts the
// command rather than partially applying it. A DEL that removed some keys and
// then errored would leave the client unable to tell what happened.
func TestDelStopsAtTheFirstBadArgument(t *testing.T) {
	s := store.New()
	s.Set([]byte("a"), []byte("1"))
	s.Set([]byte("b"), []byte("2"))

	got, _ := DelHandler(s)([]protocol.Value{
		protocol.BulkString([]byte("DEL")),
		protocol.BulkString([]byte("a")),
		protocol.Integer(99), // invalid, appears after a valid key
	})

	if _, ok := got.(protocol.Error); !ok {
		t.Fatalf("DEL with a bad trailing argument = %#v, want an error", got)
	}
	if !s.Exists([]byte("a")) {
		t.Fatal("DEL removed a key before rejecting the command; a rejected command must not be partially applied")
	}
}

// TestDelIsBinarySafe deletes a key containing bytes that are not text.
func TestDelIsBinarySafe(t *testing.T) {
	s := store.New()
	key := []byte{0x00, 0xff, '\r', '\n'}
	other := []byte("safe")
	s.Set(key, []byte("v"))
	s.Set(other, []byte("v"))

	got, fault := DelHandler(s)([]protocol.Value{
		protocol.BulkString([]byte("DEL")),
		protocol.BulkString(key),
	})
	if fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("DEL of a binary key = %#v, want Integer(1)", got)
	}
	if s.Exists(key) {
		t.Fatal("the binary key is still present after DEL")
	}
	if !s.Exists(other) {
		t.Fatal("DEL removed a key it was not asked to remove")
	}
}

// TestDelReturnsIntegerType checks the reply type, not just its value:
// redis-cli renders an Integer as (integer) 1 and a BulkString as "1", and
// clients branch on the type.
func TestDelReturnsIntegerType(t *testing.T) {
	s := store.New()
	s.Set([]byte("k"), []byte("v"))

	got, _ := DelHandler(s)(args("DEL", "k"))

	if _, ok := got.(protocol.Integer); !ok {
		t.Fatalf("DEL = %T, want protocol.Integer", got)
	}
	if wire := string(protocol.Encode(got)); wire != ":1\r\n" {
		t.Fatalf("encoded DEL reply = %q, want %q", wire, ":1\r\n")
	}
}

// TestDelDoesNotAlterOtherKeys guards against a handler that cleared the whole
// map, which would pass a single-key test and destroy a keyspace.
func TestDelDoesNotAlterOtherKeys(t *testing.T) {
	s := store.New()
	for _, k := range []string{"a", "b", "c", "d"} {
		s.Set([]byte(k), []byte("value-"+k))
	}

	if got, _ := DelHandler(s)(args("DEL", "b")); !reflect.DeepEqual(got, protocol.Integer(1)) {
		t.Fatalf("DEL b = %#v, want Integer(1)", got)
	}

	for _, k := range []string{"a", "c", "d"} {
		if !s.Exists([]byte(k)) {
			t.Fatalf("DEL b removed %q as well", k)
		}
	}
	if s.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", s.Len())
	}
}
