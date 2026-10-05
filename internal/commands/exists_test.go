package commands

import (
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

func TestExistsSingleKey(t *testing.T) {
	s := store.New(store.RealClock{})
	s.Set([]byte("k"), []byte("v"))

	got, fault := ExistsHandler(s)(args("EXISTS", "k"))
	if fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("EXISTS k = %#v, want Integer(1)", got)
	}
}

func TestExistsMissingKey(t *testing.T) {
	s := store.New(store.RealClock{})

	got, fault := ExistsHandler(s)(args("EXISTS", "nothere"))
	if fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(0) {
		t.Fatalf("EXISTS nothere = %#v, want Integer(0)", got)
	}
	if _, isErr := got.(protocol.Error); isErr {
		t.Fatal("EXISTS of a missing key returned an error")
	}
}

func TestExistsMultipleKeys(t *testing.T) {
	s := store.New(store.RealClock{})
	s.Set([]byte("a"), []byte("1"))
	s.Set([]byte("b"), []byte("2"))

	got, fault := ExistsHandler(s)(args("EXISTS", "a", "b", "c"))
	if fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(2) {
		t.Fatalf("EXISTS a b c = %#v, want Integer(2)", got)
	}
}

// TestExistsCountsDuplicateKeysTwice is the quirk the milestone calls out.
//
// EXISTS k k answers 2 when k exists, because the count is over arguments, not
// distinct keys. That looks like a bug and is not: Redis documents it as
// counting the arguments that exist, and clients use `EXISTS a b c` as a "how
// many of these are present" probe. It is also the deliberate opposite of DEL,
// where a repeated key is removed once.
func TestExistsCountsDuplicateKeysTwice(t *testing.T) {
	s := store.New(store.RealClock{})
	s.Set([]byte("k"), []byte("v"))

	got, fault := ExistsHandler(s)(args("EXISTS", "k", "k"))
	if fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(2) {
		t.Fatalf("EXISTS k k = %#v, want Integer(2): the count is over arguments, not distinct keys", got)
	}

	// The same shape with a missing key, to show the count tracks existence
	// per argument rather than the argument count.
	got, _ = ExistsHandler(s)(args("EXISTS", "k", "k", "nothere", "k"))
	if got != protocol.Integer(3) {
		t.Fatalf("EXISTS k k nothere k = %#v, want Integer(3)", got)
	}
}

// TestExistsRepeatedMissingKeyCountsZero is the mirror case: a duplicate of a
// key that does not exist still contributes nothing.
func TestExistsRepeatedMissingKeyCountsZero(t *testing.T) {
	s := store.New(store.RealClock{})

	got, fault := ExistsHandler(s)(args("EXISTS", "nothere", "nothere"))
	if fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(0) {
		t.Fatalf("EXISTS nothere nothere = %#v, want Integer(0)", got)
	}
}

func TestExistsWrongArity(t *testing.T) {
	s := store.New(store.RealClock{})

	got, fault := ExistsHandler(s)(args("EXISTS"))
	if fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("EXISTS with no keys = %#v, want protocol.Error", got)
	}
	const want = "ERR wrong number of arguments for 'exists' command"
	if string(errValue) != want {
		t.Fatalf("EXISTS with no keys = %q, want %q", errValue, want)
	}
}

func TestExistsRejectsNonBulkKey(t *testing.T) {
	inputs := []protocol.Value{
		protocol.Integer(1),
		protocol.SimpleString("k"),
		protocol.Null{},
		protocol.Array{},
	}

	const want = "ERR invalid argument for 'exists' command"

	for _, in := range inputs {
		t.Run(commandsTypeName(in), func(t *testing.T) {
			s := store.New(store.RealClock{})
			s.Set([]byte("k"), []byte("v"))

			got, fault := ExistsHandler(s)([]protocol.Value{protocol.BulkString([]byte("EXISTS")), in})
			if fault != nil {
				t.Fatalf("EXISTS reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("EXISTS with %T key = %#v, want protocol.Error", in, got)
			}
			if string(errValue) != want {
				t.Fatalf("EXISTS with %T key = %q, want %q", in, errValue, want)
			}
		})
	}
}

// TestExistsReturnsIntegerType checks the reply type, since redis-cli renders
// an Integer as (integer) 1 and a BulkString as "1".
func TestExistsReturnsIntegerType(t *testing.T) {
	s := store.New(store.RealClock{})
	s.Set([]byte("k"), []byte("v"))

	got, _ := ExistsHandler(s)(args("EXISTS", "k"))

	if _, ok := got.(protocol.Integer); !ok {
		t.Fatalf("EXISTS = %T, want protocol.Integer", got)
	}
	if wire := string(protocol.Encode(got)); wire != ":1\r\n" {
		t.Fatalf("encoded EXISTS reply = %q, want %q", wire, ":1\r\n")
	}
}

// TestExistsIsBinarySafe probes a key containing bytes that are not text.
func TestExistsIsBinarySafe(t *testing.T) {
	s := store.New(store.RealClock{})
	key := []byte{0x00, 0xff, '\r', '\n'}
	s.Set(key, []byte("v"))

	got, fault := ExistsHandler(s)([]protocol.Value{
		protocol.BulkString([]byte("EXISTS")),
		protocol.BulkString(key),
	})
	if fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("EXISTS of a binary key = %#v, want Integer(1)", got)
	}
}

// TestExistsDoesNotCreateKeys guards against an implementation that used Set
// or an upsert to answer the question.
func TestExistsDoesNotCreateKeys(t *testing.T) {
	s := store.New(store.RealClock{})

	if _, fault := ExistsHandler(s)(args("EXISTS", "nothere")); fault != nil {
		t.Fatalf("EXISTS reported an internal fault: %v", fault)
	}

	if s.Len() != 0 {
		t.Fatalf("EXISTS created %d keys; it must only read", s.Len())
	}
}
