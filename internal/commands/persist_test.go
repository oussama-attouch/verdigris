package commands

import (
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// TestPersistRemovesTTL covers the successful case, and checks the effect
// rather than only the reply: the key must outlive the TTL it used to have.
func TestPersistRemovesTTL(t *testing.T) {
	s := newExpireStore()
	s.SetWithTTL([]byte("k"), []byte("v"), 100*time.Second)

	got, fault := PersistHandler(s)(args("PERSIST", "k"))
	if fault != nil {
		t.Fatalf("PERSIST reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("PERSIST on a key with a TTL = %#v, want Integer(1)", got)
	}

	if remaining := s.TTL([]byte("k")); remaining != store.TTLNoExpiry {
		t.Fatalf("TTL after PERSIST = %v, want TTLNoExpiry", remaining)
	}

	// And TTL through the handler must now report -1.
	ttlReply, _ := TTLHandler(s)(args("TTL", "k"))
	if ttlReply != protocol.Integer(-1) {
		t.Fatalf("TTL after PERSIST = %#v, want Integer(-1)", ttlReply)
	}
}

// TestPersistWithoutTTLReturnsZero covers the first of the two false cases: a
// key that exists but has no expiry.
func TestPersistWithoutTTLReturnsZero(t *testing.T) {
	s := newExpireStore()
	s.Set([]byte("k"), []byte("v"))

	got, fault := PersistHandler(s)(args("PERSIST", "k"))
	if fault != nil {
		t.Fatalf("PERSIST reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(0) {
		t.Fatalf("PERSIST on a key with no TTL = %#v, want Integer(0)", got)
	}

	// The key must be untouched.
	if !s.Exists([]byte("k")) {
		t.Fatal("PERSIST removed a key that had no TTL")
	}
}

// TestPersistMissingReturnsZero covers the second false case. Both produce 0
// because Redis counts expiries removed, not keys found.
func TestPersistMissingReturnsZero(t *testing.T) {
	s := newExpireStore()

	got, fault := PersistHandler(s)(args("PERSIST", "nothere"))
	if fault != nil {
		t.Fatalf("PERSIST reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(0) {
		t.Fatalf("PERSIST on a missing key = %#v, want Integer(0)", got)
	}
}

// TestPersistIsNotIdempotentInItsReply pins that the second PERSIST of the
// same key answers 0: there is no longer an expiry to remove.
func TestPersistIsNotIdempotentInItsReply(t *testing.T) {
	s := newExpireStore()
	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)

	if got, _ := PersistHandler(s)(args("PERSIST", "k")); got != protocol.Integer(1) {
		t.Fatalf("first PERSIST = %#v, want Integer(1)", got)
	}
	if got, _ := PersistHandler(s)(args("PERSIST", "k")); got != protocol.Integer(0) {
		t.Fatalf("second PERSIST = %#v, want Integer(0)", got)
	}
}

// TestPersistThenExpireIsPossible checks the pair round-trips: a key made
// persistent can be given a TTL again.
func TestPersistThenExpireIsPossible(t *testing.T) {
	s := newExpireStore()
	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)

	if got, _ := PersistHandler(s)(args("PERSIST", "k")); got != protocol.Integer(1) {
		t.Fatalf("PERSIST = %#v, want Integer(1)", got)
	}
	if got, _ := ExpireHandler(s)(numericArgs("EXPIRE", []string{"k"}, 100)); got != protocol.Integer(1) {
		t.Fatalf("EXPIRE after PERSIST = %#v, want Integer(1)", got)
	}

	remaining := s.TTL([]byte("k"))
	if remaining < 99*time.Second || remaining > 100*time.Second {
		t.Fatalf("TTL after PERSIST then EXPIRE = %v, want about 100s", remaining)
	}
}

func TestPersistWrongArity(t *testing.T) {
	const want = "ERR wrong number of arguments for 'persist' command"

	for _, tt := range []struct {
		name string
		args []protocol.Value
	}{
		{"no arguments", args("PERSIST")},
		{"too many", args("PERSIST", "k", "extra")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpireStore()

			got, fault := PersistHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("PERSIST reported an internal fault: %v", fault)
			}
			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("PERSIST(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("PERSIST(%v) = %q, want %q", tt.args, errValue, want)
			}
		})
	}
}

func TestPersistRejectsNonBulkKey(t *testing.T) {
	s := newExpireStore()

	got, fault := PersistHandler(s)([]protocol.Value{protocol.BulkString([]byte("PERSIST")), protocol.Integer(1)})
	if fault != nil {
		t.Fatalf("PERSIST reported an internal fault: %v", fault)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("PERSIST with an integer key = %#v, want protocol.Error", got)
	}
	if want := "ERR invalid argument for 'persist' command"; string(errValue) != want {
		t.Fatalf("PERSIST with an integer key = %q, want %q", errValue, want)
	}
}

// TestPersistIsBinarySafe removes the expiry from a key containing bytes that
// are not text.
func TestPersistIsBinarySafe(t *testing.T) {
	s := newExpireStore()
	key := []byte{0x00, 0xff, '\r', '\n'}
	s.SetWithTTL(key, []byte("v"), time.Hour)

	got, fault := PersistHandler(s)([]protocol.Value{
		protocol.BulkString([]byte("PERSIST")),
		protocol.BulkString(key),
	})
	if fault != nil {
		t.Fatalf("PERSIST reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("PERSIST on a binary key = %#v, want Integer(1)", got)
	}
	if remaining := s.TTL(key); remaining != store.TTLNoExpiry {
		t.Fatalf("TTL after PERSIST = %v, want TTLNoExpiry", remaining)
	}
}

// TestPersistReturnsIntegerType checks the reply type, since redis-cli renders
// an Integer as (integer) 1 and a BulkString as "1".
func TestPersistReturnsIntegerType(t *testing.T) {
	s := newExpireStore()
	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)

	got, _ := PersistHandler(s)(args("PERSIST", "k"))

	if _, ok := got.(protocol.Integer); !ok {
		t.Fatalf("PERSIST = %T, want protocol.Integer", got)
	}
	if wire := string(protocol.Encode(got)); wire != ":1\r\n" {
		t.Fatalf("encoded PERSIST reply = %q, want %q", wire, ":1\r\n")
	}
}
