package commands

import (
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// newExpireStore returns a store for command-level TTL tests.
//
// These tests use a real clock rather than a fake one, deliberately. The
// handler's job is to translate between RESP values and store calls, not to
// decide what time it is, and the store's own tests already pin the timing
// behaviour exactly with a fake clock. What is asserted here is the
// translation: which reply comes back for which store answer. That means TTLs
// are chosen long enough that no assertion depends on how long the test took —
// an hour is not going to elapse mid-test.
func newExpireStore() *store.Store {
	return store.New(store.RealClock{})
}

// TestExpireAcceptsBothIntegerForms is the test that would have caught the
// bug this milestone shipped and the demo exposed.
//
// redis-cli sends `EXPIRE k 100` with the timeout as a bulk string — verified
// by capturing the bytes — while a client library may send it as a RESP
// integer. A handler that accepts only one of the two works with one kind of
// client and fails with the other, and the failure looks like a server bug.
func TestExpireAcceptsBothIntegerForms(t *testing.T) {
	const want = 100 * time.Second

	tests := []struct {
		name string
		args []protocol.Value
	}{
		{
			name: "bulk string, which is what redis-cli sends",
			args: args("EXPIRE", "k", "100"),
		},
		{
			name: "RESP integer",
			args: numericArgs("EXPIRE", []string{"k"}, 100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpireStore()
			s.Set([]byte("k"), []byte("v"))

			got, fault := ExpireHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("EXPIRE reported an internal fault: %v", fault)
			}
			if got != protocol.Integer(1) {
				t.Fatalf("EXPIRE k 100 = %#v, want Integer(1)", got)
			}

			remaining := s.TTL([]byte("k"))
			if remaining < want-time.Second || remaining > want {
				t.Fatalf("TTL after EXPIRE k 100 = %v, want about %v", remaining, want)
			}
		})
	}
}

func TestExpireOnExistingKeyReturnsOne(t *testing.T) {
	s := newExpireStore()
	s.Set([]byte("k"), []byte("v"))

	got, fault := ExpireHandler(s)(numericArgs("EXPIRE", []string{"k"}, 10))
	if fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("EXPIRE k 10 = %#v, want Integer(1)", got)
	}
	if remaining := s.TTL([]byte("k")); remaining <= 0 {
		t.Fatalf("TTL after EXPIRE = %v, want a positive duration", remaining)
	}
}

func TestExpireOnMissingKeyReturnsZero(t *testing.T) {
	s := newExpireStore()

	got, fault := ExpireHandler(s)(numericArgs("EXPIRE", []string{"nothere"}, 10))
	if fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(0) {
		t.Fatalf("EXPIRE nothere 10 = %#v, want Integer(0)", got)
	}
}

// TestExpireWithZeroDeletesTheKey checks the Redis behaviour that EXPIRE k 0
// is a delete, and that it still reports success.
func TestExpireWithZeroDeletesTheKey(t *testing.T) {
	s := newExpireStore()
	s.Set([]byte("k"), []byte("v"))

	got, fault := ExpireHandler(s)(numericArgs("EXPIRE", []string{"k"}, 0))
	if fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("EXPIRE k 0 = %#v, want Integer(1)", got)
	}
	if s.Exists([]byte("k")) {
		t.Fatal("the key survived EXPIRE k 0")
	}

	// And a subsequent GET must see it as absent.
	getReply, _ := GetHandler(s)(args("GET", "k"))
	if getReply != (protocol.Null{}) {
		t.Fatalf("GET after EXPIRE k 0 = %#v, want Null{}", getReply)
	}
}

func TestExpireWithNegativeSecondsDeletesTheKey(t *testing.T) {
	s := newExpireStore()
	s.Set([]byte("k"), []byte("v"))

	got, fault := ExpireHandler(s)(numericArgs("EXPIRE", []string{"k"}, -1))
	if fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("EXPIRE k -1 = %#v, want Integer(1)", got)
	}
	if s.Exists([]byte("k")) {
		t.Fatal("the key survived EXPIRE k -1")
	}
}

// TestExpireNonIntegerTimeout pins the exact wording of the parse error, which
// clients match on.
//
// Every case here sends a BulkString where an Integer belongs. That is what a
// client gets wrong in practice — `EXPIRE k abc`, or a mistake in how the
// request was built — and the reply is the same for both, as it is in Redis.
func TestExpireNonIntegerTimeout(t *testing.T) {
	const want = "ERR value is not an integer or out of range"

	tests := []struct {
		name string
		args []protocol.Value
	}{
		{"letters", args("EXPIRE", "k", "abc")},
		{"float", args("EXPIRE", "k", "1.5")},
		{"empty", args("EXPIRE", "k", "")},
		{"trailing junk", args("EXPIRE", "k", "10abc")},
		{"leading space", args("EXPIRE", "k", " 10")},
		{"hex", args("EXPIRE", "k", "0x10")},
		// A bulk string beyond int64 must be range-rejected, not wrapped.
		{"out of range bulk string", args("EXPIRE", "k", "99999999999999999999")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpireStore()
			s.Set([]byte("k"), []byte("v"))

			got, fault := ExpireHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("EXPIRE reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("EXPIRE(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("EXPIRE(%v) = %q, want %q", tt.args, errValue, want)
			}

			// A rejected command must not have set an expiry.
			if remaining := s.TTL([]byte("k")); remaining != store.TTLNoExpiry {
				t.Fatalf("a rejected EXPIRE still set a TTL: %v", remaining)
			}
		})
	}
}

// TestExpireHugeSecondsDoesNotWrap checks the overflow guard. A client asking
// for a TTL of MaxInt64 seconds must not end up with a negative duration,
// which would delete the key instead of preserving it indefinitely.
func TestExpireHugeSecondsDoesNotWrap(t *testing.T) {
	s := newExpireStore()
	s.Set([]byte("k"), []byte("v"))

	got, fault := ExpireHandler(s)(numericArgs("EXPIRE", []string{"k"}, 9223372036854775807))
	if fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("EXPIRE with MaxInt64 seconds = %#v, want Integer(1)", got)
	}
	if !s.Exists([]byte("k")) {
		t.Fatal("EXPIRE with a huge timeout deleted the key; the duration wrapped")
	}
	if remaining := s.TTL([]byte("k")); remaining <= 0 {
		t.Fatalf("TTL after a huge EXPIRE = %v, want a positive duration", remaining)
	}
}

func TestExpireWrongArity(t *testing.T) {
	const want = "ERR wrong number of arguments for 'expire' command"

	for _, tt := range []struct {
		name string
		args []protocol.Value
	}{
		{"no arguments", args("EXPIRE")},
		{"key only", args("EXPIRE", "k")},
		{"too many", numericArgs("EXPIRE", []string{"k"}, 10, 99)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpireStore()

			got, fault := ExpireHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("EXPIRE reported an internal fault: %v", fault)
			}
			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("EXPIRE(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("EXPIRE(%v) = %q, want %q", tt.args, errValue, want)
			}
		})
	}
}

func TestExpireRejectsNonBulkKey(t *testing.T) {
	s := newExpireStore()

	got, fault := ExpireHandler(s)([]protocol.Value{
		protocol.BulkString([]byte("EXPIRE")),
		protocol.Integer(1),
		protocol.Integer(10),
	})
	if fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("EXPIRE with an integer key = %#v, want protocol.Error", got)
	}
	if want := "ERR invalid argument for 'expire' command"; string(errValue) != want {
		t.Fatalf("EXPIRE with an integer key = %q, want %q", errValue, want)
	}
}

// TestExpireIsBinarySafe sets a TTL on a key containing bytes that are not
// text.
func TestExpireIsBinarySafe(t *testing.T) {
	s := newExpireStore()
	key := []byte{0x00, 0xff, '\r', '\n'}
	s.Set(key, []byte("v"))

	got, fault := ExpireHandler(s)(numericArgs("EXPIRE", []string{string(key)}, 100))
	if fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(1) {
		t.Fatalf("EXPIRE on a binary key = %#v, want Integer(1)", got)
	}
}

// TestExpireSecondsAreSecondsNotMilliseconds checks the unit. A handler that
// multiplied by the wrong constant would still return 1 and still set *a* TTL,
// so only measuring the remaining time catches it.
func TestExpireSecondsAreSecondsNotMilliseconds(t *testing.T) {
	s := newExpireStore()
	s.Set([]byte("k"), []byte("v"))

	if _, fault := ExpireHandler(s)(numericArgs("EXPIRE", []string{"k"}, 100)); fault != nil {
		t.Fatalf("EXPIRE reported an internal fault: %v", fault)
	}

	remaining := s.TTL([]byte("k"))
	if remaining < 99*time.Second || remaining > 100*time.Second {
		t.Fatalf("EXPIRE k 100 left %v, want about 100s", remaining)
	}
}
