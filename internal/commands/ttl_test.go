package commands

import (
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// TestTTLWithNoExpiryReturnsMinusOne pins the first sentinel on the wire.
func TestTTLWithNoExpiryReturnsMinusOne(t *testing.T) {
	s := newExpireStore()
	s.Set([]byte("k"), []byte("v"))

	got, fault := TTLHandler(s)(args("TTL", "k"))
	if fault != nil {
		t.Fatalf("TTL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(-1) {
		t.Fatalf("TTL of a key with no expiry = %#v, want Integer(-1)", got)
	}

	if wire := string(protocol.Encode(got)); wire != ":-1\r\n" {
		t.Fatalf("encoded TTL reply = %q, want %q", wire, ":-1\r\n")
	}
}

// TestTTLOfMissingKeyReturnsMinusTwo pins the second sentinel.
func TestTTLOfMissingKeyReturnsMinusTwo(t *testing.T) {
	s := newExpireStore()

	got, fault := TTLHandler(s)(args("TTL", "nothere"))
	if fault != nil {
		t.Fatalf("TTL reported an internal fault: %v", fault)
	}
	if got != protocol.Integer(-2) {
		t.Fatalf("TTL of a missing key = %#v, want Integer(-2)", got)
	}

	if wire := string(protocol.Encode(got)); wire != ":-2\r\n" {
		t.Fatalf("encoded TTL reply = %q, want %q", wire, ":-2\r\n")
	}
}

// TestTTLWithLiveTTLReturnsPositive checks the ordinary case, and that the
// reply is a whole number of seconds with no sentinel confusion.
func TestTTLWithLiveTTLReturnsPositive(t *testing.T) {
	s := newExpireStore()
	s.SetWithTTL([]byte("k"), []byte("v"), 100*time.Second)

	got, fault := TTLHandler(s)(args("TTL", "k"))
	if fault != nil {
		t.Fatalf("TTL reported an internal fault: %v", fault)
	}

	n, ok := got.(protocol.Integer)
	if !ok {
		t.Fatalf("TTL = %#v (%T), want protocol.Integer", got, got)
	}
	if n < 99 || n > 100 {
		t.Fatalf("TTL = %d, want 99..100 for a 100-second TTL", n)
	}
}

// TestTTLRoundTripNeverContradictsLiveness is the invariant that matters more
// than exact rounding at the sub-second boundary.
//
// For every TTL a key can have, the integer TTL reports must be one a client
// can trust: a live key must never read as 0 or negative, and a key that is
// already gone must report the missing sentinel. A client that uses
// `TTL k > 0` as a liveness check — which is idiomatic Redis — depends on
// exactly this.
//
// It is what caught the sub-half-second case: with plain rounding, a key set
// with PX 500 reads as 0 a few hundred microseconds later, because 499.9 ms
// rounds down. Zero is a lie about a key that is still readable.
//
// # Why this asserts a set of values and not a relation between two samples
//
// The obvious version of this test asks TTL for a value, asks the store
// whether the key still exists, and asserts the two agree. That version is
// wrong, and it failed roughly once in a hundred runs for four milestones.
//
// The two questions are separate observations, each reading the clock, so they
// describe two different instants — not one. With a 1ns TTL the key is
// guaranteed to die in between; whether the failure shows up is only a question
// of whether the goroutine is preempted inside that window. The false alarm
// reads: the handler sees a live key with sub-nanosecond remaining and
// correctly reports the floor of 1, the key then expires, and the existence
// check reports it gone. Both answers are truthful. There is no instant at
// which they conflict, because they were never taken at the same instant.
//
// No reordering fixes that: sampling liveness first just moves the race to the
// other side, where a key that died in the gap makes a live reading pair with
// the missing sentinel. The honest assertion is therefore about the TTL value
// alone, in the terms the implementation guarantees it:
//
//	live key with an expiry -> at least 1
//	key already gone        -> exactly -2
//
// Because the TTL question is asked first, a positive answer says the key was
// alive at that moment and deliberately says nothing about now — it may have
// died since, which is ordinary. Every other value is impossible whenever it
// is observed, so the half-second rounding bug this test exists for still
// fails it, and so does a wrong or newly invented sentinel.
func TestTTLRoundTripNeverContradictsLiveness(t *testing.T) {
	ttls := []time.Duration{
		time.Nanosecond,
		time.Microsecond,
		time.Millisecond,
		100 * time.Millisecond,
		499 * time.Millisecond,
		500 * time.Millisecond,
		999 * time.Millisecond,
		time.Second,
		1500 * time.Millisecond,
		2500 * time.Millisecond,
		time.Minute,
		time.Hour,
	}

	for _, ttl := range ttls {
		t.Run(ttl.String(), func(t *testing.T) {
			s := newExpireStore()
			s.SetWithTTL([]byte("k"), []byte("v"), ttl)

			got, fault := TTLHandler(s)(args("TTL", "k"))
			if fault != nil {
				t.Fatalf("TTL reported an internal fault: %v", fault)
			}

			n, ok := got.(protocol.Integer)
			if !ok {
				t.Fatalf("TTL = %#v (%T), want protocol.Integer", got, got)
			}

			// Corroboration, not a second sample to compare against. If the
			// key is still here, it was here when TTL was asked, so a missing
			// sentinel would be a contradiction and is rejected below.
			live := s.Exists([]byte("k"))

			switch {
			case live && n == -2:
				t.Fatal("key is still live but TTL reported -2 (missing); the sentinel cannot be true")
			case n >= 1:
				// Alive when asked. Whether it is alive now is not this
				// assertion's business: the TTL may simply have elapsed since,
				// and treating that as a failure is the bug this test used to
				// have. A live key reports at least 1 by construction, so this
				// is also where a sub-half-second rounding regression lands.
			case n == -2:
				// Already gone when asked. Correct, and the only way a
				// vanished key may be reported. -1 cannot appear here: the
				// store returns it only for a key with no expiry set, which
				// this test never creates.
			default:
				t.Fatalf("TTL of a key set with %v = %d, want >= 1 for a live key or -2 for a missing one; 0, -1 and values below -2 are never valid here", ttl, n)
			}
		})
	}
}

// TestTTLDoesNotReportASentinelForALiveKey is the boundary that matters most:
// a tiny but positive TTL must never come back as -1 or -2. Those values mean
// "no expiry" and "no key", and reporting either for a key that exists with a
// sub-second lifetime would be a lie a client acts on.
func TestTTLDoesNotReportASentinelForALiveKey(t *testing.T) {
	for _, ttl := range []time.Duration{
		time.Nanosecond,
		time.Microsecond,
		time.Millisecond,
		100 * time.Millisecond,
		499 * time.Millisecond,
	} {
		t.Run(ttl.String(), func(t *testing.T) {
			s := newExpireStore()
			s.SetWithTTL([]byte("k"), []byte("v"), ttl)

			got, _ := TTLHandler(s)(args("TTL", "k"))

			n, ok := got.(protocol.Integer)
			if !ok {
				t.Fatalf("TTL = %#v (%T), want protocol.Integer", got, got)
			}
			if n < 0 {
				t.Fatalf("TTL of a live key with %v left = %d, want 0 or more", ttl, n)
			}
		})
	}
}

func TestTTLWrongArity(t *testing.T) {
	const want = "ERR wrong number of arguments for 'ttl' command"

	for _, tt := range []struct {
		name string
		args []protocol.Value
	}{
		{"no arguments", args("TTL")},
		{"too many", args("TTL", "k", "extra")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpireStore()

			got, fault := TTLHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("TTL reported an internal fault: %v", fault)
			}
			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("TTL(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("TTL(%v) = %q, want %q", tt.args, errValue, want)
			}
		})
	}
}

func TestTTLRejectsNonBulkKey(t *testing.T) {
	s := newExpireStore()

	got, fault := TTLHandler(s)([]protocol.Value{protocol.BulkString([]byte("TTL")), protocol.Integer(1)})
	if fault != nil {
		t.Fatalf("TTL reported an internal fault: %v", fault)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("TTL with an integer key = %#v, want protocol.Error", got)
	}
	if want := "ERR invalid argument for 'ttl' command"; string(errValue) != want {
		t.Fatalf("TTL with an integer key = %q, want %q", errValue, want)
	}
}

// TestTTLAfterDeletionReturnsMinusTwo covers the transition a client actually
// observes: a key with a TTL, then deleted, then asked about again.
func TestTTLAfterDeletionReturnsMinusTwo(t *testing.T) {
	s := newExpireStore()
	s.SetWithTTL([]byte("k"), []byte("v"), time.Hour)

	if _, fault := DelHandler(s)(args("DEL", "k")); fault != nil {
		t.Fatalf("DEL reported an internal fault: %v", fault)
	}

	got, _ := TTLHandler(s)(args("TTL", "k"))
	if got != protocol.Integer(-2) {
		t.Fatalf("TTL after DEL = %#v, want Integer(-2)", got)
	}
}

// TestTTLIsBinarySafe asks about a key containing bytes that are not text.
func TestTTLIsBinarySafe(t *testing.T) {
	s := newExpireStore()
	key := []byte{0x00, 0xff, '\r', '\n'}
	s.SetWithTTL(key, []byte("v"), time.Hour)

	got, fault := TTLHandler(s)([]protocol.Value{
		protocol.BulkString([]byte("TTL")),
		protocol.BulkString(key),
	})
	if fault != nil {
		t.Fatalf("TTL reported an internal fault: %v", fault)
	}

	n, ok := got.(protocol.Integer)
	if !ok {
		t.Fatalf("TTL = %#v, want protocol.Integer", got)
	}
	if n < 3599 || n > 3600 {
		t.Fatalf("TTL = %d, want about 3600", n)
	}
}

// TestTTLThroughTheStoreSentinels checks the handler's translation directly:
// whatever the store answers, the wire values are exactly -1 and -2.
func TestTTLThroughTheStoreSentinels(t *testing.T) {
	if got := int64(store.TTLNoExpiry / time.Second); got != -1 {
		t.Fatalf("TTLNoExpiry maps to %d, want -1", got)
	}
	if got := int64(store.TTLMissing / time.Second); got != -2 {
		t.Fatalf("TTLMissing maps to %d, want -2", got)
	}
}
