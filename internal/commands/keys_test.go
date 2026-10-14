package commands

import (
	"sort"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// replyKeys extracts a KEYS reply as a sorted []string, so an assertion does
// not depend on the unspecified order Store.Keys returns.
func replyKeys(t *testing.T, v protocol.Value) []string {
	t.Helper()

	arr, ok := v.(protocol.Array)
	if !ok {
		t.Fatalf("reply is %T (%#v), want protocol.Array", v, v)
	}

	out := make([]string, 0, len(arr))
	for i, elem := range arr {
		bulk, ok := elem.(protocol.BulkString)
		if !ok {
			t.Fatalf("element %d is %T, want protocol.BulkString", i, elem)
		}
		out = append(out, string(bulk))
	}
	sort.Strings(out)
	return out
}

func TestKeysReturnsEveryKey(t *testing.T) {
	s := store.New(store.RealClock{})
	s.Set([]byte("alpha"), []byte("one"))
	s.Set([]byte("beta"), []byte("two"))
	s.Set([]byte("gamma"), []byte("three"))

	got, fault := KeysHandler(s)(args("KEYS", "*"))
	if fault != nil {
		t.Fatalf("KEYS reported an internal fault: %v", fault)
	}

	keys := replyKeys(t, got)
	want := []string{"alpha", "beta", "gamma"}
	if len(keys) != len(want) {
		t.Fatalf("KEYS * returned %q, want %q", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("KEYS * returned %q, want %q", keys, want)
		}
	}
}

// TestKeysEmptyStore pins the empty reply as an array rather than a null. A
// client iterating the reply branches on the difference, and Redis answers an
// empty array here too.
func TestKeysEmptyStore(t *testing.T) {
	s := store.New(store.RealClock{})

	got, fault := KeysHandler(s)(args("KEYS", "*"))
	if fault != nil {
		t.Fatalf("KEYS reported an internal fault: %v", fault)
	}

	arr, ok := got.(protocol.Array)
	if !ok {
		t.Fatalf("KEYS * on an empty store = %#v (%T), want protocol.Array", got, got)
	}
	if len(arr) != 0 {
		t.Fatalf("KEYS * on an empty store returned %d keys, want 0", len(arr))
	}

	// The wire form of an empty array is *0\r\n. Checked explicitly because
	// that is the byte sequence a client actually sees, and a null reply would
	// encode as $-1\r\n instead.
	if encoded := string(protocol.Encode(got)); encoded != "*0\r\n" {
		t.Fatalf("KEYS * encoded as %q, want %q", encoded, "*0\r\n")
	}
}

// TestKeysRejectsOtherPatterns is the restriction this milestone chose: a
// pattern that is not exactly `*` is refused rather than matched partially.
// A wrong answer would be worse than a refusal, because a client asking for
// `user:*` and receiving every key would act on keys it did not ask for.
func TestKeysRejectsOtherPatterns(t *testing.T) {
	s := store.New(store.RealClock{})
	s.Set([]byte("alpha"), []byte("one"))

	patterns := []string{
		"user:*", // a prefix glob
		"?",      // single-character glob
		"[abc]",  // character class
		"*x",     // star with a suffix
		"a*",     // prefix then star
		"",       // empty pattern
		" *",     // leading space is not trimmed
		"* ",     // trailing space is not trimmed
		"ALPHA",  // a literal, not a pattern
		"**",     // two stars is not one star
	}

	for _, p := range patterns {
		t.Run(p, func(t *testing.T) {
			got, fault := KeysHandler(s)(args("KEYS", p))
			if fault != nil {
				t.Fatalf("KEYS %q reported an internal fault: %v", p, fault)
			}
			msg := mustError(t, got)
			if msg != "ERR KEYS: only * is supported" {
				t.Fatalf("KEYS %q = %q, want %q", p, msg, "ERR KEYS: only * is supported")
			}
		})
	}
}

// TestKeysArity checks both directions: no pattern and too many arguments.
// Arity is validated before the pattern is examined, so `KEYS` alone reports
// the arity problem rather than an unsupported pattern it never received.
func TestKeysArity(t *testing.T) {
	s := store.New(store.RealClock{})

	cases := []struct {
		name string
		argv []protocol.Value
	}{
		{"no pattern", args("KEYS")},
		{"too many", args("KEYS", "*", "extra")},
		{"three extra", args("KEYS", "*", "a", "b")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, fault := KeysHandler(s)(tc.argv)
			if fault != nil {
				t.Fatalf("reported an internal fault: %v", fault)
			}
			msg := mustError(t, got)
			if msg != "ERR wrong number of arguments for 'keys' command" {
				t.Fatalf("= %q, want the arity error", msg)
			}
		})
	}
}

// TestKeysRejectsNonBulkPattern covers a pattern that arrives as another frame
// type. That is a malformed request rather than an unsupported pattern, and
// the two are reported differently so a client can tell which it did.
func TestKeysRejectsNonBulkPattern(t *testing.T) {
	s := store.New(store.RealClock{})

	cases := []struct {
		name string
		argv []protocol.Value
	}{
		{"integer", []protocol.Value{protocol.BulkString("KEYS"), protocol.Integer(123)}},
		{"array", []protocol.Value{protocol.BulkString("KEYS"), protocol.Array{protocol.BulkString("*")}}},
		{"simple string", []protocol.Value{protocol.BulkString("KEYS"), protocol.SimpleString("*")}},
		{"null", []protocol.Value{protocol.BulkString("KEYS"), protocol.Null{}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, fault := KeysHandler(s)(tc.argv)
			if fault != nil {
				t.Fatalf("reported an internal fault: %v", fault)
			}
			msg := mustError(t, got)
			if msg != "ERR invalid argument for 'keys' command" {
				t.Fatalf("= %q, want the invalid-argument error", msg)
			}
		})
	}
}

// TestKeysExcludesExpiredThroughHandler checks the command end of the
// single-instant rule: an expired key is not listed even though it is still in
// the map, because the store filters against the clock rather than trusting
// the sweeper to have run.
//
// Expiry is forced with a zero TTL rather than by advancing a clock. A TTL of
// zero stores a deadline of now, and the store treats a deadline as inclusive
// — "expires at T" means T is the first instant the key is gone — so the key is
// expired by the time the handler reads it, on any clock, with no sleeping and
// no injected time. The commands package has no fake clock by design; its
// tests assert the translation between RESP values and store answers, and the
// store's own tests pin the timing exactly.
func TestKeysExcludesExpiredThroughHandler(t *testing.T) {
	s := store.New(store.RealClock{})

	s.Set([]byte("kept"), []byte("v"))
	s.SetWithTTL([]byte("gone"), []byte("v"), 0)

	got, fault := KeysHandler(s)(args("KEYS", "*"))
	if fault != nil {
		t.Fatalf("KEYS reported an internal fault: %v", fault)
	}

	keys := replyKeys(t, got)
	if len(keys) != 1 || keys[0] != "kept" {
		t.Fatalf("KEYS * = %q, want only %q: an expired key must not be listed", keys, "kept")
	}

	// The key is still in the map — KEYS skipped it, it did not delete it.
	// Len counts the map, so it distinguishes filtering from sweeping.
	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2: KEYS must not delete expired keys", got)
	}
}

// TestKeysIsRegisteredAsReadCommand pins the AOF rule. KEYS changes nothing, so
// it must not be logged: a replayed log containing KEYS would be a log whose
// meaning depends on when it was read.
func TestKeysIsRegisteredAsReadCommand(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	if r.IsWrite("KEYS") {
		t.Fatal("KEYS is registered as a write; it must be a read so it never reaches the AOF")
	}

	// It must still be dispatchable — a read command that is not registered
	// would answer "unknown command".
	got, fault := r.Dispatch(protocol.Array{protocol.BulkString("KEYS"), protocol.BulkString("*")})
	if fault != nil {
		t.Fatalf("Dispatch(KEYS *) reported an internal fault: %v", fault)
	}
	if _, ok := got.(protocol.Array); !ok {
		t.Fatalf("Dispatch(KEYS *) = %#v (%T), want protocol.Array", got, got)
	}
}
