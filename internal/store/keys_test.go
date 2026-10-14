package store

import (
	"sort"
	"testing"
	"time"
)

// keySet collects Keys() into a sorted []string, so an assertion does not
// depend on the unspecified order the map yields.
func keySet(t *testing.T, s *Store) []string {
	t.Helper()

	keys := s.Keys()
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, string(k))
	}
	sort.Strings(out)
	return out
}

// equalKeyLists compares two already-sorted string slices.
func equalKeyLists(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestKeysReturnsAllLiveKeys(t *testing.T) {
	s, _ := newSweeperlessStorePair(t)

	for _, k := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		s.Set([]byte(k), []byte("v"))
	}

	got := keySet(t, s)
	want := []string{"alpha", "beta", "delta", "epsilon", "gamma"}
	if !equalKeyLists(got, want) {
		t.Fatalf("Keys = %q, want %q", got, want)
	}
}

// TestKeysExcludesExpired is the rule that makes KEYS usable for listing a
// keyspace: a key past its deadline is not a key, whether or not the sweeper
// has got round to removing it. The sweeper is deliberately disabled here, so
// what is tested is the filtering and not the background goroutine.
func TestKeysExcludesExpired(t *testing.T) {
	s, clk := newSweeperlessStorePair(t)

	s.Set([]byte("stays"), []byte("v"))
	s.Set([]byte("also-stays"), []byte("v"))
	s.SetWithTTL([]byte("goes"), []byte("v"), time.Minute)

	if got := keySet(t, s); len(got) != 3 {
		t.Fatalf("before expiry, Keys = %q, want 3 keys", got)
	}

	clk.Advance(time.Minute + time.Nanosecond)

	got := keySet(t, s)
	want := []string{"also-stays", "stays"}
	if !equalKeyLists(got, want) {
		t.Fatalf("after expiry, Keys = %q, want %q", got, want)
	}

	// The expired key must still be in the map — Keys skips it, it does not
	// delete it. Len counts the map, so it is the observable difference
	// between "filtered" and "swept".
	if got := s.Len(); got != 3 {
		t.Fatalf("Len = %d after Keys, want 3: Keys must not delete expired keys", got)
	}
}

// TestKeysSingleInstant is the invariant that cannot be checked by looking at
// which keys survive: it is a statement about how many times the clock was
// read. A clock that advances on every read would move the deadline for the
// later keys, so survival depends on map order rather than on the rule. The
// honest test counts the calls.
//
// The counting clock is installed at construction rather than swapped in
// afterwards. Assigning to s.clk would be a data race against the sweeper
// goroutine the store starts, and -race would rightly fail the test.
func TestKeysSingleInstant(t *testing.T) {
	counter := &countingClock{inner: NewFakeClock()}

	s := newSweeperlessStore(counter)
	t.Cleanup(s.Close)

	for i := 0; i < 5; i++ {
		s.Set([]byte{byte('a' + i)}, []byte("v"))
	}
	s.SetWithTTL([]byte("expiring"), []byte("v"), time.Minute)

	// Measure only the Keys call. Set does not read the clock (it stores a
	// zero expiry), but SetWithTTL does, which is why the count is taken
	// after both the writes and the TTL write.
	before := counter.Reads()

	got := s.Keys()
	if len(got) != 6 {
		t.Fatalf("Keys returned %d keys, want 6", len(got))
	}

	if after := counter.Reads(); after-before != 1 {
		t.Fatalf("Keys read the clock %d times, want exactly 1", after-before)
	}
}

// TestKeysReturnsCopies pins the package's central ownership rule: what leaves
// the store is the caller's, and mutating it cannot reach back into the
// keyspace. Without this, a client could corrupt a stored key by writing into
// the slice it was handed.
func TestKeysReturnsCopies(t *testing.T) {
	s, _ := newSweeperlessStorePair(t)

	s.Set([]byte("original"), []byte("v"))

	first := s.Keys()
	if len(first) != 1 {
		t.Fatalf("Keys returned %d keys, want 1", len(first))
	}

	// Scribble over every byte of the returned key.
	for i := range first[0] {
		first[0][i] = 'X'
	}

	second := s.Keys()
	if len(second) != 1 {
		t.Fatalf("Keys returned %d keys on the second call, want 1", len(second))
	}
	if string(second[0]) != "original" {
		t.Fatalf("after mutating the returned slice, Keys = %q, want %q", second[0], "original")
	}

	// The keyspace itself must still answer to the original key.
	if _, found := s.Get([]byte("original")); !found {
		t.Fatal("Get(original) not found: the returned slice aliased the stored key")
	}
	if s.Exists([]byte("XXXXXXX")) {
		t.Fatal("Exists(XXXXXXXX) is true: mutating the returned slice reached the keyspace")
	}
}

// TestKeysEmptyStore pins empty-but-not-nil. The command layer ranges over the
// result, so nil would work there, but the store's other bulk accessors return
// usable empty slices and a caller that appends should not have to care which
// it got.
func TestKeysEmptyStore(t *testing.T) {
	s, _ := newSweeperlessStorePair(t)

	got := s.Keys()
	if got == nil {
		t.Fatal("Keys on an empty store returned nil, want an empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("Keys on an empty store returned %d keys, want 0", len(got))
	}
}

// TestKeysBinarySafe checks that the key bytes survive the trip out of the map
// unchanged. The map holds strings, and the conversion back to []byte is a
// reinterpretation, but a key containing NUL, invalid UTF-8 or CRLF is exactly
// where a conversion that validated or normalized would show up.
func TestKeysBinarySafe(t *testing.T) {
	s, _ := newSweeperlessStorePair(t)

	raw := []byte("k\x00ey\xff\r\n")
	s.Set(raw, []byte("v"))

	got := s.Keys()
	if len(got) != 1 {
		t.Fatalf("Keys returned %d keys, want 1", len(got))
	}
	if string(got[0]) != string(raw) {
		t.Fatalf("Keys = %q, want %q", got[0], raw)
	}
}
