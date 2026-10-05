package store

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestStore returns a store driven by a fake clock, with the sweeper's
// tick set short enough that active deletion is observable in milliseconds,
// and with the goroutine cleaned up when the test ends.
//
// The interval is passed to the constructor rather than assigned to a package
// variable, so there is nothing to restore and no way for this helper to
// change the behaviour of a test that runs after it.
func newTestStore(t *testing.T) (*Store, *FakeClock) {
	t.Helper()

	clk := NewFakeClock()
	s := NewWithInterval(clk, 2*time.Millisecond)
	t.Cleanup(s.Close)

	return s, clk
}

// newSweeperlessStore returns a store whose sweeper cannot realistically fire
// during a test, so that "expired but not yet swept" is an observable state
// rather than a race against a background goroutine.
//
// It is the right tool for asserting Len's DBSIZE semantics and for any other
// test whose subject is the state of the map rather than the sweeper's
// behaviour. Tests about sweeping itself use newTestStore, which shortens the
// interval instead.
func newSweeperlessStore(clk Clock) *Store {
	return NewWithInterval(clk, time.Hour)
}

// newSweeperlessStorePair is newSweeperlessStore with the clock handed back
// for tests that need to advance it.
func newSweeperlessStorePair(t *testing.T) (*Store, *FakeClock) {
	t.Helper()

	clk := NewFakeClock()
	s := newSweeperlessStore(clk)
	t.Cleanup(s.Close)

	return s, clk
}

// --- TTL basics -------------------------------------------------------------

// TestSetWithTTLExpires is the central expiry test, and it does it without
// sleeping: the clock moves, not the wall.
func TestSetWithTTLExpires(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)

	if got, found := s.Get([]byte("k")); !found || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("before expiry, Get = %q, %v; want %q, true", got, found, "v")
	}

	clk.Advance(time.Minute + time.Nanosecond)

	if got, found := s.Get([]byte("k")); found {
		t.Fatalf("after expiry, Get returned %q, true; want nil, false", got)
	}
}

// TestSetWithTTLStillAlive checks the boundary from the other side: a clock
// advanced to just before the deadline must not expire the key. An off-by-one
// in the comparison would pass the test above and fail this one.
func TestSetWithTTLStillAlive(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)
	clk.Advance(time.Minute - time.Nanosecond)

	got, found := s.Get([]byte("k"))
	if !found {
		t.Fatal("key expired one nanosecond early")
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get = %q, want %q", got, "v")
	}
}

// TestGetOfExpiredKeyDeletesIt checks that passive deletion actually removes
// the entry, rather than merely reporting the key as absent. Without the
// deletion, Len would stay high and memory would leak for keys nobody reads.
func TestGetOfExpiredKeyDeletesIt(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)
	if s.Len() != 1 {
		t.Fatalf("Len = %d before expiry, want 1", s.Len())
	}

	clk.Advance(2 * time.Minute)

	if _, found := s.Get([]byte("k")); found {
		t.Fatal("Get reported the expired key as present")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d after Get of an expired key, want 0: passive deletion did not remove it", s.Len())
	}
}

// TestExistsDoesPassiveDeletionToo checks the other read path. Exists is a
// separate method from Get, so it needs its own assertion that it deletes.
func TestExistsDoesPassiveDeletionToo(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute)

	if s.Exists([]byte("k")) {
		t.Fatal("Exists reported the expired key as present")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d after Exists of an expired key, want 0", s.Len())
	}
}

func TestExpiredKeysDoNotAppearInExists(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("live"), []byte("v"), time.Hour)
	s.SetWithTTL([]byte("dying"), []byte("v"), time.Minute)

	clk.Advance(30 * time.Minute)

	if !s.Exists([]byte("live")) {
		t.Fatal("Exists(live) = false before its expiry")
	}
	if s.Exists([]byte("dying")) {
		t.Fatal("Exists(dying) = true after its expiry")
	}
}

// --- Expire -----------------------------------------------------------------

func TestExpireOnExistingKey(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set([]byte("k"), []byte("v"))

	if !s.Expire([]byte("k"), time.Minute) {
		t.Fatal("Expire on an existing key returned false")
	}
	if got := s.TTL([]byte("k")); got != time.Minute {
		t.Fatalf("TTL after Expire = %v, want %v", got, time.Minute)
	}

	clk.Advance(2 * time.Minute)
	if s.Exists([]byte("k")) {
		t.Fatal("the key outlived the TTL Expire set")
	}
}

func TestExpireOnMissingKey(t *testing.T) {
	s, _ := newTestStore(t)

	if s.Expire([]byte("nothere"), time.Minute) {
		t.Fatal("Expire on a missing key returned true")
	}
}

// TestExpireWithZeroSecondsDeletesImmediately matches Redis: EXPIRE k 0 is a
// delete spelled differently, and it reports success.
func TestExpireWithZeroSecondsDeletesImmediately(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set([]byte("k"), []byte("v"))

	if !s.Expire([]byte("k"), 0) {
		t.Fatal("Expire with a zero TTL returned false, want true")
	}
	if s.Exists([]byte("k")) {
		t.Fatal("the key survived Expire with a zero TTL")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

func TestExpireWithNegativeSecondsDeletesImmediately(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set([]byte("k"), []byte("v"))

	if !s.Expire([]byte("k"), -time.Minute) {
		t.Fatal("Expire with a negative TTL returned false, want true")
	}
	if s.Exists([]byte("k")) {
		t.Fatal("the key survived Expire with a negative TTL")
	}
}

// TestExpireRenewsAnExpiredButUnsweptKey pins the decision made under the
// write lock: a key whose deadline has passed but which the sweeper has not
// collected is renewed rather than refused.
//
// The alternative — treating it as absent because it is logically dead —
// would mean the answer depends on whether the background sweeper happened to
// have run, which makes EXPIRE nondeterministic from a client's point of
// view. Deciding while holding the lock removes that dependency.
func TestExpireRenewsAnExpiredButUnsweptKey(t *testing.T) {
	s, clk := newSweeperlessStorePair(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute)

	if !s.Expire([]byte("k"), time.Hour) {
		t.Fatal("Expire refused to renew an expired-but-unswept key")
	}
	if got := s.TTL([]byte("k")); got != time.Hour {
		t.Fatalf("TTL after renewal = %v, want %v", got, time.Hour)
	}
	if !s.Exists([]byte("k")) {
		t.Fatal("the renewed key is still considered expired")
	}

	// And the renewal is real: it must outlive the original deadline.
	clk.Advance(time.Minute)
	if !s.Exists([]byte("k")) {
		t.Fatal("the renewed key expired on the original deadline")
	}
}

// TestExpireZeroStillDeletesAnExpiredButUnsweptKey checks the other branch of
// the same decision: a non-positive TTL is a delete, and it reports that
// there was something to delete.
func TestExpireZeroStillDeletesAnExpiredButUnsweptKey(t *testing.T) {
	s, clk := newSweeperlessStorePair(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute)

	if !s.Expire([]byte("k"), 0) {
		t.Fatal("Expire with a zero TTL on an expired-but-unswept key returned false")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d after Expire k 0, want 0", s.Len())
	}
}

// TestExpireReplacesAnExistingTTL checks that a second EXPIRE moves the
// deadline rather than adding to it.
func TestExpireReplacesAnExistingTTL(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)

	clk.Advance(30 * time.Second)
	if !s.Expire([]byte("k"), time.Hour) {
		t.Fatal("Expire returned false on a live key")
	}

	// The new deadline is an hour from now, so the original minute must not
	// still be in force.
	clk.Advance(time.Minute)
	if !s.Exists([]byte("k")) {
		t.Fatal("the key expired on the old deadline; Expire did not replace it")
	}
}

// --- TTL --------------------------------------------------------------------

func TestTTLNoExpiry(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set([]byte("k"), []byte("v"))

	if got := s.TTL([]byte("k")); got != TTLNoExpiry {
		t.Fatalf("TTL of a key with no expiry = %v, want TTLNoExpiry (%v)", got, TTLNoExpiry)
	}
}

func TestTTLMissing(t *testing.T) {
	s, _ := newTestStore(t)

	if got := s.TTL([]byte("nothere")); got != TTLMissing {
		t.Fatalf("TTL of a missing key = %v, want TTLMissing (%v)", got, TTLMissing)
	}
}

// TestTTLOfExpiredKeyIsMissing checks that an expired key reports -2 rather
// than a negative remaining duration, and that TTL removes it like the other
// read paths do.
func TestTTLOfExpiredKeyIsMissing(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute)

	if got := s.TTL([]byte("k")); got != TTLMissing {
		t.Fatalf("TTL of an expired key = %v, want TTLMissing", got)
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0: TTL should have removed the expired key", s.Len())
	}
}

// TestTTLCountsDownWithTheClock advances the fake clock in steps and asserts
// the remaining time falls by exactly those steps.
func TestTTLCountsDownWithTheClock(t *testing.T) {
	s, clk := newTestStore(t)

	const total = time.Hour
	s.SetWithTTL([]byte("k"), []byte("v"), total)

	elapsed := time.Duration(0)
	for _, step := range []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Second, 10 * time.Minute} {
		clk.Advance(step)
		elapsed += step

		want := total - elapsed
		if got := s.TTL([]byte("k")); got != want {
			t.Fatalf("after %v elapsed, TTL = %v, want %v", elapsed, got, want)
		}
	}
}

// TestTTLSentinelDivisionTruncates pins the arithmetic around the sentinels,
// because it is the opposite of what it looks like.
//
// The sentinels are -1 and -2 seconds, so `int64(ttl / time.Second)` does
// yield -1 and -2 for them. That is a property of these particular constants
// and not of the representation: the constants were nearly -1 and -2
// *nanoseconds*, and in that case Go's integer division on nanoseconds would
// truncate both toward zero and give 0 for each, which on the wire means a
// key whose TTL has just elapsed rather than the two distinct answers Redis
// defines.
//
// So this test records both facts: what the division gives today, and what it
// would have given with a smaller magnitude. The handler compares against the
// sentinels explicitly rather than dividing, which is why neither number can
// silently change what clients see.
func TestTTLSentinelDivisionTruncates(t *testing.T) {
	if got := int64(TTLNoExpiry / time.Second); got != -1 {
		t.Fatalf("TTLNoExpiry / time.Second = %d, want -1", got)
	}
	if got := int64(TTLMissing / time.Second); got != -2 {
		t.Fatalf("TTLMissing / time.Second = %d, want -2", got)
	}

	// The hazard the handler avoids: a negative duration smaller than a
	// second truncates to zero, so division is only safe for sentinels whose
	// magnitude is at least a second. Asserting it here documents why the
	// constants may not be shrunk.
	if got := int64(-1 * time.Nanosecond / time.Second); got != 0 {
		t.Fatalf("-1ns / time.Second = %d, want 0 to demonstrate the hazard division would introduce below one second", got)
	}

	// The sentinels must be distinguishable from each other and from a live
	// TTL, which is what the handler's switch relies on.
	if TTLNoExpiry == TTLMissing {
		t.Fatal("the two sentinels are equal")
	}
	if TTLNoExpiry >= 0 || TTLMissing >= 0 {
		t.Fatal("a sentinel is non-negative, so it could be confused with a live TTL")
	}
}

// --- Persist ----------------------------------------------------------------

func TestPersistRemovesTTL(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)

	if !s.Persist([]byte("k")) {
		t.Fatal("Persist on a key with a TTL returned false")
	}
	if got := s.TTL([]byte("k")); got != TTLNoExpiry {
		t.Fatalf("TTL after Persist = %v, want TTLNoExpiry", got)
	}

	// And the key must now outlive the TTL it used to have.
	clk.Advance(time.Hour)
	if !s.Exists([]byte("k")) {
		t.Fatal("the key expired anyway; Persist did not clear the expiry")
	}
	if got, found := s.Get([]byte("k")); !found || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("after Persist and a long advance, Get = %q, %v; want %q, true", got, found, "v")
	}
}

func TestPersistWithoutTTLReturnsFalse(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set([]byte("k"), []byte("v"))

	if s.Persist([]byte("k")) {
		t.Fatal("Persist on a key with no TTL returned true, want false")
	}
	// The key must be untouched.
	if !s.Exists([]byte("k")) {
		t.Fatal("Persist removed a key that had no TTL")
	}
}

func TestPersistOnMissingKeyReturnsFalse(t *testing.T) {
	s, _ := newTestStore(t)

	if s.Persist([]byte("nothere")) {
		t.Fatal("Persist on a missing key returned true, want false")
	}
}

// TestPersistOnExpiredKeyReturnsFalse checks the third "false" case: a key
// that existed but whose time has passed counts as missing.
func TestPersistOnExpiredKeyReturnsFalse(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute)

	if s.Persist([]byte("k")) {
		t.Fatal("Persist on an expired key returned true")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

// --- SET clears the TTL -----------------------------------------------------

// TestSetClearsExistingTTL is the behaviour milestone 5 calls out explicitly:
// a plain SET discards any expiry, so a temporary key can be made durable by
// writing it again.
func TestSetClearsExistingTTL(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v1"), time.Minute)
	if got := s.TTL([]byte("k")); got <= 0 {
		t.Fatalf("TTL after SetWithTTL = %v, want a positive duration", got)
	}

	s.Set([]byte("k"), []byte("v2"))

	if got := s.TTL([]byte("k")); got != TTLNoExpiry {
		t.Fatalf("TTL after a plain Set = %v, want TTLNoExpiry: SET must clear the expiry", got)
	}

	clk.Advance(time.Hour)
	if !s.Exists([]byte("k")) {
		t.Fatal("the key expired on the TTL the plain Set should have cleared")
	}
	if got, _ := s.Get([]byte("k")); !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get = %q, want %q", got, "v2")
	}
}

// TestSetWithTTLReplacesExistingTTL is the mirror: a Set that carries its own
// TTL replaces the old one rather than keeping the sooner deadline.
func TestSetWithTTLReplacesExistingTTL(t *testing.T) {
	s, clk := newTestStore(t)

	s.SetWithTTL([]byte("k"), []byte("v1"), time.Minute)
	s.SetWithTTL([]byte("k"), []byte("v2"), time.Hour)

	if got := s.TTL([]byte("k")); got != time.Hour {
		t.Fatalf("TTL after the second SetWithTTL = %v, want %v", got, time.Hour)
	}

	clk.Advance(time.Minute)
	if !s.Exists([]byte("k")) {
		t.Fatal("the key expired on the replaced deadline")
	}
}

// --- Len semantics ----------------------------------------------------------

// TestLenIncludesExpiredButUnsweptKeys pins the documented choice: Len is
// DBSIZE, which counts not-yet-collected keys.
//
// The test uses a store whose sweeper cannot run, so that "expired but
// unswept" is an observable state rather than a race with the background
// goroutine.
func TestLenIncludesExpiredButUnsweptKeys(t *testing.T) {
	clk := NewFakeClock()
	s := newSweeperlessStore(clk)
	t.Cleanup(s.Close)

	s.SetWithTTL([]byte("a"), []byte("1"), time.Minute)
	s.SetWithTTL([]byte("b"), []byte("2"), time.Minute)
	s.Set([]byte("c"), []byte("3"))

	clk.Advance(2 * time.Minute)

	// Both expired keys are still in the map, so Len counts all three.
	if got := s.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3: expired-but-unswept keys are counted, as DBSIZE does", got)
	}

	// And asking about them removes them one at a time.
	if s.Exists([]byte("a")) {
		t.Fatal("Exists(a) = true after its expiry")
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d after Exists removed one expired key, want 2", got)
	}
}

// --- Active deletion --------------------------------------------------------

// TestSweeperRemovesExpiredKeys is the active-deletion test: nothing reads the
// keys, so only the background sweeper can remove them.
func TestSweeperRemovesExpiredKeys(t *testing.T) {
	s, clk := newTestStore(t)

	const keys = 50
	for i := 0; i < keys; i++ {
		s.SetWithTTL([]byte(fmt.Sprintf("k%d", i)), []byte("v"), time.Minute)
	}
	// One key that must survive, to prove the sweeper is selective.
	s.SetWithTTL([]byte("survivor"), []byte("v"), time.Hour)

	clk.Advance(2 * time.Minute)

	waitForSweep(t, s, 1)

	if !s.Exists([]byte("survivor")) {
		t.Fatal("the sweeper removed a key that had not expired")
	}
}

// TestSweeperDrainsMoreThanOneSample checks the repeat behaviour: with more
// expired keys than one sample holds, the sweeper must keep going within the
// same wake-up rather than removing twenty keys per tick.
func TestSweeperDrainsMoreThanOneSample(t *testing.T) {
	s, clk := newTestStore(t)

	// Comfortably more than sweepSampleSize, so a single-round sweeper cannot
	// finish the job.
	const keys = 5 * sweepSampleSize
	for i := 0; i < keys; i++ {
		s.SetWithTTL([]byte(fmt.Sprintf("k%d", i)), []byte("v"), time.Minute)
	}

	clk.Advance(2 * time.Minute)

	// Well inside the tick interval, so reaching zero here proves the sweeper
	// repeated rather than waiting for several ticks.
	waitForSweep(t, s, 0)
}

// TestSweeperLeavesKeysWithoutExpiryAlone is the negative case: a keyspace of
// persistent keys must be untouched by active expiration.
func TestSweeperLeavesKeysWithoutExpiryAlone(t *testing.T) {
	s, clk := newTestStore(t)

	const keys = 100
	for i := 0; i < keys; i++ {
		s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}

	// Give the sweeper several tick intervals.
	clk.Advance(time.Hour)
	time.Sleep(20 * time.Millisecond)

	if got := s.Len(); got != keys {
		t.Fatalf("Len = %d after sweeping a keyless-expiry store, want %d", got, keys)
	}
}

// TestSweepRoundRepeatRule pins the repeat condition at its boundary.
//
// It exists because the condition changed: it was
// `ratio > threshold || deleted == len(sample)`, and the second clause was
// provably dead, since a fully dead sample has a ratio of exactly 1.0 and
// 1.0 > 0.25. Removing a clause from a loop-controlling predicate deserves a
// test that would fail if the behaviour it guarded were lost, rather than only
// the confidence that the arithmetic worked out.
//
// sweepRound is called directly, so nothing here races the background sweeper
// or depends on how long a tick took.
//
// The store holds at most sweepSampleSize keys in each case, so the sample is
// every expiring key rather than a random subset. That is what makes the
// ratio deterministic: with fewer expiring keys than the sample cap, the
// sample is exact and dead/total is the ratio the method computes.
func TestSweepRoundRepeatRule(t *testing.T) {
	tests := []struct {
		name  string
		total int // keys with an expiry, at most sweepSampleSize
		dead  int // how many of them have already expired
		want  bool
	}{
		{"no keys at all", 0, 0, false},
		{"no expiring keys are dead", 4, 0, false},
		{"exactly a quarter dead does not repeat", 4, 1, false},
		{"just over a quarter dead repeats", 4, 2, true},
		{"half dead repeats", 4, 2, true},
		{"three quarters dead repeats", 4, 3, true},
		{"every key dead repeats", 4, 4, true},
		// The case the removed clause was written for: a sample capped at
		// sweepSampleSize, entirely dead. The ratio is exactly 1.0 and the
		// threshold test alone must carry it.
		{"a full sample, every key dead", sweepSampleSize, sweepSampleSize, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := NewFakeClock()
			s := newSweeperlessStore(clk)
			t.Cleanup(s.Close)

			// Lay down every key with an expiry, then age the dead ones only.
			// Two advances are used so the live keys are written after the
			// first one and keep a full hour from the current fake time.
			for i := 0; i < tt.dead; i++ {
				s.SetWithTTL([]byte(fmt.Sprintf("dead-%d", i)), []byte("v"), time.Minute)
			}
			clk.Advance(2 * time.Minute)
			for i := 0; i < tt.total-tt.dead; i++ {
				s.SetWithTTL([]byte(fmt.Sprintf("live-%d", i)), []byte("v"), time.Hour)
			}

			// The sample is taken from keys with an expiry in map-iteration
			// order, which is randomized. When the ratio is unambiguous the
			// order cannot matter and the outcome is asserted directly; when
			// a partly dead sample could contain either group, the example is
			// chosen so the answer holds whatever the order.
			got := s.sweepRound()
			if got != tt.want {
				dead := s.Len() // every key was sampled, so all that remain are live ones
				t.Fatalf("sweepRound with %d of %d expiring keys dead = %v, want %v (%d keys remain)",
					tt.dead, tt.total, got, tt.want, dead)
			}

			// Every expired key must have been removed, whatever the verdict.
			for i := 0; i < tt.dead; i++ {
				if s.Exists([]byte(fmt.Sprintf("dead-%d", i))) {
					t.Fatalf("dead-%d survived the round", i)
				}
			}
		})
	}
}

// TestSweeperDoesNotRemoveRenewedKey exercises the race that the re-check
// under the write lock exists for.
//
// The sequence is the one that matters: a key expires, and is given a fresh
// TTL before the sweeper gets round to removing it. A sweeper that acted on
// the expiry it observed when sampling — rather than re-checking at the moment
// of deletion — would delete a live key.
//
// The sweep interval here is long enough that the sweeper cannot run between
// the expiry and the renewal, so the renewal reliably wins. That makes the
// window deterministic instead of a race the test would only sometimes probe.
func TestSweeperDoesNotRemoveRenewedKey(t *testing.T) {
	s, clk := newSweeperlessStorePair(t)

	s.SetWithTTL([]byte("k"), []byte("v"), time.Minute)

	// The key is now logically expired but still present in the map, and no
	// sweeper can have run to remove it.
	clk.Advance(2 * time.Minute)

	// Renew it. Expire must succeed: nothing has removed it yet, and liveness
	// is judged by the renewed deadline from here on.
	if !s.Expire([]byte("k"), time.Hour) {
		t.Fatal("Expire refused to renew a key that was still present in the map")
	}

	// It must now be live, and stay live.
	if !s.Exists([]byte("k")) {
		t.Fatal("the renewed key is still considered expired")
	}
	if got := s.TTL([]byte("k")); got != time.Hour {
		t.Fatalf("TTL after renewal = %v, want %v", got, time.Hour)
	}

	clk.Advance(time.Minute)
	if !s.Exists([]byte("k")) {
		t.Fatal("the renewed key expired early")
	}
}

// TestCloseStopsSweeper asserts the goroutine actually exits, rather than
// Close merely signalling and returning.
func TestCloseStopsSweeper(t *testing.T) {
	clk := NewFakeClock()
	s := New(clk)

	s.Close()

	select {
	case <-s.stopped:
		// The sweeper is gone.
	default:
		t.Fatal("Close returned while the sweeper goroutine was still running")
	}
}

// TestCloseIsIdempotent checks that a second Close is a no-op rather than a
// panic on a double channel close, and that it still returns promptly.
func TestCloseIsIdempotent(t *testing.T) {
	s := New(NewFakeClock())

	s.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Close()
		s.Close()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a repeated Close blocked")
	}
}

// TestCloseDoesNotClearTheMap checks the documented behaviour: Close stops the
// sweeper and leaves the data alone.
func TestCloseDoesNotClearTheMap(t *testing.T) {
	s := New(NewFakeClock())
	s.Set([]byte("k"), []byte("v"))

	s.Close()

	if !s.Exists([]byte("k")) {
		t.Fatal("Close removed a key")
	}
	if got, found := s.Get([]byte("k")); !found || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("after Close, Get = %q, %v; want %q, true", got, found, "v")
	}
}

// --- Concurrency ------------------------------------------------------------

// TestConcurrentSetExpireGet mixes writes, expiries and reads across many
// goroutines. Under -race this is where a lock held too narrowly, a map read
// outside the critical section, or an upgrade performed without re-checking
// shows up.
func TestConcurrentSetExpireGet(t *testing.T) {
	s, clk := newTestStore(t)

	const goroutines = 16
	const iterations = 500

	var wg sync.WaitGroup
	// One goroutine advances the clock while the others work, so expiry
	// boundaries are crossed continuously rather than only at the end.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			clk.Advance(time.Millisecond)
		}
	}()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			key := []byte(fmt.Sprintf("key-%d", g%4))

			for i := 0; i < iterations; i++ {
				switch i % 5 {
				case 0:
					s.Set(key, []byte("persistent"))
				case 1:
					s.SetWithTTL(key, []byte("temporary"), time.Duration(i%50)*time.Millisecond)
				case 2:
					_, _ = s.Get(key)
				case 3:
					s.Expire(key, time.Duration(i%30)*time.Millisecond)
				default:
					_ = s.Exists(key)
					_ = s.TTL(key)
					_ = s.Persist(key)
				}
			}
		}()
	}
	wg.Wait()
}

// TestExpireDuringConcurrentRead has readers hammering one key while the clock
// crosses its deadline and other goroutines renew it. A torn read, or a delete
// that ignores a concurrent renewal, surfaces here.
func TestExpireDuringConcurrentRead(t *testing.T) {
	s, clk := newTestStore(t)

	const readers = 8
	const iterations = 500

	key := []byte("contended")
	s.SetWithTTL(key, []byte("value"), time.Millisecond)

	var wg sync.WaitGroup

	// The clock crosses the deadline repeatedly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			clk.Advance(time.Millisecond)
		}
	}()

	// A renewer keeps bringing the key back with a fresh value.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			s.SetWithTTL(key, []byte("renewed"), time.Second)
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if value, found := s.Get(key); found {
					// Whatever comes back must be one of the two values ever
					// written, never a mixture of them.
					if !bytes.Equal(value, []byte("value")) && !bytes.Equal(value, []byte("renewed")) {
						t.Errorf("Get returned %q, which was never written", value)
						return
					}
				}
				_ = s.Exists(key)
				_ = s.TTL(key)
			}
		}()
	}

	wg.Wait()
}

// TestConcurrentExpireAndPersist hammers the two methods that mutate an
// entry's expiry, on the same key, from many goroutines.
func TestConcurrentExpireAndPersist(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set([]byte("k"), []byte("v"))

	const goroutines = 8
	const iterations = 500

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if i%2 == 0 {
					s.Expire([]byte("k"), time.Hour)
				} else {
					s.Persist([]byte("k"))
				}
				_ = s.TTL([]byte("k"))
			}
		}()
	}
	wg.Wait()

	// The key must still be there and readable; which expiry it ended up with
	// depends on the interleaving and is not asserted.
	if _, found := s.Get([]byte("k")); !found {
		t.Fatal("the key vanished under concurrent Expire and Persist")
	}
}
