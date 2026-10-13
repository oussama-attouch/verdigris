package store

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/rdb"
)

// countingClock wraps a Clock and records how many times Now is called.
//
// It exists because "Snapshot reads the clock once per call" is a statement
// about the *number of calls*, and the honest way to check it is to count
// them. Inferring it from which keys survive does not work: a clock that
// advances on every read advances for the writes too, so by the time the
// snapshot runs, a deadline chosen relative to "now" has often already passed
// for reasons that have nothing to do with how many times Snapshot read. The
// first version of this test made exactly that mistake and failed for it.
type countingClock struct {
	inner Clock
	mu    sync.Mutex
	reads int
}

func (c *countingClock) Now() time.Time {
	c.mu.Lock()
	c.reads++
	c.mu.Unlock()
	return c.inner.Now()
}

// Reads returns how many times Now has been called.
func (c *countingClock) Reads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// newSnapshotStore returns a store with a fake clock and a sweeper that cannot
// fire during the test, so that what is in the map is decided entirely by the
// test rather than by a background goroutine racing the assertions.
func newSnapshotStore(t *testing.T) (*Store, *FakeClock) {
	t.Helper()
	return newSweeperlessStorePair(t)
}

// TestSnapshotOmitsExpired is the central snapshot rule: what comes back is
// what is live, and what is not live is absent.
func TestSnapshotOmitsExpired(t *testing.T) {
	s, clk := newSnapshotStore(t)

	s.Set([]byte("persistent"), []byte("forever"))
	s.SetWithTTL([]byte("short"), []byte("gone soon"), time.Minute)
	s.SetWithTTL([]byte("long"), []byte("still here"), time.Hour)

	// Nothing has expired yet, so everything is in the snapshot.
	if got := len(s.Snapshot()); got != 3 {
		t.Fatalf("before any expiry, snapshot holds %d entries, want 3", got)
	}

	clk.Advance(5 * time.Minute)

	entries := s.Snapshot()

	byKey := make(map[string]rdb.Entry, len(entries))
	for _, e := range entries {
		byKey[string(e.Key)] = e
	}

	if len(entries) != 2 {
		t.Fatalf("snapshot holds %d entries, want 2 (the expired key must be omitted); got %v", len(entries), keysOf(entries))
	}
	if _, ok := byKey["short"]; ok {
		t.Fatal("the expired key is in the snapshot")
	}
	if _, ok := byKey["persistent"]; !ok {
		t.Fatal("the persistent key is missing from the snapshot")
	}
	if _, ok := byKey["long"]; !ok {
		t.Fatal("the long-lived key is missing from the snapshot")
	}

	// The expiry travels with the entry, unchanged.
	if !byKey["long"].ExpiresAt.Equal(clk.Now().Add(time.Hour - 5*time.Minute)) {
		t.Fatalf("long's expiry = %v, want it preserved", byKey["long"].ExpiresAt)
	}
	// A key with no expiry keeps the zero time, which is how the file records
	// "no expiry" rather than "expired long ago".
	if !byKey["persistent"].ExpiresAt.IsZero() {
		t.Fatalf("persistent's expiry = %v, want the zero time", byKey["persistent"].ExpiresAt)
	}
}

// countingClock wraps a Clock and records how many times Now is called.
//
// It exists because "Snapshot reads the clock once per call" is a statement
// TestSnapshotReadsTheClockOncePerCall is the direct form of the invariant.
//
// Snapshot must judge every entry against one instant. Reading the clock per
// entry would mean an entry's fate depended on how many entries the map
// happened to yield before it, making the snapshot non-deterministic — and the
// map's iteration order is deliberately randomized by Go, so the result would
// change from run to run with no change in the data.
//
// Counting the reads is exact where inferring from surviving keys is not.
func TestSnapshotReadsTheClockOncePerCall(t *testing.T) {
	clk := &countingClock{inner: NewFakeClock()}
	s := NewWithInterval(clk, time.Hour) // sweeper must not add reads
	t.Cleanup(s.Close)

	// A mixture of entries with and without expiries, so the filter has work
	// to do and any per-entry read would be visible.
	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			s.Set([]byte(fmt.Sprintf("plain-%d", i)), []byte("v"))
		} else {
			s.SetWithTTL([]byte(fmt.Sprintf("timed-%d", i)), []byte("v"), time.Hour)
		}
	}

	before := clk.Reads()
	entries := s.Snapshot()
	after := clk.Reads()

	if len(entries) != 40 {
		t.Fatalf("snapshot holds %d entries, want 40", len(entries))
	}
	if got := after - before; got != 1 {
		t.Fatalf("Snapshot read the clock %d times for 40 entries, want exactly 1", got)
	}
}

// TestCountReadsTheClockOncePerCall is the same check for Count, which has the
// same filter and the same reason to read once.
func TestCountReadsTheClockOncePerCall(t *testing.T) {
	clk := &countingClock{inner: NewFakeClock()}
	s := NewWithInterval(clk, time.Hour)
	t.Cleanup(s.Close)

	for i := 0; i < 40; i++ {
		s.SetWithTTL([]byte(fmt.Sprintf("k%d", i)), []byte("v"), time.Hour)
	}

	before := clk.Reads()
	if got := s.Count(); got != 40 {
		t.Fatalf("Count = %d, want 40", got)
	}
	if got := clk.Reads() - before; got != 1 {
		t.Fatalf("Count read the clock %d times for 40 entries, want exactly 1", got)
	}
}

// TestSnapshotIsStableAcrossRepeatedCalls checks that repeating the snapshot
// against a frozen clock returns the same set every time.
//
// The deadlines straddle the current instant, so the snapshot must keep some
// entries and drop others. Which are which is fixed by the data alone, so an
// implementation whose answer also depended on map iteration order would
// disagree with itself between calls.
func TestSnapshotIsStableAcrossRepeatedCalls(t *testing.T) {
	s, clk := newSnapshotStore(t)

	// Offsets from -20 to +19 minutes around the frozen instant. The twenty
	// with negative offsets have already expired; the twenty with positive
	// ones have not.
	for i := 0; i < 40; i++ {
		offset := time.Duration(i-20) * time.Minute
		s.SetWithTTL([]byte(fmt.Sprintf("k%02d", i)), []byte("v"), offset)
	}

	first := s.Snapshot()
	if len(first) != 19 {
		// Offsets -20..-1 are expired (20 keys) and 0..19 are live, but an
		// offset of zero produces a deadline equal to now, which the store
		// treats as expired. So 19 keys remain, and the count is exact.
		t.Fatalf("snapshot holds %d entries, want 19 (the twenty past deadlines plus the one at exactly now are expired)", len(first))
	}

	for i := 0; i < 5; i++ {
		if got := len(s.Snapshot()); got != 19 {
			t.Fatalf("snapshot %d holds %d entries, want 19 every time", i+1, got)
		}
	}

	// Moving the clock past ten more deadlines must drop exactly those ten.
	clk.Advance(10 * time.Minute)
	if got := len(s.Snapshot()); got != 9 {
		t.Fatalf("after advancing ten minutes, snapshot holds %d entries, want 9", got)
	}
}

// TestSnapshotCopiesKeyAndValue checks the ownership rule for snapshots.
//
// A snapshot the caller can mutate into the live keyspace — or that changes
// when the keyspace does — would break the promise every other accessor makes.
func TestSnapshotCopiesKeyAndValue(t *testing.T) {
	s, _ := newSnapshotStore(t)

	s.Set([]byte("k"), []byte("original"))

	entries := s.Snapshot()
	if len(entries) != 1 {
		t.Fatalf("snapshot holds %d entries, want 1", len(entries))
	}

	// Mutating the snapshot must not reach the store.
	copy(entries[0].Value, "MUTATED!")
	copy(entries[0].Key, "MUTATED-KEY")

	got, found := s.Get([]byte("k"))
	if !found {
		t.Fatal("the key vanished after its snapshot was mutated")
	}
	if !bytes.Equal(got, []byte("original")) {
		t.Fatalf("store value = %q after mutating a snapshot, want %q", got, "original")
	}
	if s.Exists([]byte("MUTATED-KEY")) {
		t.Fatal("mutating a snapshot renamed a live key")
	}
}

// TestSnapshotPreservesNilValue checks that a key holding a nil value keeps it
// through the snapshot, because the file format and the RESP layer both
// distinguish nil from empty.
func TestSnapshotPreservesNilValue(t *testing.T) {
	s, _ := newSnapshotStore(t)

	s.Set([]byte("nil"), nil)
	s.Set([]byte("empty"), []byte{})

	byKey := make(map[string]rdb.Entry)
	for _, e := range s.Snapshot() {
		byKey[string(e.Key)] = e
	}

	if byKey["nil"].Value != nil {
		t.Fatalf("a nil value became %#v in the snapshot", byKey["nil"].Value)
	}
	if byKey["empty"].Value == nil {
		t.Fatal("an empty value became nil in the snapshot")
	}
	if len(byKey["empty"].Value) != 0 {
		t.Fatalf("an empty value became %q", byKey["empty"].Value)
	}
}

// TestSnapshotIsConsistentUnderConcurrentWrites runs snapshots against a store
// being written to. Under -race this is where an unsynchronised read of the
// map shows up; the value assertions check that a snapshot never returns a
// torn entry.
func TestSnapshotIsConsistentUnderConcurrentWrites(t *testing.T) {
	s := newSweeperlessStore(NewFakeClock())
	t.Cleanup(s.Close)

	const writers = 4
	const snapshots = 50

	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.Set([]byte(fmt.Sprintf("k%d", i%20)), []byte("value"))
				s.SetWithTTL([]byte(fmt.Sprintf("t%d", i%20)), []byte("temp"), time.Hour)
				s.Delete([]byte(fmt.Sprintf("k%d", (i+7)%20)))
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < snapshots; i++ {
			for _, e := range s.Snapshot() {
				// Every value ever written is the same string, so any other
				// content means a torn read.
				if !bytes.Equal(e.Value, []byte("value")) && !bytes.Equal(e.Value, []byte("temp")) {
					t.Errorf("snapshot returned a value never written: %q", e.Value)
					return
				}
				if len(e.Key) == 0 {
					t.Error("snapshot returned an empty key, which was never written")
					return
				}
			}
		}
	}()

	wg.Wait()
}

// --- Load -------------------------------------------------------------------

func TestLoadRestoresEntries(t *testing.T) {
	source, clk := newSnapshotStore(t)

	source.Set([]byte("a"), []byte("1"))
	source.Set([]byte("b"), []byte("two"))
	source.SetWithTTL([]byte("c"), []byte("three"), time.Hour)
	source.SetWithTTL([]byte("expired"), []byte("gone"), time.Minute)

	clk.Advance(5 * time.Minute)

	entries := source.Snapshot()

	target, _ := newSnapshotStore(t)
	unlock := target.Load(entries)
	unlock()

	if got := target.Len(); got != 3 {
		t.Fatalf("loaded store holds %d keys, want 3", got)
	}
	for key, want := range map[string]string{"a": "1", "b": "two", "c": "three"} {
		got, found := target.Get([]byte(key))
		if !found {
			t.Fatalf("key %q was not restored", key)
		}
		if string(got) != want {
			t.Fatalf("key %q = %q, want %q", key, got, want)
		}
	}
}

// TestLoadPreservesExpiry checks the deadline survives the round trip as an
// absolute instant, not as a duration restarted at load time.
//
// The loading store's clock is advanced to the same wall-clock moment as the
// saving store's, because the expiry is an absolute time and a store whose
// clock reads 12:00 should indeed consider a 13:00 deadline to be an hour
// away. Re-basing the deadline to load-time-plus-the-original-duration would
// be the bug this rules out: the key would get a fresh hour instead of the ten
// minutes it had left.
func TestLoadPreservesExpiry(t *testing.T) {
	source, clk := newSnapshotStore(t)

	source.SetWithTTL([]byte("k"), []byte("v"), time.Hour)
	clk.Advance(50 * time.Minute)

	entries := source.Snapshot()

	target, targetClk := newSnapshotStore(t)

	// Put the two clocks at the same instant. Both stores start from the same
	// fixed date, so the elapsed amount is what the source advanced by.
	targetClk.Advance(50 * time.Minute)

	unlock := target.Load(entries)
	unlock()

	remaining := target.TTL([]byte("k"))
	want := 10 * time.Minute
	if remaining != want {
		t.Fatalf("remaining TTL after load = %v, want %v", remaining, want)
	}

	// And it expires on schedule rather than being extended.
	targetClk.Advance(want + time.Second)
	if target.Exists([]byte("k")) {
		t.Fatal("the loaded key outlived its original deadline")
	}
}

// TestLoadInsertsAlreadyExpiredEntries pins the documented choice: an entry
// whose deadline has passed is loaded anyway, with its original time, and
// passive deletion removes it on first access.
//
// Dropping it during load would be the alternative, and it would mean the key
// silently disappears from every later snapshot — the file would lose data
// that was correct when it was written.
func TestLoadInsertsAlreadyExpiredEntries(t *testing.T) {
	clk := NewFakeClock()
	s := newSweeperlessStore(clk)
	t.Cleanup(s.Close)

	past := clk.Now().Add(-time.Hour)
	unlock := s.Load([]rdb.Entry{
		{Key: []byte("stale"), Value: []byte("v"), ExpiresAt: past},
	})
	unlock()

	// It is in the map — Len counts it, matching DBSIZE's treatment of
	// not-yet-collected keys.
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1: the entry is loaded with its original deadline", s.Len())
	}

	// But it is not visible, and reading it removes it.
	if s.Exists([]byte("stale")) {
		t.Fatal("an already-expired loaded key reported as present")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d after the passive delete, want 0", s.Len())
	}
}

// TestLoadDoesNotClearExisting checks that Load merges rather than replaces.
func TestLoadDoesNotClearExisting(t *testing.T) {
	s, _ := newSnapshotStore(t)

	s.Set([]byte("existing"), []byte("here"))

	unlock := s.Load([]rdb.Entry{
		{Key: []byte("loaded"), Value: []byte("there")},
	})
	unlock()

	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d after loading into a non-empty store, want 2", got)
	}
	for _, key := range []string{"existing", "loaded"} {
		if !s.Exists([]byte(key)) {
			t.Fatalf("key %q is missing after the load", key)
		}
	}
}

// TestLoadOverwritesByKey checks that a collision resolves the way a SET
// would: the incoming entry wins.
func TestLoadOverwritesByKey(t *testing.T) {
	s, _ := newSnapshotStore(t)

	s.Set([]byte("k"), []byte("old"))

	unlock := s.Load([]rdb.Entry{{Key: []byte("k"), Value: []byte("new")}})
	unlock()

	got, found := s.Get([]byte("k"))
	if !found || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("Get(k) = %q, %v; want %q, true", got, found, "new")
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
}

// TestLoadCopiesEntries checks the ownership rule holds on the way in too: a
// caller that mutates the slice it passed to Load must not change the store.
func TestLoadCopiesEntries(t *testing.T) {
	s, _ := newSnapshotStore(t)

	key := []byte("key")
	value := []byte("value")

	unlock := s.Load([]rdb.Entry{{Key: key, Value: value}})
	unlock()

	copy(key, "XXX")
	copy(value, "YYY")

	got, found := s.Get([]byte("key"))
	if !found {
		t.Fatal("the key vanished after its source slice was mutated")
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("store value = %q after mutating the source, want %q", got, "value")
	}
}

// TestLoadRoundTripsThroughTheStore is the end-to-end property at the store
// level, without a file: snapshot, load into a fresh store, snapshot again, and
// the two snapshots must describe the same keyspace.
func TestLoadRoundTripsThroughTheStore(t *testing.T) {
	source, clk := newSnapshotStore(t)

	for i := 0; i < 25; i++ {
		source.Set([]byte(fmt.Sprintf("plain-%d", i)), []byte(fmt.Sprintf("v%d", i)))
		source.SetWithTTL([]byte(fmt.Sprintf("timed-%d", i)), []byte(fmt.Sprintf("v%d", i)), time.Hour)
	}
	source.Set([]byte("nil-value"), nil)

	first := source.Snapshot()

	target, _ := newSnapshotStore(t)
	unlock := target.Load(first)
	unlock()

	second := target.Snapshot()

	if len(first) != len(second) {
		t.Fatalf("round trip changed the entry count: %d then %d", len(first), len(second))
	}

	byKey := make(map[string]rdb.Entry, len(second))
	for _, e := range second {
		byKey[string(e.Key)] = e
	}
	for _, e := range first {
		g, ok := byKey[string(e.Key)]
		if !ok {
			t.Fatalf("entry %q did not survive the round trip", e.Key)
		}
		if !bytes.Equal(g.Value, e.Value) {
			t.Fatalf("entry %q value changed: %q then %q", e.Key, e.Value, g.Value)
		}
		if (g.Value == nil) != (e.Value == nil) {
			t.Fatalf("entry %q nil-ness changed", e.Key)
		}
		if !g.ExpiresAt.Equal(e.ExpiresAt) {
			t.Fatalf("entry %q expiry changed: %v then %v", e.Key, e.ExpiresAt, g.ExpiresAt)
		}
	}

	// The clock is irrelevant to the assertions above, but advancing it keeps
	// the test honest about which entries were live.
	clk.Advance(time.Minute)
}

// keysOf is a test-formatting helper.
func keysOf(entries []rdb.Entry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, string(e.Key))
	}
	return out
}
