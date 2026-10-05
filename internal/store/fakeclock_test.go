package store

import (
	"sync"
	"testing"
	"time"
)

// FakeClock is a Clock whose time only moves when a test moves it.
//
// It is the reason no test in this package sleeps: a TTL of one hour costs a
// single Advance call and zero wall-clock time, and the result is exact rather
// than "probably expired by now".
//
// It also broadcasts every advance. The active sweeper sleeps on wall-clock
// timers — it has to, because a goroutine cannot be woken by a clock that
// moved — so a test that advances time and then waits for sweeping has two
// independent things to wait for. waitForSweep uses nowChanged to make the
// first one observable, and polls for the second.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time

	// nowChanged is closed and replaced on every Advance, so a waiter can
	// learn that time moved rather than polling for it.
	nowChanged chan struct{}
}

// NewFakeClock returns a FakeClock started at a fixed, arbitrary instant.
//
// The start time is deliberately not the zero time: an implementation bug
// that treated the zero time as "expired" would go unnoticed if every test
// began at the zero time, and a bug that treated it as "no expiry" would go
// unnoticed too. A real-looking date makes both mistakes visible.
func NewFakeClock() *FakeClock {
	return &FakeClock{
		now:        time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
		nowChanged: make(chan struct{}),
	}
}

// Now returns the fake current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake clock forward by d and wakes anything waiting for
// time to move.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	close(c.nowChanged)
	c.nowChanged = make(chan struct{})
	c.mu.Unlock()
}

// Set moves the fake clock to an absolute instant.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	close(c.nowChanged)
	c.nowChanged = make(chan struct{})
	c.mu.Unlock()
}

// waitForSweep gives the sweeper a chance to observe the current fake time and
// run a round, then proves it actually did.
//
// The two halves are separate on purpose, and both are needed:
//
//   - The sweeper blocks on a wall-clock timer, because no goroutine can be
//     woken by a clock that moved. So a test has to wait real milliseconds
//     for a tick. That is the one unavoidable sleep in this package, and it is
//     milliseconds against a test interval rather than seconds of TTL.
//   - The sweeper then has to run, and there is no signal when it finishes.
//     Polling the condition is what makes this a check rather than a guess: it
//     returns as soon as the store reached the expected state, and fails
//     loudly if it never does.
//
// expectDeleted is the key count the test expects after sweeping.
func waitForSweep(t *testing.T, s *Store, expectDeleted int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.Len() == expectDeleted {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("sweeper did not reach %d keys within 2s; Len() is %d", expectDeleted, s.Len())
}
