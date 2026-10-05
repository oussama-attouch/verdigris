package store

import (
	"time"
)

// sweeper fields. The values are Redis's own: a small sample per tick, with
// repetition while the sample is mostly dead.
const (
	// sweepSampleSize is how many keys with an expiry one round inspects.
	sweepSampleSize = 20

	// sweepRepeatThreshold is the fraction of a sample that must be expired
	// before the sweeper immediately runs another round instead of waiting
	// for the next tick. Redis uses 25%.
	sweepRepeatThreshold = 0.25
)

// DefaultSweepInterval is how often the active expiration sweeper runs when
// nothing overrides it.
const DefaultSweepInterval = 100 * time.Millisecond

// sweepInterval is how long the active expiration sweeper waits between
// rounds.
//
// This is a var rather than a const on purpose: tests override it to make
// active expiration observable without sleeping for a realistic interval.
//
// Because it is package-level mutable state, the rules for changing it after
// a sweeper has started are load-bearing:
//
//   - Set it before constructing a Store, or through SetSweepInterval, which
//     is documented for the same reason.
//   - Never mutate it while a sweeper goroutine is running: the sweeper reads
//     it from its own goroutine, so an unsynchronised write is a data race
//     that -race will report, not merely a surprise.
//
// The tests in this package change it once at the top of a test and restore
// it with t.Cleanup, which keeps the mutation outside any sweeper's lifetime
// in practice.
var sweepInterval = DefaultSweepInterval

// SetSweepInterval sets how often newly started sweepers run. A non-positive
// value restores the default.
//
// It exists so that a caller outside this package — cmd/verdigris, honouring
// a flag — can configure the interval without reaching into a package
// variable. Like the variable it writes, it is intended to be called during
// setup: changing the interval while sweepers are running races with their
// reads of it.
func SetSweepInterval(d time.Duration) {
	if d <= 0 {
		d = DefaultSweepInterval
	}
	sweepInterval = d
}

// sweep is the active deletion loop. It runs until stop is closed, then
// closes stopped so that Close can block on the goroutine actually finishing
// rather than assuming it has.
func (s *Store) sweep() {
	defer close(s.stopped)

	for {
		select {
		case <-s.stop:
			return
		case <-time.After(s.interval):
		}

		// One round may be followed immediately by another when the sample
		// was mostly expired, so a store full of dead keys drains in one
		// tick rather than at sweepSampleSize keys per tick. The loop is
		// bounded because a round that finds nothing expired stops it, and
		// therefore cannot spin: an empty store fails the repeat condition
		// immediately.
		for s.sweepRound() {
			select {
			case <-s.stop:
				return
			default:
			}
		}
	}
}

// sweepRound samples keys with an expiry, deletes the expired ones, and
// reports whether another round should run immediately.
//
// The design point is that it samples rather than scans. A full scan on every
// tick would be O(N) per interval — with a thousand keys and a 100 ms tick,
// ten thousand key visits per second competing with the actual command rate,
// for the sole purpose of finding keys that are usually not there. Sampling
// bounds the work per tick regardless of how large the keyspace grows, at the
// cost of not noticing a given expired key until it happens to be sampled (or
// read, which passive deletion covers).
func (s *Store) sweepRound() bool {
	// No clock read here. The sample only decides which keys to look at, and
	// whether each one is expired is decided later under the write lock by
	// deleteIfExpired — reading the clock twice would only widen the window
	// between observing an expiry and acting on it.

	// Collect a sample under a read lock, then release it before deleting.
	// Deleting while holding only a read lock is impossible, and holding the
	// write lock while choosing the sample would block every reader for the
	// duration of the choice.
	//
	// The sample is taken by ranging the map and stopping early. Go
	// randomizes map iteration order, so successive rounds look at different
	// keys without the store having to maintain a shuffle or an index.
	sample := make([]string, 0, sweepSampleSize)

	s.mu.RLock()
	for k, e := range s.data {
		if e.expiresAt.IsZero() {
			continue
		}
		sample = append(sample, k)
		if len(sample) == sweepSampleSize {
			// Stop at the sample size: this is the bound that makes the
			// sweeper O(1) in the size of the keyspace rather than O(N).
			break
		}
	}
	s.mu.RUnlock()

	if len(sample) == 0 {
		return false
	}

	deleted := 0
	for _, k := range sample {
		if s.deleteIfExpired(k) {
			deleted++
		}
	}

	// Repeat when the sample was mostly expired, which is the signal that
	// there is a backlog worth working through now.
	//
	// The `deleted == len(sample)` clause catches the case the ratio alone
	// cannot: when every sampled key was dead, the sample was capped at
	// sweepSampleSize and may have been the first twenty of thousands, so
	// stopping here would drain the keyspace at twenty keys per tick no
	// matter how urgently it needed draining.
	ratio := float64(deleted) / float64(len(sample))
	return ratio > sweepRepeatThreshold || deleted == len(sample)
}

// deleteIfExpired removes k if it is still expired, returning whether it
// removed anything.
//
// The re-check under the write lock is required, not defensive: the key may
// have been overwritten or renewed with a fresh TTL between the sample being
// taken and this call, and deleting on the strength of the earlier
// observation would throw away live data. The same guard is what lets the
// passive-deletion paths in Get and Exists use this method safely.
func (s *Store) deleteIfExpired(k string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, found := s.data[k]
	if !found || !e.expired(s.clk.Now()) {
		return false
	}
	delete(s.data, k)
	return true
}
