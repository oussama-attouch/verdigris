package store

import "time"

// Clock reports the current time. The store calls this instead of time.Now
// so that TTL behaviour is testable without sleeping.
//
// A single method is the whole interface on purpose. Expiration needs to know
// what time it is and nothing else — no timers, no scheduling — so a test
// double can be three lines long and every TTL test in this package is
// deterministic and instant rather than sleeping for real seconds.
type Clock interface {
	Now() time.Time
}

// RealClock is the production Clock. It calls time.Now.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }
