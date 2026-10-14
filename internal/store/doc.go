// Package store holds the in-memory keyspace: the map from keys to entries,
// the expiry bookkeeping that removes keys over time, and the concurrency
// discipline that keeps both correct under simultaneous access from every
// connection goroutine.
//
// This package is responsible for data and for the invariants that protect
// it under concurrent access. It is NOT responsible for the network layer,
// for RESP wire formats, or for command syntax and arity checking — the
// store exposes operations on keys, and the command layer decides which of
// them a given client request maps onto. It also does not own persistence
// formats; RDB and AOF will serialize the store, not live inside it.
//
// The central invariant is that the store owns its memory. Set and SetWithTTL
// copy both the key and the value they are given, and Get hands back a fresh
// copy rather than a view into the stored bytes. That single rule is what
// makes the rest of the server safe without every handler having to reason
// about buffer lifetimes: a handler may pass slices that alias the
// connection's read buffer straight to Set, and a handler may keep whatever
// Get returned for as long as it likes. Neither can corrupt the keyspace, and
// neither can be corrupted by a later command.
//
// The cost is one allocation per argument on write and one per value on read.
// That is a deliberate trade of throughput for a contract that holds no
// matter how the read loop is rearranged later. If milestone 8's benchmarks
// show the copies are the bottleneck, the answer is an explicitly unsafe read
// path with a documented contract change, not a quiet removal of the copies.
//
// Keys are strings and values are byte slices. Strings are used because Go
// slice values are not comparable and so cannot be map keys; a []byte key is
// converted with string(key), which reinterprets rather than validates, so
// binary keys round-trip intact. Values stay []byte because they are
// arbitrary payloads: a bulk string may hold NUL, invalid UTF-8, and CRLF.
//
// # Time is injected
//
// The store never calls time.Now. New takes a Clock, and every expiry decision
// reads the current time from it. Production passes RealClock{}; tests pass a
// fake whose Now is a value the test advances. This is what makes TTL
// behaviour testable at all: without it, proving that a key with a one-hour
// TTL is gone would mean waiting an hour, and every test would be a sleep with
// a timing assumption attached.
//
// One consequence is worth knowing when reading the code: expiry comparisons
// are inclusive of the deadline (!now.Before(expiresAt)), so a write with a
// TTL of zero is expired at the instant it is written rather than a tick
// later.
//
// Keys follows the same rule as Snapshot: the clock is read exactly once per
// call and every entry is filtered against that single instant, so the answer
// describes one moment rather than a window that shifts as the loop runs.
//
// # Two deletion paths
//
// An expired key is removed in two ways, and both are needed.
//
// Passive deletion happens on access: Get, Exists, TTL and Expire notice an
// expired entry on the way past, remove it, and report the key as absent. It
// costs nothing extra and guarantees a reader never observes a key it
// considers dead. On its own it leaks: a key that is written and never read
// again would sit in the map forever.
//
// Active deletion is the sweeper, a background goroutine started by New and
// stopped by Close. On a ticker it samples a bounded number of keys that have
// an expiry and removes the expired ones, so a key nobody reads still
// disappears. It samples rather than scans — see sweeper.go for why that
// distinction is the whole design — which means a given expired key is not
// guaranteed to be removed promptly, only eventually, and only if it is not
// read first.
//
// # Data model
//
// The map is keyed by string and holds entry values. An entry carries the
// value bytes and an absolute expiry, where the zero time means "no expiry".
// The expiry is absolute rather than a countdown so that it needs no
// maintenance and cannot drift when the key is untouched.
//
// # Snapshots
//
// Snapshot returns the live keyspace as a slice of rdb.Entry, and Load inserts
// such a slice back. They are the store's only interface to persistence, and
// they are deliberately value-shaped: the store does not know that a file
// exists, which path it lives at, or when a save happens. That belongs to the
// server, which owns the lifecycle question of when a snapshot is taken.
//
// Two details of Snapshot are worth knowing.
//
// The clock is read exactly once per call, and that single instant decides
// every entry. Reading it per entry would mean an entry's fate depended on how
// many entries the map happened to yield before it — and Go deliberately
// randomizes map iteration order, so the same keyspace would produce different
// snapshots on consecutive calls.
//
// Expired entries are filtered during serialization rather than swept first.
// The sweeper samples twenty expiring keys per tick, so making the snapshot
// depend on it would mean waiting an unknown number of ticks or doing a full
// scan purely to delete, then scanning again to serialize. Serialization
// visits every entry anyway, so the check costs one comparison per entry
// against a time already in hand.
//
// Load preserves expiry times and does not clear what is already there, so a
// second Load merges rather than replacing. An entry whose deadline has
// already passed is loaded anyway: it will be removed by passive deletion on
// first access, exactly as it would have been had the server received the same
// commands without restarting. Dropping it during load would mean a key
// silently vanished from every later snapshot, losing data that was correct
// when it was written.
package store
