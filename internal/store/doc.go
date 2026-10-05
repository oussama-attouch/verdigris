// Package store holds the in-memory keyspace: the map from keys to values,
// and the concurrency discipline that keeps it correct under simultaneous
// access from every connection goroutine.
//
// This package is responsible for data and for the invariants that protect
// it under concurrent access. It is NOT responsible for the network layer,
// for RESP wire formats, or for command syntax and arity checking — the
// store exposes operations on keys, and the command layer decides which of
// them a given client request maps onto. It also does not own persistence
// formats; RDB and AOF will serialize the store, not live inside it.
//
// The central invariant is that the store owns its memory. Set copies both
// the key and the value it is given, and Get hands back a fresh copy rather
// than a view into the stored bytes. That single rule is what makes the rest
// of the server safe without every handler having to reason about buffer
// lifetimes: a handler may pass slices that alias the connection's read
// buffer straight to Set, and a handler may keep whatever Get returned for as
// long as it likes. Neither can corrupt the keyspace, and neither can be
// corrupted by a later command.
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
// Milestone 4 implements this keyspace and the four commands that exercise
// it. Milestone 5 adds expiration, which will introduce a per-key entry
// struct to carry an expiry alongside the value.
package store
