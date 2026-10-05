// Package store holds the in-memory keyspace: the map from keys to values,
// the value types those keys may hold, and the expiry bookkeeping that
// removes keys over time.
//
// This package is responsible for data and for the invariants that protect
// it under concurrent access. It is NOT responsible for the network layer,
// for RESP wire formats, or for command syntax and arity checking — the
// store exposes operations on keys, and the command layer decides which of
// them a given client request maps onto. It also does not own persistence
// formats; RDB and AOF will serialize the store, not live inside it.
//
// Milestone 4 will implement the keyspace and the core write/read/delete
// operations. Milestone 5 adds expiration. Until then, the package contains
// no exported symbols.
package store
