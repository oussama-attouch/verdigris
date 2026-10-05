package store

import "sync"

// Store is a concurrent-safe keyspace. A zero-value Store is not usable;
// always construct via New.
//
// The central invariant is that the store owns its memory. Once a byte slice
// enters Set, the store never aliases it; once a byte slice leaves Get, the
// caller owns it. That single rule is what lets the rest of the server pass
// connection buffers straight in without any handler having to reason about
// when its argument bytes might be overwritten.
type Store struct {
	// mu guards data. An RWMutex rather than a Mutex because reads vastly
	// outnumber writes in a cache-shaped workload, and concurrent readers do
	// not need to block each other. -race runs this package under concurrent
	// load precisely to check that the discipline holds.
	mu sync.RWMutex

	// data maps keys to values.
	//
	// The key type is string, not []byte, because Go slice values are not
	// comparable and therefore cannot be map keys. Converting a []byte key to
	// string for lookup costs nothing: Go strings are immutable byte
	// sequences, not text, so the conversion is a reinterpretation rather
	// than a UTF-8 validation or a copy. The name suggests text; nothing
	// enforces it, and binary keys round-trip intact.
	//
	// The value type is []byte because values are binary-safe payloads.
	//
	// There is deliberately no entry struct in this milestone. Values are raw
	// byte slices and there is no per-key metadata to carry. Milestone 5 adds
	// expiration and will need one, at which point `data map[string][]byte`
	// becomes `data map[string]entry` with a value and an expiry. Introducing
	// that struct now would add a level of indirection to every access, and
	// its shape would be a guess until the TTL design is settled.
	data map[string][]byte
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

// Set stores a copy of value under a copy of key, overwriting any existing
// value at that key. The store never retains references to the caller's
// byte slices.
//
// Copying on the way in is half of the ownership invariant. Without it, a
// handler could hand over a slice into the connection's read buffer, the loop
// could read the next command into that same memory, and the stored value
// would silently change underneath a later Get.
func (s *Store) Set(key, value []byte) {
	// The string(key) conversion copies the key bytes, so the map owns its
	// own key storage. It is done before taking the lock: the copy is
	// computation on caller-owned data and needs no protection, and keeping
	// it outside shortens the critical section.
	k := string(key)

	// A nil value stays nil, and an empty non-nil value stays empty non-nil.
	// Both are valid values; the distinction between them is the RESP
	// protocol's business (a null bulk string versus a zero-length one), and
	// Get reports which it was.
	var v []byte
	if value != nil {
		v = make([]byte, len(value))
		copy(v, value)
	}

	s.mu.Lock()
	s.data[k] = v
	s.mu.Unlock()
}

// Get returns a copy of the value at key, and whether the key exists.
// A missing key returns (nil, false). The returned slice is owned by the
// caller and may be mutated.
//
// Copying on the way out is the other half of the ownership invariant. The
// lock is released before the copy is handed back, so a caller that mutates
// what it received can never touch the stored bytes — which is exactly what
// makes a retained Get result safe to keep across further store activity.
func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	stored, found := s.data[string(key)]
	s.mu.RUnlock()

	if !found {
		return nil, false
	}
	if stored == nil {
		// The key exists and holds a nil value. The bool reports existence;
		// the nil reports which value it was.
		return nil, true
	}

	out := make([]byte, len(stored))
	copy(out, stored)
	return out, true
}

// Delete removes key, returning true if the key existed.
func (s *Store) Delete(key []byte) bool {
	k := string(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, found := s.data[k]; !found {
		return false
	}
	delete(s.data, k)
	return true
}

// Exists reports whether key exists.
//
// It is a separate operation rather than a wrapper over Get because it needs
// no copy: existence is a map lookup, and answering EXISTS should not
// allocate a value the caller is going to throw away.
func (s *Store) Exists(key []byte) bool {
	s.mu.RLock()
	_, found := s.data[string(key)]
	s.mu.RUnlock()
	return found
}

// Len returns the number of keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
