package store

import (
	"sync"
	"time"
)

// entry is one key's value and its expiry.
//
// The zero value of expiresAt means "no expiry", which is why the field is a
// time.Time rather than a pointer or a bool-plus-time pair: the zero Time is
// a usable sentinel that cannot accidentally read as "expired at the epoch",
// because IsZero is checked explicitly everywhere it matters.
type entry struct {
	value     []byte
	expiresAt time.Time
}

// expired reports whether this entry has an expiry that has passed.
//
// The comparison is `!now.Before(expiresAt)` rather than `now.After(expiresAt)`.
// The two differ at exactly one instant — when the deadline equals the current
// time — and that instant is reachable in practice: a write with a TTL of zero
// stores a deadline of now, and a clock that does not advance between the
// write and the next read (which is exactly what a fake clock does, and what
// a fast real clock can do within one tick) would find the key alive forever.
// Treating the deadline as inclusive makes "expires at T" mean T is the first
// instant at which the key is gone.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Store is a concurrent-safe keyspace. A zero-value Store is not usable;
// always construct via New.
//
// The central invariant is that the store owns its memory. Once a byte slice
// enters Set or SetWithTTL, the store never aliases it; once a byte slice
// leaves Get, the caller owns it. That single rule is what lets the rest of
// the server pass connection buffers straight in without any handler having
// to reason about when its argument bytes might be overwritten.
//
// Keys expire. Two mechanisms remove them: passive deletion, where Get,
// Exists, TTL and Expire notice an expired key on the way past and remove it,
// and active deletion, where a background sweeper samples keys on a ticker so
// that a key nobody reads still disappears. The two are complements — passive
// alone leaks memory for keys that are written and never read again, and
// active alone would let a read observe a key the caller considers dead.
type Store struct {
	// mu guards data. An RWMutex rather than a Mutex because reads vastly
	// outnumber writes in a cache-shaped workload, and concurrent readers do
	// not need to block each other. -race runs this package under concurrent
	// load precisely to check that the discipline holds.
	mu sync.RWMutex

	// data maps keys to entries.
	//
	// The key type is string, not []byte, because Go slice values are not
	// comparable and therefore cannot be map keys. Converting a []byte key to
	// string for lookup costs nothing: Go strings are immutable byte
	// sequences, not text, so the conversion is a reinterpretation rather
	// than a UTF-8 validation or a copy. The name suggests text; nothing
	// enforces it, and binary keys round-trip intact.
	data map[string]entry

	// clk is the store's only source of time. Nothing in this package calls
	// time.Now directly, which is what makes TTL tests deterministic.
	clk Clock

	// interval is the sweeper's tick period, captured from the package
	// variable when the store was constructed.
	//
	// It is copied rather than read per tick so that a test can set the
	// interval, build a store, and restore the variable immediately without
	// changing the behaviour of the sweeper it just started. Reading the
	// global on every tick would make the interval mutable from outside at
	// any moment, which is a data race in waiting and makes "restore what you
	// changed" impossible to honour.
	interval time.Duration

	// stop and stopped coordinate the active expiration sweeper. stop is
	// closed by Close; stopped is closed by the sweeper goroutine as it
	// exits, so a caller — or a test asserting the goroutine is gone — can
	// wait for it rather than guessing.
	stop    chan struct{}
	stopped chan struct{}
	once    sync.Once
}

// New returns an empty Store whose expiry checks read clk, and starts the
// background sweeper that performs active deletion.
//
// The sweeper runs until Close is called. A Store that is never closed leaks
// one goroutine, so a long-lived process that creates stores dynamically must
// pair every New with a Close. Tests that only care about single-threaded
// semantics may rely on process exit, but every test in this package that
// asserts anything about sweeping calls Close explicitly.
func New(clk Clock) *Store {
	if clk == nil {
		// A nil Clock would panic on the first TTL query, deep inside a
		// handler, with a stack trace that says nothing about the caller's
		// mistake. Defaulting to the real clock keeps a zero-argument-ish
		// construction working, and New(RealClock{}) is what production
		// passes explicitly.
		clk = RealClock{}
	}

	s := &Store{
		data:     make(map[string]entry),
		clk:      clk,
		interval: sweepInterval,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	go s.sweep()
	return s
}

// Close stops the active expiration sweeper. It does not clear the map.
//
// It is idempotent: a second call is a no-op rather than a panic on a double
// channel close. It blocks until the sweeper goroutine has actually exited,
// which is what lets a test assert that no goroutine was leaked.
func (s *Store) Close() {
	s.once.Do(func() { close(s.stop) })
	<-s.stopped
}

// Set stores a copy of value under a copy of key, overwriting any existing
// value at that key. Any existing expiry is cleared.
//
// Clearing the TTL is not an oversight: plain SET in Redis discards the TTL,
// so `SET k v EX 100` followed by `SET k v2` leaves k persistent. A SET that
// silently kept the old expiry would be a surprise in the opposite direction,
// and clients rely on this to turn a temporary key into a durable one.
func (s *Store) Set(key, value []byte) {
	s.store(key, value, time.Time{})
}

// SetWithTTL stores a copy of value under a copy of key with an expiry at
// clk.Now() + ttl, overwriting any existing value and expiry.
//
// The expiry is stored as an absolute time rather than a countdown, so it
// does not need to be decremented and cannot drift when the key is not
// touched.
func (s *Store) SetWithTTL(key, value []byte, ttl time.Duration) {
	s.store(key, value, s.clk.Now().Add(ttl))
}

// store is the shared body of Set and SetWithTTL. An expiresAt of the zero
// Time means no expiry.
func (s *Store) store(key, value []byte, expiresAt time.Time) {
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
	s.data[k] = entry{value: v, expiresAt: expiresAt}
	s.mu.Unlock()
}

// Get returns a copy of the value at key, and whether the key exists.
// A missing or expired key returns (nil, false). The returned slice is owned
// by the caller and may be mutated.
//
// An expired key found on the way past is deleted — passive deletion — so a
// key that is read after its expiry does not linger until the sweeper notices
// it.
func (s *Store) Get(key []byte) ([]byte, bool) {
	k := string(key)

	// A read-lock probe first, and an upgrade to the write lock only if this
	// key turns out to be expired. The alternative — always taking Lock —
	// would serialize every reader behind every other reader on the hot path,
	// which is most of what an RWMutex exists to avoid. The cost of probing
	// is that an expired key is looked up twice; the cost of not probing is
	// paid by every read of every live key.
	//
	// The upgrade has to re-check, not assume: between releasing the read
	// lock and acquiring the write lock, another goroutine may have replaced
	// the entry with a fresh one, and deleting on the strength of the earlier
	// observation would destroy live data.
	s.mu.RLock()
	e, found := s.data[k]
	expired := found && e.expired(s.clk.Now())
	s.mu.RUnlock()

	if !found {
		return nil, false
	}
	if expired {
		s.deleteIfExpired(k)
		return nil, false
	}

	if e.value == nil {
		// The key exists and holds a nil value. The bool reports existence;
		// the nil reports which value it was.
		return nil, true
	}

	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true
}

// Delete removes key, returning true if the key existed.
//
// An expired-but-unswept key counts as not existing, so DEL of a key whose
// time has passed answers 0 rather than 1. Redis reports it the same way.
func (s *Store) Delete(key []byte) bool {
	k := string(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	e, found := s.data[k]
	if !found {
		return false
	}
	if e.expired(s.clk.Now()) {
		// It is already logically gone; remove the corpse and report absent.
		delete(s.data, k)
		return false
	}
	delete(s.data, k)
	return true
}

// Exists reports whether key exists and has not expired.
//
// It is a separate operation rather than a wrapper over Get because it needs
// no copy of the value: existence is a map lookup, and answering EXISTS
// should not allocate a value the caller is going to throw away.
func (s *Store) Exists(key []byte) bool {
	k := string(key)

	s.mu.RLock()
	e, found := s.data[k]
	expired := found && e.expired(s.clk.Now())
	s.mu.RUnlock()

	if !found {
		return false
	}
	if expired {
		s.deleteIfExpired(k)
		return false
	}
	return true
}

// Len returns the number of keys, including any that have expired but have
// not yet been swept.
//
// This matches Redis's DBSIZE, which also counts not-yet-collected keys. The
// alternative — filtering expired entries on every call — would make Len an
// O(N) operation that has to read the clock per key, and would still be a
// snapshot that goes stale immediately. Callers who need a logically exact
// count should ask per key; callers who want the memory footprint want this
// number, and it is the honest one for that purpose.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
