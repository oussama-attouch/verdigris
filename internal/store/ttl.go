package store

import "time"

// The two non-positive answers TTL can give, mirroring the -1 and -2 on the
// Redis wire.
//
// They are time.Duration values rather than, say, int64 seconds, because TTL
// returns a duration and mixing a duration with an out-of-band integer would
// force every caller to consult a separate "did it work" flag. One return
// value carries the whole answer.
//
// The magnitudes are chosen so that each sentinel equals the wire value in
// seconds: TTLNoExpiry is -1 second and TTLMissing is -2 seconds. That makes
// the constants readable as what they mean — and it is worth being explicit
// that it buys nothing else, because a natural guess is that the arithmetic
// works out:
//
//	int64(TTLNoExpiry / time.Second) == 0   // NOT -1
//
// time.Duration division is integer division on nanoseconds, so any
// sub-second negative duration truncates to zero. A caller that divides
// rather than comparing gets 0 for both sentinels, which is why the handler
// switches on the sentinels explicitly and why TestTTLSentinelDivisionTruncates
// pins this rather than the convenient version.
//
// Negative values also cannot collide with a live TTL, which is always
// strictly positive.
const (
	// TTLNoExpiry means the key exists and has no expiry set. Redis replies -1.
	TTLNoExpiry time.Duration = -1 * time.Second

	// TTLMissing means the key does not exist — or existed and has expired.
	// Redis replies -2.
	TTLMissing time.Duration = -2 * time.Second
)

// Expire sets a TTL on an existing key, returning whether it was applied.
//
// A ttl of zero or less deletes the key immediately and reports true, which
// is what Redis does: EXPIRE k 0 is a delete spelled differently. The key must
// already exist — Expire never creates one — so a missing key returns false.
//
// An expired key still sitting in the map is renewed rather than refused. The
// decision is made under the write lock, and that ordering is the point: a
// check performed before acquiring the lock would be a statement about a
// moment that has already passed by the time the write happens, so a
// concurrent SET or sweep could invalidate it. Here, holding the lock means
// the entry examined is the entry modified.
func (s *Store) Expire(key []byte, ttl time.Duration) bool {
	k := string(key)
	now := s.clk.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	e, found := s.data[k]
	if !found {
		return false
	}

	if ttl <= 0 {
		// EXPIRE k 0 is a delete. It reports whether there was anything to
		// delete, which an expired-but-unswept entry still is — the caller
		// asked for it to be gone, and it is now gone.
		delete(s.data, k)
		return true
	}

	e.expiresAt = now.Add(ttl)
	s.data[k] = e
	return true
}

// TTL returns how long key has left to live.
//
// Three outcomes, distinguished by sentinel values rather than by a bool:
//
//   - TTLMissing if the key is absent, or present but already expired. An
//     expired key found here is deleted, so this is a deletion path too.
//   - TTLNoExpiry if the key exists and has no expiry set.
//   - a positive duration otherwise.
//
// This takes the write lock even in the read-only cases, because the
// expired-key branch deletes and the three outcomes must be decided
// atomically. Probing under a read lock and upgrading would mean re-deciding
// after the upgrade, and the decision itself is the entire content of the
// method — there is no cheap "live key" fast path worth the extra complexity
// here, unlike Get, where the fast path is the common one.
func (s *Store) TTL(key []byte) time.Duration {
	k := string(key)
	now := s.clk.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	e, found := s.data[k]
	if !found {
		return TTLMissing
	}
	if e.expired(now) {
		delete(s.data, k)
		return TTLMissing
	}
	if e.expiresAt.IsZero() {
		return TTLNoExpiry
	}
	return e.expiresAt.Sub(now)
}

// Persist removes the expiry from an existing key, returning whether there was
// one to remove.
//
// It reports false both for a key that does not exist and for one that exists
// without a TTL, which is what Redis does: the reply counts expiries removed,
// not keys touched. An expired-but-unswept key counts as missing and is
// removed on the way past.
func (s *Store) Persist(key []byte) bool {
	k := string(key)
	now := s.clk.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	e, found := s.data[k]
	if !found {
		return false
	}
	if e.expired(now) {
		delete(s.data, k)
		return false
	}
	if e.expiresAt.IsZero() {
		// Nothing to persist: the key has no expiry.
		return false
	}

	e.expiresAt = time.Time{}
	s.data[k] = e
	return true
}
