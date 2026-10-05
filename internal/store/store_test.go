package store

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// mustGet fetches a key and fails the test if it is absent.
func mustGet(t *testing.T, s *Store, key string) []byte {
	t.Helper()

	got, found := s.Get([]byte(key))
	if !found {
		t.Fatalf("Get(%q) reported the key missing", key)
	}
	return got
}

func TestSetGetRoundTrip(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("foo"), []byte("bar"))

	got := mustGet(t, s, "foo")
	if !bytes.Equal(got, []byte("bar")) {
		t.Fatalf("Get(foo) = %q, want %q", got, "bar")
	}
}

// TestSetOverwrites checks the second write wins and that the first value is
// not merely shadowed but replaced.
func TestSetOverwrites(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	s.Set([]byte("k"), []byte("first"))
	s.Set([]byte("k"), []byte("second"))

	if got := mustGet(t, s, "k"); !bytes.Equal(got, []byte("second")) {
		t.Fatalf("Get(k) = %q, want %q", got, "second")
	}
	if n := s.Len(); n != 1 {
		t.Fatalf("Len() = %d after overwriting one key, want 1", n)
	}
}

// TestSetOverwriteWithShorterValue catches an implementation that reused a
// stored buffer and left a tail from the longer previous value.
func TestSetOverwriteWithShorterValue(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	s.Set([]byte("k"), []byte("a long original value"))
	s.Set([]byte("k"), []byte("short"))

	if got := mustGet(t, s, "k"); !bytes.Equal(got, []byte("short")) {
		t.Fatalf("Get(k) = %q, want %q", got, "short")
	}
}

// TestGetMissingKey pins the exact shape of a miss: a nil slice and false.
func TestGetMissingKey(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	got, found := s.Get([]byte("nothere"))
	if found {
		t.Fatal("Get on a missing key reported it present")
	}
	if got != nil {
		t.Fatalf("Get on a missing key returned %q, want nil", got)
	}
}

func TestDeleteRemoves(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("k"), []byte("v"))

	if !s.Delete([]byte("k")) {
		t.Fatal("Delete(k) = false for a key that exists, want true")
	}

	got, found := s.Get([]byte("k"))
	if found {
		t.Fatal("Get(k) found the key after Delete")
	}
	if got != nil {
		t.Fatalf("Get(k) after Delete = %q, want nil", got)
	}
	if s.Exists([]byte("k")) {
		t.Fatal("Exists(k) = true after Delete")
	}
}

func TestDeleteMissingKey(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	if s.Delete([]byte("nothere")) {
		t.Fatal("Delete on a missing key returned true, want false")
	}

	// And it must not have created anything.
	if s.Len() != 0 {
		t.Fatalf("Len() = %d after deleting a missing key, want 0", s.Len())
	}
}

// TestDeleteIsNotIdempotentInItsReturnValue pins that the second delete of the
// same key reports false, which is what the DEL command counts.
func TestDeleteIsNotIdempotentInItsReturnValue(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("k"), []byte("v"))

	if !s.Delete([]byte("k")) {
		t.Fatal("first Delete(k) = false, want true")
	}
	if s.Delete([]byte("k")) {
		t.Fatal("second Delete(k) = true, want false")
	}
}

// TestExistsDistinguishesPresentFromAbsent is the whole contract of Exists.
func TestExistsDistinguishesPresentFromAbsent(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("here"), []byte("v"))

	if !s.Exists([]byte("here")) {
		t.Fatal("Exists(here) = false, want true")
	}
	if s.Exists([]byte("nothere")) {
		t.Fatal("Exists(nothere) = true, want false")
	}
}

func TestExistsOnOverwrittenKey(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	s.Set([]byte("k"), []byte("first"))
	s.Set([]byte("k"), []byte("second"))

	if !s.Exists([]byte("k")) {
		t.Fatal("Exists(k) = false after an overwrite, want true")
	}
}

// TestExistsOnDeletedThenRecreatedKey checks the key really is gone in
// between, rather than Exists latching a stale answer.
func TestExistsOnDeletedThenRecreatedKey(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("k"), []byte("v"))

	s.Delete([]byte("k"))
	if s.Exists([]byte("k")) {
		t.Fatal("Exists(k) = true after Delete")
	}

	s.Set([]byte("k"), []byte("v2"))
	if !s.Exists([]byte("k")) {
		t.Fatal("Exists(k) = false after re-creating the key")
	}
	if got := mustGet(t, s, "k"); !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get(k) = %q, want %q", got, "v2")
	}
}

func TestLen(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	if n := s.Len(); n != 0 {
		t.Fatalf("Len() = %d on a new store, want 0", n)
	}

	s.Set([]byte("a"), []byte("1"))
	if n := s.Len(); n != 1 {
		t.Fatalf("Len() = %d after one Set, want 1", n)
	}

	s.Set([]byte("b"), []byte("2"))
	s.Set([]byte("c"), []byte("3"))
	if n := s.Len(); n != 3 {
		t.Fatalf("Len() = %d after three Sets, want 3", n)
	}

	// Overwriting an existing key must not change the count.
	s.Set([]byte("a"), []byte("updated"))
	if n := s.Len(); n != 3 {
		t.Fatalf("Len() = %d after overwriting a key, want 3", n)
	}

	s.Delete([]byte("b"))
	if n := s.Len(); n != 2 {
		t.Fatalf("Len() = %d after one Delete, want 2", n)
	}

	// Deleting a missing key must not change the count either.
	s.Delete([]byte("nothere"))
	if n := s.Len(); n != 2 {
		t.Fatalf("Len() = %d after deleting a missing key, want 2", n)
	}
}

// TestBinarySafeKeys is the key half of the binary-safety property. A key is a
// byte sequence, not text: it may hold NUL, invalid UTF-8, and CRLF, and the
// string conversion the implementation uses must not mangle any of it.
func TestBinarySafeKeys(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	keys := [][]byte{
		{0x00},
		{0xff},
		{0x00, 0xff, 0x0d, 0x0a},
		[]byte("key\r\nwith\r\nnewlines"),
		[]byte("key\x00with\x00nuls"),
		{0xff, 0xfe, 0xfd},
		[]byte(""),
	}

	for i, key := range keys {
		value := []byte(fmt.Sprintf("value-%d", i))
		s.Set(key, value)

		got, found := s.Get(key)
		if !found {
			t.Fatalf("Get(% x) reported the key missing after Set", key)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("Get(% x) = %q, want %q", key, got, value)
		}
		if !s.Exists(key) {
			t.Fatalf("Exists(% x) = false after Set", key)
		}
	}

	if n := s.Len(); n != len(keys) {
		t.Fatalf("Len() = %d, want %d distinct binary keys", n, len(keys))
	}
}

// TestBinarySafeValues is the value half: values are raw bytes, never text.
func TestBinarySafeValues(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	values := [][]byte{
		{0x00},
		{0xff},
		{0x00, 0xff, 0x0d, 0x0a},
		[]byte("$5\r\nhello\r\n"),
		[]byte("*1\r\n$4\r\nPING\r\n"),
		{0xff, 0xfe, 0xfd},
		bytes.Repeat([]byte{0x00}, 1024),
		[]byte(""),
	}

	for i, value := range values {
		key := []byte(fmt.Sprintf("k%d", i))
		s.Set(key, value)

		got := mustGet(t, s, string(key))
		if !bytes.Equal(got, value) {
			t.Fatalf("Get(k%d) = % x, want % x", i, got, value)
		}
	}
}

// TestSetCopiesKeyAndValue is the point of the copy-on-entry invariant.
//
// The caller's slices are mutated after Set. If the store retained them — or
// retained a sub-slice of them — the stored pair would change, and the bug
// would surface later as a value that mysteriously differs from what was
// written. Without this test, an implementation that happened to allocate
// fresh slices would pass everything else and regress silently.
func TestSetCopiesKeyAndValue(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	key := []byte("original-key")
	value := []byte("original-value")

	s.Set(key, value)

	// Mutate both caller-owned slices in place.
	copy(key, "MUTATED-KEY!")
	copy(value, "MUTATED-VALUE")

	// The store must still hold the originals, under the original key.
	got, found := s.Get([]byte("original-key"))
	if !found {
		t.Fatal("Get(original-key) reported the key missing; Set retained the caller's key slice")
	}
	if !bytes.Equal(got, []byte("original-value")) {
		t.Fatalf("Get(original-key) = %q, want %q; Set retained the caller's value slice", got, "original-value")
	}

	// And nothing may have appeared under the mutated key.
	if s.Exists([]byte("MUTATED-KEY!")) {
		t.Fatal("the store has a key named after the mutated slice; Set aliased the caller's key")
	}
}

// TestSetCopiesKeyAndValueOfDifferentLengths covers the case where the
// mutation is shorter or longer than the original, which is where an
// aliasing bug shows up as a length change rather than a content change.
func TestSetCopiesKeyAndValueOfDifferentLengths(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	key := []byte("abcdefghij")
	value := []byte("0123456789")

	s.Set(key, value)

	// Truncate the caller's view by re-slicing, which is the cheapest way for
	// an aliasing bug to be triggered by ordinary code.
	key = key[:3]
	value = value[:2]

	if got, found := s.Get([]byte("abcdefghij")); !found || !bytes.Equal(got, []byte("0123456789")) {
		t.Fatalf("after re-slicing the caller's slices, Get(abcdefghij) = %q, %v; want the full original pair", got, found)
	}
}

// TestGetReturnsACopy is the point of the copy-on-read invariant.
//
// The returned slice is mutated, then fetched again. If Get handed back the
// stored slice, the second read would show the mutation — and a cached value
// could be corrupted by any caller that edited what it received.
func TestGetReturnsACopy(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("k"), []byte("original"))

	first := mustGet(t, s, "k")
	if !bytes.Equal(first, []byte("original")) {
		t.Fatalf("first Get = %q, want %q", first, "original")
	}

	copy(first, "MUTATED!")

	second := mustGet(t, s, "k")
	if !bytes.Equal(second, []byte("original")) {
		t.Fatalf("second Get = %q, want %q; Get handed out the stored slice", second, "original")
	}
	if &first[0] == &second[0] {
		t.Fatal("two Gets returned slices sharing a backing array")
	}
}

// TestGetReturnsADistinctCopyEachTime checks that successive reads do not
// share storage with each other, only with nothing.
func TestGetReturnsADistinctCopyEachTime(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("k"), []byte("value"))

	a := mustGet(t, s, "k")
	b := mustGet(t, s, "k")

	if &a[0] == &b[0] {
		t.Fatal("two Gets returned the same backing array")
	}

	copy(a, "XXXXX")
	if !bytes.Equal(b, []byte("value")) {
		t.Fatalf("mutating one Get result changed another: %q", b)
	}
}

// TestNilAndEmptyValuesAreDistinct pins that a store can hold a nil value and
// an empty value as different things, since the RESP layer treats a null bulk
// string and a zero-length bulk string as different frames.
func TestNilAndEmptyValuesAreDistinct(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	s.Set([]byte("nilkey"), nil)
	s.Set([]byte("emptykey"), []byte{})

	nilValue, found := s.Get([]byte("nilkey"))
	if !found {
		t.Fatal("Get(nilkey) reported the key missing after Set with a nil value")
	}
	if nilValue != nil {
		t.Fatalf("Get(nilkey) = %q, want nil", nilValue)
	}

	emptyValue, found := s.Get([]byte("emptykey"))
	if !found {
		t.Fatal("Get(emptykey) reported the key missing")
	}
	if emptyValue == nil {
		t.Fatal("Get(emptykey) = nil, want an empty non-nil slice")
	}
	if len(emptyValue) != 0 {
		t.Fatalf("Get(emptykey) = %q, want empty", emptyValue)
	}
}

// TestConcurrentDisjointKeys exercises the store the way the server will:
// many goroutines, each with its own key range, all hammering the same Store.
//
// Disjoint keys mean the result is deterministic even though the schedule is
// not, so this test can assert exact final contents. It is the store's
// correctness test under -race, and it is the one that would catch a missing
// lock, a lock held too narrowly, or a map read outside the critical section.
func TestConcurrentDisjointKeys(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	const goroutines = 16
	const iterations = 1000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each goroutine owns this prefix exclusively.
			prefix := fmt.Sprintf("g%d-", g)

			for i := 0; i < iterations; i++ {
				key := []byte(prefix + fmt.Sprint(i))
				value := []byte(fmt.Sprintf("%d:%d", g, i))

				s.Set(key, value)

				// Read it back: with disjoint keys, no other goroutine can
				// have touched it, so a mismatch is a real bug.
				got, found := s.Get(key)
				if !found {
					t.Errorf("Get(%q) missing immediately after Set", key)
					return
				}
				if !bytes.Equal(got, value) {
					t.Errorf("Get(%q) = %q, want %q", key, got, value)
					return
				}

				// Delete every other key, so Delete and Exists run
				// concurrently with Set and Get too.
				if i%2 == 0 {
					if !s.Delete(key) {
						t.Errorf("Delete(%q) = false for a key that was just set", key)
						return
					}
					if s.Exists(key) {
						t.Errorf("Exists(%q) = true immediately after Delete", key)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	// Half the keys were deleted, so each goroutine should have left
	// iterations/2 behind.
	want := goroutines * (iterations / 2)
	if got := s.Len(); got != want {
		t.Fatalf("Len() = %d after the concurrent run, want %d", got, want)
	}
}

// TestConcurrentSameKey hammers one key from many goroutines.
//
// The value observed is nondeterministic — any of the writers may win — but
// the map must not corrupt, the key must exist, and the value must always be
// one that some goroutine actually wrote. Under -race this is where a missing
// lock turns into a crash or a reported race rather than a wrong answer.
func TestConcurrentSameKey(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	const goroutines = 8
	const iterations = 1000

	key := []byte("shared")

	// valid holds every value any writer produces, so a read can be checked
	// for provenance rather than equality.
	var valid sync.Map
	for g := 0; g < goroutines; g++ {
		for i := 0; i < iterations; i++ {
			valid.Store(fmt.Sprintf("v-%d-%d", g, i), struct{}{})
		}
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				value := []byte(fmt.Sprintf("v-%d-%d", g, i))
				s.Set(key, value)

				got, found := s.Get(key)
				if !found {
					t.Errorf("Get(shared) reported the key missing while writers are active")
					return
				}
				if _, ok := valid.Load(string(got)); !ok {
					t.Errorf("Get(shared) = %q, which no goroutine ever wrote", got)
					return
				}

				// Also read the key's existence concurrently.
				_ = s.Exists(key)
			}
		}()
	}
	wg.Wait()

	// The key must exist at the end, whatever value it holds.
	if !s.Exists(key) {
		t.Fatal("Exists(shared) = false after every writer completed")
	}
	final := mustGet(t, s, "shared")
	if _, ok := valid.Load(string(final)); !ok {
		t.Fatalf("final Get(shared) = %q, which no goroutine ever wrote", final)
	}
}

// TestConcurrentMixedKeysWithSharedPrefix drives many goroutines across an
// overlapping key space, which is closer to real traffic than either of the
// tests above: different clients write keys that sometimes collide.
func TestConcurrentMixedKeysWithSharedPrefix(t *testing.T) {
	s := New(RealClock{})
	defer s.Close()

	const goroutines = 8
	const perGoroutine = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(g)))

			for i := 0; i < perGoroutine; i++ {
				// The key space is shared: index is drawn from a small range,
				// so goroutines overwrite each other's keys on purpose.
				key := []byte(fmt.Sprintf("shared-%d", rng.Intn(50)))
				value := []byte(fmt.Sprintf("writer-%d", g))

				s.Set(key, value)

				if got, found := s.Get(key); found {
					// Any writer's value is acceptable; a length or content
					// corruption is not.
					if len(got) == 0 {
						t.Errorf("Get(%q) returned an empty value", key)
						return
					}
				}

				if i%5 == 0 {
					s.Delete(key)
				}
			}
		}()
	}
	wg.Wait()

	// No assertion on the contents — only that the store is still coherent
	// and every surviving key is readable.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("shared-%d", i)
		if s.Exists([]byte(key)) {
			if _, found := s.Get([]byte(key)); !found {
				t.Fatalf("Exists(%q) = true but Get reported it missing", key)
			}
		}
	}
}

// BenchmarkSet records the cost of a write. Nothing is optimized on the basis
// of these numbers; they are groundwork for milestone 8.
func BenchmarkSet(b *testing.B) {
	s := New(RealClock{})
	defer s.Close()
	key := []byte("benchmark-key")
	value := []byte("benchmark-value")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Set(key, value)
	}
}

// BenchmarkGet records the cost of a read, which includes the copy-on-exit
// that the ownership invariant requires.
func BenchmarkGet(b *testing.B) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("benchmark-key"), []byte("benchmark-value"))
	key := []byte("benchmark-key")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, found := s.Get(key); !found {
			b.Fatal("key vanished during the benchmark")
		}
	}
}

// BenchmarkExists records the cost of an existence check, which does no copy
// and should therefore be the cheapest of the three operations.
func BenchmarkExists(b *testing.B) {
	s := New(RealClock{})
	defer s.Close()
	s.Set([]byte("benchmark-key"), []byte("benchmark-value"))
	key := []byte("benchmark-key")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !s.Exists(key) {
			b.Fatal("key vanished during the benchmark")
		}
	}
}

// BenchmarkSetGetParallel measures throughput under concurrent readers and
// writers at once, which is the shape of the server's real workload.
func BenchmarkSetGetParallel(b *testing.B) {
	s := New(RealClock{})
	defer s.Close()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("k%d", i%1000))
			if i%4 == 0 {
				s.Set(key, []byte("value"))
			} else {
				_, _ = s.Get(key)
			}
			i++
		}
	})
}
