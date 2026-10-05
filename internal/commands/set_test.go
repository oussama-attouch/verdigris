package commands

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// TestSetStoresThePair is the handler-level happy path, plus the check that
// matters most: the value is actually retrievable from the store afterwards,
// not merely acknowledged with OK.
func TestSetStoresThePair(t *testing.T) {
	s := store.New(store.RealClock{})
	h := SetHandler(s)

	got, fault := h(args("SET", "foo", "bar"))
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("SET = %#v, want SimpleString(\"OK\")", got)
	}

	// The reply alone proves nothing; the store must hold the pair.
	stored, found := s.Get([]byte("foo"))
	if !found {
		t.Fatal("SET replied OK but the key is not in the store")
	}
	if !bytes.Equal(stored, []byte("bar")) {
		t.Fatalf("stored value = %q, want %q", stored, "bar")
	}
}

func TestSetWrongArity(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
		want protocol.Value
	}{
		{"no key or value", args("SET"), protocol.Error("ERR wrong number of arguments for 'set' command")},
		{"key without value", args("SET", "foo"), protocol.Error("ERR wrong number of arguments for 'set' command")},
		// Extra tokens look like the EX/PX forms this milestone does not
		// support yet, so Redis calls that a syntax error rather than an
		// arity error. Matching that distinction keeps the reply honest about
		// which mistake the client made.
		{"too many arguments", args("SET", "foo", "bar", "baz"), protocol.Error("ERR syntax error")},
		{"unknown option", args("SET", "foo", "bar", "NX"), protocol.Error("ERR syntax error")},
		{"EX without a value", args("SET", "foo", "bar", "EX"), protocol.Error("ERR syntax error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := store.New(store.RealClock{})

			got, fault := SetHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("SET(%v) = %#v, want %#v", tt.args, got, tt.want)
			}
			if s.Len() != 0 {
				t.Fatalf("a rejected SET still wrote %d keys", s.Len())
			}
		})
	}
}

func TestSetRejectsNonBulkArguments(t *testing.T) {
	inputs := []struct {
		name string
		args []protocol.Value
	}{
		{"integer key", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.Integer(1), protocol.BulkString([]byte("v"))}},
		{"simple string key", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.SimpleString("k"), protocol.BulkString([]byte("v"))}},
		{"null key", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.Null{}, protocol.BulkString([]byte("v"))}},
		{"integer value", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.BulkString([]byte("k")), protocol.Integer(1)}},
		{"null value", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.BulkString([]byte("k")), protocol.Null{}}},
		{"array value", []protocol.Value{protocol.BulkString([]byte("SET")), protocol.BulkString([]byte("k")), protocol.Array{}}},
	}

	const want = "ERR invalid argument for 'set' command"

	for _, tt := range inputs {
		t.Run(tt.name, func(t *testing.T) {
			s := store.New(store.RealClock{})

			got, fault := SetHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("SET(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("SET(%v) = %q, want %q", tt.args, errValue, want)
			}
			if s.Len() != 0 {
				t.Fatalf("a rejected SET still wrote %d keys", s.Len())
			}
		})
	}
}

// TestSetOverwritesThroughTheHandler checks the handler does not add its own
// existence check, since SET is an unconditional overwrite in Redis.
func TestSetOverwritesThroughTheHandler(t *testing.T) {
	s := store.New(store.RealClock{})
	h := SetHandler(s)

	if got, _ := h(args("SET", "k", "first")); got != protocol.SimpleString("OK") {
		t.Fatalf("first SET = %#v", got)
	}
	if got, _ := h(args("SET", "k", "second")); got != protocol.SimpleString("OK") {
		t.Fatalf("second SET = %#v", got)
	}

	stored, found := s.Get([]byte("k"))
	if !found || !bytes.Equal(stored, []byte("second")) {
		t.Fatalf("stored value = %q, %v; want %q", stored, found, "second")
	}
}

// TestSetIsBinarySafeThroughTheHandler passes a key and value containing NUL,
// invalid UTF-8 and CRLF. The handler must not interpret either.
func TestSetIsBinarySafeThroughTheHandler(t *testing.T) {
	s := store.New(store.RealClock{})
	h := SetHandler(s)

	key := []byte{0x00, 0xff, '\r', '\n'}
	value := []byte{0xff, 0x00, '\r', '\n', 0xfe}

	got, fault := h([]protocol.Value{
		protocol.BulkString([]byte("SET")),
		protocol.BulkString(key),
		protocol.BulkString(value),
	})
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("SET = %#v, want OK", got)
	}

	stored, found := s.Get(key)
	if !found {
		t.Fatal("binary key not found after SET")
	}
	if !bytes.Equal(stored, value) {
		t.Fatalf("stored value = % x, want % x", stored, value)
	}
}

// TestSetWithEmptyKeyAndValue checks the degenerate-but-legal shapes: an empty
// key and an empty value are both valid bulk strings in Redis.
func TestSetWithEmptyKeyAndValue(t *testing.T) {
	s := store.New(store.RealClock{})
	h := SetHandler(s)

	if got, _ := h(args("SET", "", "")); got != protocol.SimpleString("OK") {
		t.Fatalf("SET with empty key and value = %#v, want OK", got)
	}

	stored, found := s.Get([]byte(""))
	if !found {
		t.Fatal("the empty key is not in the store after SET")
	}
	if len(stored) != 0 {
		t.Fatalf("stored value = %q, want empty", stored)
	}
}

// TestSetStoresDirectlyWithoutCopyingInTheHandler is a contract check rather
// than a behaviour check: the handler must pass the argument slices straight
// through, because the store is what guarantees ownership. If the handler
// copied, it would be doing the store's job twice on every write.
//
// It is asserted indirectly, by mutating the caller's slice after the call and
// confirming the store is unaffected — which is true either way, and so this
// test documents the guarantee the store provides rather than the handler's
// internals.
func TestSetStoresDirectlyWithoutCopyingInTheHandler(t *testing.T) {
	s := store.New(store.RealClock{})
	h := SetHandler(s)

	key := []byte("original-key")
	value := []byte("original-value")

	if _, fault := h([]protocol.Value{
		protocol.BulkString([]byte("SET")),
		protocol.BulkString(key),
		protocol.BulkString(value),
	}); fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}

	copy(key, "MUTATED-KEY!")
	copy(value, "MUTATED-VALUE")

	stored, found := s.Get([]byte("original-key"))
	if !found || !bytes.Equal(stored, []byte("original-value")) {
		t.Fatalf("after mutating the caller's slices, stored = %q, %v; want the originals", stored, found)
	}
}

// --- SET with EX and PX -----------------------------------------------------

// TestSetWithEX checks the seconds form end to end through the handler, and
// measures the resulting TTL rather than only the reply: a handler that
// multiplied by the wrong constant would still answer OK.
func TestSetWithEX(t *testing.T) {
	s := newExpireStore()

	got, fault := SetHandler(s)(numericArgs("SET", []string{"k", "v", "EX"}, 10))
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("SET k v EX 10 = %#v, want OK", got)
	}

	value, found := s.Get([]byte("k"))
	if !found || !bytes.Equal(value, []byte("v")) {
		t.Fatalf("Get = %q, %v; want %q, true", value, found, "v")
	}

	remaining := s.TTL([]byte("k"))
	if remaining < 9*time.Second || remaining > 10*time.Second {
		t.Fatalf("TTL after SET k v EX 10 = %v, want about 10s", remaining)
	}

	// And the handler round trip the demo performs.
	ttlReply, _ := TTLHandler(s)(args("TTL", "k"))
	if n, ok := ttlReply.(protocol.Integer); !ok || n != 10 {
		t.Fatalf("TTL k = %#v, want Integer(10)", ttlReply)
	}
}

// TestSetWithPX checks the milliseconds form, including that the unit differs
// from EX.
func TestSetWithPX(t *testing.T) {
	s := newExpireStore()

	got, fault := SetHandler(s)(numericArgs("SET", []string{"k", "v", "PX"}, 500))
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("SET k v PX 500 = %#v, want OK", got)
	}

	// The TTL must be in the half-second range, not 500 seconds.
	remaining := s.TTL([]byte("k"))
	if remaining <= 0 || remaining > time.Second {
		t.Fatalf("TTL after SET k v PX 500 = %v, want about 500ms", remaining)
	}

	// Rounding to the nearest second makes this 1, which is the case the
	// milestone calls out: truncation would report 0.
	ttlReply, _ := TTLHandler(s)(args("TTL", "k"))
	if n, ok := ttlReply.(protocol.Integer); !ok || n != 1 {
		t.Fatalf("TTL k after PX 500 = %#v, want Integer(1)", ttlReply)
	}
}

// TestSetOptionsAreCaseInsensitive matches Redis, where `set k v ex 10` is as
// valid as the upper-case form.
func TestSetOptionsAreCaseInsensitive(t *testing.T) {
	for _, option := range []string{"EX", "ex", "Ex", "eX", "PX", "px", "Px"} {
		t.Run(option, func(t *testing.T) {
			s := newExpireStore()

			got, fault := SetHandler(s)(numericArgs("SET", []string{"k", "v", option}, 60))
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}
			if got != protocol.SimpleString("OK") {
				t.Fatalf("SET k v %s 60 = %#v, want OK", option, got)
			}
			if remaining := s.TTL([]byte("k")); remaining <= 0 {
				t.Fatalf("SET k v %s 60 left no TTL", option)
			}
		})
	}
}

// TestSetWithUnsupportedOption is the syntax-error path. NX, XX, GET and
// KEEPTTL are real Redis options this milestone does not implement, and
// refusing them is the point: silently ignoring `NX` would let a client
// believe it had performed a conditional write.
func TestSetWithUnsupportedOption(t *testing.T) {
	for _, option := range []string{"NX", "XX", "GET", "KEEPTTL", "nope", ""} {
		t.Run(option, func(t *testing.T) {
			s := newExpireStore()

			got, fault := SetHandler(s)(numericArgs("SET", []string{"k", "v", option}, 10))
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}
			if got != protocol.Error("ERR syntax error") {
				t.Fatalf("SET k v %s 10 = %#v, want a syntax error", option, got)
			}

			// A rejected command must not have written anything.
			if s.Exists([]byte("k")) {
				t.Fatal("a rejected SET still wrote the key")
			}
		})
	}
}

// TestSetWithNonBulkOption checks the option position's type. A non-bulk value
// there is a malformed request, which Redis reports as a syntax error.
func TestSetWithNonBulkOption(t *testing.T) {
	s := newExpireStore()

	got, fault := SetHandler(s)([]protocol.Value{
		protocol.BulkString([]byte("SET")),
		protocol.BulkString([]byte("k")),
		protocol.BulkString([]byte("v")),
		protocol.Integer(1),
		protocol.Integer(10),
	})
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.Error("ERR syntax error") {
		t.Fatalf("SET with an integer option = %#v, want a syntax error", got)
	}
}

// TestSetWithNonIntegerExpiry pins the parse error, which must match EXPIRE's
// wording.
func TestSetWithNonIntegerExpiry(t *testing.T) {
	const want = "ERR value is not an integer or out of range"

	for _, tt := range []struct {
		name string
		args []protocol.Value
	}{
		{"letters", args("SET", "k", "v", "EX", "abc")},
		{"empty", args("SET", "k", "v", "EX", "")},
		{"float", args("SET", "k", "v", "PX", "1.5")},
		{"trailing junk", args("SET", "k", "v", "EX", "10abc")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpireStore()

			got, fault := SetHandler(s)(tt.args)
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}
			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("SET(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("SET(%v) = %q, want %q", tt.args, errValue, want)
			}
			if s.Exists([]byte("k")) {
				t.Fatal("a rejected SET still wrote the key")
			}
		})
	}
}

// TestSetEXWithZeroOrNegativeDeletesImmediately checks that a non-positive
// expiry behaves as Redis does: the write succeeds and the key is already
// gone.
func TestSetEXWithZeroOrNegativeDeletesImmediately(t *testing.T) {
	for _, amount := range []int64{0, -1} {
		t.Run(strconv.FormatInt(amount, 10), func(t *testing.T) {
			s := newExpireStore()

			got, fault := SetHandler(s)(numericArgs("SET", []string{"k", "v", "EX"}, amount))
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}
			if got != protocol.SimpleString("OK") {
				t.Fatalf("SET k v EX %d = %#v, want OK", amount, got)
			}
			if s.Exists([]byte("k")) {
				t.Fatalf("the key survived SET k v EX %d", amount)
			}
		})
	}
}

// TestSetHugeExpiryDoesNotWrap is the overflow guard for the EX and PX path.
// A wrapped duration would turn "expire in 292 years" into a negative one and
// delete the key.
func TestSetHugeExpiryDoesNotWrap(t *testing.T) {
	for _, option := range []string{"EX", "PX"} {
		t.Run(option, func(t *testing.T) {
			s := newExpireStore()

			got, fault := SetHandler(s)([]protocol.Value{
				protocol.BulkString([]byte("SET")),
				protocol.BulkString([]byte("k")),
				protocol.BulkString([]byte("v")),
				protocol.BulkString([]byte(option)),
				protocol.Integer(9223372036854775807),
			})
			if fault != nil {
				t.Fatalf("SET reported an internal fault: %v", fault)
			}
			if got != protocol.SimpleString("OK") {
				t.Fatalf("SET with a huge %s = %#v, want OK", option, got)
			}
			if !s.Exists([]byte("k")) {
				t.Fatalf("SET k v %s MaxInt64 deleted the key; the duration wrapped", option)
			}
			if remaining := s.TTL([]byte("k")); remaining <= 0 {
				t.Fatalf("TTL after a huge %s = %v, want a positive duration", option, remaining)
			}
		})
	}
}

// TestSetWithoutExpiryClearsAnExistingTTL is the behaviour the milestone
// singles out, exercised through the handler rather than the store.
func TestSetWithoutExpiryClearsAnExistingTTL(t *testing.T) {
	s := newExpireStore()

	// The demo's exact sequence.
	if got, _ := SetHandler(s)(numericArgs("SET", []string{"k", "v1", "EX"}, 100)); got != protocol.SimpleString("OK") {
		t.Fatalf("SET k v1 EX 100 = %#v", got)
	}
	ttlReply, _ := TTLHandler(s)(args("TTL", "k"))
	if n, ok := ttlReply.(protocol.Integer); !ok || n != 100 {
		t.Fatalf("TTL after SET k v1 EX 100 = %#v, want Integer(100)", ttlReply)
	}

	if got, _ := SetHandler(s)(args("SET", "k", "v2")); got != protocol.SimpleString("OK") {
		t.Fatalf("SET k v2 = %#v", got)
	}

	ttlReply, _ = TTLHandler(s)(args("TTL", "k"))
	if ttlReply != protocol.Integer(-1) {
		t.Fatalf("TTL after a plain SET = %#v, want Integer(-1): SET must clear the expiry", ttlReply)
	}

	// And the value is the new one, still readable.
	getReply, _ := GetHandler(s)(args("GET", "k"))
	if bs, ok := getReply.(protocol.BulkString); !ok || !bytes.Equal(bs, []byte("v2")) {
		t.Fatalf("GET after the overwrite = %#v, want BulkString(\"v2\")", getReply)
	}
}

// TestSetEXThenPXReplacesTheDeadline checks that a second timed SET moves the
// deadline rather than adding to it or keeping the shorter one.
func TestSetEXThenPXReplacesTheDeadline(t *testing.T) {
	s := newExpireStore()

	if got, _ := SetHandler(s)(numericArgs("SET", []string{"k", "v", "EX"}, 100)); got != protocol.SimpleString("OK") {
		t.Fatalf("SET k v EX 100 = %#v", got)
	}
	if got, _ := SetHandler(s)(numericArgs("SET", []string{"k", "v", "PX"}, 500)); got != protocol.SimpleString("OK") {
		t.Fatalf("SET k v PX 500 = %#v", got)
	}

	// The deadline is now half a second away, not a hundred seconds.
	remaining := s.TTL([]byte("k"))
	if remaining > time.Second {
		t.Fatalf("TTL after SET k v PX 500 = %v, want about 500ms: the EX deadline was not replaced", remaining)
	}
}

// TestSetEXMissingValueIsSyntaxError covers the partial option tail, which is
// a shape error rather than a short command.
func TestSetEXMissingValueIsSyntaxError(t *testing.T) {
	s := newExpireStore()

	got, fault := SetHandler(s)(args("SET", "k", "v", "EX"))
	if fault != nil {
		t.Fatalf("SET reported an internal fault: %v", fault)
	}
	if got != protocol.Error("ERR syntax error") {
		t.Fatalf("SET k v EX = %#v, want a syntax error", got)
	}
}
