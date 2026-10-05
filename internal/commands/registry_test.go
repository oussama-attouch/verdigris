package commands

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// mustError asserts a reply is a RESP Error frame and returns its message.
func mustError(t *testing.T, v protocol.Value) string {
	t.Helper()

	errValue, ok := v.(protocol.Error)
	if !ok {
		t.Fatalf("reply is %T (%#v), want protocol.Error", v, v)
	}
	return string(errValue)
}

// commandsTypeName names a test subtest after a value's type, tolerating nil.
// reflect.TypeOf(nil) is a nil Type, and calling String on it panics.
func commandsTypeName(v protocol.Value) string {
	if v == nil {
		return "nil"
	}
	return reflect.TypeOf(v).String()
}

func TestRegisterAndDispatch(t *testing.T) {
	r := NewRegistry()

	called := 0
	r.Register("HELLO", func(a []protocol.Value) protocol.Value {
		called++
		return protocol.SimpleString("HI")
	})

	got := r.Dispatch(protocol.Array{protocol.BulkString([]byte("HELLO"))})
	if got != protocol.SimpleString("HI") {
		t.Fatalf("Dispatch = %#v, want SimpleString(\"HI\")", got)
	}
	if called != 1 {
		t.Fatalf("handler called %d times, want 1", called)
	}
}

// TestDispatchIsCaseInsensitive matches Redis, where command names are
// case-insensitive. A client sending `ping` must reach the same handler as
// `PING`, and the registration itself is normalized too.
func TestDispatchIsCaseInsensitive(t *testing.T) {
	r := NewRegistry()
	r.Register("ping", Ping) // registered lower case on purpose

	for _, name := range []string{"ping", "PING", "Ping", "pInG"} {
		t.Run(name, func(t *testing.T) {
			got := r.Dispatch(protocol.Array{protocol.BulkString([]byte(name))})
			if got != protocol.SimpleString("PONG") {
				t.Fatalf("Dispatch(%q) = %#v, want SimpleString(\"PONG\")", name, got)
			}
		})
	}
}

func TestDispatchUnknownCommand(t *testing.T) {
	r := DefaultRegistry()

	got := r.Dispatch(protocol.Array{protocol.BulkString([]byte("NOTACOMMAND"))})

	msg := mustError(t, got)
	if !strings.Contains(msg, "NOTACOMMAND") {
		t.Fatalf("unknown-command error %q does not name the command", msg)
	}
	if !strings.HasPrefix(msg, "ERR ") {
		t.Fatalf("unknown-command error %q does not start with \"ERR \"", msg)
	}
}

// TestDispatchUnknownCommandIsUppercased pins the exact reply the milestone's
// redis-cli demo asserts. A client sending lower case must still see the name
// upper-cased, because that is what Redis echoes back.
func TestDispatchUnknownCommandIsUppercased(t *testing.T) {
	r := DefaultRegistry()

	got := r.Dispatch(protocol.Array{protocol.BulkString([]byte("notacommand"))})

	const want = "ERR unknown command 'NOTACOMMAND'"
	if msg := mustError(t, got); msg != want {
		t.Fatalf("reply = %q, want %q", msg, want)
	}
}

func TestDispatchRejectsNonArray(t *testing.T) {
	r := DefaultRegistry()

	// A bare bulk string, a simple string, an integer, a null, and nil are
	// all things a client could put on the wire that are not commands.
	inputs := []protocol.Value{
		protocol.BulkString([]byte("PING")),
		protocol.SimpleString("PING"),
		protocol.Integer(1),
		protocol.Null{},
		nil,
	}

	for _, in := range inputs {
		t.Run(commandsTypeName(in), func(t *testing.T) {
			got := r.Dispatch(in)

			if msg := mustError(t, got); msg != "ERR unknown command" {
				t.Fatalf("Dispatch(%#v) = %q, want \"ERR unknown command\"", in, msg)
			}
		})
	}
}

func TestDispatchRejectsEmptyArray(t *testing.T) {
	r := DefaultRegistry()

	got := r.Dispatch(protocol.Array{})

	if msg := mustError(t, got); msg != "ERR empty command" {
		t.Fatalf("Dispatch(empty array) = %q, want \"ERR empty command\"", msg)
	}
}

func TestDispatchRejectsNonBulkCommandName(t *testing.T) {
	r := DefaultRegistry()

	// Real clients always send the command name as a bulk string. An integer,
	// a simple string, a nested array, or a null in that position is a
	// malformed request rather than an unknown command.
	inputs := []protocol.Value{
		protocol.Integer(1),
		protocol.SimpleString("PING"),
		protocol.Array{protocol.BulkString([]byte("PING"))},
		protocol.Null{},
		nil,
	}

	for _, name := range inputs {
		t.Run(commandsTypeName(name), func(t *testing.T) {
			got := r.Dispatch(protocol.Array{name})

			if msg := mustError(t, got); msg != "ERR invalid command name" {
				t.Fatalf("Dispatch([%#v]) = %q, want \"ERR invalid command name\"", name, msg)
			}
		})
	}
}

// TestDispatchPassesFullArgumentList checks that the handler sees args[0] and
// every argument after it, not a stripped copy. A registry that trimmed the
// command name would break every handler's arity arithmetic.
func TestDispatchPassesFullArgumentList(t *testing.T) {
	r := NewRegistry()

	var seen []protocol.Value
	r.Register("CAPTURE", func(a []protocol.Value) protocol.Value {
		seen = a
		return protocol.SimpleString("OK")
	})

	in := protocol.Array{
		protocol.BulkString([]byte("CAPTURE")),
		protocol.BulkString([]byte("one")),
		protocol.Integer(2),
		protocol.Null{},
	}
	r.Dispatch(in)

	if !reflect.DeepEqual(seen, []protocol.Value(in)) {
		t.Fatalf("handler saw %#v, want %#v", seen, in)
	}
}

func TestDispatchDoesNotAlterTheReply(t *testing.T) {
	r := NewRegistry()

	// A Null reply must come back as Null: a registry that coerced empty
	// replies into errors would break every future command that returns one.
	r.Register("NULLY", func([]protocol.Value) protocol.Value { return protocol.Null{} })

	if got := r.Dispatch(protocol.Array{protocol.BulkString([]byte("NULLY"))}); got != (protocol.Null{}) {
		t.Fatalf("Dispatch = %#v, want Null{}", got)
	}
}

func TestNames(t *testing.T) {
	r := DefaultRegistry()

	got := r.Names()
	want := []string{"ECHO", "PING"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Names() = %v, want %v", got, want)
	}
}

// TestNamesIsSorted guards the contract that callers can rely on the order
// without sorting again, including for names registered out of order.
func TestNamesIsSorted(t *testing.T) {
	r := NewRegistry()
	for _, name := range []string{"ZULU", "alpha", "MIKE", "bravo"} {
		r.Register(name, Ping)
	}

	got := r.Names()
	want := []string{"ALPHA", "BRAVO", "MIKE", "ZULU"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Names() = %v, want %v (sorted, uppercased)", got, want)
	}
}

func TestNamesOnEmptyRegistry(t *testing.T) {
	r := NewRegistry()

	got := r.Names()
	if len(got) != 0 {
		t.Fatalf("Names() = %v, want empty", got)
	}
	// A caller appending to the result must not be able to write into the
	// registry's own storage, which is why Names returns a fresh slice.
	if got == nil {
		t.Log("Names() returned nil for an empty registry; callers must treat nil and empty alike")
	}
}

// TestRegisterDuplicatePanics pins the documented failure mode. A duplicate
// registration can only happen at init time, where a silent shadow would be
// far harder to diagnose than a panic.
func TestRegisterDuplicatePanics(t *testing.T) {
	r := NewRegistry()
	r.Register("PING", Ping)

	defer func() {
		if recover() == nil {
			t.Fatal("duplicate Register did not panic")
		}
	}()

	r.Register("ping", Ping) // same name, different case
}

// TestDefaultRegistryHasTheBuiltins checks the registry the server actually
// uses, so a missing Register call fails here rather than over the wire.
func TestDefaultRegistryHasTheBuiltins(t *testing.T) {
	r := DefaultRegistry()

	for _, name := range []string{"PING", "ECHO"} {
		got := r.Dispatch(protocol.Array{protocol.BulkString([]byte(name)), protocol.BulkString([]byte("x"))})
		if _, isErr := got.(protocol.Error); isErr && name == "PING" {
			// PING with an argument is valid; only a wrong-arity reply would
			// be an error, and this input has the right arity.
			t.Fatalf("PING is not registered: %#v", got)
		}
	}

	if names := r.Names(); !reflect.DeepEqual(names, []string{"ECHO", "PING"}) {
		t.Fatalf("DefaultRegistry names = %v, want [ECHO PING]", names)
	}
}

// TestDispatchIsSafeForConcurrentUse matters because the server dispatches
// from one goroutine per connection. Under -race this fails loudly if the map
// is read without the lock.
func TestDispatchIsSafeForConcurrentUse(t *testing.T) {
	r := DefaultRegistry()

	const goroutines = 8
	const iterations = 200

	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < iterations; j++ {
				r.Dispatch(protocol.Array{protocol.BulkString([]byte("PING"))})
				r.Dispatch(protocol.Array{protocol.BulkString([]byte("ECHO")), protocol.BulkString([]byte("x"))})
				r.Dispatch(protocol.Array{protocol.BulkString([]byte("NOPE"))})
				_ = r.Names()
			}
		}()
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}
