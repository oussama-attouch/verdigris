package commands

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
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
	r.Register("HELLO", func(a []protocol.Value) (protocol.Value, error) {
		called++
		return protocol.SimpleString("HI"), nil
	})

	got, fault := r.Dispatch(protocol.Array{protocol.BulkString([]byte("HELLO"))})
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}
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
			got, fault := r.Dispatch(protocol.Array{protocol.BulkString([]byte(name))})
			if fault != nil {
				t.Fatalf("Dispatch reported an internal fault: %v", fault)
			}
			if got != protocol.SimpleString("PONG") {
				t.Fatalf("Dispatch(%q) = %#v, want SimpleString(\"PONG\")", name, got)
			}
		})
	}
}

func TestDispatchUnknownCommand(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	got, fault := r.Dispatch(protocol.Array{protocol.BulkString([]byte("NOTACOMMAND"))})
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}

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
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	got, fault := r.Dispatch(protocol.Array{protocol.BulkString([]byte("notacommand"))})
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}

	const want = "ERR unknown command 'NOTACOMMAND'"
	if msg := mustError(t, got); msg != want {
		t.Fatalf("reply = %q, want %q", msg, want)
	}
}

func TestDispatchRejectsNonArray(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

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
			got, fault := r.Dispatch(in)
			if fault != nil {
				// A malformed request is the client's problem, not an
				// internal fault. Reporting one here would mean the read loop
				// logged a bug that does not exist and answered with a
				// generic error instead of the specific one.
				t.Fatalf("Dispatch reported an internal fault for a client error: %v", fault)
			}

			if msg := mustError(t, got); msg != "ERR unknown command" {
				t.Fatalf("Dispatch(%#v) = %q, want \"ERR unknown command\"", in, msg)
			}
		})
	}
}

func TestDispatchRejectsEmptyArray(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	got, fault := r.Dispatch(protocol.Array{})
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}

	if msg := mustError(t, got); msg != "ERR empty command" {
		t.Fatalf("Dispatch(empty array) = %q, want \"ERR empty command\"", msg)
	}
}

func TestDispatchRejectsNonBulkCommandName(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

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
			got, fault := r.Dispatch(protocol.Array{name})
			if fault != nil {
				t.Fatalf("Dispatch reported an internal fault for a client error: %v", fault)
			}

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
	r.Register("CAPTURE", func(a []protocol.Value) (protocol.Value, error) {
		seen = a
		return protocol.SimpleString("OK"), nil
	})

	in := protocol.Array{
		protocol.BulkString([]byte("CAPTURE")),
		protocol.BulkString([]byte("one")),
		protocol.Integer(2),
		protocol.Null{},
	}
	if _, fault := r.Dispatch(in); fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}

	if !reflect.DeepEqual(seen, []protocol.Value(in)) {
		t.Fatalf("handler saw %#v, want %#v", seen, in)
	}
}

func TestDispatchDoesNotAlterTheReply(t *testing.T) {
	r := NewRegistry()

	// A Null reply must come back as Null: a registry that coerced empty
	// replies into errors would break every command that returns one.
	r.Register("NULLY", func([]protocol.Value) (protocol.Value, error) { return protocol.Null{}, nil })

	got, fault := r.Dispatch(protocol.Array{protocol.BulkString([]byte("NULLY"))})
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}
	if got != (protocol.Null{}) {
		t.Fatalf("Dispatch = %#v, want Null{}", got)
	}
}

// TestDispatchPropagatesInternalFault is the test for the whole point of the
// (Value, error) signature: a handler bug must reach the caller as an error,
// separately from the reply, so the read loop can log it and substitute a
// generic message instead of forwarding the fault text to the client.
func TestDispatchPropagatesInternalFault(t *testing.T) {
	r := NewRegistry()

	sentinel := errors.New("something went wrong inside the handler")
	r.Register("BROKEN", func([]protocol.Value) (protocol.Value, error) {
		// A real handler bug: no reply, and an error describing it.
		return nil, sentinel
	})

	reply, fault := r.Dispatch(protocol.Array{protocol.BulkString([]byte("BROKEN"))})

	if !errors.Is(fault, sentinel) {
		t.Fatalf("Dispatch fault = %v, want the handler's error", fault)
	}
	if reply != nil {
		t.Fatalf("Dispatch reply = %#v alongside a fault, want nil", reply)
	}

	// The fault text must not be reachable through the reply, which is what
	// the client would receive if the loop forwarded it.
	if encoded := protocol.Encode(reply); len(encoded) != 0 {
		t.Fatalf("a faulted dispatch produced reply bytes %q, want none", encoded)
	}
}

// TestDispatchDomainErrorsAreNotFaults is the other half of that contract, and
// the one that is easy to get backwards: a handler returning a protocol.Error
// is answering the client correctly, and must not be reported as a bug.
func TestDispatchDomainErrorsAreNotFaults(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	for _, in := range []protocol.Value{
		protocol.Array{protocol.BulkString([]byte("PING")), protocol.BulkString([]byte("a")), protocol.BulkString([]byte("b"))},
		protocol.Array{protocol.BulkString([]byte("ECHO"))},
		protocol.Array{protocol.BulkString([]byte("GET"))},
		protocol.Array{protocol.BulkString([]byte("NOTACOMMAND"))},
	} {
		reply, fault := r.Dispatch(in)
		if fault != nil {
			t.Fatalf("Dispatch(%#v) reported an internal fault for a domain error: %v", in, fault)
		}
		if _, ok := reply.(protocol.Error); !ok {
			t.Fatalf("Dispatch(%#v) = %#v, want a protocol.Error", in, reply)
		}
	}
}

func TestNames(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	got := r.Names()
	want := []string{"CLIENT", "COMMAND", "DEL", "ECHO", "EXISTS", "EXPIRE", "GET", "PERSIST", "PING", "SAVE", "SET", "TTL"}

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
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	for _, name := range []string{"PING", "ECHO", "SET", "GET", "DEL", "EXISTS", "EXPIRE", "TTL", "PERSIST", "SAVE", "CLIENT", "COMMAND"} {
		got, fault := r.Dispatch(protocol.Array{
			protocol.BulkString([]byte(name)),
			protocol.BulkString([]byte("a")),
			protocol.BulkString([]byte("b")),
		})
		if fault != nil {
			t.Fatalf("%s reported an internal fault: %v", name, fault)
		}

		if errValue, isErr := got.(protocol.Error); isErr {
			// A registered command named the command in its reply; an
			// unregistered one produces exactly the unknown-command error.
			if strings.Contains(string(errValue), "unknown command") {
				t.Fatalf("%s is not registered: %#v", name, got)
			}
		}
	}

	want := []string{"CLIENT", "COMMAND", "DEL", "ECHO", "EXISTS", "EXPIRE", "GET", "PERSIST", "PING", "SAVE", "SET", "TTL"}
	if names := r.Names(); !reflect.DeepEqual(names, want) {
		t.Fatalf("DefaultRegistry names = %v, want %v", names, want)
	}
}

// TestDispatchIsSafeForConcurrentUse matters because the server dispatches
// from one goroutine per connection. Under -race this fails loudly if the map
// is read without the lock.
func TestDispatchIsSafeForConcurrentUse(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	const goroutines = 8
	const iterations = 200

	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < iterations; j++ {
				// Concurrent reads of the registry map...
				_, _ = r.Dispatch(protocol.Array{protocol.BulkString([]byte("PING"))})
				_, _ = r.Dispatch(protocol.Array{protocol.BulkString([]byte("ECHO")), protocol.BulkString([]byte("x"))})
				_, _ = r.Dispatch(protocol.Array{protocol.BulkString([]byte("NOPE"))})

				// ...and concurrent writes and reads of the store those
				// handlers share, which is the new surface this milestone
				// adds to the same goroutine fan-out.
				_, _ = r.Dispatch(protocol.Array{
					protocol.BulkString([]byte("SET")),
					protocol.BulkString([]byte("shared")),
					protocol.BulkString([]byte("value")),
				})
				_, _ = r.Dispatch(protocol.Array{
					protocol.BulkString([]byte("GET")),
					protocol.BulkString([]byte("shared")),
				})
				_, _ = r.Dispatch(protocol.Array{
					protocol.BulkString([]byte("EXISTS")),
					protocol.BulkString([]byte("shared")),
				})

				_ = r.Names()
			}
		}()
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

// TestRegisterWriteMarksAndRegisterDoesNot is the distinction the AOF rests on:
// a write command must be identifiable so the server can log it, and a read
// command must not be, so the log stays a record of changes rather than of
// traffic.
func TestRegisterWriteMarksAndRegisterDoesNot(t *testing.T) {
	r := NewRegistry()
	noop := func([]protocol.Value) (protocol.Value, error) { return protocol.SimpleString("OK"), nil }

	r.Register("READS", noop)
	r.RegisterWrite("WRITES", noop)

	if !r.IsWrite("WRITES") {
		t.Error("IsWrite(WRITES) = false after RegisterWrite, want true")
	}
	if r.IsWrite("READS") {
		t.Error("IsWrite(READS) = true after Register, want false")
	}
}

// TestIsWriteIsCaseInsensitive matters because the server calls it with the
// command name exactly as the client sent it — redis-cli sends lower case —
// while RegisterWrite stores the uppercased form. A case-sensitive lookup
// would silently stop logging every command from a normal client.
func TestIsWriteIsCaseInsensitive(t *testing.T) {
	r := NewRegistry()
	r.RegisterWrite("SET", func([]protocol.Value) (protocol.Value, error) {
		return protocol.SimpleString("OK"), nil
	})

	for _, name := range []string{"SET", "set", "Set", "sEt"} {
		if !r.IsWrite(name) {
			t.Errorf("IsWrite(%q) = false, want true: the lookup must be case-insensitive", name)
		}
	}
	if r.IsWrite("SETX") {
		t.Error("IsWrite(SETX) = true, want false; a prefix of a registered name is not registered")
	}
}

// TestIsWriteOnUnknownCommand pins the safe default. An unregistered name is
// not a write, so a command nobody classified is never logged — under-logging
// loses one command after a restart, while over-logging replays operations
// that never happened.
func TestIsWriteOnUnknownCommand(t *testing.T) {
	r := NewRegistry()

	if r.IsWrite("NOPE") {
		t.Error("IsWrite on an empty registry = true, want false")
	}

	r.Register("PING", Ping)
	if r.IsWrite("PING") {
		t.Error("IsWrite(PING) = true for a registry that never called RegisterWrite")
	}
}

// TestDefaultRegistryClassifiesWrites pins the classification itself. It is a
// literal list rather than a property, because the point is to catch a command
// being added to DefaultRegistry without a decision about whether it belongs
// in the log — the failure would otherwise be a write that silently stops
// surviving a restart.
func TestDefaultRegistryClassifiesWrites(t *testing.T) {
	r := DefaultRegistry(store.New(store.RealClock{}), nil)

	writes := []string{"SET", "DEL", "EXPIRE", "PERSIST"}
	for _, name := range writes {
		if !r.IsWrite(name) {
			t.Errorf("IsWrite(%s) = false, want true: %s changes the keyspace", name, name)
		}
	}

	// SAVE is the deliberate omission. It writes a file, but it does not
	// change the keyspace, so logging it would make the log's meaning depend
	// on when a snapshot happened. TTL is a query.
	reads := []string{"PING", "ECHO", "GET", "EXISTS", "TTL", "CLIENT", "COMMAND", "SAVE"}
	for _, name := range reads {
		if r.IsWrite(name) {
			t.Errorf("IsWrite(%s) = true, want false: %s does not change the keyspace", name, name)
		}
	}

	// Every write must also be dispatchable. A name marked as a write but
	// never registered would be logged by the server and then fail to replay.
	for _, name := range writes {
		if _, fault := r.Dispatch(protocol.Array{protocol.BulkString([]byte(name))}); fault != nil {
			t.Errorf("Dispatch(%s) reported a fault: %v", name, fault)
		}
	}
}
