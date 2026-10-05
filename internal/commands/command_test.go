package commands

import (
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// defaultRegistryForTest builds the registry the server uses. It exists so the
// wiring tests read the same either way the store is obtained.
func defaultRegistryForTest() *Registry {
	return DefaultRegistry(store.New(store.RealClock{}))
}

// TestCommandReturnsEmptyArray covers the stub. Real Redis returns command
// metadata; an empty array is enough that a probing client learns "no
// commands advertised" instead of receiving an error and failing the
// connection.
func TestCommandReturnsEmptyArray(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
	}{
		{"bare COMMAND", args("COMMAND")},
		{"COMMAND DOCS", args("COMMAND", "DOCS")},
		{"COMMAND DOCS with a name", args("COMMAND", "DOCS", "GET")},
		{"COMMAND COUNT", args("COMMAND", "COUNT")},
		{"COMMAND INFO", args("COMMAND", "INFO", "GET")},
		{"COMMAND INFO with several names", args("COMMAND", "INFO", "GET", "SET", "DEL")},
		{"COMMAND LIST", args("COMMAND", "LIST")},
		{"an unknown subcommand is still an empty array", args("COMMAND", "FOO")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fault := CommandHandler()(tt.args)
			if fault != nil {
				t.Fatalf("COMMAND reported an internal fault: %v", fault)
			}

			arr, ok := got.(protocol.Array)
			if !ok {
				t.Fatalf("COMMAND %v = %#v (%T), want protocol.Array", tt.args[1:], got, got)
			}
			if len(arr) != 0 {
				t.Fatalf("COMMAND %v = %#v, want an empty array", tt.args[1:], arr)
			}
		})
	}
}

// TestCommandEncodesAsAnEmptyArray pins the wire form, which is the thing a
// probing client actually parses: `*0\r\n`.
func TestCommandEncodesAsAnEmptyArray(t *testing.T) {
	got, fault := CommandHandler()(args("COMMAND", "DOCS"))
	if fault != nil {
		t.Fatalf("COMMAND reported an internal fault: %v", fault)
	}

	if wire := string(protocol.Encode(got)); wire != "*0\r\n" {
		t.Fatalf("encoded COMMAND reply = %q, want %q", wire, "*0\r\n")
	}
}

// TestCommandAcceptsAStrippedArgumentList covers the len(args) == 0 case,
// which Dispatch rejects before reaching a handler but a direct call can
// produce.
func TestCommandAcceptsAStrippedArgumentList(t *testing.T) {
	got, fault := CommandHandler()(nil)
	if fault != nil {
		t.Fatalf("COMMAND reported an internal fault: %v", fault)
	}

	arr, ok := got.(protocol.Array)
	if !ok {
		t.Fatalf("COMMAND with no arguments = %#v (%T), want protocol.Array", got, got)
	}
	if len(arr) != 0 {
		t.Fatalf("COMMAND with no arguments = %#v, want an empty array", arr)
	}
}

// TestCommandDoesNotDependOnArgumentTypes checks the stub ignores its
// arguments entirely. A COMMAND probe with a malformed subcommand should still
// get the empty array rather than an error, because the reply is the same for
// every form and there is nothing to reject.
func TestCommandDoesNotDependOnArgumentTypes(t *testing.T) {
	inputs := []protocol.Value{
		protocol.Integer(1),
		protocol.SimpleString("DOCS"),
		protocol.Null{},
		protocol.Array{protocol.BulkString([]byte("DOCS"))},
	}

	for _, in := range inputs {
		t.Run(commandsTypeName(in), func(t *testing.T) {
			got, fault := CommandHandler()([]protocol.Value{protocol.BulkString([]byte("COMMAND")), in})
			if fault != nil {
				t.Fatalf("COMMAND reported an internal fault: %v", fault)
			}
			if _, ok := got.(protocol.Array); !ok {
				t.Fatalf("COMMAND with %T argument = %#v (%T), want protocol.Array", in, got, got)
			}
		})
	}
}

// TestCommandIsRegisteredInTheDefaultRegistry checks the wiring.
func TestCommandIsRegisteredInTheDefaultRegistry(t *testing.T) {
	r := defaultRegistryForTest()

	got, fault := r.Dispatch(protocol.Array{
		protocol.BulkString([]byte("COMMAND")),
		protocol.BulkString([]byte("DOCS")),
	})
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}

	arr, ok := got.(protocol.Array)
	if !ok {
		t.Fatalf("COMMAND DOCS through Dispatch = %#v (%T), want protocol.Array", got, got)
	}
	if len(arr) != 0 {
		t.Fatalf("COMMAND DOCS through Dispatch = %#v, want an empty array", arr)
	}
}
