package commands

import (
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// TestClientSetInfoReturnsOK covers the compatibility no-op. Modern Redis
// clients announce themselves with CLIENT SETINFO on connect; answering OK
// keeps them from logging a warning the user cannot act on.
func TestClientSetInfoReturnsOK(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
	}{
		{"lib-name", args("CLIENT", "SETINFO", "lib-name", "foo")},
		{"lib-ver", args("CLIENT", "SETINFO", "lib-ver", "1.0")},
		{"redis-cli's own announcement", args("CLIENT", "SETINFO", "lib-name", "redis-cli")},
		// Real clients send both attributes, and the value may be empty.
		{"empty value", args("CLIENT", "SETINFO", "lib-name", "")},
		{"extra arguments are tolerated", args("CLIENT", "SETINFO", "lib-name", "foo", "extra")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fault := ClientHandler()(tt.args)
			if fault != nil {
				t.Fatalf("CLIENT reported an internal fault: %v", fault)
			}
			if got != protocol.SimpleString("OK") {
				t.Fatalf("CLIENT %v = %#v, want SimpleString(\"OK\")", tt.args[1:], got)
			}
		})
	}
}

// TestClientSetInfoIsCaseInsensitive matches command-name lookup: subcommands
// are case-insensitive in Redis too, and a client sending `setinfo` must not
// get an error where `SETINFO` would have been accepted.
func TestClientSetInfoIsCaseInsensitive(t *testing.T) {
	for _, sub := range []string{"SETINFO", "setinfo", "SetInfo", "sEtInFo"} {
		t.Run(sub, func(t *testing.T) {
			got, fault := ClientHandler()(args("CLIENT", sub, "lib-name", "foo"))
			if fault != nil {
				t.Fatalf("CLIENT reported an internal fault: %v", fault)
			}
			if got != protocol.SimpleString("OK") {
				t.Fatalf("CLIENT %s = %#v, want OK", sub, got)
			}
		})
	}
}

// TestClientUnknownSubcommand pins that unimplemented subcommands are refused
// rather than silently accepted. CLIENT SETNAME and CLIENT GETNAME have real
// semantics this server does not implement, and pretending otherwise would
// hand a client nonsense on the read-back.
func TestClientUnknownSubcommand(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
		want string
	}{
		{"FOO", args("CLIENT", "FOO"), "ERR unknown subcommand 'FOO'"},
		{"SETNAME", args("CLIENT", "SETNAME", "myname"), "ERR unknown subcommand 'SETNAME'"},
		{"GETNAME", args("CLIENT", "GETNAME"), "ERR unknown subcommand 'GETNAME'"},
		{"ID", args("CLIENT", "ID"), "ERR unknown subcommand 'ID'"},
		{"INFO", args("CLIENT", "INFO"), "ERR unknown subcommand 'INFO'"},
		{"empty subcommand", args("CLIENT", ""), "ERR unknown subcommand ''"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fault := ClientHandler()(tt.args)
			if fault != nil {
				t.Fatalf("CLIENT reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("CLIENT %s = %#v, want protocol.Error", tt.name, got)
			}
			if string(errValue) != tt.want {
				t.Fatalf("CLIENT %s = %q, want %q", tt.name, errValue, tt.want)
			}
		})
	}
}

// TestClientSubcommandKeepsClientCapitalization checks that the error echoes
// the subcommand as the client sent it. Redis does the same, and it makes the
// error easier to match against the client's own code.
func TestClientSubcommandKeepsClientCapitalization(t *testing.T) {
	got, fault := ClientHandler()(args("CLIENT", "notasub"))
	if fault != nil {
		t.Fatalf("CLIENT reported an internal fault: %v", fault)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("CLIENT = %#v, want protocol.Error", got)
	}
	if want := "ERR unknown subcommand 'notasub'"; string(errValue) != want {
		t.Fatalf("CLIENT notasub = %q, want %q", errValue, want)
	}
}

func TestClientWrongArity(t *testing.T) {
	t.Run("no subcommand", func(t *testing.T) {
		got, fault := ClientHandler()(args("CLIENT"))
		if fault != nil {
			t.Fatalf("CLIENT reported an internal fault: %v", fault)
		}

		errValue, ok := got.(protocol.Error)
		if !ok {
			t.Fatalf("bare CLIENT = %#v, want protocol.Error", got)
		}
		const want = "ERR wrong number of arguments for 'client' command"
		if string(errValue) != want {
			t.Fatalf("bare CLIENT = %q, want %q", errValue, want)
		}
	})

	t.Run("no arguments at all", func(t *testing.T) {
		got, fault := ClientHandler()(nil)
		if fault != nil {
			t.Fatalf("CLIENT reported an internal fault: %v", fault)
		}
		if _, ok := got.(protocol.Error); !ok {
			t.Fatalf("CLIENT with no arguments = %#v, want protocol.Error", got)
		}
	})
}

func TestClientRejectsNonBulkSubcommand(t *testing.T) {
	inputs := []protocol.Value{
		protocol.Integer(1),
		protocol.SimpleString("SETINFO"),
		protocol.Null{},
		protocol.Array{},
	}

	const want = "ERR invalid argument for 'client' command"

	for _, in := range inputs {
		t.Run(commandsTypeName(in), func(t *testing.T) {
			got, fault := ClientHandler()([]protocol.Value{protocol.BulkString([]byte("CLIENT")), in})
			if fault != nil {
				t.Fatalf("CLIENT reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("CLIENT with %T subcommand = %#v, want protocol.Error", in, got)
			}
			if string(errValue) != want {
				t.Fatalf("CLIENT with %T subcommand = %q, want %q", in, errValue, want)
			}
		})
	}
}

// TestClientIsRegisteredInTheDefaultRegistry checks the wiring, since a
// handler that exists but is never registered is invisible to clients.
func TestClientIsRegisteredInTheDefaultRegistry(t *testing.T) {
	r := defaultRegistryForTest()

	got, fault := r.Dispatch(protocol.Array{
		protocol.BulkString([]byte("CLIENT")),
		protocol.BulkString([]byte("SETINFO")),
		protocol.BulkString([]byte("lib-name")),
		protocol.BulkString([]byte("test")),
	})
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("CLIENT SETINFO through Dispatch = %#v, want OK", got)
	}
}
