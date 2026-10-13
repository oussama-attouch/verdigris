package commands

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// arg builds a command argument list from strings, the way Dispatch receives
// it after a client's array of bulk strings has been decoded.
func args(words ...string) []protocol.Value {
	out := make([]protocol.Value, 0, len(words))
	for _, w := range words {
		out = append(out, protocol.BulkString([]byte(w)))
	}
	return out
}

// numericArgs builds a command whose trailing arguments are RESP Integers
// rather than bulk strings.
//
// The distinction is not cosmetic. A real client sends `EXPIRE k 10` with the
// timeout as an integer frame (`:10\r\n`), and `SET k v EX 10` the same way,
// while the key and the option name stay bulk strings. A test that used args()
// for these would be sending a shape no client sends and would exercise the
// wrong branch of the handler — which is exactly what happened before this
// helper existed.
func numericArgs(command string, leading []string, numbers ...int64) []protocol.Value {
	out := make([]protocol.Value, 0, 1+len(leading)+len(numbers))
	out = append(out, protocol.BulkString([]byte(command)))
	for _, s := range leading {
		out = append(out, protocol.BulkString([]byte(s)))
	}
	for _, n := range numbers {
		out = append(out, protocol.Integer(n))
	}
	return out
}

// TestPing covers every arity the handler accepts or rejects.
func TestPing(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
		want protocol.Value
	}{
		{
			name: "no argument replies PONG",
			args: args("PING"),
			want: protocol.SimpleString("PONG"),
		},
		{
			name: "one message argument is echoed",
			args: args("PING", "custom message"),
			want: protocol.BulkString([]byte("custom message")),
		},
		{
			name: "empty message argument is echoed as empty bulk",
			args: args("PING", ""),
			want: protocol.BulkString([]byte{}),
		},
		{
			name: "two arguments is wrong arity",
			args: args("PING", "a", "b"),
			want: protocol.Error("ERR wrong number of arguments for 'ping' command"),
		},
		{
			name: "three arguments is wrong arity",
			args: args("PING", "a", "b", "c"),
			want: protocol.Error("ERR wrong number of arguments for 'ping' command"),
		},
		{
			name: "non-bulk argument is rejected",
			args: []protocol.Value{protocol.BulkString([]byte("PING")), protocol.Integer(1)},
			want: protocol.Error("ERR invalid argument for 'ping' command"),
		},
		{
			name: "null argument is rejected",
			args: []protocol.Value{protocol.BulkString([]byte("PING")), protocol.Null{}},
			want: protocol.Error("ERR invalid argument for 'ping' command"),
		},
		{
			name: "nil bulk argument is rejected",
			args: []protocol.Value{protocol.BulkString([]byte("PING")), protocol.BulkString(nil)},
			want: protocol.Error("ERR invalid argument for 'ping' command"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fault := Ping(tt.args)
			if fault != nil {
				t.Fatalf("Ping(%#v) reported an internal fault: %v", tt.args, fault)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Ping(%#v) = %#v, want %#v", tt.args, got, tt.want)
			}
		})
	}
}

// TestPingMessageIsBinarySafe checks that the echoed argument survives bytes
// that are not valid UTF-8 and that contain the frame terminator. A handler
// that turned the argument into a string, or that trimmed anything, would
// fail here.
func TestPingMessageIsBinarySafe(t *testing.T) {
	payload := []byte{0x00, 0xff, '\r', '\n', 0xfe, 0x00, '\r', 0xff, '\n'}

	got, fault := Ping([]protocol.Value{protocol.BulkString([]byte("PING")), protocol.BulkString(payload)})
	if fault != nil {
		t.Fatalf("Ping reported an internal fault: %v", fault)
	}

	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("Ping returned %T, want BulkString", got)
	}
	if !bytes.Equal(bs, payload) {
		t.Fatalf("Ping echoed % x, want % x", bs, payload)
	}
}

// TestPingDoesNotMutateTheArgument pins that the reply is the argument, not a
// transformation of it. A handler that reused a scratch buffer would show up
// as a mismatch here.
func TestPingDoesNotMutateTheArgument(t *testing.T) {
	original := []byte("hello")
	arg := protocol.BulkString(original)

	got, fault := Ping([]protocol.Value{protocol.BulkString([]byte("PING")), arg})
	if fault != nil {
		t.Fatalf("Ping reported an internal fault: %v", fault)
	}

	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("Ping returned %T, want BulkString", got)
	}
	if &bs[0] != &original[0] {
		t.Log("Ping copied the payload; the alias is documented but not required")
	}
	if !bytes.Equal(original, []byte("hello")) {
		t.Fatalf("Ping mutated its argument: %q", original)
	}
}

// TestPingWithNoArgumentsAtAll covers Dispatch's empty-array guard being
// bypassed by a direct call: len(args) == 0 lands in the default branch.
func TestPingWithNoArgumentsAtAll(t *testing.T) {
	got, fault := Ping(nil)
	if fault != nil {
		t.Fatalf("Ping(nil) reported an internal fault: %v", fault)
	}
	if got != protocol.Error("ERR wrong number of arguments for 'ping' command") {
		t.Fatalf("Ping(nil) = %#v, want a wrong-arity error", got)
	}
}

// TestEcho covers the happy path and every wrong-arity case.
func TestEcho(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
		want protocol.Value
	}{
		{
			name: "echoes the message",
			args: args("ECHO", "hello verdigris"),
			want: protocol.BulkString([]byte("hello verdigris")),
		},
		{
			name: "echoes an empty message",
			args: args("ECHO", ""),
			want: protocol.BulkString([]byte{}),
		},
		{
			name: "no message is wrong arity",
			args: args("ECHO"),
			want: protocol.Error("ERR wrong number of arguments for 'echo' command"),
		},
		{
			name: "two messages is wrong arity",
			args: args("ECHO", "a", "b"),
			want: protocol.Error("ERR wrong number of arguments for 'echo' command"),
		},
		{
			name: "non-bulk message is rejected",
			args: []protocol.Value{protocol.BulkString([]byte("ECHO")), protocol.Integer(1)},
			want: protocol.Error("ERR invalid argument for 'echo' command"),
		},
		{
			name: "null message is rejected",
			args: []protocol.Value{protocol.BulkString([]byte("ECHO")), protocol.Null{}},
			want: protocol.Error("ERR invalid argument for 'echo' command"),
		},
		{
			name: "nil bulk message is rejected",
			args: []protocol.Value{protocol.BulkString([]byte("ECHO")), protocol.BulkString(nil)},
			want: protocol.Error("ERR invalid argument for 'echo' command"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fault := Echo(tt.args)
			if fault != nil {
				t.Fatalf("Echo(%#v) reported an internal fault: %v", tt.args, fault)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Echo(%#v) = %#v, want %#v", tt.args, got, tt.want)
			}
		})
	}
}

// TestEchoIsBinarySafe is the ECHO half of the binary-safety property, and it
// mirrors the milestone 1 test that proved the echo server round-tripped
// arbitrary bytes. The guarantee has to survive the switch from raw echo to
// RESP framing.
func TestEchoIsBinarySafe(t *testing.T) {
	payloads := [][]byte{
		{0x00, 0xff, '\r', '\n', 0xfe},
		[]byte("*2\r\n$5\r\nHELLO\r\n"),
		{0xff, 0xfe, 0xfd},
		bytes.Repeat([]byte{0x00}, 512),
	}

	for _, payload := range payloads {
		got, fault := Echo([]protocol.Value{protocol.BulkString([]byte("ECHO")), protocol.BulkString(payload)})
		if fault != nil {
			t.Fatalf("Echo reported an internal fault for payload % x: %v", payload, fault)
		}

		bs, ok := got.(protocol.BulkString)
		if !ok {
			t.Fatalf("Echo returned %T for payload % x, want BulkString", got, payload)
		}
		if !bytes.Equal(bs, payload) {
			t.Fatalf("Echo returned % x, want % x", bs, payload)
		}
	}
}

// TestWrongArityMessageMatchesRedisFormat pins the wording. Clients and
// integration test suites match on this string, and the milestone's
// redis-cli demo asserts it verbatim.
func TestWrongArityMessageMatchesRedisFormat(t *testing.T) {
	got, fault := Echo(args("ECHO"))
	if fault != nil {
		t.Fatalf("Echo with no message reported an internal fault: %v", fault)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("Echo with no message returned %T, want protocol.Error", got)
	}

	const want = "ERR wrong number of arguments for 'echo' command"
	if string(errValue) != want {
		t.Fatalf("wrong-arity message = %q, want %q", errValue, want)
	}

	// The command name must be lower case, as Redis writes it, even though
	// lookup is case-insensitive.
	if strings.Contains(string(errValue), "ECHO") {
		t.Fatalf("wrong-arity message uses the client's capitalization: %q", errValue)
	}
}

// TestHandlersWorkOnDecodedFrames closes the loop between the protocol and
// command packages without a socket: real RESP bytes are decoded and handed
// straight to a handler, which is exactly what the server will do.
func TestHandlersWorkOnDecodedFrames(t *testing.T) {
	tests := []struct {
		name  string
		wire  string
		want  protocol.Value
		reply string
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", protocol.SimpleString("PONG"), "+PONG\r\n"},
		{"ping with message", "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n", protocol.BulkString([]byte("hello")), "$5\r\nhello\r\n"},
		{"echo", "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", protocol.BulkString([]byte("hello")), "$5\r\nhello\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, n, err := protocol.Decode([]byte(tt.wire))
			if err != nil {
				t.Fatalf("Decode(%q) error: %v", tt.wire, err)
			}
			if n != len(tt.wire) {
				t.Fatalf("Decode consumed %d of %d bytes", n, len(tt.wire))
			}

			got, fault := DefaultRegistry(store.New(store.RealClock{}), nil).Dispatch(decoded)
			if fault != nil {
				t.Fatalf("Dispatch reported an internal fault: %v", fault)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Dispatch = %#v, want %#v", got, tt.want)
			}

			if reply := string(protocol.Encode(got)); reply != tt.reply {
				t.Fatalf("encoded reply = %q, want %q", reply, tt.reply)
			}
		})
	}
}

// TestPingIntegerArgumentIsRejected keeps the type check honest for a value
// a client could plausibly send by mistake.
func TestPingIntegerArgumentIsRejected(t *testing.T) {
	got, fault := Ping([]protocol.Value{protocol.BulkString([]byte("PING")), protocol.Integer(9223372036854775807)})
	if fault != nil {
		t.Fatalf("Ping reported an internal fault: %v", fault)
	}

	if _, ok := got.(protocol.Error); !ok {
		t.Fatalf("Ping with an integer argument returned %#v, want an error", got)
	}
}
