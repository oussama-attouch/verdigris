package commands

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// TestEchoHappyPath is the plain case the milestone's demo relies on.
func TestEchoHappyPath(t *testing.T) {
	got, fault := Echo(args("ECHO", "hello verdigris"))
	if fault != nil {
		t.Fatalf("Echo reported an internal fault: %v", fault)
	}

	want := protocol.BulkString([]byte("hello verdigris"))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Echo = %#v, want %#v", got, want)
	}
}

// TestEchoWrongArity covers every count that is not exactly two, including
// zero arguments, which can only be reached by calling the handler directly
// because Dispatch rejects an empty array first.
func TestEchoWrongArity(t *testing.T) {
	tests := []struct {
		name string
		args []protocol.Value
	}{
		{"no arguments", nil},
		{"name only", args("ECHO")},
		{"too many arguments", args("ECHO", "a", "b")},
		{"far too many arguments", args("ECHO", "a", "b", "c", "d")},
	}

	const want = "ERR wrong number of arguments for 'echo' command"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, fault := Echo(tt.args)
			if fault != nil {
				t.Fatalf("Echo(%v) reported an internal fault: %v", tt.args, fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("Echo(%v) = %#v, want protocol.Error", tt.args, got)
			}
			if string(errValue) != want {
				t.Fatalf("Echo(%v) = %q, want %q", tt.args, errValue, want)
			}
		})
	}
}

// TestEchoEmptyMessageIsNotAnError distinguishes "no message" from "an empty
// message". The first is an arity error; the second is a legitimate request
// for an empty bulk string, and the reply is `$0\r\n\r\n`, not a null.
func TestEchoEmptyMessageIsNotAnError(t *testing.T) {
	got, fault := Echo(args("ECHO", ""))
	if fault != nil {
		t.Fatalf("Echo reported an internal fault: %v", fault)
	}

	if _, isErr := got.(protocol.Error); isErr {
		t.Fatalf("Echo with an empty message returned an error: %#v", got)
	}

	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("Echo returned %T, want BulkString", got)
	}
	if bs == nil {
		t.Fatal("Echo returned a nil BulkString, want an empty one")
	}
	if len(bs) != 0 {
		t.Fatalf("Echo returned %q, want empty", bs)
	}

	const wantWire = "$0\r\n\r\n"
	if wire := string(protocol.Encode(got)); wire != wantWire {
		t.Fatalf("encoded reply = %q, want %q", wire, wantWire)
	}
}

// TestEchoRejectsNonBulkMessage covers the type check. A client sending an
// integer or a nested array where a message belongs has a bug, and the reply
// should say so rather than coercing the value.
func TestEchoRejectsNonBulkMessage(t *testing.T) {
	inputs := []protocol.Value{
		protocol.Integer(1),
		protocol.SimpleString("hi"),
		protocol.Array{protocol.BulkString([]byte("hi"))},
		protocol.Null{},
		protocol.BulkString(nil),
		nil,
	}

	const want = "ERR invalid argument for 'echo' command"

	for _, in := range inputs {
		t.Run(commandsTypeName(in), func(t *testing.T) {
			got, fault := Echo([]protocol.Value{protocol.BulkString([]byte("ECHO")), in})
			if fault != nil {
				t.Fatalf("Echo reported an internal fault: %v", fault)
			}

			errValue, ok := got.(protocol.Error)
			if !ok {
				t.Fatalf("Echo with %T argument = %#v, want protocol.Error", in, got)
			}
			if string(errValue) != want {
				t.Fatalf("Echo with %T argument = %q, want %q", in, errValue, want)
			}
		})
	}
}

// TestEchoRoundTripsThroughTheWire is the end-to-end shape of the command,
// minus the socket: encode a request, decode it, dispatch it, encode the
// reply, and check the exact bytes. This is the sequence the server performs,
// so a break anywhere in the protocol or command layer shows up here.
func TestEchoRoundTripsThroughTheWire(t *testing.T) {
	request := protocol.Array{
		protocol.BulkString([]byte("ECHO")),
		protocol.BulkString([]byte("hello verdigris")),
	}

	wire := protocol.Encode(request)

	decoded, n, err := protocol.Decode(wire)
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", wire, err)
	}
	if n != len(wire) {
		t.Fatalf("Decode consumed %d of %d bytes", n, len(wire))
	}

	reply, fault := DefaultRegistry(store.New()).Dispatch(decoded)
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}

	const want = "$15\r\nhello verdigris\r\n"
	if got := string(protocol.Encode(reply)); got != want {
		t.Fatalf("reply wire = %q, want %q", got, want)
	}
}

// TestEchoBinarySafeThroughTheWire repeats that sequence with a payload that
// contains the frame terminator and bytes that are not valid UTF-8, which is
// the case a length-prefixed protocol exists to handle.
func TestEchoBinarySafeThroughTheWire(t *testing.T) {
	payload := []byte{0x00, 0xff, '\r', '\n', 0xfe, '$', '*', 0x00}

	request := protocol.Array{
		protocol.BulkString([]byte("ECHO")),
		protocol.BulkString(payload),
	}

	wire := protocol.Encode(request)

	decoded, _, err := protocol.Decode(wire)
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	reply, fault := DefaultRegistry(store.New()).Dispatch(decoded)
	if fault != nil {
		t.Fatalf("Dispatch reported an internal fault: %v", fault)
	}
	replyWire := protocol.Encode(reply)

	// Decoding the reply must give back exactly the payload that went in.
	roundTripped, n, err := protocol.Decode(replyWire)
	if err != nil {
		t.Fatalf("Decode(reply) error: %v (bytes %q)", err, replyWire)
	}
	if n != len(replyWire) {
		t.Fatalf("Decode(reply) consumed %d of %d bytes", n, len(replyWire))
	}

	bs, ok := roundTripped.(protocol.BulkString)
	if !ok {
		t.Fatalf("reply decoded to %T, want BulkString", roundTripped)
	}
	if !bytes.Equal(bs, payload) {
		t.Fatalf("reply payload = % x, want % x", bs, payload)
	}
}
