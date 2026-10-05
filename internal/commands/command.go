package commands

import "github.com/Oussama-Att/verdigris/internal/protocol"

// CommandHandler returns a Handler for the COMMAND command.
//
//	COMMAND [subcommand ...] -> Array([]Value{})
//
// This is a stub, and an empty array is a lie of omission rather than of
// substance: real Redis returns a description of every command it supports,
// and clients use COMMAND DOCS to discover them. Answering with nothing means
// a probing client learns "this server advertises no commands" instead of
// receiving an error, which is enough to stop it from failing the connection
// while making no promise this server cannot keep.
//
// Every arity is accepted, including the bare command, because the reply is
// the same for all of them and there is no form worth rejecting. Building the
// real reply is a later milestone; it would mean giving the Registry a way to
// describe its handlers, which is a design change rather than a data change.
func CommandHandler() Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		return protocol.Array{}, nil
	}
}
