package commands

import (
	"strings"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// ClientHandler returns a Handler for the CLIENT command.
//
// Only one subcommand is implemented, and deliberately so:
//
//	CLIENT SETINFO <attr> <value> -> SimpleString("OK")
//
// Redis 7 clients (redis-py, go-redis, Lettuce, and others) send CLIENT
// SETINFO on connect to announce their library name and version. A server that
// answers with an unknown-subcommand error is still usable, but the client
// logs a warning on every connection, which is noise the user cannot act on.
// Answering OK is the compatibility no-op that makes those clients quiet.
//
// Every other subcommand reports an error rather than pretending to work.
// CLIENT SETNAME, CLIENT GETNAME, CLIENT ID and CLIENT INFO all have real
// semantics this server does not implement, and silently accepting them would
// be worse than refusing: a client that sets a name and later reads it back
// would get nonsense.
func ClientHandler() Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) < 2 {
			return wrongArityReply("client")
		}

		sub, ok := args[1].(protocol.BulkString)
		if !ok {
			return invalidArgumentReply("client")
		}

		// Subcommand names are case-insensitive, like command names.
		switch strings.ToUpper(string(sub)) {
		case "SETINFO":
			// The remaining arguments are not validated. Real Redis requires
			// exactly <attr> <value>, but the point of this handler is to keep
			// modern clients quiet, and rejecting a variant we have not seen
			// would reintroduce the warning it exists to suppress.
			return protocol.SimpleString("OK"), nil

		default:
			return protocol.Error("ERR unknown subcommand '" + string(sub) + "'"), nil
		}
	}
}
