package commands

import "github.com/Oussama-Att/verdigris/internal/protocol"

// SaveFunc is the interface SAVE uses to trigger a save. Decoupling the
// handler from the server keeps the handler unit-testable without a running
// server.
//
// The return value is an error rather than a bool because every failure here
// is a disk failure: the path is missing, the directory is gone, the disk is
// full, the rename was refused. None of those are things a client can act on
// by changing its request, so they travel the internal-fault channel and the
// client sees a generic error while the operator sees the real one in the log.
type SaveFunc func() error

// SaveHandler returns a Handler for the SAVE command.
//
//	SAVE -> SimpleString("OK")
//
// Arity must be exactly 1. SAVE takes no arguments in Redis, and a client
// sending one has a bug worth reporting rather than ignoring.
//
// The save is synchronous: the reply arrives after the snapshot is durable, so
// a client that sees OK can rely on the data having reached disk. That is the
// distinction from BGSAVE, which this server does not implement — a client
// sending BGSAVE receives ERR unknown command, which is honest about the fact
// that no background save exists rather than silently doing a blocking one.
func SaveHandler(save SaveFunc) Handler {
	return func(args []protocol.Value) (protocol.Value, error) {
		if len(args) != 1 {
			return wrongArityReply("save")
		}

		if err := save(); err != nil {
			// (nil, err) is the internal-fault shape: the read loop logs the
			// real error and answers ERR internal error. Returning a
			// protocol.Error here instead would send the filesystem's own
			// message to the client, which is both a leak and useless to it.
			return nil, err
		}

		return protocol.SimpleString("OK"), nil
	}
}
