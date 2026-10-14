// Package aof appends write commands to a log and replays them on startup.
package aof

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// ErrCorrupt is returned when the AOF's bytes violate the RESP grammar.
// A frame that is merely cut short is not corruption: it is the expected
// result of a crash mid-write, and Replay truncates the file to the last
// complete command and returns the valid prefix. Any grammar violation is
// fatal, wherever it appears, because no crash produces bytes that are
// present and wrong.
var ErrCorrupt = errors.New("aof: corrupt file")

// ErrClosed is returned by Append and Sync after Close. It exists so a
// caller that races a shutdown sees a definite "this log is finished"
// rather than a write that silently went nowhere.
var ErrClosed = errors.New("aof: closed")

// ErrEmptyCommand is returned by Append for a command with no arguments.
//
// A zero-argument command encodes as the perfectly valid frame `*0\r\n`,
// which replays as a command the dispatch layer cannot name. Refusing it at
// the log is the last point where the caller can still be told what it did,
// and it keeps the log's contents to frames that mean something.
var ErrEmptyCommand = errors.New("aof: empty command")

// FsyncPolicy controls when the AOF flushes its writes to durable
// storage.
type FsyncPolicy int

const (
	// FsyncAlways syncs after every Append. The most durable and the
	// slowest; a successful Append implies the bytes are on disk.
	FsyncAlways FsyncPolicy = iota

	// FsyncEverySec syncs from a background goroutine once per second.
	// A hard kill loses at most the last second of writes. Redis's
	// default.
	FsyncEverySec

	// FsyncNo never syncs except once on Close. The OS decides when to
	// flush. Fastest; least durable.
	FsyncNo
)

// String renders the policy as its CLI name.
//
// The three names are the contract ParseFsyncPolicy accepts, so String and
// Parse are exact inverses and a value that survives a round trip through
// text is unchanged.
func (p FsyncPolicy) String() string {
	switch p {
	case FsyncAlways:
		return "always"
	case FsyncEverySec:
		return "everysec"
	case FsyncNo:
		return "no"
	default:
		// An out-of-range policy has no CLI name. Rendering the number keeps
		// the result diagnosable instead of silently claiming to be "always",
		// which is what a bare zero-value fallthrough would do.
		return fmt.Sprintf("FsyncPolicy(%d)", int(p))
	}
}

// ParseFsyncPolicy maps the CLI names "always", "everysec", "no" to
// their policy. Any other value is an error.
//
// The match is case-sensitive. Accepting "ALWAYS" would mean the flag's
// accepted language is larger than the documented one, and a typo like
// "EverySec" would then be silently honoured rather than reported.
func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "always":
		return FsyncAlways, nil
	case "everysec":
		return FsyncEverySec, nil
	case "no":
		return FsyncNo, nil
	default:
		return FsyncNo, fmt.Errorf("aof: unknown fsync policy %q, want always, everysec, or no", s)
	}
}

// AOF is a concurrent-safe append-only log of RESP-encoded commands.
type AOF struct {
	mu     sync.Mutex
	f      *os.File
	policy FsyncPolicy
	stop   chan struct{}
	done   chan struct{}
	closed bool
}

// Open opens path for appending, creating it if it does not exist. If
// policy is FsyncEverySec, a background goroutine is started that syncs
// once per second; it is stopped by Close.
func Open(path string, policy FsyncPolicy) (*AOF, error) {
	// Append-only, so a reopened log continues where the last process left
	// off and never rewrites a byte a replayer has already consumed. 0644
	// matches the RDB file: readable by an operator or a RESP-aware tool,
	// writable only by the owner.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("aof: open %s: %w", path, err)
	}

	a := &AOF{f: f, policy: policy}

	if policy == FsyncEverySec {
		// The channels are created only when a goroutine will use them, so
		// Close's nil check is what decides whether there is anything to
		// stop — there is no separate boolean that could disagree with
		// reality.
		a.stop = make(chan struct{})
		a.done = make(chan struct{})
		go a.syncLoop()
	}

	return a, nil
}

// syncLoop fsyncs once per second until Close signals it to stop.
//
// The ticker's error is deliberately dropped. A failed fsync is worth
// knowing about, but there is no caller to return it to — this goroutine
// outlives every Append — and the alternative, stopping the loop, would
// trade a transient disk error for permanent silence on a log the operator
// believes is being synced every second. The next tick retries.
func (a *AOF) syncLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// done is closed exactly once, by this goroutine, which is what makes
	// Close's receive a reliable "the loop has exited" signal.
	defer close(a.done)

	for {
		select {
		case <-a.stop:
			return
		case <-ticker.C:
			_ = a.Sync()
		}
	}
}

// Append writes one RESP-encoded command. cmd is the argument list
// including the command name (cmd[0]). Under FsyncAlways, Append syncs
// before returning. Safe for concurrent use.
//
// The encode and the write happen under the same lock. Encoding outside it
// would let two goroutines interleave their bytes in one buffer, and the
// mutex is also what orders the writes themselves: os.File is safe for
// concurrent use, but the contract here is stronger than "no torn bytes" —
// the log must contain each command as one uninterrupted frame.
func (a *AOF) Append(cmd []protocol.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return fmt.Errorf("aof: append: %w", ErrClosed)
	}

	if len(cmd) == 0 {
		// Checked under the lock, after the closed check, so a caller racing
		// a shutdown is told the log is closed rather than being sent to fix
		// an argument that was never going to be written.
		return fmt.Errorf("aof: append: %w", ErrEmptyCommand)
	}

	frame := protocol.Encode(protocol.Array(cmd))
	if frame == nil {
		// Encode returns nil only for a nil Value, and an Array — including
		// the empty one rejected above — always encodes to bytes. Guarding
		// anyway keeps a silent zero-byte append, which would replay as a
		// command that never happened, from ever reaching the file.
		return errors.New("aof: encode command: nothing to write")
	}

	// One Write for the whole frame. A partial write would leave a
	// half-command in the log, which Replay would have to treat as the
	// truncated tail it is — but writing it in one call means a short write
	// only comes from a genuine I/O failure, which is reported here.
	if _, err := a.f.Write(frame); err != nil {
		return fmt.Errorf("aof: append: %w", err)
	}

	if a.policy == FsyncAlways {
		if err := a.f.Sync(); err != nil {
			return fmt.Errorf("aof: append sync: %w", err)
		}
	}

	return nil
}

// Sync flushes the underlying file to durable storage. Idempotent.
//
// The lock is held across the fsync rather than released after reading the
// file handle. File.Sync on a closed descriptor is not a documented
// success, and the alternative — snapshotting the handle, unlocking, then
// syncing — lets Close close the descriptor in the gap. Blocking an
// appender for the duration of a flush is the cheaper of the two costs.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return fmt.Errorf("aof: sync: %w", ErrClosed)
	}
	if err := a.f.Sync(); err != nil {
		return fmt.Errorf("aof: sync: %w", err)
	}
	return nil
}

// Close flushes, syncs, and stops the everysec ticker if one is
// running. Idempotent.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Idempotent by early return, and the second caller gets nil rather
	// than a spurious "already closed" failure: shutdown paths often run
	// twice (a signal handler and a deferred call), and the second one has
	// nothing to report.
	if a.closed {
		return nil
	}
	a.closed = true

	if a.stop != nil {
		// Closing the channel, then waiting for done, is what guarantees
		// the ticker goroutine cannot call Sync after the file is closed
		// below. Without the wait, Close's own Sync and a racing tick could
		// touch the descriptor at the same time.
		close(a.stop)
		<-a.done
	}

	// Sync before close: the final second of writes under everysec, and
	// everything under no, would otherwise be handed to the OS page cache
	// and lost on a hard kill. The error is still reported, because the
	// caller asking to shut down cleanly deserves to know the last flush
	// failed.
	err := a.f.Sync()

	if cerr := a.f.Close(); err == nil {
		err = cerr
	}

	if err != nil {
		return fmt.Errorf("aof: close: %w", err)
	}
	return nil
}

// Replay reads path and returns the commands it contains, in order. A
// truncated tail — a partial command at the end of the file — is
// expected after a crash: the file is truncated to the last complete
// command and the valid prefix is returned. A protocol error is
// genuine corruption and is wrapped in ErrCorrupt. A missing file
// returns an error wrapping os.ErrNotExist.
func Replay(path string) ([][]protocol.Value, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		// Wrapped, not swallowed: the caller separates "this is the first
		// run" from "this log is unreadable" with errors.Is(err,
		// os.ErrNotExist), and that survives the wrap.
		return nil, fmt.Errorf("aof: read %s: %w", path, err)
	}

	cmds := [][]protocol.Value{}
	consumed := 0

	for len(data) > 0 {
		v, n, err := protocol.Decode(data)
		switch {
		case err == nil:
			// The file is defined as RESP arrays, so anything else at a
			// frame boundary is not a command this log could have written.
			// Unlike a protocol error this cannot be blamed on a torn tail;
			// the bytes parsed perfectly and are still wrong.
			arr, ok := v.(protocol.Array)
			if !ok {
				return nil, fmt.Errorf("%w: frame at offset %d is %T, want an array", ErrCorrupt, consumed, v)
			}
			cmds = append(cmds, []protocol.Value(arr))
			consumed += n
			data = data[n:]

		case errors.Is(err, protocol.ErrIncomplete):
			// The tail. A crash mid-write leaves a partial command here and
			// nowhere else, so this is the one place a short frame is
			// ordinary rather than corrupt. Rewriting the file to end at
			// the last complete command means the next Open appends after
			// the garbage instead of preserving it forever, and the next
			// Replay has a clean file to read.
			//
			// len(data) is necessarily positive here: the loop condition
			// guarantees it, and ErrIncomplete guarantees Decode consumed
			// none of it. So there are always trailing bytes to cut.
			if err := os.Truncate(path, int64(consumed)); err != nil {
				return nil, fmt.Errorf("aof: truncate %s to %d bytes: %w", path, consumed, err)
			}
			return cmds, nil

		default:
			// Anything else is corruption, wherever it sits. ErrIncomplete is
			// the only signal that means "the file stops in the middle of a
			// frame": the decoder reports it precisely when it ran out of
			// bytes, which can only happen at the end of the file. A grammar
			// violation is a different fact — bytes are present and wrong —
			// and no crash produces it, so it is never treated as a torn
			// tail, even when it happens to be the last thing in the file.
			//
			// Being strict here is what makes the tail rule sound. A log that
			// truncated its garbage would silently discard a region it could
			// not read, and the caller would get a shorter keyspace with no
			// indication that anything was dropped.
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}

	return cmds, nil
}
