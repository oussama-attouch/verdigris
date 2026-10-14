package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Oussama-Att/verdigris/internal/aof"
	"github.com/Oussama-Att/verdigris/internal/commands"
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/rdb"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// DefaultPort is the TCP port Verdigris listens on by default. 6379 is
// Redis's own default. Verdigris is not Redis, but the point of speaking
// RESP is that a client pointed at the default port should just work.
const DefaultPort = 6379

// drainPollInterval is how often Shutdown re-checks whether the handlers have
// finished. It bounds the delay between the last connection closing and
// Shutdown returning, and it is short enough to be invisible to a human
// pressing Ctrl-C while being long enough that a stuck handler costs nothing.
const drainPollInterval = 5 * time.Millisecond

// maxFrameSize bounds a single accumulated frame, in bytes.
//
// Without a bound, a client can send a bulk-string header declaring gigabytes
// and then dribble bytes forever: Decode keeps reporting ErrIncomplete, the
// accumulation buffer keeps growing, and one connection consumes all the
// memory on the machine. The declaration is not trustworthy — it is attacker
// input — so the ceiling is enforced on what has actually arrived.
//
// Redis's own proto-max-bulk-len defaults to 512 MB. This is deliberately
// smaller: nothing PING and ECHO can carry needs more, and the limit can be
// raised when a later milestone has commands whose payloads justify it.
const maxFrameSize = 64 << 20 // 64 MiB

// errFrameTooLarge reports a client that has sent more bytes without
// completing a frame than maxFrameSize allows. It is a sentinel so the check
// can be recognised, and it is deliberately phrased like the protocol
// package's errors because that is how it reaches the client: an ERR reply
// followed by a closed connection.
var errFrameTooLarge = errors.New("frame exceeds the configured limit")

// readBufSize is the size of the stack buffer a handler fills per read, in
// bytes. It is a starting point rather than a frame limit: the per-connection
// accumulation buffer grows past it for a large command, and a command may
// also span several reads.
const readBufSize = 4096

// Server is a TCP server that speaks RESP: it decodes requests, dispatches
// them through a command registry, and encodes the replies.
//
// A zero-value Server is not usable: it has no channels and no connection
// table. Always construct one with New.
type Server struct {
	// addr is the address passed to New, kept so Listen can report it and so
	// the binding decision is made in exactly one place.
	addr string

	// store is the keyspace every connection on this server shares, held so
	// that Shutdown can stop its expiration sweeper and so Save can read it.
	// The handlers also hold it, handed over through the registry; this
	// reference exists for lifecycle and persistence, not for command
	// dispatch.
	store *store.Store

	// rdbPath is the snapshot file, or empty when persistence is disabled.
	// An empty path makes both load and save no-ops rather than errors, so
	// the rest of the server never has to ask whether persistence is on.
	rdbPath string

	// aofPath is the append-only log, or empty when it is disabled. It is
	// kept alongside rdbPath rather than replacing it because the two are
	// not alternatives for the whole process lifetime: the log is the
	// startup source of truth, while SAVE still writes snapshots to rdbPath
	// on demand.
	aofPath string

	// aof is the open log every connection appends write commands to. It is
	// nil when no log is configured, which is the check the read loop makes.
	//
	// It is set by New after the existing log has been replayed, so the
	// server never appends to a file whose contents it has not yet applied —
	// doing so would interleave this run's writes with a previous run's
	// unread ones and make the log unreplayable.
	aof *aof.AOF

	// registry routes decoded commands to handlers. It is set by New and read
	// by every connection goroutine; a Registry is safe for concurrent use,
	// which is what makes that sharing safe.
	//
	// Tests in this package may replace it with a registry holding their own
	// handlers.
	registry *commands.Registry

	// mu guards ln. It is written by Listen and read by Serve, Addr, and
	// Shutdown, which may all run on different goroutines — cmd/verdigris
	// calls Shutdown from its signal-handling path.
	mu sync.Mutex

	// ln is the listening socket. It is nil until Listen succeeds and is set
	// back to nil by Shutdown, so the field can never be used to reach a
	// listener that has already been closed.
	ln net.Listener

	// conns holds every connection currently being handled, keyed by the
	// net.Conn value itself. Shutdown uses it to force-close connections that
	// are still open when the shutdown deadline expires; without that, a
	// client that connects and then goes silent would block a handler on a
	// read forever, and no amount of waiting would end the process.
	//
	// sync.Map rather than map+mutex because this is exactly its documented
	// use case: keys are written once by one goroutine and read rarely, and
	// the hot path is the per-connection Add/Delete with no contention.
	conns sync.Map

	// done is closed when the accept loop returns, whatever the reason. It
	// exists because Shutdown must not return while Serve is still running:
	// a caller that shuts down and immediately rebinds the same port would
	// otherwise race the accept loop that still holds the old listener. Tests
	// also use it to observe that Serve really did exit.
	done chan struct{}

	// serving records whether the accept loop has ever started, and doneOnce
	// guards the close of done. Without the flag, a Server whose Serve was
	// never called could never be shut down, because nothing would ever close
	// done.
	serving  bool
	doneOnce sync.Once

	// listenerOnce and shutdownOnce make Listen and Shutdown idempotent, so a
	// double call is a no-op rather than a double close.
	listenerOnce sync.Once
	shutdownOnce sync.Once

	// listenErr records the result of the first Listen. listenerOnce means
	// only the first caller runs the bind; later callers must still see the
	// same outcome rather than a silent nil.
	listenErr error
}

// New returns a Server that will listen on addr. It does not listen yet.
//
// addr is in net.Listen's "host:port" form. ":0" asks the kernel for a free
// port, which is what tests use so they never collide with a real server.
//
// clk is the store's clock. Production passes store.RealClock{}; a test that
// wants to control expiry without sleeping passes its own.
//
// sweepInterval is how often the store's active expiration sweeper runs. A
// non-positive value uses the store's default.
//
// rdbPath is the snapshot file. An empty path disables persistence entirely:
// nothing is loaded and nothing is written. A non-empty path is loaded here,
// during construction, so that a client which connects the instant Listen
// returns sees the restored keyspace rather than an empty one — and so that a
// corrupt file stops the process before it ever accepts a connection.
//
// The returned Server answers the built-in commands (see
// commands.DefaultRegistry) against a store created here. One store is
// created and handed to the registry, whose handlers close over it, so every
// connection this server accepts shares one keyspace — the property that
// makes two clients see each other's writes.
//
// It returns an error only for a persistence failure: an unreadable or corrupt
// snapshot. A missing file is not an error — it means this is the first run.
//
// This form configures no append-only log. It is exactly NewWithAOF with an
// empty aofPath, kept because a caller that wants snapshot persistence only —
// and the tests that predate the log — should not have to name a policy they
// are not using.
func New(addr string, clk store.Clock, sweepInterval time.Duration, rdbPath string) (*Server, error) {
	return NewWithAOF(addr, clk, sweepInterval, rdbPath, "", aof.FsyncEverySec)
}

// NewWithAOF returns a Server that will listen on addr, with both persistence
// mechanisms available.
//
// aofPath is the append-only log. When it is non-empty the log is the
// startup source of truth: it is replayed before Listen, and rdbPath is not
// read at all. The two are not both loaded because they describe the same
// keyspace by different means, and applying a snapshot on top of a log would
// either resurrect keys the log had deleted or discard writes the log
// records, depending on which ran second. The log wins because it is the
// finer-grained record: it holds individual commands, so it can represent
// states a periodic snapshot never captured.
//
// rdbPath keeps its meaning under an AOF: SAVE still writes snapshots there,
// and it is the only thing that reads them back. Setting both disables
// nothing — it means "log my writes, and let me take a snapshot on demand".
//
// aofPolicy chooses when the log is fsynced. See aof.FsyncPolicy: everysec is
// Redis's default and the right one for a server that has not been asked
// otherwise.
//
// Startup order is store, registry, log, listen. It matters: the log is
// replayed through the registry, so the registry must exist first, and the
// handle the read loop appends to is opened last, after the replay has
// finished reading. A corrupt log stops construction before a single
// connection is accepted.
func NewWithAOF(addr string, clk store.Clock, sweepInterval time.Duration, rdbPath string, aofPath string, aofPolicy aof.FsyncPolicy) (*Server, error) {
	if clk == nil {
		clk = store.RealClock{}
	}

	// The registry is built with the server's own Save, which is the one place
	// a command reaches back into the server. s.Save is a method value, so it
	// binds the receiver before the struct is fully assembled — which is what
	// makes it possible to pass here at all, and why the registry does not
	// need a second Register call afterwards.
	//
	// The alternative — the store owning the path and exposing Save itself —
	// would put a filesystem path inside the keyspace, which currently knows
	// nothing about durability; see docs/DECISIONS.md for the tradeoff.
	kv := store.NewWithInterval(clk, sweepInterval)

	s := &Server{
		addr:    addr,
		store:   kv,
		rdbPath: rdbPath,
		aofPath: aofPath,
		done:    make(chan struct{}),
	}
	s.registry = commands.DefaultRegistry(kv, s.Save)

	// Exactly one source is loaded, and the selection is made in one place so
	// the rule cannot drift between the load and the log line that explains
	// it.
	var err error
	if aofPath != "" {
		if rdbPath != "" {
			// Said out loud rather than silently: an operator who passed both
			// flags and then restarts expecting their snapshot to be there
			// needs to know it was not consulted, and the alternative —
			// quietly loading one of the two — is the kind of behavior that
			// gets discovered as data loss.
			log.Printf("aof: -aof-path is set, so %s is not loaded; SAVE still writes it", rdbPath)
		}
		err = s.loadAOF(aofPolicy)
	} else {
		err = s.load()
	}

	if err != nil {
		// The sweeper started by NewWithInterval is running and would outlive
		// a failed construction, so it is stopped before returning. A caller
		// that receives an error here has nothing to close. The log handle, if
		// one was opened, is closed by loadAOF's own failure path.
		kv.Close()
		return nil, err
	}

	return s, nil
}

// load reads the snapshot into the store. An empty path or a missing file is
// not an error; anything else is.
func (s *Server) load() error {
	if s.rdbPath == "" {
		return nil
	}

	entries, err := rdb.Read(s.rdbPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// First run. Not an error, and not silently ignored either — it is
			// simply the expected state of a server starting with persistence
			// enabled before anything has been saved.
			log.Printf("no snapshot at %s; starting empty", s.rdbPath)
			return nil
		}
		// A corrupt or unreadable file is fatal. Falling back to an empty
		// keyspace would be the worst possible answer: the operator would see
		// a working server and conclude their data had been lost by something
		// else, while the bad file sat there ready to fail again on the next
		// restart.
		return err
	}

	// Load holds the store's write lock until the returned function is
	// called. Nothing else can touch the keyspace until then, which is the
	// strongest available statement that startup is single-threaded.
	unlock := s.store.Load(entries)
	unlock()

	log.Printf("loaded %d keys from %s", len(entries), s.rdbPath)
	return nil
}

// loadAOF replays the append-only log into the store and then opens it for
// appending. It is the startup half of the AOF: everything below it is the
// command path.
//
// The replay dispatches through the registry rather than reimplementing each
// command, which is what keeps the log format and the command semantics from
// drifting apart: there is exactly one SET, and replay calls it. The reply is
// discarded because there is no client, and — the part that is easy to get
// wrong — the replay does not append, or the log would re-log every command
// it read on every start and grow without bound.
//
// A missing file is a first run, not an error. A corrupt one is fatal: the
// bytes parsed far enough to prove the file is not merely short, so
// continuing would mean serving a keyspace reconstructed from a log whose
// middle is known to be wrong.
func (s *Server) loadAOF(policy aof.FsyncPolicy) error {
	cmds, err := aof.Replay(s.aofPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("no append-only log at %s; starting empty", s.aofPath)
		} else {
			// Wrapped with the path: aof.Replay's message names the file, but
			// the caller sees this as "the server would not start", and the
			// reason has to survive that framing.
			return fmt.Errorf("aof: replay %s: %w", s.aofPath, err)
		}
	}

	for i, cmd := range cmds {
		// A command that fails to apply is a bug rather than bad input: the
		// log was written by this program, from commands the registry already
		// accepted once. Refusing to start is the honest response, because
		// continuing would silently drop a write the client was told had
		// happened.
		if _, fault := s.registry.Dispatch(protocol.Array(cmd)); fault != nil {
			return fmt.Errorf("aof: replay %s: command %d (%.32q) failed: %w", s.aofPath, i, describeArgs(cmd), fault)
		}
	}

	if len(cmds) > 0 {
		log.Printf("replayed %d commands from %s", len(cmds), s.aofPath)
	}

	// Opened only now, after the replay has finished reading. Appending to a
	// file whose contents have not been applied yet would interleave this
	// run's commands with a previous run's unread ones, and the resulting log
	// would replay in an order that never happened.
	handle, err := aof.Open(s.aofPath, policy)
	if err != nil {
		return fmt.Errorf("aof: open %s: %w", s.aofPath, err)
	}
	s.aof = handle

	log.Printf("appending to %s (%s fsync)", s.aofPath, policy)
	return nil
}

// Save writes the current keyspace to the snapshot file. An empty rdbPath
// makes it a no-op that reports success, so a caller does not have to check
// whether persistence is enabled before asking for a save.
//
// This is exported because the SAVE command needs it: the handler receives
// this method as a SaveFunc and has no other route to the filesystem. It is
// the one place a command reaches back into the server, and the alternative —
// the store owning the path — is recorded in docs/DECISIONS.md.
//
// Snapshot takes the store's read lock and returns copies, so this runs
// without blocking readers and without aliasing live data. The write itself
// happens outside any store lock: serializing a large keyspace while holding
// the keyspace would stall every client for the duration of a disk write.
func (s *Server) Save() error {
	if s.rdbPath == "" {
		return nil
	}

	entries := s.store.Snapshot()
	if err := rdb.Write(s.rdbPath, entries); err != nil {
		return err
	}

	log.Printf("saved %d keys to %s", len(entries), s.rdbPath)
	return nil
}

// Close stops the store's active expiration sweeper. It does not clear the
// keyspace, and it is safe to call more than once.
//
// Shutdown calls this after draining connections and saving. A caller that
// never started listening — a Server constructed and discarded — must call
// this directly, or the sweeper goroutine outlives it.
func (s *Server) Close() {
	s.store.Close()
}

// Listen starts the TCP listener and returns once the listener is bound.
// It does not block; the caller must call Serve.
func (s *Server) Listen() error {
	s.listenerOnce.Do(func() {
		ln, err := net.Listen("tcp", s.addr)
		if err != nil {
			s.listenErr = err
			return
		}
		s.mu.Lock()
		s.ln = ln
		s.mu.Unlock()
		log.Printf("listening on %s", ln.Addr())
	})
	return s.listenErr
}

// Addr returns the address the server is listening on, or nil if Listen has
// not succeeded or the server has been shut down.
//
// After a successful Listen this is the only way to learn the real port when
// the caller passed ":0" — the kernel chose the port, and only the listener
// knows which one.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve runs the accept loop and blocks until Shutdown is called.
// Every accepted connection is handled on its own goroutine.
func (s *Server) Serve() error {
	// Signal the end of the accept loop from the top, so that every return
	// path — including the misuse check below — releases anything waiting in
	// Shutdown. Closing twice is impossible because of doneOnce.
	defer s.doneOnce.Do(func() { close(s.done) })

	s.mu.Lock()
	ln := s.ln
	s.serving = true
	s.mu.Unlock()
	if ln == nil {
		return errors.New("server: Serve called before a successful Listen")
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener reports net.ErrClosed. That is what Shutdown
			// looks like from in here, so it ends the loop with no error.
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			// Any other accept error is transient in practice — a per-socket
			// failure, an interrupted call, a resource limit that clears. One
			// failed accept says nothing about the next one, so the loop keeps
			// going rather than taking the whole server down with it.
			log.Printf("accept: %v", err)
			continue
		}

		// The connection is registered before the goroutine starts, so
		// Shutdown can never observe an empty table while a handler is on its
		// way to registering itself.
		s.conns.Store(conn, struct{}{})
		go s.handleConnection(conn)
	}
}

// Shutdown stops the listener gracefully. In-flight connections are given up
// to the context's deadline to complete before their goroutines are
// abandoned.
//
// It is safe to call more than once and safe to call from a goroutine other
// than the one running Serve.
//
// The snapshot is written after the connections have drained and before this
// returns, on both the clean path and the drain-timeout path: a shutdown that
// gave up on a stuck client has still stopped accepting new writes, and
// discarding everything the server was holding because one connection was slow
// would be the wrong trade. A save failure on the timeout path is folded into
// the returned error rather than replacing it, so the caller learns both that
// draining timed out and that the snapshot did not land.
//
// The sweeper is stopped on every path out — hence the deferred Close — so a
// timeout during draining cannot leave a background goroutine behind. Close
// runs after the save, because the save reads the keyspace the sweeper guards.
func (s *Server) Shutdown(ctx context.Context) (err error) {
	defer s.Close()

	// Close the listener first, so no new connections arrive while we are
	// draining. Closing it is also what unblocks Accept and ends Serve.
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		ln := s.ln
		s.ln = nil
		s.mu.Unlock()

		if ln != nil {
			// A failure here means the socket was already closed. Nothing
			// useful is left to do, and returning it would disguise a clean
			// shutdown as an error.
			_ = ln.Close()
		}
	})

	// Wait for the accept loop to leave and for the handlers to drain.
	//
	// Both halves matter. Waiting only for the handlers would let Shutdown
	// return while Serve was still inside Accept holding the old listener,
	// which breaks any caller that shuts down and then rebinds the same port.
	// Handlers remove themselves from the table as they finish, so an empty
	// table means every handler has returned or is about to.
	//
	// Polling rather than a WaitGroup is deliberate: the table has to be
	// consulted anyway (it is what closeAllConns needs on the timeout path),
	// and one source of truth is worth more here than a marginally faster
	// wake-up. A ticker is used instead of a bare loop so a stuck handler
	// costs no CPU.
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()

	for {
		if s.drained() {
			return s.persistOnShutdown(nil)
		}
		select {
		case <-ctx.Done():
			// The deadline passed with work still in flight. Signal every
			// remaining handler by closing its connection, which unblocks the
			// read it is sitting on. No handler is left unsignalled when this
			// function returns; their goroutines then exit on their own.
			// Closing a net.Conn is documented as safe while another
			// goroutine is blocked reading from it.
			s.closeAllConns()
			return s.persistOnShutdown(ctx.Err())
		case <-ticker.C:
			// Re-check whether the handlers have finished.
		}
	}
}

// persistOnShutdown closes the log and writes the snapshot, combining either
// failure with err, which is either nil or the reason draining gave up.
//
// The log is closed first. It is the finer-grained record, so it is the one
// that must not be left unsynced: Close flushes whatever the everysec ticker
// has not yet written, and under FsyncNo it is the only sync the process ever
// performs. A snapshot written after it is consistent with the log, because
// both describe the same keyspace at the same moment — the drain has already
// finished, so nothing can be mutating either.
//
// The snapshot is still written when a log is configured. SAVE remains the
// manual-snapshot mechanism, and a shutdown that skipped it would silently
// change what an operator's -rdb-path means depending on an unrelated flag.
//
// Both failures are reported rather than one overwriting the other: "shutdown
// timed out", "the log could not be flushed" and "the snapshot could not be
// written" are independent facts, and an operator seeing only the first would
// not know how much of their data is actually on disk.
func (s *Server) persistOnShutdown(err error) error {
	if s.aof != nil {
		err = errors.Join(err, s.aof.Close())
	}

	err = errors.Join(err, s.Save())
	return err
}

// handleConnection owns one client connection for its whole life: setup, the
// read/dispatch loop, and teardown. It is the thinnest possible shell around
// readCommands; everything below it is a detail of reading, decoding, or
// cleanup rather than part of the loop's shape.
func (s *Server) handleConnection(conn net.Conn) {
	remote := conn.RemoteAddr().String()
	log.Printf("accepted %s", remote)

	defer func() {
		// Removing before closing keeps Shutdown's drain check honest: once
		// this entry is gone, this handler has no more work to do.
		s.conns.Delete(conn)
		_ = conn.Close()
		log.Printf("closed %s", remote)
	}()

	// Nagle's algorithm holds small writes back to coalesce them. This server
	// writes one reply per command, so Nagle would add latency to every
	// round trip and buy nothing. Redis disables it on client sockets for the
	// same reason.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	readCommands(remote, bufio.NewReader(conn), bufio.NewWriter(conn), s.registry, s.aof)
}

// readCommands is the loop: read bytes, decode one frame, dispatch it, log it
// if it is a write, encode the reply, write it, repeat. It returns when the
// client disconnects or the stream desynchronizes, and it never interprets a
// command — that is the registry's job.
//
// It is a free function rather than a method because it needs exactly four
// things: where to read, where to write, who decides, and where to log. That
// makes the shape of the loop readable on its own, and testable without a
// Server. aofLog is nil when no log is configured, which is the ordinary case
// for a test and for a server started without -aof-path.
func readCommands(remote string, reader *bufio.Reader, writer *bufio.Writer, registry *commands.Registry, aofLog *aof.AOF) {
	chunk := make([]byte, readBufSize)

	// buf accumulates bytes that have arrived but do not yet form a complete
	// frame. It is per-connection state, which is what makes it correct: RESP
	// frames are length-prefixed, so a frame may be split across any number of
	// reads at any byte offset, chosen by the network.
	//
	// It starts nil and is appended into, so memory is allocated on first use
	// at the size of the data actually received rather than a guess made at
	// connect time.
	var buf []byte

	for {
		// Decode as many complete frames as the buffer holds. A client is
		// allowed to send several commands in one write — pipelining — and
		// each one must be answered in order.
		//
		// incomplete is a loop-carried flag rather than a plain break, because
		// "the buffer is empty" and "the buffer holds a partial frame" both
		// mean read more, while "the buffer holds a whole frame" means handle
		// it and try again. Collapsing the first two into one break would
		// leave a partial frame being re-decoded forever without ever reading
		// the bytes that would complete it.
		incomplete := false

		for len(buf) > 0 {
			value, n, err := protocol.Decode(buf)

			if errors.Is(err, protocol.ErrIncomplete) {
				incomplete = true
				break
			}

			if err != nil {
				// A grammar violation is terminal for the connection. The
				// bytes cannot become valid by waiting, so tell the client
				// what was wrong and hang up rather than leaving it guessing.
				log.Printf("protocol error from %s: %v", remote, err)
				_ = writeReply(writer, protocol.Error("ERR Protocol error: "+err.Error()))
				return
			}

			// Advance past this frame before handling it, so buf is already
			// correct for the next iteration whatever the handler does.
			buf = buf[n:]

			// The reply is encoded and written before the next read, which is
			// what makes it safe for a handler to return a bulk string that
			// aliases buf: nothing overwrites those bytes until the reply has
			// left the process.
			reply, fault := registry.Dispatch(value)
			if fault != nil {
				// An internal fault is a bug on our side, not something the
				// client did. The detail goes to the log where an operator can
				// act on it; the client gets a generic error, because the
				// fault text would leak implementation detail it cannot use
				// and must not depend on.
				log.Printf("internal fault from %s: %v", remote, fault)
				reply = protocol.Error("ERR internal error")
			}

			// The log entry is written here, after the command has run and
			// before the reply leaves. That ordering is the whole durability
			// contract: a client never sees OK for a write that was not
			// logged, so a restart cannot lose a command the client was told
			// had succeeded. Logging first instead would record commands
			// that then failed; logging after the reply would open a window
			// where a crash loses a write the client already believes is
			// durable.
			//
			// A logging failure does not change the reply. The command has
			// already run — its effect is in the keyspace every other client
			// can see — so replacing its reply with an error would tell the
			// client the write did not happen when it did, and a client that
			// retries on error would perform it twice. The failure is logged
			// where an operator can see it, and the reply stands.
			if aofLog != nil && fault == nil {
				logWrite(remote, registry, aofLog, value)
			}

			if writeErr := writeReply(writer, reply); writeErr != nil {
				log.Printf("write to %s: %v", remote, writeErr)
				return
			}
		}

		// Read more when there is nothing left to decode: either the buffer is
		// empty because every frame in it was handled, or it holds a partial
		// frame that needs the rest of its bytes.
		if len(buf) != 0 && !incomplete {
			continue
		}

		read, err := readChunk(reader, chunk, len(buf))
		if read > 0 {
			buf = append(buf, chunk[:read]...)
			continue
		}
		if err != nil {
			if !isClientDisconnect(err) {
				log.Printf("read from %s: %v", remote, err)
			}
			// A client disconnect is the normal way a connection ends. A
			// partial frame left in buf is discarded: there is no reply to
			// send, because the request never finished.
			return
		}
	}
}

// logWrite appends a write command to the log, and does nothing for a command
// that is not a write.
//
// The name is taken from the first argument and matched case-insensitively,
// which is the same normalization the registry used to find the handler: the
// command that ran is the command that is logged, with no second opinion about
// what counts as a write.
//
// Failures are logged and not returned. See the call site for why the reply
// must stand regardless.
func logWrite(remote string, registry *commands.Registry, aofLog *aof.AOF, value protocol.Value) {
	name, ok := extractName(value)
	if !ok || !registry.IsWrite(name) {
		return
	}

	args, ok := commandArgs(value)
	if !ok {
		// Unreachable for a real client: every command arrives as an array of
		// bulk strings, which is what the decoder produces. If it ever
		// happens, the alternative to skipping is writing a frame whose
		// contents are not what ran, and a log that is wrong is worse than a
		// log that is short — the wrong entry survives every restart.
		log.Printf("aof: %s: write command %q has a non-bulk-string argument; not logged", remote, name)
		return
	}

	// Append is called here, after the command has run and before the reply is
	// written by the caller, so a successful reply implies a log entry.
	if err := aofLog.Append(args); err != nil {
		log.Printf("aof: %s: append %q: %v", remote, name, err)
	}
}

// extractName returns the command name from a decoded frame: the first
// argument, as a string.
//
// A frame that is not an array, or whose first element is not a bulk string,
// has no command name. The registry will have answered such a frame with an
// error and dispatched nothing, so there is nothing to log and no fault to
// report — false here means "not a command", not "something went wrong".
func extractName(value protocol.Value) (string, bool) {
	arr, ok := value.(protocol.Array)
	if !ok || len(arr) == 0 {
		return "", false
	}
	name, ok := arr[0].(protocol.BulkString)
	if !ok {
		return "", false
	}
	return string(name), true
}

// commandArgs returns the frame's arguments as the values Append expects.
//
// The conversion is an assertion rather than a coercion. A command from a
// real client is an array of bulk strings and converts losslessly; anything
// else reports false, because turning a value of another type into a bulk
// string would write a frame that differs from the command that ran, and the
// log would then replay something the client never sent.
func commandArgs(value protocol.Value) ([]protocol.Value, bool) {
	arr, ok := value.(protocol.Array)
	if !ok {
		return nil, false
	}
	args := make([]protocol.Value, len(arr))
	for i, arg := range arr {
		bulk, ok := arg.(protocol.BulkString)
		if !ok {
			return nil, false
		}
		args[i] = bulk
	}
	return args, true
}

// describeArgs renders a command's arguments for a log line, for the replay
// failure message. It is deliberately lossy and bounded: its input is a log
// entry that may hold arbitrary bytes, and a failure message that dumped
// megabytes of them would be unreadable exactly when it is needed.
//
// fmt rather than a String method call because Value's method set does not
// include String — the implementations have it, the interface does not — so
// this goes through the Stringer check at run time.
func describeArgs(cmd []protocol.Value) string {
	var b strings.Builder
	for i, arg := range cmd {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%v", arg)
	}
	return b.String()
}

// readChunk reads the next piece of a request into chunk, returning the
// number of bytes read.
//
// Its one job beyond calling Read is enforcing maxFrameSize. The check runs
// before the read, so the buffer is never allowed to grow past the ceiling
// even temporarily, and it reports the overflow as a protocol error so the
// loop above treats it like any other desynchronized stream: answer, close.
func readChunk(reader *bufio.Reader, chunk []byte, buffered int) (int, error) {
	if buffered >= maxFrameSize {
		return 0, fmt.Errorf("%w: frame exceeds the %d-byte limit", errFrameTooLarge, maxFrameSize)
	}
	if buffered+len(chunk) > maxFrameSize {
		chunk = chunk[:maxFrameSize-buffered]
	}
	return reader.Read(chunk)
}

// isClientDisconnect reports whether a read error is just the client going
// away, which is not worth logging.
//
// io.EOF is the polite close. The other case is a connection reset, and it
// matters in practice: redis-cli sends its command, reads the reply, and then
// resets the socket rather than half-closing it. On Windows that surfaces as
// WSAECONNRESET — "An existing connection was forcibly closed by the remote
// host" — and on Unix as ECONNRESET. Logging it would print an alarming line
// for every single command a user runs, which is worse than useless: it
// trains the reader to ignore the log.
//
// The check is by error string rather than syscall.Errno, because the
// platform-specific errno values differ and syscall.ECONNRESET is not
// portable across Windows and Unix builds without build-tagged files. A
// string match is coarse but safe here: the cost of a false positive is one
// unlogged disconnect, and the cost of a false negative is log noise.
func isClientDisconnect(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "forcibly closed") || // Windows
		strings.Contains(msg, "connection reset by peer") // Unix
}

// writeReply encodes a reply and flushes it, so the client sees it without
// waiting for the connection to close or the buffer to fill. A buffered
// writer without an explicit flush would make the server appear to hang.
func writeReply(writer *bufio.Writer, reply protocol.Value) error {
	// Encode returns nil only for a nil Value, which no handler should
	// produce. Writing nothing would leave the client waiting for a reply
	// that is never coming, so it is turned into an error frame instead.
	encoded := protocol.Encode(reply)
	if encoded == nil {
		encoded = protocol.Encode(protocol.Error("ERR internal error: no reply"))
	}

	if _, err := writer.Write(encoded); err != nil {
		return err
	}
	return writer.Flush()
}

// anyActive reports whether any connection is still being handled.
func (s *Server) anyActive() bool {
	active := false
	s.conns.Range(func(_, _ any) bool {
		active = true
		return false // stop at the first entry; we only need to know if one exists
	})
	return active
}

// drained reports whether Shutdown has nothing left to wait for: the accept
// loop has ended and no handler is still holding a connection.
func (s *Server) drained() bool {
	s.mu.Lock()
	serving := s.serving
	s.mu.Unlock()

	// A Server whose Serve was never called has no accept loop to wait for;
	// the callers that matter (main and the tests) always call it, but
	// Shutdown must still terminate for one that does not.
	acceptDone := !serving
	if serving {
		select {
		case <-s.done:
			acceptDone = true
		default:
		}
	}

	return acceptDone && !s.anyActive()
}

// closeAllConns closes every connection still being handled. Called when the
// shutdown deadline expires, to unblock handlers that are parked on a read
// and would otherwise never wake up on their own.
func (s *Server) closeAllConns() {
	s.conns.Range(func(key, _ any) bool {
		if conn, ok := key.(net.Conn); ok {
			_ = conn.Close()
		}
		return true
	})
}
