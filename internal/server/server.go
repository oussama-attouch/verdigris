package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Oussama-Att/verdigris/internal/commands"
	"github.com/Oussama-Att/verdigris/internal/protocol"
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
	// that Shutdown can stop its expiration sweeper. The handlers also hold
	// it, handed over through the registry; this reference exists for
	// lifecycle, not for data access.
	store *store.Store

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
// The returned Server answers the built-in commands (see
// commands.DefaultRegistry) against a store created here. One store is
// created and handed to the registry, whose handlers close over it, so every
// connection this server accepts shares one keyspace — the property that
// makes two clients see each other's writes.
func New(addr string, clk store.Clock, sweepInterval time.Duration) *Server {
	if clk == nil {
		clk = store.RealClock{}
	}

	// Constructing the store once and handing the same pointer to the
	// registry is the whole mechanism by which two clients see each other's
	// writes. The server also keeps the pointer, so Shutdown can stop the
	// sweeper goroutine the store started.
	//
	// The interval goes in as an argument rather than through a package-level
	// setter, so this store's tick period is stated on the line that builds
	// it. A non-positive value is normalised inside the store.
	kv := store.NewWithInterval(clk, sweepInterval)

	return &Server{
		addr:     addr,
		store:    kv,
		registry: commands.DefaultRegistry(kv),
		done:     make(chan struct{}),
	}
}

// Close stops the store's active expiration sweeper. It does not clear the
// keyspace, and it is safe to call more than once.
//
// Shutdown calls this after draining connections. A caller that never started
// listening — a Server constructed and discarded — must call this directly,
// or the sweeper goroutine outlives it.
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
// The store's expiration sweeper is stopped on every path out, including both
// error paths, which is why the Close is deferred rather than placed at the
// end: a timeout during draining must not leave a background goroutine behind.
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
			return nil
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
			return ctx.Err()
		case <-ticker.C:
			// Re-check whether the handlers have finished.
		}
	}
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

	readCommands(remote, bufio.NewReader(conn), bufio.NewWriter(conn), s.registry)
}

// readCommands is the loop: read bytes, decode one frame, dispatch it, encode
// the reply, write it, repeat. It returns when the client disconnects or the
// stream desynchronizes, and it never interprets a command — that is the
// registry's job.
//
// It is a free function rather than a method because it needs exactly three
// things: where to read, where to write, and who decides. That makes the
// shape of the loop readable on its own, and testable without a Server.
func readCommands(remote string, reader *bufio.Reader, writer *bufio.Writer, registry *commands.Registry) {
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
