package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
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

// Server is a TCP server that echoes every line it reads back to the client
// that sent it.
//
// A zero-value Server is not usable: it has no channels and no connection
// table. Always construct one with New.
type Server struct {
	// addr is the address passed to New, kept so Listen can report it and so
	// the binding decision is made in exactly one place.
	addr string

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
func New(addr string) *Server {
	return &Server{
		addr: addr,
		done: make(chan struct{}),
	}
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
func (s *Server) Shutdown(ctx context.Context) error {
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

// handleConnection serves exactly one client connection until it ends.
//
// net.Conn is not safe for concurrent use, so this connection is owned by
// this goroutine for its whole life and is never touched by another one —
// with the deliberate exception of Shutdown closing it to break a blocked
// read, which the net package documents as safe.
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

	// Nagle's algorithm holds small writes back to coalesce them. This
	// server writes one line per line read, so Nagle would only add latency
	// and buy nothing. Redis disables it on client sockets for the same
	// reason.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// ReadBytes, not ReadString: RESP bulk strings are binary-safe, and
		// converting to a string here would hide the raw bytes behind UTF-8
		// semantics before anything has had a chance to inspect them.
		//
		// ReadBytes('\n') also makes partial reads a non-issue. Whatever has
		// arrived so far is buffered and returned only once the terminator is
		// seen, so a line split across TCP segments is reassembled before the
		// handler looks at it.
		line, err := reader.ReadBytes('\n')

		if len(line) > 0 {
			// Write the exact bytes that were read, terminator included.
			// Writing line plus a fresh "\r\n" would double the terminator,
			// and trimming it (as ReadString callers often do) would corrupt
			// the framing milestone 2 depends on.
			if _, writeErr := writer.Write(line); writeErr != nil {
				log.Printf("write to %s: %v", remote, writeErr)
				return
			}
			// Flush per line. A buffered writer without an explicit flush
			// would hold the echo until the buffer filled or the connection
			// closed, which turns an echo server into a silent one.
			if flushErr := writer.Flush(); flushErr != nil {
				log.Printf("write to %s: %v", remote, flushErr)
				return
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				// The client disconnected cleanly. That is the normal way a
				// connection ends, not a fault, so nothing is logged.
				return
			}
			// Anything else is a real failure. It is logged once here and
			// nowhere else, then this handler gives up on this connection.
			log.Printf("read from %s: %v", remote, err)
			return
		}
	}
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
