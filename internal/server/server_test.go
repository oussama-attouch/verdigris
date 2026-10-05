package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/commands"
	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// testTimeout bounds every blocking step in these tests. It is generous
// because it only ever fires when something is actually broken: on a healthy
// machine a localhost round trip is sub-millisecond.
const testTimeout = 5 * time.Second

// discardLogs silences the package's logging for the duration of a test. The
// server logs every accept and close, which is exactly what we want in
// production and pure noise in a test run.
func discardLogs(t *testing.T) {
	t.Helper()
	previous := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(previous) })
}

// client reads RESP replies off a connection while holding on to bytes it has
// read but not yet decoded.
//
// That residue is not an optimization, it is the difference between correct
// and incorrect observation. A server answering three pipelined commands
// delivers all three replies in one TCP segment; a helper that started from
// an empty buffer on every call would discard the second and third, then
// block waiting for data the server had already sent.
type client struct {
	t    *testing.T
	conn net.Conn
	acc  []byte
}

// newClient dials addr and registers the connection for cleanup. With the
// deadline set, a server that never replies fails the test quickly instead of
// hanging until the go test timeout.
func newClient(t *testing.T, addr string) *client {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%s): %v", addr, err)
	}
	if err := conn.SetDeadline(time.Now().Add(testTimeout)); err != nil {
		_ = conn.Close()
		t.Fatalf("SetDeadline: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &client{t: t, conn: conn}
}

// send writes raw bytes, the way a client puts a request on the wire.
func (c *client) send(raw []byte) {
	c.t.Helper()

	if _, err := c.conn.Write(raw); err != nil {
		c.t.Fatalf("Write(%q): %v", raw, err)
	}
}

// next reads and returns the next complete RESP frame.
func (c *client) next() protocol.Value {
	c.t.Helper()

	chunk := make([]byte, 512)
	for {
		value, n, err := protocol.Decode(c.acc)
		switch {
		case err == nil:
			c.acc = c.acc[n:]
			return value
		case errors.Is(err, protocol.ErrIncomplete):
			// Need more bytes.
		default:
			c.t.Fatalf("reply did not decode: %v (bytes %q)", err, c.acc)
		}

		read, readErr := c.conn.Read(chunk)
		if read > 0 {
			c.acc = append(c.acc, chunk[:read]...)
			continue
		}
		if readErr != nil {
			c.t.Fatalf("connection ended before a complete reply arrived: %v (bytes %q)", readErr, c.acc)
		}
	}
}

// expect reads one reply and asserts it equals want.
func (c *client) expect(want protocol.Value) {
	c.t.Helper()

	got := c.next()
	if !valueEqual(got, want) {
		c.t.Fatalf("reply = %#v, want %#v", got, want)
	}
}

// expectAll reads len(want) replies and asserts each in order.
func (c *client) expectAll(want ...protocol.Value) {
	c.t.Helper()

	for i, w := range want {
		got := c.next()
		if !valueEqual(got, w) {
			c.t.Fatalf("reply %d = %#v, want %#v", i, got, w)
		}
	}
}

// mustError asserts the next reply is a RESP Error frame and returns it.
func (c *client) mustError() protocol.Error {
	c.t.Helper()

	got := c.next()
	errValue, ok := got.(protocol.Error)
	if !ok {
		c.t.Fatalf("reply = %#v (%T), want protocol.Error", got, got)
	}
	return errValue
}

// valueEqual compares two decoded values, tolerating the slice types
// (BulkString, Array) that have no == operator.
func valueEqual(a, b protocol.Value) bool {
	switch av := a.(type) {
	case protocol.BulkString:
		bv, ok := b.(protocol.BulkString)
		return ok && bytes.Equal(av, bv)
	case protocol.Array:
		bv, ok := b.(protocol.Array)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !valueEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}

// pingFrame is `*1\r\n$4\r\nPING\r\n`, the request redis-cli sends for PING.
var pingFrame = protocol.Encode(protocol.Array{protocol.BulkString([]byte("PING"))})

// commandFrame builds the wire form of a command from string arguments.
func commandFrame(words ...string) []byte {
	args := make(protocol.Array, 0, len(words))
	for _, w := range words {
		args = append(args, protocol.BulkString([]byte(w)))
	}
	return protocol.Encode(args)
}

// startServer binds 127.0.0.1:0 and begins serving.
//
// Port 0 asks the kernel for a free port, so tests never collide with a real
// Verdigris on 6379, with a redis-server, or with each other. Cleanup shuts
// the server down and waits for Serve to return, so a failing test cannot
// leak a listener or a goroutine.
func startServer(t *testing.T) (addr string, shutdown func()) {
	t.Helper()

	srv := New("127.0.0.1:0")
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	serveDone := make(chan error, 1)
	// serving is closed by the goroutine the instant it starts, so this
	// function cannot return before Serve is inside its accept loop. Without
	// that handshake, a test that immediately calls Shutdown can win the race
	// against the goroutine even being scheduled.
	serving := make(chan struct{})
	go func() {
		close(serving)
		serveDone <- srv.Serve()
	}()
	<-serving

	var once sync.Once
	shutdown = func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			if err := srv.Shutdown(ctx); err != nil {
				t.Errorf("Shutdown: %v", err)
			}
			select {
			case err := <-serveDone:
				if err != nil {
					t.Errorf("Serve returned %v, want nil after Shutdown", err)
				}
			case <-time.After(testTimeout):
				t.Error("Serve did not return after Shutdown")
			}
		})
	}

	t.Cleanup(shutdown)

	return srv.Addr().String(), shutdown
}

// --- Task 6: the required integration tests ---------------------------------

// TestPingOverTCP is the milestone's acceptance criterion, exercised over a
// real socket instead of by calling the handler.
func TestPingOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send([]byte("*1\r\n$4\r\nPING\r\n"))

	c.expect(protocol.SimpleString("PONG"))
}

// TestPingWithMessageOverTCP covers the second form of PING, where the
// argument comes back as a bulk string.
func TestPingWithMessageOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send(commandFrame("PING", "custom message"))

	c.expect(protocol.BulkString([]byte("custom message")))
}

// TestEchoOverTCP is the plain ECHO case.
func TestEchoOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send(commandFrame("ECHO", "hello verdigris"))

	c.expect(protocol.BulkString([]byte("hello verdigris")))
}

// TestUnknownCommandOverTCP checks the reply is a RESP Error frame naming the
// command, which is a different frame type from the replies above and the
// first thing a client shows a user when they make a mistake.
func TestUnknownCommandOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send(commandFrame("NOTACOMMAND"))

	errValue := c.mustError()

	if !bytes.Contains([]byte(errValue), []byte("NOTACOMMAND")) {
		t.Fatalf("error reply %q does not name the command", errValue)
	}
	if !bytes.HasPrefix([]byte(errValue), []byte("ERR ")) {
		t.Fatalf("error reply %q does not start with \"ERR \"", errValue)
	}
}

// TestWrongArityOverTCP pins the exact message redis-cli shows for `ECHO`
// with no argument, which the milestone demo asserts.
func TestWrongArityOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send(commandFrame("ECHO"))

	const want = "ERR wrong number of arguments for 'echo' command"
	if got := string(c.mustError()); got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

// TestPipeliningOverTCP writes three commands in a single Write call and
// expects three replies in order. This is the case that proves the read loop
// drains everything it has buffered rather than answering one command per
// read, and the case that would silently pass with a client helper that
// discarded the residue between replies.
func TestPipeliningOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	var batch []byte
	batch = append(batch, pingFrame...)
	batch = append(batch, commandFrame("PING", "second")...)
	batch = append(batch, commandFrame("ECHO", "third")...)

	c.send(batch)

	c.expectAll(
		protocol.SimpleString("PONG"),
		protocol.BulkString([]byte("second")),
		protocol.BulkString([]byte("third")),
	)
}

// TestBinarySafeEchoOverTCP is the most important test in this file.
//
// The payload contains NUL, 0xFF, invalid UTF-8, and an embedded CRLF, and it
// is sent as a length-prefixed bulk string. A read loop that scanned for a
// newline — the milestone 1 design — would truncate this frame at the
// embedded CRLF and desynchronize the connection. Only a loop that
// accumulates bytes and lets the decoder decide can carry it.
func TestBinarySafeEchoOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	payload := []byte{0x00, 0xff, '\r', '\n', 0xfe, 0x00, '\r', 0xff, '\n', 'P', 'I', 'N', 'G'}

	c := newClient(t, addr)
	c.send(commandFrame("ECHO", string(payload)))

	c.expect(protocol.BulkString(payload))
}

// TestMalformedFrameClosesConnection checks that a grammar violation is
// answered with an error frame and then a hang-up. The client must not be
// left waiting on a connection the server has given up on.
func TestMalformedFrameClosesConnection(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send([]byte("?this is not RESP\r\n"))

	errValue := c.mustError()
	if !bytes.Contains([]byte(errValue), []byte("Protocol error")) {
		t.Fatalf("error reply %q does not mention a protocol error", errValue)
	}

	// The server must now close the connection.
	buf := make([]byte, 64)
	for {
		_, err := c.conn.Read(buf)
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return // clean close, as intended
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatal("connection was still open after a protocol error")
		}
		return // a reset is also an acceptable close
	}
}

// TestPartialFrameAcrossReads sends one frame in several pieces and expects
// the server to wait for the rest rather than answering early or failing. The
// network may split a frame at any byte, so this is the case the whole
// accumulated-buffer design exists for.
func TestPartialFrameAcrossReads(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	frame := commandFrame("ECHO", "split across reads")
	// Split inside the payload, not at a frame boundary.
	cut := bytes.IndexByte(frame, 'a') + 3
	if cut <= 0 || cut >= len(frame) {
		t.Fatalf("test bug: bad split point %d for frame %q", cut, frame)
	}

	c := newClient(t, addr)

	// First piece: the reply must NOT arrive yet.
	c.send(frame[:cut])
	if err := c.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if n, err := c.conn.Read(make([]byte, 64)); err == nil {
		t.Fatalf("server replied after only %d of %d bytes (read %d more)", cut, len(frame), n)
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("read error = %v, want a timeout while waiting for the rest of the frame", err)
		}
	}

	// Second piece: now the reply must come.
	if err := c.conn.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	c.send(frame[cut:])

	c.expect(protocol.BulkString([]byte("split across reads")))
}

// TestLargeFrameExceedsReadBuffer sends a payload larger than the handler's
// scratch buffer, so the frame arrives across several reads and the
// per-connection buffer has to grow. A fixed-size buffer without an append
// would truncate or fail here.
func TestLargeFrameExceedsReadBuffer(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	payload := bytes.Repeat([]byte("verdigris!"), 2000) // 20 KB, well over readBufSize
	if len(payload) <= readBufSize {
		t.Fatalf("test bug: payload of %d bytes does not exceed the %d-byte read buffer", len(payload), readBufSize)
	}

	c := newClient(t, addr)
	c.send(commandFrame("ECHO", string(payload)))

	c.expect(protocol.BulkString(payload))
}

// TestEmptyEchoOverTCP distinguishes an empty message from a missing one: the
// reply is `$0\r\n\r\n`, not an arity error.
func TestEmptyEchoOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send(commandFrame("ECHO", ""))

	c.expect(protocol.BulkString([]byte{}))
}

// TestNonArrayCommandOverTCP covers a frame that is valid RESP but not a
// command. The server must answer with an error rather than closing, because
// the connection is still synchronized.
func TestNonArrayCommandOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send([]byte("+JUST A STRING\r\n"))

	c.mustError()

	// The connection must still work afterwards.
	c.send(pingFrame)
	c.expect(protocol.SimpleString("PONG"))
}

// TestConcurrentConnections proves per-connection isolation: each client gets
// its own replies, never another client's.
func TestConcurrentConnections(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	const clients = 8

	type result struct {
		want string
		got  string
		err  error
	}
	results := make(chan result, clients)

	for i := range clients {
		go func() {
			payload := "payload-" + strconv.Itoa(i)

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				results <- result{want: payload, err: err}
				return
			}
			defer conn.Close()

			if err := conn.SetDeadline(time.Now().Add(testTimeout)); err != nil {
				results <- result{want: payload, err: err}
				return
			}
			if _, err := conn.Write(commandFrame("ECHO", payload)); err != nil {
				results <- result{want: payload, err: err}
				return
			}

			// Decode off the socket directly rather than through the client
			// helper, which is bound to *testing.T and therefore unsafe to
			// call from a goroutine.
			var acc []byte
			chunk := make([]byte, 512)
			for {
				value, n, decErr := protocol.Decode(acc)
				if decErr == nil {
					acc = acc[n:]
					bs, ok := value.(protocol.BulkString)
					if !ok {
						results <- result{want: payload, err: errors.New("reply was not a bulk string")}
						return
					}
					results <- result{want: payload, got: string(bs)}
					return
				}
				if !errors.Is(decErr, protocol.ErrIncomplete) {
					results <- result{want: payload, err: decErr}
					return
				}
				read, readErr := conn.Read(chunk)
				if read > 0 {
					acc = append(acc, chunk[:read]...)
					continue
				}
				if readErr != nil {
					results <- result{want: payload, err: readErr}
					return
				}
			}
		}()
	}

	for range clients {
		res := <-results
		if res.err != nil {
			t.Fatalf("client for %q: %v", res.want, res.err)
		}
		if res.got != res.want {
			t.Fatalf("client wanted %q but got %q", res.want, res.got)
		}
	}
}

// TestClientDisconnectIsNotFatal checks that a client hanging up does not
// stop the server from accepting the next one.
func TestClientDisconnectIsNotFatal(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	first := newClient(t, addr)
	first.send(pingFrame)
	first.expect(protocol.SimpleString("PONG"))
	if err := first.conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second := newClient(t, addr)
	second.send(pingFrame)
	second.expect(protocol.SimpleString("PONG"))
}

// TestAbruptDisconnectMidFrame checks a client that vanishes partway through
// a frame. The handler is left holding a partial frame it will never finish;
// it must exit rather than spin or leak.
func TestAbruptDisconnectMidFrame(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send([]byte("*2\r\n$4\r\nECHO\r\n$5\r\nhel"))
	if err := c.conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The server must still be serving.
	next := newClient(t, addr)
	next.send(pingFrame)
	next.expect(protocol.SimpleString("PONG"))
}

// TestBufferedReaderIsNotUsedForFraming is a regression guard for the design
// decision this milestone turns on. The old loop read with ReadBytes('\n'),
// which cannot carry a bulk string containing a newline. This payload proves
// the current loop does not do that: it holds several newlines before its
// real terminator.
func TestBufferedReaderIsNotUsedForFraming(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	payload := []byte("line one\nline two\nline three\n")

	c := newClient(t, addr)
	c.send(commandFrame("ECHO", string(payload)))

	c.expect(protocol.BulkString(payload))
}

// --- Lifecycle tests carried over from milestone 1 --------------------------

// TestGracefulShutdown checks the shutdown contract: Serve returns rather
// than hanging, Shutdown reports success, and the listener really is closed.
func TestGracefulShutdown(t *testing.T) {
	discardLogs(t)

	srv := New("127.0.0.1:0")
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := srv.Addr().String()

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()

	// One exchange first, so shutdown is exercised with a connection that has
	// actually been served rather than an idle listener.
	c := newClient(t, addr)
	c.send(pingFrame)
	c.expect(protocol.SimpleString("PONG"))

	// Close the client so its handler ends on its own and the drain is the
	// fast path. The timeout path is covered by the abandoned-connection test.
	if err := c.conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	start := time.Now()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > testTimeout {
		t.Fatalf("Shutdown took %v, want well under %v", elapsed, testTimeout)
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil after a clean shutdown", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Serve did not return after Shutdown")
	}

	if conn, err := net.Dial("tcp", addr); err == nil {
		_ = conn.Close()
		t.Fatal("Dial succeeded after Shutdown; the listener should be closed")
	}
}

// TestShutdownSignalsAbandonedConnection covers the timeout path. A client
// that connects and then says nothing holds a handler open; if the deadline
// expires, Shutdown must report it and must still signal that handler rather
// than leaving it parked on a read forever.
func TestShutdownSignalsAbandonedConnection(t *testing.T) {
	discardLogs(t)

	srv := New("127.0.0.1:0")
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()

	// Open a connection and deliberately send nothing.
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := srv.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil after Shutdown closed the listener", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Serve did not return after Shutdown")
	}

	if err := conn.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection is still open; Shutdown should have closed it on timeout")
	}
}

// TestServeBeforeListen fails loudly instead of blocking or panicking, since
// using a Server without Listen is the one way to misuse this package.
func TestServeBeforeListen(t *testing.T) {
	discardLogs(t)

	errCh := make(chan error, 1)
	go func() { errCh <- New("127.0.0.1:0").Serve() }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Serve before Listen returned nil, want an error")
		}
	case <-time.After(testTimeout):
		t.Fatal("Serve before Listen blocked instead of returning an error")
	}
}

// TestAddrIsPopulated pins the contract tests rely on when they pass ":0":
// after Listen, Addr reports a real, non-zero port.
func TestAddrIsPopulated(t *testing.T) {
	discardLogs(t)

	srv := New("127.0.0.1:0")
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	t.Cleanup(func() { _ = srv.Shutdown(ctx) })

	addr := srv.Addr()
	if addr == nil {
		t.Fatal("Addr() returned nil after a successful Listen")
	}

	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatalf("Addr() = %q, not a host:port address: %v", addr, err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("Addr() port %q is not a number: %v", port, err)
	}
	if n == 0 {
		t.Fatalf("Addr() = %q, port is still 0; Listen should have bound a real port", addr)
	}
}

// TestListenIsIdempotent documents that a second Listen does not rebind and
// does not lose the first address.
func TestListenIsIdempotent(t *testing.T) {
	discardLogs(t)

	srv := New("127.0.0.1:0")
	if err := srv.Listen(); err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	first := srv.Addr().String()

	if err := srv.Listen(); err != nil {
		t.Fatalf("second Listen: %v", err)
	}
	if second := srv.Addr().String(); second != first {
		t.Fatalf("second Listen changed the address from %s to %s", first, second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestStartServerHelperIsIsolated guards the test harness itself: two servers
// started in the same process must get different ports, or every test above
// would be silently sharing one.
func TestStartServerHelperIsIsolated(t *testing.T) {
	discardLogs(t)

	first, _ := startServer(t)
	second, _ := startServer(t)

	if first == second {
		t.Fatalf("both servers bound %s; the ephemeral port is not actually ephemeral", first)
	}
}

// TestCustomRegistryIsUsed checks the seam that lets a future milestone or a
// test swap in a different command set: the server dispatches through
// whatever registry it was given.
func TestCustomRegistryIsUsed(t *testing.T) {
	discardLogs(t)

	srv := New("127.0.0.1:0")
	srv.registry = commands.NewRegistry() // deliberately empty
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(func() {
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		cancel()
		select {
		case <-serveDone:
		case <-time.After(testTimeout):
			t.Error("Serve did not return after Shutdown")
		}
	})

	c := newClient(t, srv.Addr().String())
	c.send(commandFrame("PING"))

	// The custom registry has no PING, so the reply must be an unknown-command
	// error rather than PONG.
	errValue := c.mustError()
	if !bytes.Contains([]byte(errValue), []byte("unknown command")) {
		t.Fatalf("reply = %q, want an unknown-command error", errValue)
	}
}

// TestDefaultPort pins the default, because it is a compatibility promise: a
// client that defaults to 6379 must find Verdigris there.
func TestDefaultPort(t *testing.T) {
	if DefaultPort != 6379 {
		t.Fatalf("DefaultPort = %d, want 6379", DefaultPort)
	}
}

// TestReadChunkEnforcesFrameLimit covers the resource bound on accumulated
// bytes: without it, a client can declare a huge bulk string and dribble
// bytes forever while the buffer grows without limit.
//
// The cases are built against a tiny stand-in chunk rather than a real
// maxFrameSize buffer, because the interesting behaviour is the two boundary
// conditions — "already at the ceiling" and "this read would cross it" — and
// driving 64 MiB through a socket to reach them would cost the suite real
// time and prove nothing extra. The arithmetic is written in terms of
// maxFrameSize, so it stays correct if the constant moves.
func TestReadChunkEnforcesFrameLimit(t *testing.T) {
	const payload = "would-be-payload"

	t.Run("exactly at the limit is refused without reading", func(t *testing.T) {
		reader := bufio.NewReader(strings.NewReader(payload))

		// Prime the reader's internal buffer. bufio.Reader.Buffered reports 0
		// until the first fill happens, so asserting on it without this would
		// pass for the wrong reason — it would read 0 because nothing has been
		// pulled in yet, not because readChunk left the data alone.
		if _, err := reader.Peek(1); err != nil {
			t.Fatalf("Peek: %v", err)
		}

		n, err := readChunk(reader, make([]byte, 16), maxFrameSize)
		if !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("readChunk at the limit returned %v, want errFrameTooLarge", err)
		}
		if n != 0 {
			t.Fatalf("readChunk at the limit read %d bytes, want 0", n)
		}
		if reader.Buffered() != len(payload) {
			t.Fatalf("readChunk consumed from the reader while rejecting the frame; %d bytes buffered, want %d",
				reader.Buffered(), len(payload))
		}
	})

	t.Run("past the limit is refused too", func(t *testing.T) {
		reader := bufio.NewReader(strings.NewReader(payload))

		if _, err := readChunk(reader, make([]byte, 16), maxFrameSize+1); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("readChunk past the limit returned %v, want errFrameTooLarge", err)
		}
	})

	t.Run("a read that would cross the limit is truncated to fit", func(t *testing.T) {
		reader := bufio.NewReader(strings.NewReader(payload))

		// buffered + len(chunk) > maxFrameSize, so the chunk is clipped to
		// leave exactly maxFrameSize bytes buffered once it is appended.
		buffered := maxFrameSize - 2

		n, err := readChunk(reader, make([]byte, 64), buffered)
		if err != nil {
			t.Fatalf("readChunk returned %v, want nil", err)
		}
		if n != 2 {
			t.Fatalf("readChunk read %d bytes, want 2 (the room left below the limit)", n)
		}
		if buffered+n != maxFrameSize {
			t.Fatalf("buffered %d + read %d = %d, want exactly maxFrameSize %d", buffered, n, buffered+n, maxFrameSize)
		}
	})

	t.Run("well below the limit reads normally", func(t *testing.T) {
		reader := bufio.NewReader(strings.NewReader(payload))

		n, err := readChunk(reader, make([]byte, 64), 0)
		if err != nil {
			t.Fatalf("readChunk returned %v, want nil", err)
		}
		if n != len(payload) {
			t.Fatalf("readChunk read %d bytes, want all %d of the payload", n, len(payload))
		}
	})
}
