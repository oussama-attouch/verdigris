package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"
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

// dial connects to addr and registers the connection for cleanup. With the
// deadline set, a server that never replies fails the test quickly instead of
// hanging until the go test timeout.
func dial(t *testing.T, addr string) net.Conn {
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

	return conn
}

// readExactly reads exactly n bytes, failing the test if the server sends
// fewer. Used for line echoes, where the expected length is known.
func readExactly(t *testing.T, conn net.Conn, n int) []byte {
	t.Helper()

	got := make([]byte, n)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("reading %d echoed bytes: %v", n, err)
	}
	return got
}

// closeWrite half-closes the client's sending side. The server reads until it
// sees this EOF and echoes anything it still holds, which lets a test send a
// payload that has no '\n' in it and still get a deterministic answer with no
// sleeps.
func closeWrite(t *testing.T, conn net.Conn) {
	t.Helper()

	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("connection is %T, not *net.TCPConn", conn)
	}
	if err := tcp.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
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

	shutdown = func() {
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
	}

	t.Cleanup(shutdown)

	return srv.Addr().String(), shutdown
}

// TestEchoSingleLine is the smallest statement of milestone 1: one line in,
// the same bytes out.
func TestEchoSingleLine(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	conn := dial(t, addr)
	const line = "hello\r\n"
	if _, err := conn.Write([]byte(line)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := readExactly(t, conn, len(line))
	if string(got) != line {
		t.Fatalf("echo = %q, want %q", got, line)
	}
}

// TestEchoMultipleLines checks that a single connection can carry several
// exchanges and that they come back in order, one per line, with none of them
// merged or dropped.
func TestEchoMultipleLines(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	conn := dial(t, addr)

	lines := []string{"first\r\n", "second\r\n", "third\r\n"}
	for i, line := range lines {
		if _, err := conn.Write([]byte(line)); err != nil {
			t.Fatalf("Write line %d: %v", i, err)
		}

		got := readExactly(t, conn, len(line))
		if string(got) != line {
			t.Fatalf("line %d: echo = %q, want %q", i, got, line)
		}
	}
}

// TestEchoPreservesBinaryBytes is the test that catches a reader which
// converts to string too early.
//
// The payload mixes NUL, 0xFF, CR and LF, and ends with a terminator so the
// server's line-based read has something to trigger on. If the handler used
// ReadString and re-encoded, or trimmed the terminator, or wrote the whole
// read buffer instead of the bytes it actually read, this comparison fails.
func TestEchoPreservesBinaryBytes(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	conn := dial(t, addr)

	// 0x00 NUL, 0xFF (not valid UTF-8 on its own), CR, LF, and an
	// interleaved sequence of all four.
	payload := []byte{0x00, 0xff, '\r', '\n', 0xfe, 0x00, '\r', 0xff, '\n'}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := readExactly(t, conn, len(payload))
	if !bytes.Equal(got, payload) {
		t.Fatalf("binary round trip failed\n sent: % x\n got:  % x", payload, got)
	}
}

// TestEchoBinaryWithoutTerminator closes the write side instead of sending a
// newline, which proves the handler echoes what it holds on EOF rather than
// discarding a final partial line.
func TestEchoBinaryWithoutTerminator(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	conn := dial(t, addr)

	payload := []byte{0x00, 0xff, '\r', 0x01, 0xfe}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	closeWrite(t, conn)

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("binary round trip failed\n sent: % x\n got:  % x", payload, got)
	}
}

// TestGracefulShutdown checks the whole shutdown contract: Serve returns
// rather than hanging, Shutdown reports success, and the listener really is
// closed afterwards.
func TestGracefulShutdown(t *testing.T) {
	discardLogs(t)

	srv := New("127.0.0.1:0")
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := srv.Addr().String()

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()

	// One exchange first, so the shutdown is exercised with a connection that
	// has actually been served rather than an idle listener.
	conn := dial(t, addr)
	if _, err := conn.Write([]byte("before shutdown\r\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := readExactly(t, conn, len("before shutdown\r\n")); string(got) != "before shutdown\r\n" {
		t.Fatalf("echo = %q, want %q", got, "before shutdown\r\n")
	}

	// Close the client so its handler ends on its own and the drain is the
	// fast path. The timeout path is covered by TestShutdownSignalsAbandoned.
	if err := conn.Close(); err != nil {
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

	// The listener must be closed, so a new connection is refused. This is
	// the observable meaning of "graceful shutdown" for a client.
	if c, err := net.Dial("tcp", addr); err == nil {
		_ = c.Close()
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

	// Open a connection and deliberately send nothing. Its handler blocks in
	// ReadBytes and will not finish on its own.
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

	// The abandoned handler was signalled by having its connection closed, so
	// the client sees the read end immediately rather than staying open.
	if err := conn.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection is still open; Shutdown should have closed it on timeout")
	}
}

// TestConcurrentConnections proves the per-connection isolation is real: each
// client must get back its own payload, never another client's, and never a
// concatenation of both.
func TestConcurrentConnections(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	const clients = 8

	type result struct {
		payload string
		got     string
		err     error
	}
	results := make(chan result, clients)

	for i := range clients {
		go func() {
			payload := "payload-" + strconv.Itoa(i) + "\r\n"

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				results <- result{payload: payload, err: err}
				return
			}
			defer conn.Close()

			if err := conn.SetDeadline(time.Now().Add(testTimeout)); err != nil {
				results <- result{payload: payload, err: err}
				return
			}
			if _, err := conn.Write([]byte(payload)); err != nil {
				results <- result{payload: payload, err: err}
				return
			}

			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, buf); err != nil {
				results <- result{payload: payload, err: err}
				return
			}
			results <- result{payload: payload, got: string(buf)}
		}()
	}

	for range clients {
		res := <-results
		if res.err != nil {
			t.Fatalf("client for %q: %v", res.payload, res.err)
		}
		if res.got != res.payload {
			t.Fatalf("client sent %q but got %q", res.payload, res.got)
		}
	}
}

// TestAddrIsPopulated pins the contract tests and anything else rely on when
// they pass ":0": after Listen, Addr reports a real, non-zero port.
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

// TestEchoIsLineBufferedNotByteBuffered documents the framing this milestone
// chose, so that milestone 2 has an explicit starting point: the handler
// replies once it has a complete line, and a lone '\r' is not a line.
func TestEchoIsLineBufferedNotByteBuffered(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	conn := dial(t, addr)

	// Send CR only — no terminator yet, so no echo should be produced.
	if _, err := conn.Write([]byte("partial\r")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Deadlines are made generous rather than short here, because this test
	// asserts an absence. A short deadline would pass on a slow machine for
	// the wrong reason.
	if err := conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if n, err := conn.Read(make([]byte, 16)); err == nil {
		t.Fatalf("read %d bytes before the line was terminated; handler is not line-buffered", n)
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("read error = %v, want a timeout", err)
		}
	}

	// Complete the line; now the whole thing comes back at once, with the
	// terminator preserved.
	if err := conn.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := conn.Write([]byte("\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	const want = "partial\r\n"
	got := readExactly(t, conn, len(want))
	if string(got) != want {
		t.Fatalf("echo = %q, want %q", got, want)
	}
}

// TestDefaultPort pins the default, because it is a compatibility promise: a
// client that defaults to 6379 must find Verdigris there.
func TestDefaultPort(t *testing.T) {
	if DefaultPort != 6379 {
		t.Fatalf("DefaultPort = %d, want 6379", DefaultPort)
	}
}
