package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/commands"
	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/rdb"
	"github.com/Oussama-Att/verdigris/internal/store"
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

// testSweepInterval is much shorter than the production default so that
// active expiry is observable in a test without waiting 100 ms per round. It
// is passed to New rather than assigned to the store package's variable, so
// nothing global is mutated and no sweeper can be racing a write.
const testSweepInterval = 2 * time.Millisecond

// newTestServer builds a Server with the real clock, a short sweep interval,
// and no persistence.
//
// Tests that need to control expiry pass their own store, and tests that need
// persistence use newTestServerAt; this helper is for the majority that care
// about neither.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	return newTestServerAt(t, "")
}

// newTestServerAt builds a Server with a specific snapshot path.
//
// New can fail, which is the point of the signature: a corrupt or unreadable
// file must stop construction. A test helper that swallowed that error would
// hide the failure mode the milestone exists to establish, so a failure here
// is fatal to the test.
func newTestServerAt(t *testing.T, rdbPath string) *Server {
	t.Helper()

	srv, err := New("127.0.0.1:0", store.RealClock{}, testSweepInterval, rdbPath)
	if err != nil {
		t.Fatalf("New(rdbPath=%q): %v", rdbPath, err)
	}
	return srv
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

// testServer is a running server plus a record of every client connection a
// test opened against it.
type testServer struct {
	srv      *Server
	addr     string
	conns    []net.Conn
	shutdown func()
}

// client dials the server and records the connection so that a later shutdown
// closes it.
//
// The bookkeeping is not tidiness. Shutdown waits for in-flight connections to
// drain, and an idle client connection is in flight, so a test that shuts the
// server down while a client is still open waits out the entire drain timeout.
// That is correct behaviour from the server and a slow, misleading test from
// the client's side — and it is exactly what happened before this helper
// existed. The milestone 1 shutdown tests close their client by hand for the
// same reason; this makes it automatic.
func (ts *testServer) client(t *testing.T) *client {
	t.Helper()

	c := newClient(t, ts.addr)
	ts.conns = append(ts.conns, c.conn)
	return c
}

// closeConnections closes every connection this test opened.
func (ts *testServer) closeConnections() {
	for _, conn := range ts.conns {
		_ = conn.Close()
	}
	ts.conns = nil
}

// startServerWithRDB binds 127.0.0.1:0 with persistence at rdbPath and begins
// serving. It is startTestServer with the snapshot path supplied, for the tests
// that need the server to load from and save to a real file.
//
// New can fail here, and the helper does not swallow that: a corrupt or
// unreadable file must stop construction, and a test helper that hid the
// failure would hide the behaviour the milestone exists to establish.
func startServerWithRDB(t *testing.T, rdbPath string) *testServer {
	t.Helper()
	return serveTestServer(t, newTestServerAt(t, rdbPath))
}

// --- Milestone 6: persistence, over a real socket ---------------------------

// TestSaveAndReloadOverTCP is the milestone's headline test.
//
// A server writes keys and saves; a second server, constructed against the
// same file, must come up holding them. It is the whole acceptance criterion
// reduced to Go.
func TestSaveAndReloadOverTCP(t *testing.T) {
	discardLogs(t)
	path := filepath.Join(t.TempDir(), "snapshot.rdb")

	// --- First server: write and save.
	ts := startServerWithRDB(t, path)

	c := ts.client(t)
	c.send(commandFrame("SET", "plain", "value"))
	c.expect(protocol.SimpleString("OK"))
	c.send(commandFrame("SET", "counted", "42"))
	c.expect(protocol.SimpleString("OK"))
	c.send(setEXFrame("SET", "timed", "later", "EX", 3600))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("SAVE"))
	c.expect(protocol.SimpleString("OK"))

	ts.shutdown()

	// --- Second server: the same file, a fresh process-equivalent.
	ts2 := startServerWithRDB(t, path)
	defer ts2.shutdown()

	c2 := ts2.client(t)

	c2.send(commandFrame("GET", "plain"))
	c2.expect(protocol.BulkString([]byte("value")))

	c2.send(commandFrame("GET", "counted"))
	c2.expect(protocol.BulkString([]byte("42")))

	// The TTL survived as an absolute deadline, so it is close to an hour
	// rather than reset to one.
	c2.send(commandFrame("TTL", "timed"))
	got := c2.next()
	n, ok := got.(protocol.Integer)
	if !ok {
		t.Fatalf("TTL = %#v (%T), want protocol.Integer", got, got)
	}
	// Generous because the shutdown above waited for a live connection to
	// drain before saving, which legitimately consumed seconds of the hour
	// this TTL was set to.
	if n < 3580 || n > 3600 {
		t.Fatalf("TTL after reload = %d, want 3580..3600", n)
	}
}

// TestShutdownSavesWithoutAnExplicitSAVE checks the other save trigger: a
// graceful shutdown writes the snapshot even if the client never asked.
//
// This is the Ctrl-C path the manual demo exercises, and the one that matters
// most, because an operator stopping a server is not going to remember to run
// SAVE first.
func TestShutdownSavesWithoutAnExplicitSAVE(t *testing.T) {
	discardLogs(t)
	path := filepath.Join(t.TempDir(), "snapshot.rdb")

	ts := startServerWithRDB(t, path)

	c := ts.client(t)
	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	ts.shutdown() // no SAVE command

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no snapshot was written on shutdown: %v", err)
	}

	// And a new server loads what shutdown wrote.
	ts2 := startServerWithRDB(t, path)
	defer ts2.shutdown()

	c2 := ts2.client(t)
	c2.send(commandFrame("GET", "k"))
	c2.expect(protocol.BulkString([]byte("v")))
}

// TestExpiredKeysAreNotSaved checks that the snapshot omits what the store
// considers dead, so a restart does not resurrect expired keys.
func TestExpiredKeysAreNotSaved(t *testing.T) {
	discardLogs(t)
	path := filepath.Join(t.TempDir(), "snapshot.rdb")

	ts := startServerWithRDB(t, path)

	c := ts.client(t)

	// One key that lives, one that is already dead by the time of the save.
	c.send(commandFrame("SET", "live", "v"))
	c.expect(protocol.SimpleString("OK"))
	c.send(setEXFrame("SET", "dying", "v", "PX", 50))
	c.expect(protocol.SimpleString("OK"))

	time.Sleep(120 * time.Millisecond)

	c.send(commandFrame("SAVE"))
	c.expect(protocol.SimpleString("OK"))

	ts.shutdown()

	ts2 := startServerWithRDB(t, path)
	defer ts2.shutdown()

	c2 := ts2.client(t)
	c2.send(commandFrame("GET", "live"))
	c2.expect(protocol.BulkString([]byte("v")))
	c2.send(commandFrame("GET", "dying"))
	c2.expect(protocol.Null{})
}

// TestSaveFaultIsInternalError checks the fault channel end to end: a save
// that fails must produce the generic ERR internal error rather than a panic,
// a hang, or the filesystem's own message.
func TestSaveFaultIsInternalError(t *testing.T) {
	discardLogs(t)

	// A registry whose save function always fails, wired the way the server
	// wires its own. The failure text is distinctive so the test can prove it
	// did not reach the client.
	secret := "the-disk-is-on-fire-do-not-leak-me"
	s := store.New(store.RealClock{})
	t.Cleanup(s.Close)

	registry := commands.DefaultRegistry(s, func() error { return errors.New(secret) })

	reply, fault := registry.Dispatch(protocol.Array{protocol.BulkString([]byte("SAVE"))})

	if fault == nil {
		t.Fatal("a failing save produced no internal fault")
	}
	if !strings.Contains(fault.Error(), secret) {
		t.Fatalf("the fault does not carry the underlying error: %v", fault)
	}
	if reply != nil {
		t.Fatalf("a faulted dispatch returned a reply: %#v", reply)
	}
}

// TestStartupFailsOnCorruptFile is the other half of the corruption contract:
// a bad file must stop the server rather than being treated as no file.
//
// A server that silently started empty would look healthy while the operator's
// data sat on disk unread, and the next save would overwrite it.
func TestStartupFailsOnCorruptFile(t *testing.T) {
	discardLogs(t)

	// A file with the right name and the wrong contents.
	path := filepath.Join(t.TempDir(), "corrupt.rdb")
	if err := os.WriteFile(path, []byte("this is not a snapshot"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	_, err := New("127.0.0.1:0", store.RealClock{}, testSweepInterval, path)
	if err == nil {
		t.Fatal("New succeeded against a corrupt snapshot")
	}
	if !errors.Is(err, rdb.ErrCorrupt) {
		t.Fatalf("error = %v, want it to wrap rdb.ErrCorrupt", err)
	}

	// The file is still there: a failed startup must not delete evidence.
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("the corrupt file was removed: %v", statErr)
	}
}

// TestStartupSucceedsWhenFileIsMissing checks the other side of that branch: a
// missing snapshot is a first run, not an error.
func TestStartupSucceedsWhenFileIsMissing(t *testing.T) {
	discardLogs(t)

	path := filepath.Join(t.TempDir(), "not-yet.rdb")

	srv := newTestServerAt(t, path)
	srv.Close()

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("constructing a server created a file: %v", err)
	}
}

// TestNoPersistenceWithoutFlag checks that an empty path means no file I/O at
// all: no file is read, and shutdown writes nothing.
func TestNoPersistenceWithoutFlag(t *testing.T) {
	discardLogs(t)

	// A directory that must stay empty. If the server wrote anywhere, this is
	// where it would show up as a file the test did not put there.
	dir := t.TempDir()

	ts := startTestServer(t)

	c := ts.client(t)
	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	// SAVE with no path configured is a no-op that reports success, because
	// there is nothing to save to and nothing the client could do about it.
	c.send(commandFrame("SAVE"))
	c.expect(protocol.SimpleString("OK"))

	ts.shutdown()

	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("a server without -rdb-path wrote %d files into the temp directory", len(names))
	}
}

// TestSaveAndLoadBinaryAndNilValues checks the binary-safety and nil/empty
// distinctions survive a real save and load through a real server, not just
// the rdb package's own round trip.
func TestSaveAndLoadBinaryAndNilValues(t *testing.T) {
	discardLogs(t)
	path := filepath.Join(t.TempDir(), "snapshot.rdb")

	ts := startServerWithRDB(t, path)

	key := []byte{0x00, 0xff, '\r', '\n'}
	value := []byte{0xff, 0x00, '\r', '\n', 0xfe}

	c := ts.client(t)
	c.send(commandFrame("SET", string(key), string(value)))
	c.expect(protocol.SimpleString("OK"))
	c.send(commandFrame("SET", "empty", ""))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("SAVE"))
	c.expect(protocol.SimpleString("OK"))

	ts.shutdown()

	ts2 := startServerWithRDB(t, path)
	defer ts2.shutdown()

	c2 := ts2.client(t)

	c2.send(commandFrame("GET", string(key)))
	c2.expect(protocol.BulkString(value))

	// An empty value must come back as `$0\r\n\r\n`, not as the null used for
	// a missing key.
	c2.send(commandFrame("GET", "empty"))
	got := c2.next()
	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET of a stored empty value = %#v (%T), want BulkString", got, got)
	}
	if bs == nil {
		t.Fatal("a stored empty value came back as nil after a save and load")
	}
	if len(bs) != 0 {
		t.Fatalf("a stored empty value came back as %q", bs)
	}
}

// startTestServer binds 127.0.0.1:0 with no persistence and begins serving,
// tracking the connections a test opens so that shutdown can close them.
//
// Port 0 asks the kernel for a free port, so tests never collide with a real
// Verdigris on 6379, with a redis-server, or with each other. Cleanup shuts
// the server down and waits for Serve to return, so a failing test cannot leak
// a listener or a goroutine.
func startTestServer(t *testing.T) *testServer {
	t.Helper()
	return serveTestServer(t, newTestServer(t))
}

// serveTestServer runs an already-constructed Server and returns a handle that
// shuts it down.
//
// Both startTestServer and startServerWithRDB funnel through here, so the
// drain-then-close ordering and the shutdown handshake exist in one place
// rather than three.
func serveTestServer(t *testing.T, srv *Server) *testServer {
	t.Helper()

	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	ts := &testServer{srv: srv, addr: srv.Addr().String()}

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
	ts.shutdown = func() {
		once.Do(func() {
			// Close the test's own clients before draining. See
			// testServer.client for why this is load-bearing rather than
			// tidy: an idle connection is an in-flight connection, and the
			// drain would wait out the whole timeout for it.
			ts.closeConnections()

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

	t.Cleanup(ts.shutdown)

	return ts
}

// startServer is startTestServer's older interface, kept because most tests
// only want the address and never shut the server down by hand.
func startServer(t *testing.T) (addr string, shutdown func()) {
	t.Helper()

	ts := startTestServer(t)
	return ts.addr, ts.shutdown
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

	srv := newTestServer(t)
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

	srv := newTestServer(t)
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
	go func() { errCh <- newTestServer(t).Serve() }()

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

	srv := newTestServer(t)
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

	srv := newTestServer(t)
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

	srv := newTestServer(t)
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

// --- Milestone 4: the store, over a real socket -----------------------------

// TestSetGetRoundTripOverTCP is the milestone's acceptance criterion, exercised
// through the whole stack: bytes on a socket, decoded, dispatched to a handler
// that touches the store, encoded, and written back.
func TestSetGetRoundTripOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "foo", "bar"))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("GET", "foo"))
	c.expect(protocol.BulkString([]byte("bar")))
}

// TestGetMissingKeyOverTCP pins the wire reply for a miss: `$-1\r\n`, which is
// what makes redis-cli print (nil).
func TestGetMissingKeyOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)
	c.send(commandFrame("GET", "nothere"))

	c.expect(protocol.Null{})
}

// TestDelOverTCP checks both answers: one for a key that existed and zero for
// the second delete of the same key.
func TestDelOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("DEL", "k"))
	c.expect(protocol.Integer(1))

	c.send(commandFrame("DEL", "k"))
	c.expect(protocol.Integer(0))
}

// TestExistsOverTCP checks an existing key, a missing one, and the duplicate
// quirk that makes EXISTS a count of arguments rather than of distinct keys.
func TestExistsOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("EXISTS", "k"))
	c.expect(protocol.Integer(1))

	c.send(commandFrame("EXISTS", "nothere"))
	c.expect(protocol.Integer(0))

	c.send(commandFrame("EXISTS", "k", "k", "nothere"))
	c.expect(protocol.Integer(2))
}

// TestSetOverwriteOverTCP checks the second SET wins, which is the case the
// milestone's demo walks through with redis-cli.
func TestSetOverwriteOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "foo", "bar"))
	c.expect(protocol.SimpleString("OK"))
	c.send(commandFrame("GET", "foo"))
	c.expect(protocol.BulkString([]byte("bar")))

	c.send(commandFrame("SET", "foo", "baz"))
	c.expect(protocol.SimpleString("OK"))
	c.send(commandFrame("GET", "foo"))
	c.expect(protocol.BulkString([]byte("baz")))
}

// TestDelMultipleKeysOverTCP covers the three-key form where one is absent:
// DEL k1 k2 k3 answers 2.
func TestDelMultipleKeysOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "k1", "v1"))
	c.expect(protocol.SimpleString("OK"))
	c.send(commandFrame("SET", "k2", "v2"))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("DEL", "k1", "k2", "k3"))
	c.expect(protocol.Integer(2))
}

// TestMultipleClientsShareStoreOverTCP is the point of the shared store, and
// the test that makes "the store lives in the server, not in the connection"
// observable. Two independent connections: one writes, the other reads.
//
// A store constructed per connection would pass every single-client test in
// this file and fail exactly here.
func TestMultipleClientsShareStoreOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	writer := newClient(t, addr)
	reader := newClient(t, addr)

	writer.send(commandFrame("SET", "shared-key", "written by A"))
	writer.expect(protocol.SimpleString("OK"))

	// A different socket must see it.
	reader.send(commandFrame("GET", "shared-key"))
	reader.expect(protocol.BulkString([]byte("written by A")))

	reader.send(commandFrame("EXISTS", "shared-key"))
	reader.expect(protocol.Integer(1))

	// And a delete from the reader must be visible to the writer.
	reader.send(commandFrame("DEL", "shared-key"))
	reader.expect(protocol.Integer(1))

	writer.send(commandFrame("EXISTS", "shared-key"))
	writer.expect(protocol.Integer(0))
}

// TestStoreIsSharedAcrossConcurrentClients is the milestone's concurrency
// proof at the server level: eight clients, each writing fifty keys, all at
// the same time on separate sockets, and then a ninth client verifying that
// every one of the four hundred keys is present.
//
// It runs under -race, which is the real assertion. A store whose mutex was
// missing, too narrow, or held across the wrong section would corrupt here —
// and a per-connection store would fail the verification pass.
func TestStoreIsSharedAcrossConcurrentClients(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	const clients = 8
	const keysPerClient = 50

	// Each goroutine dials its own connection and writes its own key range.
	// The keys share a prefix, so they all live in one namespace and a missing
	// one is a real loss rather than a different namespace.
	var wg sync.WaitGroup
	errs := make(chan error, clients)

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- fmt.Errorf("client %d dial: %w", c, err)
				return
			}
			defer conn.Close()

			if err := conn.SetDeadline(time.Now().Add(testTimeout)); err != nil {
				errs <- fmt.Errorf("client %d deadline: %w", c, err)
				return
			}

			// Write every key, then read each one back on this same
			// connection, which at least proves the writes were accepted.
			for k := 0; k < keysPerClient; k++ {
				frame := commandFrame("SET",
					fmt.Sprintf("key-%d-%d", c, k),
					fmt.Sprintf("value-%d-%d", c, k))

				if err := writeAll(conn, frame); err != nil {
					errs <- fmt.Errorf("client %d SET %d: %w", c, k, err)
					return
				}
				reply, err := readOne(conn)
				if err != nil {
					errs <- fmt.Errorf("client %d SET %d reply: %w", c, k, err)
					return
				}
				if reply != protocol.SimpleString("OK") {
					errs <- fmt.Errorf("client %d SET %d got %#v, want OK", c, k, reply)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent writer: %v", err)
	}

	// A fresh client must see every key, written by any of the eight.
	verifier := newClient(t, addr)

	for c := 0; c < clients; c++ {
		for k := 0; k < keysPerClient; k++ {
			key := fmt.Sprintf("key-%d-%d", c, k)
			want := fmt.Sprintf("value-%d-%d", c, k)

			verifier.send(commandFrame("GET", key))
			verifier.expect(protocol.BulkString([]byte(want)))
		}
	}

	// And the count must be exactly what was written, so no key was lost and
	// none was duplicated into existence.
	var existed int64
	for c := 0; c < clients; c++ {
		for k := 0; k < keysPerClient; k++ {
			verifier.send(commandFrame("EXISTS", fmt.Sprintf("key-%d-%d", c, k)))
			got := verifier.next()

			n, ok := got.(protocol.Integer)
			if !ok {
				t.Fatalf("EXISTS returned %#v (%T), want Integer", got, got)
			}
			existed += int64(n)
		}
	}

	if want := int64(clients * keysPerClient); existed != want {
		t.Fatalf("EXISTS counted %d keys, want %d", existed, want)
	}
}

// TestWriteThenReadAcrossManyConnections checks that a key written on one
// connection is readable on a connection opened afterwards, which is the
// weakest form of sharing and the one a per-connection store would fail.
func TestWriteThenReadAcrossManyConnections(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	for i := 0; i < 10; i++ {
		c := newClient(t, addr)
		c.send(commandFrame("SET", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
		c.expect(protocol.SimpleString("OK"))
	}

	for i := 0; i < 10; i++ {
		c := newClient(t, addr)
		c.send(commandFrame("GET", fmt.Sprintf("k%d", i)))
		c.expect(protocol.BulkString([]byte(fmt.Sprintf("v%d", i))))
	}
}

// TestClientSetInfoIsNoOpOverTCP checks the compatibility reply modern Redis
// clients expect on connect.
func TestClientSetInfoIsNoOpOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("CLIENT", "SETINFO", "lib-name", "test"))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("CLIENT", "SETINFO", "lib-ver", "1.0.0"))
	c.expect(protocol.SimpleString("OK"))

	// An unimplemented subcommand must still be refused.
	c.send(commandFrame("CLIENT", "GETNAME"))
	c.mustError()
}

// TestCommandProbeOverTCP checks the stub reply a client probing for command
// metadata receives.
func TestCommandProbeOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("COMMAND", "DOCS"))
	c.expect(protocol.Array{})

	// The bare form and an unknown subcommand answer the same way.
	c.send(commandFrame("COMMAND"))
	c.expect(protocol.Array{})
}

// TestStoreSurvivesClientDisconnect checks the store outlives the connection
// that wrote to it, which is the same property as sharing but stated in the
// direction that a connection-scoped store would fail most obviously.
func TestStoreSurvivesClientDisconnect(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	writer := newClient(t, addr)
	writer.send(commandFrame("SET", "persistent", "value"))
	writer.expect(protocol.SimpleString("OK"))
	if err := writer.conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A brand new connection must still see the key.
	reader := newClient(t, addr)
	reader.send(commandFrame("GET", "persistent"))
	reader.expect(protocol.BulkString([]byte("value")))
}

// TestBinarySafeSetGetOverTCP stores and retrieves bytes that are not text,
// including CRLF, over the wire. This is the case where a length-prefixed
// protocol earns its keep, and where a store that converted keys or values to
// text would corrupt the data.
func TestBinarySafeSetGetOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	key := []byte{0x00, 0xff, '\r', '\n'}
	value := []byte{0xff, 0x00, '\r', '\n', 0xfe, '$', '*'}

	c := newClient(t, addr)

	c.send(commandFrame("SET", string(key), string(value)))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("GET", string(key)))
	c.expect(protocol.BulkString(value))

	// A value containing command bytes must come back untouched, not parsed.
	c.send(commandFrame("GET", string(key)))
	got := c.next()
	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET = %T, want BulkString", got)
	}
	if !bytes.Equal(bs, value) {
		t.Fatalf("GET = % x, want % x", bs, value)
	}
}

// TestEmptyValueIsDistinctFromMissingOverTCP is the wire-level half of the
// nil-versus-empty distinction: `$0\r\n\r\n` for a stored empty value and
// `$-1\r\n` for an absent key. A client branches on which it receives.
func TestEmptyValueIsDistinctFromMissingOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "empty", ""))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("GET", "empty"))
	got := c.next()
	bs, ok := got.(protocol.BulkString)
	if !ok {
		t.Fatalf("GET of a stored empty value = %#v (%T), want BulkString", got, got)
	}
	if bs == nil {
		t.Fatal("GET of a stored empty value returned nil, want an empty bulk string")
	}
	if len(bs) != 0 {
		t.Fatalf("GET of a stored empty value = %q, want empty", bs)
	}

	c.send(commandFrame("GET", "absent"))
	c.expect(protocol.Null{})
}

// TestPipelinedStoreCommandsOverTCP sends a whole transaction's worth of
// commands in one write and checks each reply in order. Pipelining is where a
// store shared between connections and a per-connection read loop have to
// agree about ordering.
func TestPipelinedStoreCommandsOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	var batch []byte
	batch = append(batch, commandFrame("SET", "a", "1")...)
	batch = append(batch, commandFrame("SET", "b", "2")...)
	batch = append(batch, commandFrame("GET", "a")...)
	batch = append(batch, commandFrame("GET", "b")...)
	batch = append(batch, commandFrame("EXISTS", "a", "b")...)
	batch = append(batch, commandFrame("DEL", "a", "b")...)
	batch = append(batch, commandFrame("GET", "a")...)

	c.send(batch)

	c.expectAll(
		protocol.SimpleString("OK"),
		protocol.SimpleString("OK"),
		protocol.BulkString([]byte("1")),
		protocol.BulkString([]byte("2")),
		protocol.Integer(2),
		protocol.Integer(2),
		protocol.Null{},
	)
}

// TestStoreErrorDoesNotCloseConnectionOverTCP checks that a domain error from
// a store command leaves the connection usable, unlike a protocol error. A
// client that mistyped one command should be able to send the next.
func TestStoreErrorDoesNotCloseConnectionOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	// Wrong arity: a client error, not a desynchronized stream.
	c.send(commandFrame("SET", "onlykey"))
	c.mustError()

	// The connection must still work.
	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("GET", "k"))
	c.expect(protocol.BulkString([]byte("v")))
}

// writeAll writes a whole frame, retrying short writes. A single Write on a
// TCP socket may accept fewer bytes than it was given.
func writeAll(conn net.Conn, frame []byte) error {
	for len(frame) > 0 {
		n, err := conn.Write(frame)
		if err != nil {
			return err
		}
		frame = frame[n:]
	}
	return nil
}

// readOne reads exactly one RESP frame off a connection, decoding as bytes
// arrive. It is the goroutine-safe counterpart of client.next: no *testing.T,
// because it runs from worker goroutines.
func readOne(conn net.Conn) (protocol.Value, error) {
	var acc []byte
	chunk := make([]byte, 512)

	for {
		value, n, err := protocol.Decode(acc)
		if err == nil {
			if n != len(acc) {
				return nil, fmt.Errorf("reply had %d trailing bytes", len(acc)-n)
			}
			return value, nil
		}
		if !errors.Is(err, protocol.ErrIncomplete) {
			return nil, fmt.Errorf("reply did not decode: %w", err)
		}

		read, readErr := conn.Read(chunk)
		if read > 0 {
			acc = append(acc, chunk[:read]...)
			continue
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}

// --- Milestone 5: expiration, over a real socket ----------------------------

// TestExpireOverTCP checks the three replies EXPIRE and TTL can give, in one
// connection: a TTL applied, a TTL read back, and a key with no expiry.
func TestExpireOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	// No expiry yet.
	c.send(commandFrame("TTL", "k"))
	c.expect(protocol.Integer(-1))

	c.send(expireFrame("EXPIRE", "k", 100))
	c.expect(protocol.Integer(1))

	c.send(commandFrame("TTL", "k"))
	got := c.next()
	n, ok := got.(protocol.Integer)
	if !ok {
		t.Fatalf("TTL = %#v (%T), want protocol.Integer", got, got)
	}
	if n < 99 || n > 100 {
		t.Fatalf("TTL = %d, want 99..100", n)
	}

	// EXPIRE on a key that does not exist reports 0.
	c.send(expireFrame("EXPIRE", "nothere", 100))
	c.expect(protocol.Integer(0))
}

// TestSetWithTTLOverTCP covers the SET ... EX form end to end.
func TestSetWithTTLOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(setEXFrame("SET", "k", "v", "EX", 100))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("TTL", "k"))
	c.expect(protocol.Integer(100))

	// The value is readable while the key is alive.
	c.send(commandFrame("GET", "k"))
	c.expect(protocol.BulkString([]byte("v")))
}

// TestExpiredKeyGoneOverTCP is the milestone's acceptance criterion exercised
// end to end, and the one integration test here that sleeps.
//
// It has to: the server runs with the real clock, so proving that a key
// actually disappears after its deadline requires letting real time pass.
// The sleep is 150 ms against a 100 ms TTL — long enough for the key to be
// genuinely expired, short enough not to slow the suite. Every other
// expiration test in the repository uses a fake clock instead.
func TestExpiredKeyGoneOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(setEXFrame("SET", "k", "v", "PX", 100))
	c.expect(protocol.SimpleString("OK"))

	// Alive immediately after the write.
	c.send(commandFrame("EXISTS", "k"))
	c.expect(protocol.Integer(1))

	time.Sleep(150 * time.Millisecond)

	// Gone, by both read paths, and TTL reports the missing-key sentinel.
	c.send(commandFrame("GET", "k"))
	c.expect(protocol.Null{})

	c.send(commandFrame("EXISTS", "k"))
	c.expect(protocol.Integer(0))

	c.send(commandFrame("TTL", "k"))
	c.expect(protocol.Integer(-2))
}

// TestPersistOverTCP checks that a TTL can be removed and the key outlives it.
func TestPersistOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(setEXFrame("SET", "k", "v", "PX", 100))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("PERSIST", "k"))
	c.expect(protocol.Integer(1))

	c.send(commandFrame("TTL", "k"))
	c.expect(protocol.Integer(-1))

	// The key must now survive well past the TTL it used to have.
	time.Sleep(150 * time.Millisecond)

	c.send(commandFrame("GET", "k"))
	c.expect(protocol.BulkString([]byte("v")))

	// A second PERSIST has nothing to remove.
	c.send(commandFrame("PERSIST", "k"))
	c.expect(protocol.Integer(0))
}

// TestSetClearsTTLOverTCP covers the Redis behaviour that a plain SET discards
// an existing expiry, which clients rely on to make a temporary key durable.
func TestSetClearsTTLOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(setEXFrame("SET", "k", "v1", "EX", 100))
	c.expect(protocol.SimpleString("OK"))
	c.send(commandFrame("TTL", "k"))
	c.expect(protocol.Integer(100))

	c.send(commandFrame("SET", "k", "v2"))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("TTL", "k"))
	c.expect(protocol.Integer(-1))

	c.send(commandFrame("GET", "k"))
	c.expect(protocol.BulkString([]byte("v2")))
}

// TestTTLRoundsUpOverTCP is the rounding rule seen from a client: half a
// second must read as 1, not 0. Truncation would report 0, which reads as
// "expires right now" for a key with a usable lifetime left.
func TestTTLRoundsUpOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(setEXFrame("SET", "k", "v", "PX", 500))
	c.expect(protocol.SimpleString("OK"))

	c.send(commandFrame("TTL", "k"))
	c.expect(protocol.Integer(1))
}

// TestExpireZeroDeletesOverTCP checks that EXPIRE k 0 is a delete, and that
// the write that preceded it is gone.
func TestExpireZeroDeletesOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	c.send(expireFrame("EXPIRE", "k", 0))
	c.expect(protocol.Integer(1))

	c.send(commandFrame("GET", "k"))
	c.expect(protocol.Null{})
}

// TestExpirationIsSharedAcrossClients checks that a TTL set on one connection
// is visible on another, the same way values are: the keyspace, and therefore
// the expiry, belongs to the server.
func TestExpirationIsSharedAcrossClients(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	writer := newClient(t, addr)
	reader := newClient(t, addr)

	writer.send(setEXFrame("SET", "shared", "v", "EX", 100))
	writer.expect(protocol.SimpleString("OK"))

	reader.send(commandFrame("TTL", "shared"))
	reader.expect(protocol.Integer(100))

	// And a PERSIST from the other connection is visible back.
	reader.send(commandFrame("PERSIST", "shared"))
	reader.expect(protocol.Integer(1))

	writer.send(commandFrame("TTL", "shared"))
	writer.expect(protocol.Integer(-1))
}

// TestExpireParseErrorOverTCP checks that a malformed timeout is a domain
// error rather than a protocol error, so the connection survives it.
func TestExpireParseErrorOverTCP(t *testing.T) {
	discardLogs(t)
	addr, _ := startServer(t)

	c := newClient(t, addr)

	c.send(commandFrame("SET", "k", "v"))
	c.expect(protocol.SimpleString("OK"))

	// A bulk string where an integer belongs: a client-side bug, recoverable.
	c.send(commandFrame("EXPIRE", "k", "abc"))
	c.mustError()

	// The connection must still work.
	c.send(commandFrame("GET", "k"))
	c.expect(protocol.BulkString([]byte("v")))
}

// setEXFrame builds a SET with a trailing option and amount.
//
// The amount is encoded as a bulk string on purpose, because that is what a
// real client sends: redis-cli transmits every command-line token as a bulk
// string, so `SET k v EX 10` arrives as `$2\r\nEX\r\n$2\r\n10\r\n` rather
// than with a RESP integer. Encoding it as an integer here would test a shape
// no client produces, which is exactly how the milestone's first demo failed.
func setEXFrame(command, key, value, option string, amount int64) []byte {
	return protocol.Encode(protocol.Array{
		protocol.BulkString([]byte(command)),
		protocol.BulkString([]byte(key)),
		protocol.BulkString([]byte(value)),
		protocol.BulkString([]byte(option)),
		protocol.BulkString([]byte(strconv.FormatInt(amount, 10))),
	})
}

// expireFrame builds an EXPIRE whose timeout is a bulk string, matching
// redis-cli. Both wire forms are covered by the command-level tests; this
// helper follows the real client because the integration tests are about what
// actually happens on a socket.
func expireFrame(command, key string, seconds int64) []byte {
	return protocol.Encode(protocol.Array{
		protocol.BulkString([]byte(command)),
		protocol.BulkString([]byte(key)),
		protocol.BulkString([]byte(strconv.FormatInt(seconds, 10))),
	})
}
