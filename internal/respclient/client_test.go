package respclient

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/server"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// testTimeout bounds every blocking step. On a healthy machine a localhost
// round trip is sub-millisecond, so this only fires when something is broken.
const testTimeout = 5 * time.Second

// runningServer is a Verdigris instance the test can stop deliberately, which
// the restart test needs and the other tests never do.
type runningServer struct {
	srv  *server.Server
	done chan struct{}
}

// shutdown stops the server and waits for its accept loop to finish.
func (r *runningServer) shutdown(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := r.srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-r.done:
	case <-time.After(testTimeout):
		t.Fatal("Serve did not return after Shutdown")
	}
}

// startVerdigrisAt runs a Verdigris server on a specific address.
func startVerdigrisAt(t *testing.T, addr string) *runningServer {
	t.Helper()

	srv, err := server.New(addr, store.RealClock{}, time.Hour, "")
	if err != nil {
		t.Fatalf("server.New(%s): %v", addr, err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen(%s): %v", addr, err)
	}

	r := &runningServer{srv: srv, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		_ = srv.Serve()
	}()

	t.Cleanup(func() {
		// Safe on an already-stopped server: Shutdown is idempotent, and the
		// restart test stops one deliberately before this runs.
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = srv.Shutdown(ctx)
		select {
		case <-r.done:
		case <-time.After(testTimeout):
			t.Error("Serve did not return after Shutdown")
		}
	})

	return r
}

// reserveAddr returns a 127.0.0.1 address that was free a moment ago. The port
// is released immediately, so a caller must bind it promptly.
func reserveAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return addr
}

// startVerdigris runs a real Verdigris server on an ephemeral port and returns
// its address, with cleanup arranged.
//
// The server is in-process rather than a separate binary so the tests are
// hermetic: no port collisions with a developer's own server, no environment
// that has to be running before `go test` works, and no orphan process if a
// test fails mid-way. testing picks the port, the kernel assigns it, and
// t.Cleanup shuts it down.
//
// This package may not import internal/server — that restriction is the point
// of it being a pure client — but the test can, because a test that had to
// speak raw TCP to set up its own fixture would be testing two things at once.
func startVerdigris(t *testing.T) string {
	t.Helper()

	srv, err := server.New("127.0.0.1:0", store.RealClock{}, time.Hour, "")
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve()
	}()

	addr := srv.Addr().String()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		// Shutdown closes the listener, drains, and stops the store's sweeper.
		// The error is not actionable in cleanup: the assertions have already
		// run, and failing here would report a teardown problem as a test
		// failure.
		_ = srv.Shutdown(ctx)
		select {
		case <-serveDone:
		case <-time.After(testTimeout):
			t.Error("Serve did not return after Shutdown")
		}
	})

	return addr
}

// mustDial dials addr and registers the client for cleanup.
func mustDial(t *testing.T, addr string) *Client {
	t.Helper()

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial(%s): %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// bulkString asserts a reply is a bulk string and returns it as a Go string.
func bulkString(t *testing.T, v protocol.Value) string {
	t.Helper()

	bulk, ok := v.(protocol.BulkString)
	if !ok {
		t.Fatalf("reply is %T (%#v), want protocol.BulkString", v, v)
	}
	return string(bulk)
}

func TestDoPingReturnsPong(t *testing.T) {
	c := mustDial(t, startVerdigris(t))

	got, err := c.Do("PING")
	if err != nil {
		t.Fatalf("Do(PING): %v", err)
	}
	if got != protocol.SimpleString("PONG") {
		t.Fatalf("PING = %#v, want SimpleString(\"PONG\")", got)
	}
}

// TestDoSetAndGet is the round trip the UI is built on: a write, then a read of
// what was written, over the same connection.
func TestDoSetAndGet(t *testing.T) {
	c := mustDial(t, startVerdigris(t))

	got, err := c.Do("SET", "alpha", "one")
	if err != nil {
		t.Fatalf("Do(SET): %v", err)
	}
	if got != protocol.SimpleString("OK") {
		t.Fatalf("SET = %#v, want SimpleString(\"OK\")", got)
	}

	got, err = c.Do("GET", "alpha")
	if err != nil {
		t.Fatalf("Do(GET): %v", err)
	}
	if s := bulkString(t, got); s != "one" {
		t.Fatalf("GET alpha = %q, want %q", s, "one")
	}

	// A missing key is a null reply, not an error. The UI branches on this to
	// decide between `exists: true` and `exists: false`, so it is pinned here.
	got, err = c.Do("GET", "nope")
	if err != nil {
		t.Fatalf("Do(GET missing): %v", err)
	}
	if _, isNull := got.(protocol.Null); !isNull {
		t.Fatalf("GET nope = %#v (%T), want protocol.Null", got, got)
	}

	// KEYS is the other read the UI leans on, and its reply is an array.
	got, err = c.Do("KEYS", "*")
	if err != nil {
		t.Fatalf("Do(KEYS): %v", err)
	}
	arr, ok := got.(protocol.Array)
	if !ok {
		t.Fatalf("KEYS * = %#v (%T), want protocol.Array", got, got)
	}
	if len(arr) != 1 || bulkString(t, arr[0]) != "alpha" {
		t.Fatalf("KEYS * = %#v, want one entry %q", arr, "alpha")
	}
}

// TestDoUnknownCommandReturnsErrorFrame pins the distinction the whole client
// depends on: a server saying "no such command" is a successful round trip
// carrying an error value, not a failure of Do. A client that turned error
// frames into Go errors could not tell a typo from a dead server, and the UI
// reports those two very differently.
func TestDoUnknownCommandReturnsErrorFrame(t *testing.T) {
	c := mustDial(t, startVerdigris(t))

	got, err := c.Do("NOTACOMMAND")
	if err != nil {
		t.Fatalf("Do(NOTACOMMAND) returned a Go error %v; an error frame is a reply, not a transport failure", err)
	}

	errValue, ok := got.(protocol.Error)
	if !ok {
		t.Fatalf("reply is %T (%#v), want protocol.Error", got, got)
	}
	if len(errValue) == 0 {
		t.Fatal("error frame is empty")
	}
}

// TestConcurrentDo is the lock's reason for existing. RESP has no request
// identifier, so two commands interleaved on one socket would each read the
// other's reply; under -race this fails loudly if the discipline is wrong, and
// without the lock the value assertions below would fail intermittently as
// replies crossed.
func TestConcurrentDo(t *testing.T) {
	c := mustDial(t, startVerdigris(t))

	const goroutines = 8
	const iterations = 50

	// Each goroutine uses a key only it touches, so a value read back by the
	// wrong command is detectable: goroutine g must always see its own value.
	done := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			key := fmt.Sprintf("key-%d", g)
			want := fmt.Sprintf("value-%d", g)
			for i := 0; i < iterations; i++ {
				if _, err := c.Do("SET", key, want); err != nil {
					done <- fmt.Errorf("goroutine %d: SET: %w", g, err)
					return
				}
				got, err := c.Do("GET", key)
				if err != nil {
					done <- fmt.Errorf("goroutine %d: GET: %w", g, err)
					return
				}
				if s := string(got.(protocol.BulkString)); s != want {
					done <- fmt.Errorf("goroutine %d: GET %s = %q, want %q", g, key, s, want)
					return
				}
			}
			done <- nil
		}(g)
	}

	for g := 0; g < goroutines; g++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	// Every goroutine's key must be present, which also proves no reply was
	// lost to a desynchronized stream.
	got, err := c.Do("KEYS", "*")
	if err != nil {
		t.Fatalf("Do(KEYS): %v", err)
	}
	arr, ok := got.(protocol.Array)
	if !ok {
		t.Fatalf("KEYS * = %#v (%T), want protocol.Array", got, got)
	}
	if len(arr) != goroutines {
		t.Fatalf("KEYS * returned %d keys, want %d", len(arr), goroutines)
	}
}

// TestReconnectAfterServerRestart is the failure the UI will actually meet: the
// server it is talking to is restarted, and the connection it holds is dead
// while the address is still valid. The in-flight command fails — that is
// honest, its fate is unknown — and the next one reconnects and works.
func TestReconnectAfterServerRestart(t *testing.T) {
	// The second server must bind the *same* address, because the point is
	// that the client keeps dialing one string while the server behind it
	// changes. Binding :0 twice would hand out two ports and test nothing.
	//
	// The port is reserved by opening and closing a listener, which is racy in
	// principle — something else could take it in the gap — but far better
	// than a hardcoded port that collides with whatever a developer is already
	// running. If it is taken, the Listen below fails and says so.
	addr := reserveAddr(t)

	first := startVerdigrisAt(t, addr)

	c := mustDial(t, addr)
	if _, err := c.Do("SET", "survivor", "yes"); err != nil {
		t.Fatalf("SET before restart: %v", err)
	}

	// Close the connection before stopping the server. Shutdown waits for
	// in-flight connections to drain, and an idle open socket is an in-flight
	// connection, so leaving it open would make the shutdown below wait out its
	// whole timeout. Closing it here is also what the client would experience:
	// it holds no connection, and the address is still valid.
	if err := c.Close(); err != nil {
		t.Fatalf("Close before restart: %v", err)
	}

	// Stop the server the way a restart does: the listener closes and the
	// process goes away.
	first.shutdown(t)

	// A second server on the same address, with a fresh empty keyspace — so
	// "survivor" being gone is expected and is what proves these are two
	// different servers rather than one that never went away.
	startVerdigrisAt(t, addr)

	// The client holds no connection and the address was briefly dead. What
	// must work is the next call: Do notices there is no usable connection,
	// dials the address again, and the command succeeds against the new server.
	got, err := c.Do("PING")
	if err != nil {
		t.Fatalf("Do(PING) after restart: %v; the client must reconnect on the next call", err)
	}
	if got != protocol.SimpleString("PONG") {
		t.Fatalf("PING after restart = %#v, want PONG", got)
	}

	// And it is talking to the new server, not a cached view of the old one.
	got, err = c.Do("GET", "survivor")
	if err != nil {
		t.Fatalf("GET after restart: %v", err)
	}
	if _, isNull := got.(protocol.Null); !isNull {
		t.Fatalf("GET survivor = %#v, want null: the restarted server starts empty", got)
	}

	if _, err := c.Do("SET", "after", "restart"); err != nil {
		t.Fatalf("SET after restart: %v", err)
	}
	if got, err := c.Do("GET", "after"); err != nil {
		t.Fatalf("GET after restart: %v", err)
	} else if s := bulkString(t, got); s != "restart" {
		t.Fatalf("GET after = %q, want %q", s, "restart")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c := mustDial(t, startVerdigris(t))

	if _, err := c.Do("PING"); err != nil {
		t.Fatalf("Do(PING): %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}

	// A closed client is not a broken one: it dials again on the next Do.
	got, err := c.Do("PING")
	if err != nil {
		t.Fatalf("Do after Close: %v", err)
	}
	if got != protocol.SimpleString("PONG") {
		t.Fatalf("PING after Close = %#v, want PONG", got)
	}
}

// TestDoWithNoArgs covers the one caller error Do can detect without a server.
// The client is constructed directly rather than dialed, so the check runs
// before any connection is needed.
func TestDoWithNoArgs(t *testing.T) {
	c := &Client{addr: "127.0.0.1:1"}

	if _, err := c.Do(); err == nil {
		t.Fatal("Do() with no arguments returned nil error")
	}

	// Close on a never-dialed client must also be clean, which is the same
	// nil-connection path a defer hits after a failed Dial.
	if err := c.Close(); err != nil {
		t.Fatalf("Close on a never-dialed client: %v, want nil", err)
	}
}

// TestNewUnconnectedDefersDial is the constructor cmd/verdigris-ui depends on:
// it must return a usable client for an address that is not listening, and the
// failure must surface from the first command rather than from construction.
func TestNewUnconnectedDefersDial(t *testing.T) {
	// Reserved and released, so nothing is listening there when the call is
	// made. The point is that construction succeeds anyway.
	dead := reserveAddr(t)

	c := NewUnconnected(dead)
	defer c.Close()

	if c.Addr() != dead {
		t.Fatalf("Addr() = %q, want %q", c.Addr(), dead)
	}

	if _, err := c.Do("PING"); err == nil {
		t.Fatal("Do against an address with nothing listening returned nil error")
	}

	// Still usable afterwards: a failed dial is not a poisoned client.
	if _, err := c.Do("PING"); err == nil {
		t.Fatal("second Do against a dead address returned nil error")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close after failed dials: %v", err)
	}
}

// TestDoLeavesClientUsableAfterServerDies checks that a client whose server
// goes away mid-life reports the loss and then recovers, which is the loop the
// UI's shared client lives in.
func TestDoLeavesClientUsableAfterServerDies(t *testing.T) {
	addr := reserveAddr(t)
	running := startVerdigrisAt(t, addr)

	c := mustDial(t, addr)
	if _, err := c.Do("SET", "k", "v"); err != nil {
		t.Fatalf("SET: %v", err)
	}

	// Close the client's connection so the server can shut down without
	// waiting out its drain, and so the next command meets a dead socket.
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	running.shutdown(t)

	// The address is now dead: this must fail rather than hang or panic.
	if _, err := c.Do("GET", "k"); err == nil {
		t.Fatal("GET against a stopped server returned nil error")
	}

	// Bring a server back on the same address; the client must recover.
	startVerdigrisAt(t, addr)

	got, err := c.Do("PING")
	if err != nil {
		t.Fatalf("PING after the server came back: %v", err)
	}
	if got != protocol.SimpleString("PONG") {
		t.Fatalf("PING = %#v, want PONG", got)
	}
}

// TestCloneCopiesNestedValues exercises the value copier directly. Decode
// aliases the input buffer for bulk strings, so a value that outlives the read
// must be a copy — including bulk strings nested inside arrays, which is
// exactly what a KEYS reply is.
func TestCloneCopiesNestedValues(t *testing.T) {
	src := protocol.Array{
		protocol.BulkString([]byte("alpha")),
		protocol.Integer(7),
		protocol.SimpleString("OK"),
		protocol.Error("ERR nope"),
		protocol.Null{},
		protocol.Array{protocol.BulkString([]byte("nested"))},
	}

	got := clone(src)

	// Mutating the original's buffers must not be visible through the clone.
	original := src[0].(protocol.BulkString)
	for i := range original {
		original[i] = 'X'
	}
	nestedOriginal := src[5].(protocol.Array)[0].(protocol.BulkString)
	for i := range nestedOriginal {
		nestedOriginal[i] = 'Y'
	}

	arr, ok := got.(protocol.Array)
	if !ok {
		t.Fatalf("clone returned %T, want protocol.Array", got)
	}
	if s := string(arr[0].(protocol.BulkString)); s != "alpha" {
		t.Fatalf("cloned element 0 = %q, want %q: the copy aliased the source", s, "alpha")
	}
	if s := string(arr[5].(protocol.Array)[0].(protocol.BulkString)); s != "nested" {
		t.Fatalf("cloned nested element = %q, want %q: nested bulk strings must be copied too", s, "nested")
	}

	// The value types are returned unchanged and still compare equal.
	if arr[1] != protocol.Integer(7) {
		t.Fatalf("cloned integer = %#v, want Integer(7)", arr[1])
	}
	if arr[2] != protocol.SimpleString("OK") {
		t.Fatalf("cloned simple string = %#v", arr[2])
	}
	if arr[3] != protocol.Error("ERR nope") {
		t.Fatalf("cloned error = %#v", arr[3])
	}
	if _, isNull := arr[4].(protocol.Null); !isNull {
		t.Fatalf("cloned null = %#v", arr[4])
	}

	// A nil bulk string survives as nil: null and empty are different frames.
	gotNil := clone(protocol.BulkString(nil))
	if bulk, ok := gotNil.(protocol.BulkString); !ok || bulk != nil {
		t.Fatalf("clone(nil bulk string) = %#v, want a nil BulkString", gotNil)
	}
}
