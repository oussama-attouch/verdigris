// Package respclient is a minimal RESP client: one TCP connection, one command
// at a time.
//
// It exists so that a program which is not the server can talk to Verdigris the
// same way redis-cli does. cmd/verdigris-ui is the reason it was written, but
// nothing here knows about HTTP or the UI: the package imports internal/protocol
// and the standard library and nothing else.
//
// That restriction is the same one cmd/vg-bench follows, and it is deliberate.
// A client that reached into internal/store or internal/server would be
// bypassing the protocol, and a bug in it could corrupt the keyspace through a
// path no other client can take. Everything this package can do, a redis-cli
// user can do.
//
// # One connection, one lock
//
// A Client holds a single connection and a mutex. Do takes the mutex for the
// whole round trip — write the command, read the reply — because RESP has no
// request identifier: two commands interleaved on one socket would each read
// the other's reply. That makes a Client a serializer, which is the right shape
// for a UI issuing a handful of commands per page view and the wrong shape for
// a load generator. cmd/vg-bench opens one connection per client for exactly
// that reason.
//
// # Failures reconnect
//
// A transport failure closes the connection and returns the error. The next Do
// dials a fresh one and retries the command once, so a server restart costs the
// request that was in flight and nothing after it. A command is never silently
// re-sent after it may have been executed, because the retry only happens when
// the failure occurred before any reply was read and the connection is known
// dead — see do.
package respclient

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// readBufSize is the size of the first read buffer for a reply. It covers a
// small array such as a KEYS reply for a handful of keys without growing;
// larger replies grow the buffer once and keep the larger size for the life of
// the client.
const readBufSize = 64 * 1024

// Client is a single-connection RESP client. It is safe for concurrent
// use; each Do holds the connection for one round trip.
type Client struct {
	addr string
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex

	// in accumulates bytes of a reply that has not fully arrived. It is only
	// touched under mu. It is kept across calls, and replaced rather than
	// resliced after a reply is decoded, so that the buffer a returned value
	// may alias is never reused by a later command.
	in []byte
}

// Dial connects to addr. It does not send any command.
//
// A successful Dial means a TCP connection was established, not that anything
// RESP-shaped is listening on the other end; the first Do is what discovers
// that. Callers that want to know the server is answering should Dial and then
// send PING.
func Dial(addr string) (*Client, error) {
	c := &Client{addr: addr}
	if err := c.dial(); err != nil {
		return nil, err
	}
	return c, nil
}

// NewUnconnected returns a client for addr that holds no connection. The first
// Do dials.
//
// It exists for the case Dial cannot serve: a caller that must come up before
// the server it talks to is reachable, and wants the reconnect-on-demand
// behavior rather than an error at startup. cmd/verdigris-ui is exactly that —
// a UI that refused to start until Verdigris was up could not be used to
// discover that Verdigris was down.
//
// A client from here is usable immediately and reports a dial failure from the
// first Do rather than from construction. Callers that want to report a
// startup problem should still use Dial and inspect its error.
func NewUnconnected(addr string) *Client {
	return &Client{addr: addr}
}

// dial establishes the connection. The caller must hold mu, or be the
// constructor before the Client is shared.
func (c *Client) dial() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("respclient: dial %s: %w", c.addr, err)
	}

	// Nagle would hold a small command back waiting for more bytes to coalesce,
	// and this client sends one command and waits for its reply — there is
	// never a second packet to coalesce with. The server disables Nagle on its
	// side for the same reason.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	c.conn = conn
	c.r = bufio.NewReaderSize(conn, readBufSize)
	c.in = nil
	return nil
}

// Do sends one command and returns the reply. The variadic args are
// the command name followed by its arguments, all treated as bulk
// strings.
//
// Do is safe for concurrent use. On a transport error, the connection
// is closed and the call returns the error; the next Do dials a fresh
// connection.
//
// A RESP error frame is not a transport error: it is returned as a
// protocol.Error value with a nil error, because the server answering "no such
// command" is a successful round trip. A caller that wants to treat error
// frames as failures checks the returned value's type.
func (c *Client) Do(args ...string) (protocol.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("respclient: Do needs at least a command name")
	}

	// Encoded before the lock is taken: it touches no shared state, and
	// building the frame inside the critical section would lengthen the window
	// every other caller waits behind.
	frame := protocol.Encode(command(args))

	c.mu.Lock()
	defer c.mu.Unlock()

	// Two attempts, and the second one only ever happens when the connection
	// was missing at the start of the first — that is the restart case, where
	// the command provably never reached a server. The count is bounded rather
	// than looped so that a server which accepts connections and then dies
	// immediately cannot spin here.
	for attempt := 0; attempt < 2; attempt++ {
		if c.conn == nil {
			if err := c.dial(); err != nil {
				return nil, err
			}
		}

		reply, err := c.roundtrip(frame)
		if err == nil {
			return reply, nil
		}

		// The connection is unusable; dropping it is what makes the next Do
		// reconnect rather than reuse a dead socket.
		c.discard()

		// Not retried. The command was written to a connection that then
		// failed, so the server may already have executed it — a lost reply
		// and a lost command are indistinguishable from here. Re-sending a SET
		// is harmless but re-sending an EXPIRE or a DEL is a different
		// operation, and a client that guesses is worse than one that reports.
		// The error reaches the caller, and the *next* Do reconnects.
		return nil, err
	}

	// Unreachable: every path through the loop returns.
	return nil, errors.New("respclient: exhausted attempts")
}

// roundtrip writes one encoded command and reads exactly one reply.
//
// The caller must hold mu.
func (c *Client) roundtrip(frame []byte) (protocol.Value, error) {
	if _, err := c.conn.Write(frame); err != nil {
		return nil, fmt.Errorf("respclient: write to %s: %w", c.addr, err)
	}

	for {
		v, n, err := protocol.Decode(c.in)
		if err == nil {
			// The decoded value may alias c.in, which this client will reuse
			// for the next command. Copying before returning is what lets a
			// caller hold a reply — or hand it to encoding/json — without
			// racing a later Do, which under a shared client is a real
			// possibility rather than a theoretical one.
			out := clone(v)

			// A fresh buffer rather than a reslice: the bytes already handed
			// out through out are no longer referenced, and starting clean
			// means a large reply does not pin its buffer for every later
			// command.
			c.in = nil
			_ = n
			return out, nil
		}
		if !errors.Is(err, protocol.ErrIncomplete) {
			// The stream is desynchronized: the bytes cannot become valid by
			// waiting for more. Reported as a transport-class failure, since
			// the only recovery is a new connection.
			return nil, fmt.Errorf("respclient: decode reply from %s: %w", c.addr, err)
		}

		// Grow when there is no spare capacity. Without this, feeding a
		// sub-slice back to bufio.Read leaves it reading into zero remaining
		// space and returning 0, nil forever.
		if cap(c.in) == len(c.in) {
			grown := make([]byte, len(c.in), 2*cap(c.in)+512)
			copy(grown, c.in)
			c.in = grown
		}

		read, rerr := c.r.Read(c.in[len(c.in):cap(c.in)])
		if read > 0 {
			c.in = c.in[:len(c.in)+read]
			continue
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				// io.EOF would read as "nothing more to read" to a caller that
				// is not expecting a stream to end, so it is reported as the
				// connection loss it is.
				return nil, fmt.Errorf("respclient: %s closed the connection before replying", c.addr)
			}
			return nil, fmt.Errorf("respclient: read from %s: %w", c.addr, rerr)
		}
		// A read of zero bytes with no error is not progress; looping on it
		// would spin, so it is treated as a dead connection.
		return nil, fmt.Errorf("respclient: read from %s returned no bytes and no error", c.addr)
	}
}

// discard closes the connection and forgets it, so the next Do dials again.
//
// The caller must hold mu.
func (c *Client) discard() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.r = nil
	c.in = nil
}

// Close closes the underlying connection. Idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		// Already closed, or never dialed. Reporting an error here would make
		// the common defer c.Close() pattern noisy for no benefit: there is
		// nothing left to release.
		return nil
	}

	err := c.conn.Close()
	c.conn = nil
	c.r = nil
	c.in = nil

	if err != nil {
		return fmt.Errorf("respclient: close %s: %w", c.addr, err)
	}
	return nil
}

// Addr returns the address the client connects to.
func (c *Client) Addr() string { return c.addr }

// command builds the RESP array for a command line.
func command(args []string) protocol.Array {
	arr := make(protocol.Array, len(args))
	for i, a := range args {
		arr[i] = protocol.BulkString(a)
	}
	return arr
}

// clone deep-copies a decoded value.
//
// Internal/protocol's Decode aliases the input buffer for bulk strings, which
// is correct for a decoder talking to a connection buffer and wrong for a value
// that outlives the read. Copying here keeps the aliasing an implementation
// detail of this package instead of a rule every caller has to remember.
func clone(v protocol.Value) protocol.Value {
	switch v := v.(type) {
	case protocol.BulkString:
		// A nil bulk string stays nil: null and empty are different frames and
		// the caller branches on which arrived.
		if v == nil {
			return protocol.BulkString(nil)
		}
		// append into a nil slice rather than bytes.Clone so the empty case
		// produces a non-nil empty slice, matching what Decode reported.
		out := make([]byte, len(v))
		copy(out, v)
		return protocol.BulkString(out)

	case protocol.Array:
		out := make(protocol.Array, len(v))
		for i, elem := range v {
			out[i] = clone(elem)
		}
		return out

	default:
		// SimpleString, Error, Integer and Null are value types with no
		// reference to the buffer, so they need no copy.
		return v
	}
}
