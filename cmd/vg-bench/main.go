// Command vg-bench measures the throughput and latency of any RESP server.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// mode names the workload a run performs. Each is a shape of traffic rather
// than a command, because what a server costs to serve depends on the mix.
type mode string

const (
	// modePing measures the round trip with no keyspace touched at all. It is
	// the floor: whatever a server costs per request regardless of the data,
	// this is it.
	modePing mode = "ping"

	// modeSet writes. Every key is distinct, so the map grows for the whole
	// run and no write is ever an overwrite.
	modeSet mode = "set"

	// modeGet reads. It reuses a bounded key space, so a run measures reads
	// rather than the cost of allocating a new key per lookup.
	modeGet mode = "get"

	// modeMixed alternates SET and GET in equal numbers, which is the closest
	// of the four to what a real workload looks like.
	modeMixed mode = "mixed"
)

// getKeySpace bounds how many distinct keys modeGet and the GET half of
// modeMixed draw from.
//
// The keys match what modeSet writes — key:<i> — so running set and then get
// against the same server measures reads of keys that are actually present.
// Run alone, get still only reaches 1000 keys, so a fresh server answers
// mostly misses: a miss is a round trip and is timed the same way, which is
// deliberate. The harness measures the protocol, not the hit rate.
const getKeySpace = 1000

// Valid modes, in the order they are reported in the usage text.
var modes = []mode{modePing, modeSet, modeGet, modeMixed}

// parseMode maps a -mode argument to its workload. Anything unrecognized is an
// error rather than a silent fallback to ping: a typo that quietly benchmarks
// the wrong workload produces numbers that look fine and mean nothing.
func parseMode(s string) (mode, error) {
	for _, m := range modes {
		if string(m) == s {
			return m, nil
		}
	}
	return "", fmt.Errorf("unknown mode %q, want one of: ping, set, get, mixed", s)
}

// modeNames renders the valid modes for the usage line.
func modeNames() string {
	out := ""
	for i, m := range modes {
		if i > 0 {
			out += "|"
		}
		out += string(m)
	}
	return out
}

// client is one benchmark client: a connection, a request generator, and the
// latencies it observed.
//
// The generator state is per-client and never shared, which is what keeps the
// measured path free of synchronization. The key sequence is deliberately
// identical across clients — client c issues key:<op> for the same op numbers —
// so a run is reproducible rather than depending on how the goroutines
// interleave.
type client struct {
	id   int
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	// in is the accumulation buffer for a reply that has not fully arrived. It
	// persists across operations because a reply may be split across reads at
	// any byte offset, and clearing it between operations would discard the
	// bytes of the next reply that arrived with the last one.
	in []byte

	// scratch is reused for encoding each request, so the hot path allocates
	// nothing per operation. It is grown, never shrunk.
	scratch []byte

	// lat is one duration per completed operation, recorded by the caller.
	lat []time.Duration

	// opIndex is the generator's position, used in both the warmup and the
	// measured phase so the two produce the same traffic.
	opIndex int
}

// request returns the RESP bytes for this client's next operation and advances
// the generator.
//
// The buffer is reused across calls, so the result is only valid until the next
// call. That is safe because the caller writes it before asking for another.
func (c *client) request(m mode) []byte {
	c.scratch = c.scratch[:0]

	switch m {
	case modePing:
		c.opIndex++

		// Both the reply and the request are built by internal/protocol, so the
		// measurement covers this project's encoder as well as the server's
		// decoder. That is the point of the ADR: the protocol package is
		// exercised in both directions against both servers.
		return protocol.Encode(protocol.Array{
			protocol.BulkString("PING"),
		})

	case modeSet:
		key := "key:" + strconv.Itoa(c.opIndex)
		value := "value:" + strconv.Itoa(c.opIndex)
		c.opIndex++
		return protocol.Encode(protocol.Array{
			protocol.BulkString("SET"),
			protocol.BulkString(key),
			protocol.BulkString(value),
		})

	case modeGet:
		key := "key:" + strconv.Itoa(c.opIndex%getKeySpace)
		c.opIndex++
		return protocol.Encode(protocol.Array{
			protocol.BulkString("GET"),
			protocol.BulkString(key),
		})

	default: // modeMixed
		// Even ops write, odd ops read, so every pair is one of each and the
		// 50/50 split holds exactly rather than on average.
		op := c.opIndex
		c.opIndex++
		if op%2 == 0 {
			key := "key:" + strconv.Itoa(op)
			return protocol.Encode(protocol.Array{
				protocol.BulkString("SET"),
				protocol.BulkString(key),
				protocol.BulkString("value:" + strconv.Itoa(op)),
			})
		}
		return protocol.Encode(protocol.Array{
			protocol.BulkString("GET"),
			protocol.BulkString("key:" + strconv.Itoa(op%getKeySpace)),
		})
	}
}

// readReply reads exactly one complete RESP frame.
//
// Decode is called on the accumulated buffer after every read, and it reports
// ErrIncomplete until the whole frame has arrived. The bytes consumed are then
// removed by reslicing rather than copying: bufio already owns the memory the
// remainder points into, and a copy per reply would be paid on every measured
// operation.
//
// A reply is read but never interpreted. The harness measures the protocol, so
// an error frame from the server is a reply like any other — it arrived, it
// took that long, and it is counted. Only a transport failure or a byte stream
// that cannot be decoded is a benchmark error.
func (c *client) readReply() error {
	for {
		v, n, err := protocol.Decode(c.in)
		if err == nil {
			_ = v
			c.in = c.in[n:]
			return nil
		}
		if !errors.Is(err, protocol.ErrIncomplete) {
			return fmt.Errorf("decode reply: %w", err)
		}

		// Grow when the buffer has no spare capacity. Without this, feeding a
		// sub-slice back into bufio.Read leaves it reading into zero remaining
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
				// The server closed the connection mid-reply. Reported as a
				// plain error rather than io.EOF so the caller does not have
				// to distinguish "done reading" from "lost the server".
				return errors.New("connection closed before the reply was complete")
			}
			return fmt.Errorf("read reply: %w", rerr)
		}
		// A read of zero bytes with no error is not progress. Looping on it
		// would spin; treating it as a failure stops the run and reports it.
		return errors.New("read returned no bytes and no error")
	}
}

// run performs ops operations in mode m, optionally recording each latency.
//
// An operation that fails ends the run for this client and returns the error.
// The partial latency slice is still returned, because the operations that did
// complete are real measurements — discarding them would make a run that lost
// its server halfway look like it never happened.
func (c *client) run(m mode, ops int, record bool) ([]time.Duration, error) {
	for i := 0; i < ops; i++ {
		req := c.request(m)

		// t0 is taken before the write so the measurement covers the whole
		// round trip: encode, write, server processing, read, decode. Timing
		// only the read would hide the cost the client pays to send.
		t0 := time.Now()
		if _, err := c.w.Write(req); err != nil {
			return c.lat, fmt.Errorf("write request: %w", err)
		}
		// Flush per operation. The server answers one reply per request, so a
		// buffered write that waited for the buffer to fill would measure
		// nothing at all — bufio only sends when told to.
		if err := c.w.Flush(); err != nil {
			return c.lat, fmt.Errorf("flush request: %w", err)
		}
		if err := c.readReply(); err != nil {
			return c.lat, err
		}
		if record {
			c.lat = append(c.lat, time.Since(t0))
		}
	}
	return c.lat, nil
}

// result is one client's contribution to the run.
type result struct {
	lat []time.Duration
	err error
}

// benchClient dials its own connection, warms up, then runs the measured phase.
//
// Every client gets its own TCP connection, which is what -clients means: N
// concurrent connections, not N goroutines sharing one. A shared connection
// would serialize the requests and measure a single-connection pipeline.
func benchClient(addr string, m mode, id, warmup, requests int, results chan<- result) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		results <- result{err: fmt.Errorf("client %d: dial %s: %w", id, addr, err)}
		return
	}
	defer conn.Close()

	if tcp, ok := conn.(*net.TCPConn); ok {
		// The server disables Nagle on its side; the client must do the same or
		// the small request packets sit in the client's own buffer waiting for
		// an ACK that the request/response pattern never batches. A benchmark
		// that left this on would measure Nagle's timer.
		_ = tcp.SetNoDelay(true)
	}

	c := &client{
		id:   id,
		conn: conn,
		r:    bufio.NewReaderSize(conn, 64*1024),
		w:    bufio.NewWriterSize(conn, 64*1024),
		lat:  make([]time.Duration, 0, requests),
	}

	// Warmup is the same workload with the same generator, so the measured
	// phase starts from the same key position it would have reached anyway —
	// the only difference is that nothing is recorded.
	if warmup > 0 {
		if _, err := c.run(m, warmup, false); err != nil {
			results <- result{err: fmt.Errorf("client %d: warmup: %w", id, err)}
			return
		}
	}

	lat, err := c.run(m, requests, true)
	results <- result{lat: lat, err: err}
}

// report is everything a run measured.
type report struct {
	Addr     string `json:"addr"`
	Mode     string `json:"mode"`
	Clients  int    `json:"clients"`
	Requests int    `json:"requests"`
	Ops      int    `json:"ops"`

	// ElapsedS and OpsPerSec come from one clock reading at the start of the
	// run and one at the end, so they are unaffected by the system clock's
	// granularity — the run spans thousands of ticks.
	ElapsedS  float64 `json:"elapsed_s"`
	OpsPerSec float64 `json:"ops_per_sec"`

	// MeanMs is the average round trip: total time divided by operations.
	//
	// It is reported alongside the percentiles because it is measured over one
	// long interval rather than one short one per operation, which makes it the
	// only latency figure here that survives a coarse system clock. On a
	// platform whose time.Now advances in ~0.5ms steps, a per-operation
	// percentile can only ever read 0 or one step, while this average is
	// accurate to a small fraction of a microsecond once the run is long
	// enough. See the package comment for the full explanation.
	MeanMs float64 `json:"mean_ms"`

	// The percentiles are nearest-rank over per-operation timings. They are
	// exact on a platform with a fine-grained monotonic clock and quantized on
	// one without; MeanMs is what to trust when the two disagree.
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`

	Errors int `json:"errors"`
}

// percentile returns the value at rank p (0..1) using the nearest-rank method.
//
// Nearest-rank rather than interpolation because these are durations that
// actually happened: "p99 = 1.87ms" says some request took 1.87ms, which is
// what an operator wants to know. An interpolated 1.9ms would be a number no
// request ever observed.
//
// sorted must be ascending and non-empty.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p * float64(len(sorted)))
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// millis renders a duration as milliseconds, rounded to three decimals.
//
// Three rather than two because this harness talks to a server over a loopback
// socket, where a round trip can take tens of microseconds. Rounded to two
// decimals those reads print as "0.00ms" — which looks like a broken timer
// rather than a fast server, and hides exactly the differences a benchmark of
// local servers is trying to show. A microsecond is 0.001ms, so three decimals
// is the coarsest resolution that never rounds a real measurement to zero.
func millis(d time.Duration) float64 {
	ms, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", float64(d)/float64(time.Millisecond)), 64)
	return ms
}

// summarize folds the per-client results into one report.
func summarize(addr string, m mode, clients, requests int, elapsed time.Duration, results []result) report {
	all := make([]time.Duration, 0, clients*requests)
	errCount := 0

	for _, r := range results {
		all = append(all, r.lat...)
		if r.err != nil {
			errCount++
			fmt.Fprintf(os.Stderr, "vg-bench: %v\n", r.err)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	rep := report{
		Addr:     addr,
		Mode:     string(m),
		Clients:  clients,
		Requests: requests,
		// Ops is the number attempted, not the number that succeeded. Throughput
		// is therefore the attempted rate, and errors says how many of those
		// attempts did not complete — reporting only successes as ops would
		// quietly raise the measured rate whenever a server dropped requests.
		Ops:      clients * requests,
		Errors:   errCount,
		P50Ms:    millis(percentile(all, 0.50)),
		P95Ms:    millis(percentile(all, 0.95)),
		P99Ms:    millis(percentile(all, 0.99)),
		MaxMs:    millis(percentile(all, 1.0)),
		ElapsedS: elapsed.Seconds(),
	}
	if rep.ElapsedS > 0 {
		rep.OpsPerSec = float64(rep.Ops) / rep.ElapsedS

		// Mean latency is derived from the run's own elapsed time rather than
		// from the sum of the per-operation durations, because it is precisely
		// those durations that a coarse clock rounds to zero. Over one long
		// interval the same total is measured accurately, so this is the figure
		// that stays meaningful when the percentiles cannot.
		rep.MeanMs = millis(elapsed) / float64(rep.Ops)
	}
	return rep
}

// printText writes the human-readable report.
func printText(rep report) {
	// Clients and requests are two numbers that are easy to confuse, and every
	// rate below is their product, so the line that states them says which is
	// which.
	fmt.Printf("addr=%s mode=%s clients=%d requests=%d (per client)\n", rep.Addr, rep.Mode, rep.Clients, rep.Requests)
	fmt.Printf("ops=%d elapsed=%.3fs ops/sec=%.0f\n", rep.Ops, rep.ElapsedS, rep.OpsPerSec)
	fmt.Printf("mean=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms\n", rep.MeanMs, rep.P50Ms, rep.P95Ms, rep.P99Ms, rep.MaxMs)
	fmt.Printf("errors=%d\n", rep.Errors)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "host:port of the RESP server to benchmark")
	modeName := flag.String("mode", string(modePing), "workload to run: "+modeNames())
	clients := flag.Int("clients", 1, "number of concurrent connections, each with its own request stream")
	requests := flag.Int("requests", 1000, "measured operations per client")
	warmup := flag.Int("warmup", 100, "unmeasured operations per client, same mode, run first")
	asJSON := flag.Bool("json", false, "print the report as a single JSON object instead of text")
	flag.Parse()

	// Validation is fatal with exit 2, matching the server's treatment of a bad
	// flag: this is an invocation error, not a run that went wrong.
	m, err := parseMode(*modeName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vg-bench: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}
	if *clients < 1 {
		fmt.Fprintf(os.Stderr, "vg-bench: invalid -clients %d: must be at least 1\n", *clients)
		flag.Usage()
		os.Exit(2)
	}
	if *requests < 1 {
		fmt.Fprintf(os.Stderr, "vg-bench: invalid -requests %d: must be at least 1\n", *requests)
		flag.Usage()
		os.Exit(2)
	}
	if *warmup < 0 {
		fmt.Fprintf(os.Stderr, "vg-bench: invalid -warmup %d: must not be negative\n", *warmup)
		flag.Usage()
		os.Exit(2)
	}

	results := make(chan result, *clients)

	// The clock starts after every goroutine is spawned but before any of them
	// is guaranteed to have dialed. Connection setup is therefore inside the
	// measurement, and for the short runs used to smoke-test the harness it is
	// a visible share of the elapsed time — which is the honest reading: those
	// connections are work the run had to do.
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			benchClient(*addr, m, id, *warmup, *requests, results)
		}(i)
	}

	// Collected in a separate goroutine so the channel's buffer can never fill
	// and block a client: with -clients buffered slots, the last client to
	// finish would otherwise be able to send and the first could not, and the
	// run would deadlock while holding a result nobody could receive.
	collected := make([]result, 0, *clients)
	done := make(chan struct{})
	go func() {
		for r := range results {
			collected = append(collected, r)
		}
		close(done)
	}()

	wg.Wait()
	elapsed := time.Since(start)
	close(results)
	<-done

	rep := summarize(*addr, m, *clients, *requests, elapsed, collected)

	if *asJSON {
		encoded, err := json.Marshal(rep)
		if err != nil {
			fmt.Fprintf(os.Stderr, "vg-bench: marshal report: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(encoded))
	} else {
		printText(rep)
	}

	// A run with errors still prints its numbers — they describe real work —
	// but exits non-zero, so a script that chains benchmarks stops instead of
	// recording a report from a server that was failing underneath it.
	if rep.Errors > 0 {
		os.Exit(1)
	}
}
