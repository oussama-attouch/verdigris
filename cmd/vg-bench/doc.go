// Command vg-bench measures the throughput and latency of any RESP server.
//
// It exists for one reason: an honest comparison. The claim this project makes
// is that Verdigris speaks RESP well enough to be measured against Redis with
// the same client, and the only way to support that claim is to have exactly
// one client. The same binary, the same flags, and the same generated workload
// run against whatever is listening on -addr, so the only variable between two
// runs is which server is at the other end of the socket.
//
// That is also why the harness is written rather than borrowed. redis-benchmark
// is a fine tool, but it is compiled against Redis's own client library: run it
// against Redis and the client half of the measurement is the code Redis ships,
// while run against Verdigris it is still that code, exercised against a
// different server. Comparing the two results says something about the two
// servers but nothing about the two implementations of the client — and the
// client is half of what this project built. A Go harness uses this project's
// internal/protocol to encode every request and decode every reply, so the
// protocol package's performance is inside the measurement instead of hidden
// behind it.
//
// It is a client and nothing else. It imports internal/protocol and the
// standard library, and deliberately does not import internal/store or
// internal/server: a harness that reached into the server it was measuring
// could not be pointed at Redis at all, and would stop being a fair comparison
// the moment it did something the wire protocol does not allow.
//
// It reports operations per second, a mean round-trip latency, the
// p50/p95/p99/max round-trip latency, and an error count, as text for a human
// or as one JSON object for a script. Each measured operation is timed from
// just before the request is written to just after the reply is decoded, so
// the number covers the whole round trip: encoding, the socket, the server,
// and decoding.
//
// # Read the mean, not just the percentiles, on Windows
//
// The percentiles are computed from one clock reading per operation, and the
// system clock is not always fine-grained enough for that. On the Windows
// machine this harness was developed on, time.Now advances in steps of about
// 0.5ms: two back-to-back readings return the same value essentially always,
// a 1ms sleep measures as 1.5ms, and a 100us busy-wait measures as 300us+.
//
// The consequence is that a per-operation duration is rounded to a whole
// number of ticks. A loopback round trip of tens of microseconds reads as
// exactly zero, so p50 prints as 0.000ms and the percentiles above it are
// quantized to multiples of the tick. That is a limitation of the clock, not
// of the server, and it is not something this program can fix portably —
// stdlib-only Go has no cross-platform high-resolution timer to reach for.
//
// MeanMs is reported for exactly this reason. It is total elapsed time divided
// by operations, measured over the whole run rather than per operation, so the
// quantization error is amortized across thousands of ticks and the figure
// stays accurate to a small fraction of a microsecond on a run of any real
// length. Throughput is unaffected for the same reason. When the percentiles
// and the mean disagree — as they will on Windows, where p50 reads zero —
// believe the mean, and treat the percentiles as an upper bound on the tail
// rather than as a picture of the distribution.
//
// # Not a load-testing framework
//
// There is no scenario language, no ramp-up curve, no mixed-traffic modelling
// beyond the four built-in modes, and no result history. It runs one fixed
// workload for a fixed number of operations on each of N connections and
// prints what happened. Those are the measurements this project needed, and
// adding the rest would make the tool harder to trust without making it more
// honest.
//
// # Not a regression tracker
//
// It has no baseline, no thresholds, and no notion of a run being better or
// worse than a previous one. Throughput measured this way depends on the
// machine, the operating system's TCP stack, and what else is running; two
// numbers from two days are not comparable without far more context than a
// benchmark binary can carry. Recording runs and interpreting them is the job
// of whatever invokes this, which is why the numbers are also available as
// JSON.
//
// # What it will not tell you
//
// A high ops/sec here means the server answers this workload quickly over a
// loopback socket. It says nothing about persistence, memory efficiency, or
// behaviour under a workload this harness does not generate — a `-mode set` run
// with no `-rdb-path` and no `-aof-path` is measuring a server with durability
// switched off, and that is a fact about the run that belongs next to the
// number, not inside it.
package main
