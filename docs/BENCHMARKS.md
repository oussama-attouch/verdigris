# Benchmarks

## What was measured

Two RESP servers on the same machine, same client, same workload:

- **Verdigris** — this project. AOF persistence enabled with `everysec`
  fsync. One goroutine per connection. Store protected by a `sync.RWMutex`,
  with copy-on-entry and copy-on-exit.
- **Redis 5.0.14.1** — the real Redis server, from the tporadowski Windows
  build (`malloc=jemalloc-5.2.1-redis`, `bits=64`). Default configuration
  except RDB snapshotting disabled (`--save ""`) to match Verdigris's
  durability profile. `appendonly` is `no` on both sides of the comparison:
  Verdigris's `everysec` policy does not fsync on the hot path, so neither
  server pays a disk round trip per write.

Client: `cmd/vg-bench`, using `internal/protocol` for both encode and
decode. Same binary, same flags, both servers. 10,000 requests per client,
200 warmup per client, at 1 / 10 / 50 concurrent clients.

The only variable between any two runs below is `-addr`.

## Environment

- OS: Windows 11 Home, kernel 10.0.26300
- Go: go1.27.1 windows/amd64
- CPU: 12th Gen Intel(R) Core(TM) i5-12450H — 8 cores, 12 logical processors
- RAM: 15.7 GB
- Both servers bound to 127.0.0.1, client on the same host

## The clock caveat

Windows' default system timer resolution makes `time.Now()` advance in steps
of about 0.5 ms. This was measured rather than assumed: back-to-back readings
of two adjacent `time.Now()` calls differ by 0 ns in 199,995 of 200,000
samples on this machine, a `Sleep(1ms)` reports as 1.537 ms, and a 100 µs
busy-wait reports as 339 µs. The full reasoning is recorded in the ADR at
[DECISIONS.md](DECISIONS.md), "Latency percentiles are quantized by the
system clock".

Consequence: **`p50` reads 0.000 ms on both servers, and `p95` is dominated by
the clock tick rather than by server behavior.** Any per-request duration
smaller than a tick is rounded to zero, so the percentiles cannot resolve
sub-millisecond latency at all. The reliable metrics here are **throughput
(ops/sec)** and the **mean latency**, which is computed from total elapsed
time over the whole run rather than from per-request samples, and therefore
amortizes the tick across thousands of measurements.

The two servers run on the same host against the same clock, so the comparison
stays fair even though the absolute latency figures do not mean much.

## Throughput

### PING

| Clients | Verdigris ops/sec | Redis ops/sec | Ratio (V/R) |
|---|---|---|---|
| 1 | 8,145 | 7,234 | 1.13× |
| 10 | 55,159 | 12,465 | 4.42× |
| 50 | 48,553 | 11,961 | 4.06× |

### SET

| Clients | Verdigris ops/sec | Redis ops/sec | Ratio (V/R) |
|---|---|---|---|
| 1 | 6,449 | 8,536 | 0.76× |
| 10 | 14,617 | 12,326 | 1.19× |
| 50 | 23,890 | 11,924 | 2.00× |

### GET

| Clients | Verdigris ops/sec | Redis ops/sec | Ratio (V/R) |
|---|---|---|---|
| 1 | 9,647 | 7,675 | 1.26× |
| 10 | 56,399 | 12,279 | 4.59× |
| 50 | 54,197 | 11,979 | 4.52× |

### MIXED

| Clients | Verdigris ops/sec | Redis ops/sec | Ratio (V/R) |
|---|---|---|---|
| 1 | 7,366 | 8,104 | 0.91× |
| 10 | 37,437 | 12,193 | 3.07× |
| 50 | 39,474 | 11,856 | 3.33× |

## Latency

Mean latency in milliseconds, from total elapsed time divided by operations.
Percentiles are included for completeness and carry the clock caveat above.

### Verdigris (127.0.0.1:6380)

| Clients | Mode | mean ms | p50 ms | p95 ms | p99 ms | max ms | errors |
|---|---|---|---|---|---|---|---|
| 1 | ping | 0.123 | 0.000 | 0.582 | 0.676 | 1.097 | 0 |
| 10 | ping | 0.018 | 0.000 | 0.612 | 0.764 | 5.949 | 0 |
| 50 | ping | 0.021 | 0.000 | 1.959 | 2.649 | 8.618 | 0 |
| 1 | set | 0.155 | 0.000 | 0.585 | 0.682 | 1.403 | 0 |
| 10 | set | 0.068 | 0.000 | 1.646 | 2.501 | 24.251 | 0 |
| 50 | set | 0.042 | 0.000 | 4.030 | 7.243 | 25.650 | 0 |
| 1 | get | 0.104 | 0.000 | 0.573 | 0.618 | 1.659 | 0 |
| 10 | get | 0.018 | 0.000 | 0.595 | 0.751 | 15.051 | 0 |
| 50 | get | 0.018 | 0.000 | 2.188 | 3.108 | 18.272 | 0 |
| 1 | mixed | 0.136 | 0.000 | 0.595 | 0.669 | 1.525 | 0 |
| 10 | mixed | 0.027 | 0.000 | 1.048 | 1.515 | 13.821 | 0 |
| 50 | mixed | 0.025 | 0.000 | 3.457 | 4.833 | 59.907 | 0 |

### Redis 5.0.14.1 (127.0.0.1:6381)

| Clients | Mode | mean ms | p50 ms | p95 ms | p99 ms | max ms | errors |
|---|---|---|---|---|---|---|---|
| 1 | ping | 0.138 | 0.000 | 0.622 | 0.740 | 4.233 | 0 |
| 10 | ping | 0.080 | 0.000 | 1.700 | 2.100 | 10.762 | 0 |
| 50 | ping | 0.084 | 0.000 | 7.500 | 9.029 | 41.288 | 0 |
| 1 | set | 0.117 | 0.000 | 0.607 | 0.701 | 3.206 | 0 |
| 10 | set | 0.081 | 0.000 | 1.694 | 1.993 | 10.969 | 0 |
| 50 | set | 0.084 | 0.000 | 7.437 | 8.825 | 30.290 | 0 |
| 1 | get | 0.130 | 0.000 | 0.593 | 0.688 | 6.558 | 0 |
| 10 | get | 0.081 | 0.000 | 1.710 | 2.232 | 11.529 | 0 |
| 50 | get | 0.083 | 0.000 | 7.496 | 9.084 | 17.532 | 0 |
| 1 | mixed | 0.123 | 0.000 | 0.611 | 0.696 | 2.969 | 0 |
| 10 | mixed | 0.082 | 0.000 | 1.701 | 2.050 | 5.540 | 0 |
| 50 | mixed | 0.084 | 0.000 | 7.573 | 9.237 | 40.727 | 0 |

## Where the differences come from

The headline is that **Redis is not faster on this machine.** At one client the
two are level — 8,145 vs 7,234 ops/sec on PING, a 13% difference that is inside
run-to-run noise. Above one client they separate sharply, and the shape of the
separation is the interesting part: Redis's throughput is *flat* at roughly
11,000–12,000 ops/sec from 10 clients all the way to 200, while Verdigris
reaches 45,000–56,000. Extending the sweep past the reported matrix shows this
plainly (PING, 5,000 requests per client):

| Clients | Verdigris ops/sec | Redis ops/sec |
|---|---|---|
| 1 | 7,050 | 7,326 |
| 10 | 45,286 | 11,391 |
| 50 | 50,798 | 11,440 |
| 100 | 45,933 | 11,681 |
| 200 | 44,458 | 11,294 |

Redis's number does not move with concurrency, and its `p95` climbs from
1.869 ms at 10 clients to 24.093 ms at 200 while its throughput stays put. That
is the signature of a fixed serialization point: adding parallel clients makes
each one wait longer and buys no additional throughput.

What is *not* the explanation is worth stating too, because it is the obvious
guess and the data does not support it:

- **Redis being single-threaded does not explain a gap this size.** A single
  thread parsing RESP and serving from an in-memory dict should manage far more
  than 11,000 round trips per second; Redis on Linux routinely does. The
  ceiling here is a property of this Windows build, not of Redis's design.
- **Verdigris's locking does not explain its win.** The RWMutex and the
  copy-on-read are costs Verdigris pays and Redis does not. Verdigris wins
  despite them, which means the difference is elsewhere.
- **The measurements are not an artifact of ordering or warmup.** Re-running
  PING at 10 clients with Redis first and Verdigris second gives Redis 10,705
  and Verdigris 37,339 — the same relationship in the opposite order.

The honest summary is therefore narrower than "Verdigris beats Redis":
**on this Windows host, with the tporadowski Redis 5.0.14.1 build, Verdigris
serves 3–4× the throughput above one client, and the reason is a fixed
per-operation serialization cost in that Redis build rather than anything
about Redis on Linux.** No profiling was done to identify the precise
mechanism, so that remains an inference from the shape of the curve, not a
measurement.

The costs Verdigris pays, for the record, are real and are where they were
expected to be:

- **A mutex around the store.** At one client there is no contention; at 50 the
  lock is the limit for `GET`, which is why `GET` tops out at ~54,000 while
  `PING`, which touches no shared state, reaches ~55,000 and holds there.
- **A copy on every read and write.** `Store.Get` returns a fresh `[]byte` and
  `Store.Set` copies key and value, which is what makes the mutex sound — a
  caller cannot mutate memory another connection is reading.
- **The Go protocol layer is inside the measurement.** `protocol.Encode` and
  `protocol.Decode` run on every request on both sides of the socket.
- **`SET` is the slowest mode on both servers** (Verdigris 23,890 at 50
  clients, against 54,197 for `GET`), because it allocates and because
  Verdigris appends every write to the AOF under a second mutex.

## What this does not measure

- **Latency at the sub-millisecond level.** The clock caveat above means
  per-request percentiles below ~0.5 ms are not resolvable. The means are
  trustworthy; the `p50`/`p95` columns are not.
- **Redis on Linux.** Every number here is from the Windows port. Redis's
  reputation for throughput comes from Linux, where it uses `epoll`; the
  Windows build uses a different I/O path entirely, and this report makes no
  claim about the former.
- **Throughput above 200 clients.** The reported matrix stops at 50; the
  scaling table above reaches 200 and shows both servers plateauing.
- **Memory usage.** No metric was collected.
- **Persistence cost.** `-aof-fsync always` would be dramatically slower on
  Verdigris, and RDB snapshotting was disabled on Redis. The comparison is
  honest for the durability profile used and says nothing about the others.
- **Realistic key distributions.** `SET` writes a distinct key per operation
  and `GET` reads from a 1,000-key window, so neither exercises eviction,
  large values, or a working set that does not fit in cache.

## Reproducing

```powershell
go build -o vg-bench.exe ./cmd/vg-bench
go build -o verdigris.exe ./cmd/verdigris

# Verdigris on 6380, AOF with everysec fsync
.\verdigris.exe -port 6380 -aof-path "$env:TEMP\vg.aof" -aof-fsync everysec

# Redis on 6381, snapshots disabled
& "$env:LOCALAPPDATA\Programs\redis\redis-server.exe" --port 6381 --save '""'

# One run of the matrix
.\vg-bench.exe -addr 127.0.0.1:6380 -mode ping -clients 10 -requests 10000 -warmup 200
.\vg-bench.exe -addr 127.0.0.1:6381 -mode ping -clients 10 -requests 10000 -warmup 200
```

Add `-json` for the machine-readable form. On Windows, note that
`--save ""` is stripped by PowerShell before it reaches the process, which
leaves Redis unable to start; `--save '""'` passes the empty string through.
