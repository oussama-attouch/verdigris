# Verdigris Roadmap

Each milestone has a definition of done and a verifiable demo. A milestone
is complete when all three hold: the code exists, the tests pass, and the
demo can be performed from a fresh `redis-cli` connection.

| # | Milestone | Status | Definition of done | Demo |
|---|---|---|---|---|
| 1 | TCP echo server | **Done** | Server accepts a connection, echoes bytes back | A raw TCP client's line comes back verbatim |
| 2 | RESP decoder | **Done** | Decodes Simple Strings, Errors, Integers, Bulk Strings, Arrays, and the null forms | Decoded in isolation: `go test ./internal/protocol/` — the `redis-cli` demo lands in milestone 3, when the decoder is wired into the server |
| 3 | RESP encoder + PING/ECHO | **Done** | Encodes replies; `PING` → `+PONG`, `ECHO x` → `$1\r\nx` | `redis-cli PING` returns `PONG` |
| 4 | Store + SET/GET/DEL/EXISTS | **Done** | A concurrent-safe map with the four core commands | `redis-cli SET foo bar` then `GET foo` returns `bar` |
| 5 | Expiration (EXPIRE/TTL) | **Done** | Passive + active deletion of expired keys | `redis-cli SET k v EX 2` vanishes after 2 s |
| 6 | RDB persistence | **Done** | Point-in-time snapshot on shutdown, load on startup | Restart server, data survives |
| 7 | AOF persistence | **Done** | Append-only log of writes, replayed on startup | Kill server with SIGKILL, restart, writes survive |
| 8 | Benchmarking | **Done** | Measured PING/SET/GET throughput vs. Redis baseline | A short report in `docs/BENCHMARKS.md` |
| 9 | KEYS command | Done | Returns all non-expired keys; only * supported | `redis-cli KEYS *` lists the keyspace |

Milestone 1's original demo was `nc localhost 6379`. Windows ships no `nc`,
so the demo is any raw TCP client instead — see the README's Running section
for a PowerShell one-liner. The `redis-cli` demos from milestone 3 onward are
unaffected.

Milestone 2's demo was originally "`PING` sent by `redis-cli` is parsed into a
command". That cannot be performed yet: `redis-cli` sends its bytes to the
server, and nothing calls the decoder until milestone 3 wires `Decode` into
`handleConnection`. The decoder is proved instead by its own test suite, which
covers every frame type, every malformed case, and every possible split point
of a frame arriving over TCP.

## Ground rules

- Standard library only. A dependency requires an ADR in `docs/DECISIONS.md`.
- Goroutine per connection in the network layer; the store is protected by a
  mutex, so the concurrency model never leaks into the data layer.
- Correctness before performance.
- One milestone = one commit (or a small sequence of commits with clear
  boundaries).

## Non-goals

- Replication, clustering, Sentinel
- Pub/Sub (may become milestone 9 if time permits)
- Lua scripting, streams, transactions
- Replacing Redis in any production context
