# Verdigris Roadmap

Each milestone has a definition of done and a verifiable demo. A milestone
is complete when all three hold: the code exists, the tests pass, and the
demo can be performed from a fresh `redis-cli` connection.

| # | Milestone | Definition of done | Demo |
|---|---|---|---|
| 1 | TCP echo server | Server accepts a connection, echoes bytes back | `nc localhost 6379` echoes typed lines |
| 2 | RESP decoder | Decodes Simple Strings, Errors, Integers, Bulk Strings, Arrays | `PING` sent by `redis-cli` is parsed into a command |
| 3 | RESP encoder + PING/ECHO | Encodes replies; `PING` → `+PONG`, `ECHO x` → `$1\r\nx` | `redis-cli PING` returns `PONG` |
| 4 | Store + SET/GET/DEL/EXISTS | A concurrent-safe map with the four core commands | `redis-cli SET foo bar` then `GET foo` returns `bar` |
| 5 | Expiration (EXPIRE/TTL) | Passive + active deletion of expired keys | `redis-cli SET k v EX 2` vanishes after 2 s |
| 6 | RDB persistence | Point-in-time snapshot on shutdown, load on startup | Restart server, data survives |
| 7 | AOF persistence | Append-only log of writes, replayed on startup | Kill server with SIGKILL, restart, writes survive |
| 8 | Benchmarking | Measured PING/SET/GET throughput vs. Redis baseline | A short report in `docs/BENCHMARKS.md` |

## Ground rules

- Standard library only. A dependency requires an ADR in `docs/DECISIONS.md`.
- Single-threaded event loop. Parallelism is a later project, not this one.
- Correctness before performance.
- One milestone = one commit (or a small sequence of commits with clear
  boundaries).

## Non-goals

- Replication, clustering, Sentinel
- Pub/Sub (may become milestone 9 if time permits)
- Lua scripting, streams, transactions
- Replacing Redis in any production context
