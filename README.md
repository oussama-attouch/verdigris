# Verdigris

A Redis-compatible key-value store, written from scratch in Go.

## Status

Milestones 1–5 complete. The server is a working string store with key
expiration: `SET`, `GET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL` and `PERSIST`
operate on a keyspace shared by every connection, with `SET ... EX/PX` for
time-limited writes. Expired keys are removed both on access and by a
background sweeper, and every TTL test runs on an injected clock rather than
sleeping. No persistence yet.

## Why

Verdigris exists to answer a question that using a framework never asks:
what is actually happening underneath? A data server is a socket, a byte
buffer, a parser, a map with expiry rules, and a story about what survives
a crash — and building each of those by hand is the only way to stop taking
them for granted. This project is deliberately not a wrapper around Redis,
not an exercise in wiring a web framework together, and not a portfolio
toy: it is an attempt to understand the engine by building the engine.

## Non-goals

- Not a drop-in replacement for Redis
- Not pursuing 100% command coverage
- Not chasing raw benchmark parity with Redis
- Not using third-party frameworks for the server, protocol, or store

## Roadmap

See [docs/ROADMAP.md](docs/ROADMAP.md).

## Building

```bash
go build ./...
go run ./cmd/verdigris
```

## Running

```bash
go run ./cmd/verdigris              # listens on :6379
go run ./cmd/verdigris -port 7000
```

The server speaks RESP, so the real client is the right way to try it:

```bash
redis-cli PING                       # PONG
redis-cli ECHO "hello verdigris"     # "hello verdigris"
redis-cli SET foo bar                # OK
redis-cli GET foo                    # "bar"
redis-cli EXISTS foo                 # (integer) 1
redis-cli DEL foo                    # (integer) 1
redis-cli GET foo                    # (nil)
redis-cli SET session abc EX 5       # expires in five seconds
redis-cli TTL session                # (integer) 5, then counts down
redis-cli NOTACOMMAND                # (error) ERR unknown command 'NOTACOMMAND'
```

The keyspace is shared by every connection, so a value set by one client is
visible to the next. Command names are case-insensitive, as they are in Redis.
Any RESP client works, not just `redis-cli`; on Windows without one,
PowerShell's `TcpClient` can send the raw bytes:

```powershell
$c = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 6379)
$s = $c.GetStream()
$msg = [Text.Encoding]::ASCII.GetBytes("*1`r`n`$4`r`nPING`r`n")
$s.Write($msg, 0, $msg.Length)
$buf = New-Object byte[] 64
$n = $s.Read($buf, 0, 64)
[Text.Encoding]::ASCII.GetString($buf, 0, $n)   # prints +PONG
$c.Close()
```

Stop it with Ctrl-C; the server closes the listener, waits for in-flight
connections, and exits 0. `-port` outside 1–65535 prints usage and exits 2.

## License

MIT
