# Verdigris

A Redis-compatible key-value store, written from scratch in Go.

## Status

Milestones 1–3 complete. The server speaks RESP: it decodes commands,
dispatches them through a registry, and encodes the replies. `PING` and `ECHO`
work, verified against the real `redis-cli`. No data store, no persistence,
and no commands beyond those two.

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
redis-cli PING "custom message"      # "custom message"
redis-cli NOTACOMMAND                # (error) ERR unknown command 'NOTACOMMAND'
redis-cli ECHO                       # (error) ERR wrong number of arguments for 'echo' command
```

Command names are case-insensitive, as they are in Redis. Any RESP client
works, not just `redis-cli`; on Windows without one, PowerShell's `TcpClient`
can send the raw bytes:

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
