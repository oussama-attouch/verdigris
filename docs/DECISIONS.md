# Decisions

Every non-obvious choice gets an entry. Format: date · decision · context ·
alternatives · reason.

## 2026-10-14 — AOF file format is RESP

Context: Redis's AOF is a sequence of RESP-encoded commands. An
alternative would be a custom binary format, a JSON log, or a text log
of "SET foo bar" lines.

Decision: the file is exactly the bytes a client would send. A command
is a RESP array of bulk strings:
`*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`.

Reason: the format is already implemented (protocol.Encode), trivially
readable by any RESP-aware tool, and a future DEBUG LOAD could pipe the
file to a running server as-is. JSON loses binary-safety; a text log
requires a second parser with its own edge cases.

## 2026-10-14 — fsync policy: everysec default, matching Redis

Context: fsync is the durability primitive. Always → one disk
round-trip per command. No → OS decides, can buffer minutes.

Decision: three policies selected by a flag. Default everysec.
- `always` — fsync after every append.
- `everysec` — background goroutine fsyncs once per second.
- `no` — OS flushes on its own.

Everysec loses at most ~1 second of writes on a hard kill. Redis's own
default, and the correct default here.

Alternatives: always by default (correct but one disk round-trip per
write, which is the wrong default for a server that has not been asked
for it); no by default (fastest, but the log's entire reason to exist
over RDB is the smaller loss window, and letting the OS buffer for
minutes gives that up).

The tail is the second half of the decision. A crash mid-write leaves a
partial command at the end of the file, and that is expected rather than
corrupt, so Replay truncates the file to the last complete command and
returns the valid prefix. A protocol error before the tail is genuine
corruption and is fatal. The split follows Redis and follows the
evidence: a partial last frame is what a crash produces, while a bad
byte in the middle cannot be produced by any crash and can only mean the
file was damaged or written by something else.

## 2026-10-05 — RDB file format: magic + version + count + entries + CRC32

Context: The store needs a file format that round-trips keys, values, and
expiry times without losing binary-safety, and that a future change can
evolve without invalidating old files.

Decision: a simple binary format.

    magic       9 bytes  "VERDIGRIS"
    version     1 byte   currently 1
    count       8 bytes  big-endian uint64
    entries     count × entry
    crc32       4 bytes  big-endian uint32, over everything above

    entry:
      expiresAt   8 bytes  big-endian int64 nanoseconds since Unix epoch,
                           or 0 if no expiry
      nilFlag     1 byte   1 when the value was nil, 0 otherwise
      keyLen      4 bytes  big-endian uint32
      key         keyLen bytes
      valLen      4 bytes  big-endian uint32
      value       valLen bytes

Big-endian throughout, matching every standard network protocol. The
version byte lets a future milestone change the format without rejecting
old files outright. The CRC lets corruption be detected rather than
silently producing garbage data.

Alternatives: mimic Redis's RDB format exactly (large, complex, has
opcodes and version-specific quirks that add no demonstration value); JSON
(loses binary-safety, blows up size); a key-value text format (same
problem). The chosen format is small enough to read end-to-end in a minute
and covers every case the store can produce.

The nilFlag byte is an addition to the original layout, made after the
round-trip test caught it: a nil value and an empty value both have zero
length, so without a flag the two collapse into one on disk. They are not
interchangeable, because the RESP layer renders them as `$-1` (null) and
`$0` (empty), which clients branch on. Without the flag a restart would
silently turn every nil value into an empty one.

## 2026-10-05 — The server owns the snapshot path, not the store

Context: SAVE has to reach the filesystem from a command handler, which
means something must own the path and the load/save logic. Two shapes were
possible: give the store a path and let it expose Load and Save, or keep
the path in the server and have the server do the file I/O through
rdb.Write and rdb.Read.

Decision: the server owns the path. `Server.Save` reads the store's
snapshot and writes it; the SAVE handler receives that method as a
SaveFunc. The store knows nothing about files, and the rdb package knows
nothing about either.

Reason: the store is a keyspace with a documented ownership invariant and
no other knowledge of the outside world. Giving it a filesystem path would
mean the data structure also decides when durability happens, which is a
server-lifecycle question: the snapshot is taken at shutdown after
connections drain, and via a command that has to be routed to the running
process. Keeping the path in the server leaves the store importable and
testable with no filesystem at all, and it puts the one boundary crossing
(a command reaching back into the server) in a single, named place —
SaveFunc — rather than spreading it through the keyspace.

Cost: SAVE is the first command that needs the server, so
commands.DefaultRegistry takes a SaveFunc parameter. That is the seam, and
it is explicit rather than implicit.

## 2026-10-05 — RDB saves on shutdown and via SAVE, not periodically

Context: Redis saves on a schedule (seconds-changes and changes-keys
heuristics) in addition to shutdown. A scheduled save is stateful, couples
the store to a background timer with its own config surface, and adds a
failure mode (partial writes) that this milestone does not need to
demonstrate.

Decision: RDB saves on graceful shutdown (SIGINT/SIGTERM) and in response
to the SAVE command. No periodic save.

Consequence: a hard kill (SIGKILL) loses writes since the last SAVE, and
this is documented as a limitation. Milestone 7's AOF closes that gap.

## 2026-10-05 — Integral arguments accept both RESP forms

Context: The milestone specification said EXPIRE's timeout "must be a RESP
Integer (`:100\r\n`)". Implementing that literally made the milestone's own
redis-cli demo fail on four of its twelve commands.

Capturing the bytes off the wire settled it: redis-cli sends every
command-line token as a **bulk string**. `EXPIRE k 100` arrives as
`*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$3\r\n100\r\n` and `SET k v EX 5` as
`*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n5\r\n`. There is
no RESP integer anywhere in either request.

Decision: `parseIntegerArg` accepts a RESP Integer *or* a BulkString holding
a strict base-10 integer, and every numeric argument — EXPIRE's timeout,
SET's EX and PX counts — goes through it. Redis accepts both, and so must a
server that claims compatibility.

Reason: A handler that accepts only the integer form works with client
libraries that know an argument is numeric and fails with redis-cli, and the
failure looks like a server bug rather than a parsing gap, because the client
is sending something entirely reasonable. The bulk-string path is parsed with
strconv.ParseInt so that range errors are reported rather than wrapped.

## 2026-10-05 — TTL reports at least 1 second for a live key

Context: The specification required TTL to round to the nearest second, with
`SET k v PX 500` reading as 1 rather than 0. Plain rounding does not achieve
that: by the time the reply is built a few hundred microseconds have passed
since the write, so 499.9 ms remains and math.Round gives 0. The server's own
integration test caught this, and the milestone's demo would have shown it.

Decision: round to the nearest second, then floor the result at 1 for any key
that is still alive. A live key never reports 0.

Reason: zero is a lie about a key that exists. The idiomatic client check
`TTL k > 0` would conclude the key is gone while it is still readable, and
the next GET would contradict it. Redis guarantees at least 1 second for a key
with a TTL, and the property that no integer TTL ever contradicts a key's
liveness is worth more than exact rounding below one second. The actual expiry
deadline stays exact, so `PX 200` really does expire in 200 ms; only the
reported count is floored.

## 2026-10-05 — Time is injected via a Clock interface

Context: Expiration requires "what time is it". A store that calls
time.Now() directly is untestable for TTL behaviour — every TTL test would
need a real sleep, which is slow and flaky.

Decision: `store.New(clk Clock)` takes a Clock interface with a single
`Now() time.Time` method. Production passes `store.RealClock{}`; tests pass
a `FakeClock` whose Now() returns a value the test can advance. The store
never calls time.Now() directly.

Cost: one interface and one field on the Store. In exchange, every TTL test
is deterministic and instant.

## 2026-10-05 — Active expiration samples, does not scan

Context: The naive active sweeper walks every key on every tick. With N keys
and a 100 ms tick, that is O(N) per 100 ms — 10000 scans/sec on a 1000-key
store, which dwarfs the actual command rate and would dominate any profile.

Decision: mimic Redis's own design. On each tick, sample up to 20 keys that
have an expiry, delete the expired ones, and if more than 25% of the sample
was expired, repeat immediately. Bounded work per tick, and the repetition
means a store full of expired keys drains quickly rather than at 20 keys
per 100 ms.

Alternatives: full scan (rejected — O(N) per tick); a sorted structure keyed
by expiry time (correct but requires a second index, which the store does
not yet have and does not need for these volumes).

## 2026-10-05 — Entry struct replaces raw []byte values

Context: Milestone 4 deliberately kept `map[string][]byte` and noted that
expiration would change it. Expiration adds per-key metadata (an absolute
expiry time), which the map cannot carry.

Decision: `map[string]entry`, where `entry{value []byte, expiresAt time.Time}`
and a zero `expiresAt` means "no expiry". Contained change inside `store.go`;
nothing outside the package observes it.

The `entry` struct is unexported. The Store's public API is unchanged except
for the new `Expire`, `TTL`, `Persist`, `SetWithTTL`, and the
expiration-aware `Get`/`Exists`.

## 2026-10-05 — Handler signature returns (Value, error)

Context: The milestone 3 `Handler` type returned only a `protocol.Value`.
Domain errors (wrong arity, unknown command) are protocol values and are
correct in that position, but an internal fault — a bug in a handler — had
nowhere to go except into a RESP error frame, where the client would see it
verbatim. That conflates "the client did something wrong" with "we have a
bug", and it leaks implementation detail to the client.

Decision: change `Handler` and `Registry.Dispatch` to return
`(protocol.Value, error)`. The Value is the reply, always sent to the
client. The error, when non-nil, is an internal fault the loop logs and
converts to a generic `ERR internal error` reply. Handlers return
`protocol.Error(...)` for domain errors and `(nil, err)` for internal
faults.

Cost: PING and ECHO change signature trivially (they return `(reply, nil)`).
Existing tests need one added return value. Not worth deferring.

## 2026-10-05 — Argument byte lifetime: store copies on entry and exit

Context: Handlers receive `BulkString` arguments that alias the connection's
read buffer. The read loop writes the reply before the next read, so in
milestone 3 the aliasing was safe. That safety is a property of the loop,
not of the handler contract — a refactor that reads ahead would silently
corrupt every handler that holds onto argument bytes.

Decision: the store copies on entry and on exit. `Store.Set` copies both
key and value; `Store.Get` returns a fresh copy. Handlers may pass aliased
argument bytes directly to the store without copying; handlers may retain
what `Store.Get` returns without worrying about lifetimes. The read loop
stays as-is — its ordering becomes a performance property, not a correctness
one.

Cost: one allocation per argument on write, one per value on read. For a
store that is not yet measured, correctness wins. If milestone 8's benchmarks
show the copies are the bottleneck, revisit with an explicitly unsafe read
path and a documented contract change.

## 2026-10-05 — Strict CRLF only; no bare-LF leniency

Context: Real Redis accepts a bare LF on length lines when reading commands,
a leniency left over from the inline-command era. Our decoder rejects it —
`\r` must precede every `\n`.

Decision: stay strict. Real clients (redis-cli, all language SDKs) send CRLF
and nothing else; the lenient path is a Redis compatibility accommodation
for hand-typed input over telnet, which is not a use case this project has.

If a future milestone discovers a real client that relies on bare-LF, this
is a deliberate, documented loosening and not an oversight. It is not
expected to happen.

## 2026-10-05 — Serve does not take a context

Context: `handleConnection` grows a decode loop this milestone. When
`Shutdown` closes the listener and then the connection, an in-flight read
returns an error and the handler returns cleanly. The caller does not need
a per-request cancellation mechanism.

Decision: keep `Serve()` as-is; `Shutdown(ctx)` remains the only
cancellation surface. Adding `Serve(ctx)` would be redundant — the listener
close *is* the cancellation signal. Revisit only if a future milestone needs
mid-handler interruption that closing the connection cannot provide.

## 2026-10-05 — Fuzz target for the RESP decoder

Context: Milestone 2's hand-written tests found a real overflow bug —
`n+2 > int64(c.len())` wraps negative on MaxInt64 input, causing a slice
panic from a 21-byte packet. That bug was found because a specific test was
written to look for it. A fuzz target is the systematic version of that
instinct.

Reason: `Decode` is a pure []byte → Value function with no state and no
setup, which is the ideal shape for fuzzing. The target asserts exactly one
invariant — Decode must not panic — and the seed corpus is the hand-written
test suite. Go's fuzzing is part of the standard toolchain, so this adds no
dependency and no build complexity.

Scope note: the fuzzer is bounded to 60 seconds in milestone 2. It is not
run as part of `go test ./...` (Go's fuzzing infra runs the seed corpus in
normal mode, but exploratory fuzzing is a separate invocation). A future
milestone may run it for longer in CI once the surface area grows.

## 2026-10-05 — RESP values: sealed interface with concrete types

Context: A decoded RESP frame must be representable in Go in a way that the
command layer can switch on and the encoder can serialize back. Three shapes
were considered.

Alternatives:
- `interface{}` with ad-hoc type assertions at call sites. Un-typed, easy to
  mis-cast at runtime, no help from the compiler.
- A single tagged struct with a Kind enum and per-kind fields. Simple, but
  permits constructing invalid values (Kind: BulkString with the Integer
  field set), and forces every consumer to check Kind before every field
  access.
- A sealed interface with one concrete type per RESP type. Chosen.

Reason: concrete types make invalid states unrepresentable, match how
encoding/json models tagged unions, and give the command layer a natural
type switch. Adding RESP3 types later (Map, Set, Boolean, Double) is a new
type, not a change to a shared struct. The cost — a package-level interface
and six small types — is trivial against the clarity it buys.

## 2026-10-05 — gcc on PATH requires a per-invocation preamble

Context: The DSH harness environment block is captured at session start
and does not reflect PATH changes made during the session. Every pwsh
tool call inherits that stale block, so gcc is not on PATH unless the
current invocation sets it explicitly.

Reason: A one-line preamble reads Machine + User PATH fresh from the
registry and assigns it to $env:Path. It works regardless of harness
state and matches the pattern any Windows CI runner uses.

## 2026-10-05 — Race detector enabled via user-level MinGW-w64

Context: go test -race requires cgo, which requires a C compiler. Chocolatey
cannot install one in this environment (unelevated session, approval prompts
disabled), and winget is not present. The race detector is the single most
valuable tool for the concurrency-sensitive milestones ahead — the store's
mutex in milestone 4, the AOF flush in milestone 7, and the connection
tracking that is already in place.

Alternatives: MinGW-w64 via chocolatey (unusable here); MSVC via Visual Studio
Build Tools (multi-GB install, overkill for cgo); WSL gcc (WSL enumeration is
access-denied on this machine).

Reason: winlibs.com's UCRT/POSIX/SEH build is a self-contained zip that
extracts under %LOCALAPPDATA%\Programs\mingw64 with no administrator rights,
matches the pattern already used for the Go install, and is trivially
reversible by deleting the directory and removing one PATH entry. gcc on PATH
plus CGO_ENABLED=1 enables the race detector everywhere without admin access.

## 2026-10-05 — Concurrency model: goroutine per connection

Context: Redis is single-threaded with an epoll/kqueue event loop. Go is built
around blocking sockets with goroutines. Writing an epoll-based event loop in
Go is possible (via syscall or golang.org/x/sys) but fights the language and
adds significant complexity to milestone 1.

Alternatives: single-threaded epoll loop (Redis-faithful, higher cost, more
subtle bugs); a fixed worker pool (unnecessary for this project's scale).

Reason: goroutine per connection is what Go is designed for, and it delivers
correct concurrent connection handling with less code. The store's design
(a mutex around the map) makes the concurrency model invisible to milestones
2–5. If milestone 8's benchmarks show the goroutine model is the bottleneck,
the event loop can be revisited then — and the store won't need to change.

## 2026-10-05 — Language: Go

Context: The project needs raw sockets, concurrency primitives, and a
small compiled binary with no runtime dependencies.

Alternatives: Rust (stronger type system, steeper learning curve); Java
with NIO (familiar from prior work, heavy boilerplate).

Reason: Go's standard library is unusually complete for this kind of
systems work (`net`, `bufio`, `sync`, `os` cover nearly everything). The
compiled single-binary output makes distribution trivial. Rust was
considered but rejected on time-to-first-milestone grounds. The point of
the project is understanding, and Go lets that understanding arrive sooner.

## 2026-10-05 — Standard library only

Context: Every framework available in Go would solve part of this project
for us.

Alternatives: Gin or Echo for HTTP (irrelevant here — Redis does not
speak HTTP); `net/http` as a stand-in for raw TCP (would obscure the
networking layer entirely).

Reason: The project exists to understand what the frameworks do. Using
one would defeat the point.

## 2026-10-05 — Toolchain: Go installed user-level, no administrator rights

Context: Go was absent from this machine, and the session runs unelevated
with approval prompts disabled. Chocolatey is installed but cannot take its
package lock file without an elevated shell, so `choco install golang`
failed — it is not a viable path here.

Alternatives: `winget install GoLang.Go` (winget is not installed on this
machine); the official `.msi` (needs administrator rights to install);
elevating the shell (not possible in this session).

Reason: The official Go distribution zip was unpacked to
`%LOCALAPPDATA%\Programs\go` and its `bin` directory appended to the user
`PATH`. This needs no administrator rights, is trivially reversible, and
installs the same toolchain the MSI would. The environment-specific cost is
that new shells must be started after `PATH` changes — worth recording,
because a future session seeing "go: not found" should suspect `PATH`
propagation rather than a missing install.

## 2026-10-05 — Test client: redis-cli from the tporadowski/redis Windows build

Context: Every milestone's demo needs a real `redis-cli`. Redis has no
official Windows build; the original Windows port (`microsoftarchive/redis`)
has been archived since Redis 3.0.504 (2016).

Alternatives: `microsoftarchive/redis` 3.0.504 (archived, roughly a decade
old); Redis under WSL (WSL distro enumeration is access-denied on this
machine); Redis inside Docker (`docker` CLI is present but the daemon is not
running).

Reason: `tporadowski/redis` v5.0.14.1 is a maintained Windows port and gives
`redis-cli.exe`, `redis-server.exe`, and `redis-benchmark.exe` — the last of
which milestone 8's comparison needs for a baseline. Installed user-level to
`%LOCALAPPDATA%\Programs\redis`. Two caveats to remember: Windows
`redis-cli` has no `--version` flag (use `redis-server --version` for the
build), and it is Redis 5-era, so it predates RESP3 and `HELLO`. Neither
affects milestones 1–8.
