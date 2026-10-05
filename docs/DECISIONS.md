# Decisions

Every non-obvious choice gets an entry. Format: date · decision · context ·
alternatives · reason.

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
