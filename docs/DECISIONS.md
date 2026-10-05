# Decisions

Every non-obvious choice gets an entry. Format: date · decision · context ·
alternatives · reason.

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
