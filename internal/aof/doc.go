// Package aof appends write commands to a log and replays them on startup.
//
// This package is a log, not a keyspace. It knows the file format and nothing
// about the store: Append takes an argument list, Replay returns a slice of
// them, and neither has a reference back to the keys that produced them. That
// direction is deliberate — internal/server imports this package and nothing
// here imports it back — and it is what lets the format be tested without a
// server, a client, or a clock.
//
// It is the second persistence mechanism, and it answers a different question
// from internal/rdb. A snapshot records the state of the keyspace at one
// instant and must be written whole; a log records the commands that produced
// that state and is written one command at a time. The practical difference is
// what a crash costs. A snapshot loses everything written since the last save,
// which may be minutes; a log under the default policy loses at most the last
// second, because the only writes at risk are the ones the background ticker
// has not yet flushed.
//
// # Why RESP
//
// The file is exactly the bytes a client would send: a command is a RESP array
// of bulk strings, so `SET foo bar` is the twenty-two bytes
// `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`. The format is already
// implemented — protocol.Encode writes it and protocol.Decode reads it — so
// this package adds a file and a policy rather than a parser. It also means
// the log is readable by any RESP-aware tool, and a future DEBUG LOAD can pipe
// it to a running server as-is, which no custom binary format could offer
// without a second implementation on the other side.
//
// Bulk strings are binary-safe, so a key or value containing NUL, invalid
// UTF-8, or a literal CRLF round-trips unchanged. A JSON log would need
// escaping that changes the bytes; a text log of "SET foo bar" lines would
// need a second parser with its own quoting rules and its own edge cases.
//
// # The durability contract
//
// Three policies, selected by a flag and parsed by ParseFsyncPolicy:
//
//	always    fsync after every Append. A successful Append is on disk.
//	everysec  a background goroutine fsyncs once per second. The default.
//	no        the operating system flushes when it chooses.
//
// Everysec is the default because it is the right trade for a server: one
// disk round-trip per second rather than one per command, in exchange for a
// window no longer than that on a hard kill. Always is for callers who cannot
// tolerate the window at all, and no is for a log whose contents are
// reproducible or disposable. Close always syncs, whatever the policy.
//
// # The truncation contract
//
// The file has two failure shapes and they are not treated alike.
//
// A truncated tail is a frame cut short at the end of the file, which the
// RESP decoder reports as an incomplete frame: it ran out of bytes. That is
// the expected result of a crash mid-write, so Replay truncates the file to
// the last complete command and returns the commands before it. The command
// that was being written is lost; everything acknowledged earlier survives.
// This is why the log is append-only: a partial write can only ever damage
// the last frame, because no earlier byte is ever revisited.
//
// A protocol error is a grammar violation — bytes that are present and wrong.
// It is fatal wherever it appears, including at the very end of the file,
// and Replay refuses the whole file with an error wrapping ErrCorrupt rather
// than returning the valid prefix. No crash produces one, so it can only mean
// the file was damaged or written by something else; and a server that loaded
// the readable prefix would present a keyspace that never existed while
// silently dropping everything after the damage.
package aof
