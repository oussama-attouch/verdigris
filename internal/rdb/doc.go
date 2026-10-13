// Package rdb reads and writes Verdigris snapshot files.
//
// This package is a serialization library. It knows the file format and
// nothing about the store: an Entry is a plain struct of byte slices and a
// time, with no reference back to the keyspace it came from. That direction is
// deliberate — internal/store imports this package to build and consume
// snapshots, and the reverse import would be a cycle.
//
// # Format
//
//	magic       9 bytes  "VERDIGRIS"
//	version     1 byte   currently 1
//	count       8 bytes  big-endian uint64
//	entries     count × entry
//	crc32       4 bytes  big-endian uint32, over every byte above
//
//	entry:
//	  expiresAt   8 bytes  big-endian uint64 nanoseconds since the Unix epoch,
//	                       or 0 when the key has no expiry
//	  nilFlag     1 byte   1 when the value was nil, 0 otherwise
//	  keyLen      4 bytes  big-endian uint32
//	  key         keyLen bytes
//	  valLen      4 bytes  big-endian uint32
//	  value       valLen bytes
//
// The nilFlag byte exists because a nil value and an empty one both have zero
// length, and the two are not interchangeable: the RESP layer renders them as
// `$-1` (null) and `$0` (empty), which clients branch on. Without the flag a
// restart would turn every nil value into an empty one.
//
// Big-endian throughout, so a hex dump of a file reads top to bottom in
// significance order and matches every other network format. The version byte
// exists so a later change to the layout can be detected and either migrated
// or refused explicitly, rather than being misread as corrupt data. The CRC
// exists because the alternative to detecting corruption is serving it.
//
// Keys and values are length-prefixed byte strings and are never interpreted:
// NUL, invalid UTF-8, CRLF, and even a byte sequence that looks like this
// package's own header all round-trip unchanged.
//
// # Atomicity
//
// Write never modifies the target file in place. It writes a complete frame to
// <path>.tmp, syncs it, and renames it over the target. A crash at any point
// before the rename leaves the previous snapshot exactly as it was, and a
// crash during the rename cannot leave a half-written file: rename is atomic
// on POSIX systems, and os.Rename is documented to replace the destination
// atomically on Windows. The worst outcome of any failure is a stale snapshot,
// never a torn one.
//
// The temp file is removed on every failure path, so a failed save leaves no
// debris beside the real file.
//
// # Corruption
//
// Read is strict. A file whose magic, version, checksum, or internal lengths
// are wrong is refused with an error wrapping ErrCorrupt, and no entry from it
// is returned. There is no partial recovery and no attempt to use the entries
// that happened to parse: a snapshot that is partly unreadable is a snapshot
// whose contents cannot be trusted, and a server that loads half of it would
// present a keyspace that never existed.
//
// A missing file is not corruption. Read reports it as an error wrapping
// os.ErrNotExist so the caller can tell "this is the first run" from "this
// file is broken" — the first is ordinary, the second must stop the server.
//
// Every length is checked against the bytes actually remaining before it is
// used to slice or to size an allocation, so a crafted file declaring a
// four-gigabyte key fails immediately rather than attempting to honour it.
package rdb
