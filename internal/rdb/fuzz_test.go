package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// FuzzRead asserts the two properties that must hold for any byte sequence fed
// to the decoder: it must not panic, and it must not believe a size the file
// does not contain.
//
// The second is the one worth fuzzing. A length-prefixed format reads numbers
// out of the file and then trusts them to describe how much more file there
// is; the RESP decoder had exactly this bug in milestone 2, where an unchecked
// length wrapped a comparison and panicked a connection goroutine. Here the
// consequence would be worse, because the input is a file an operator points
// the server at rather than a packet from a client.
//
// A file is written into a temp directory on every iteration rather than
// decoding bytes in memory, so the fuzzer exercises the real Read path —
// including the os.ReadFile error handling — rather than a helper that only
// exists for tests.
//
// Run with:
//
//	go test -run=^$ -fuzz=FuzzRead -fuzztime=60s ./internal/rdb/
func FuzzRead(f *testing.F) {
	// Build the valid seeds with the real encoder, so they cannot drift from
	// the format. A seed that no longer parses would waste the fuzzer's first
	// seconds rediscovering the header.
	mustEncode := func(entries []Entry) []byte {
		data, err := encode(entries)
		if err != nil {
			f.Fatalf("building a fuzz seed: %v", err)
		}
		return data
	}

	// An empty file: shorter than any valid header.
	f.Add([]byte{})
	// A valid empty snapshot.
	f.Add(mustEncode(nil))
	// A valid snapshot with entries, with and without expiry.
	f.Add(mustEncode([]Entry{
		{Key: []byte("k"), Value: []byte("v")},
		{Key: []byte("binary"), Value: []byte{0x00, 0xff, '\r', '\n'}},
	}))
	// The smallest well-formed file: a header and a CRC, no entries.
	f.Add(mustEncode([]Entry{}))
	// A truncated header.
	f.Add([]byte(magic))
	// Correct magic, wrong version.
	f.Add(append([]byte(magic), 0xff))
	// A count field claiming more entries than any plausible body holds.
	hugeCount := append([]byte(magic), version)
	var count [countLen]byte
	binary.BigEndian.PutUint64(count[:], math.MaxUint64)
	hugeCount = append(hugeCount, count[:]...)
	var crc [crcLen]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(hugeCount))
	f.Add(append(hugeCount, crc[:]...))
	// A valid header whose CRC is wrong.
	f.Add(append([]byte(magic+string(rune(version))), bytes.Repeat([]byte{0xff}, headerLen-1+crcLen)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz.rdb")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write fuzz input: %v", err)
		}

		entries, err := Read(path)

		if err != nil {
			// Every failure must be one of the two documented shapes. An
			// error that was ErrNotExist for a file that exists would make a
			// corrupt snapshot look like a first run — the exact confusion the
			// contract exists to prevent.
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Read returned an undocumented error: %v", err)
			}
			if len(entries) != 0 {
				t.Fatalf("Read returned %d entries alongside an error", len(entries))
			}
			return
		}

		// Success means the bytes really were a well-formed frame. Round-
		// tripping them must reproduce the same entry set, which catches a
		// decoder that accepted a file it could not faithfully describe.
		reencoded, encodeErr := encode(entries)
		if encodeErr != nil {
			t.Fatalf("a decoded file failed to re-encode: %v", encodeErr)
		}
		again, readErr := decode(reencoded)
		if readErr != nil {
			t.Fatalf("re-decoding a re-encoded file failed: %v", readErr)
		}
		if len(again) != len(entries) {
			t.Fatalf("round trip changed the entry count: %d then %d", len(entries), len(again))
		}
		for i := range entries {
			if !bytes.Equal(again[i].Key, entries[i].Key) || !bytes.Equal(again[i].Value, entries[i].Value) {
				t.Fatalf("round trip changed entry %d", i)
			}
			if !again[i].ExpiresAt.Equal(entries[i].ExpiresAt) {
				t.Fatalf("round trip changed entry %d's expiry", i)
			}
		}
	})
}
