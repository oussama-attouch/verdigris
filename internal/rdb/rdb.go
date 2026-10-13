// Package rdb reads and writes Verdigris snapshot files.
package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"time"
)

// magic is the file's first nine bytes. It is long enough that a snapshot
// cannot be confused with a text file or another binary format by accident,
// and short enough to read at a glance in a hex dump.
const magic = "VERDIGRIS"

// version is the format revision written by this package. A file whose version
// is not this value is refused rather than parsed optimistically.
const version = 1

// File layout sizes, in bytes.
const (
	magicLen     = len(magic) // 9
	versionLen   = 1
	countLen     = 8
	expiresAtLen = 8
	lengthLen    = 4
	crcLen       = 4

	// nilFlagLen is the byte that records whether a value was nil rather than
	// an empty slice.
	nilFlagLen = 1

	// headerLen is everything before the first entry.
	headerLen = magicLen + versionLen + countLen

	// trailerLen is the CRC that follows the last entry.
	trailerLen = crcLen

	// entryFixedLen is the per-entry overhead: expiry, the nil flag, and two
	// length fields.
	entryFixedLen = expiresAtLen + nilFlagLen + 2*lengthLen
)

// ErrCorrupt is returned when the file's bytes do not match the format.
//
// It is a sentinel rather than a text error because the caller's response is
// the same for every cause — refuse to start — and because the distinction
// that matters to a caller is corrupt versus missing, not which byte was
// wrong. The detail is wrapped inside so a log line can still say why.
var ErrCorrupt = errors.New("rdb: corrupt file")

// Entry is one key-value pair with optional expiry. Exported because the
// store constructs snapshots and rdb serializes them.
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt time.Time // zero means no expiry
}

// Write serializes entries to path atomically. The write is:
//
//  1. create <path>.tmp
//  2. write the full frame
//  3. fsync the temp file
//  4. os.Rename over path
//
// A crash before the rename leaves the previous file untouched.
func Write(path string, entries []Entry) error {
	// A nil slice and an empty slice both mean "no entries" and both produce
	// a file with a zero count, so a store that has never been written to
	// still round-trips as an empty store rather than as a missing file.
	if entries == nil {
		entries = []Entry{}
	}

	frame, err := encode(entries)
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"

	// Create, not CreateTemp: the temp name is part of the contract, since a
	// reader looking at the directory after a failed save should be able to
	// see that no debris was left. os.CreateTemp would append a random suffix
	// that nothing cleans up.
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("rdb: create temp file: %w", err)
	}

	// From here on, any failure path must remove the temp file. The deferred
	// cleanup is keyed on a flag rather than being unconditional, because the
	// success path renames the file out from under this name and removing it
	// afterwards would be a no-op at best and a race at worst.
	committed := false
	defer func() {
		if !committed {
			// Closing twice is harmless and the error is not actionable, so
			// both are ignored here; the caller already has the real failure.
			_ = f.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := f.Write(frame); err != nil {
		return fmt.Errorf("rdb: write temp file: %w", err)
	}

	// Sync before rename is the whole point of the sequence. Without it the
	// rename could land while the data behind it is still only in the page
	// cache, so a power loss would leave a correctly-named file full of
	// zeroes — precisely the outcome the temp-and-rename dance exists to
	// prevent. On Windows this maps to FlushFileBuffers.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("rdb: sync temp file: %w", err)
	}

	// Close before rename. On Windows, renaming a file that is still open by
	// this process fails, and on every platform a rename should not be racing
	// a pending buffered write.
	if err := f.Close(); err != nil {
		return fmt.Errorf("rdb: close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rdb: replace snapshot: %w", err)
	}

	committed = true
	return nil
}

// encode builds the complete file image in memory.
//
// The whole frame is assembled before anything touches the disk so that a
// serialization failure — an entry too large for its length field — is
// reported without having created a temp file at all.
func encode(entries []Entry) ([]byte, error) {
	// The CRC covers every preceding byte, so it is computed over the frame as
	// it is built rather than by re-reading the buffer afterwards. One pass,
	// no second traversal, and no chance of the two drifting apart.
	h := crc32.NewIEEE()

	frame := make([]byte, 0, headerLen+len(entries)*entryFixedLen+trailerLen)
	frame = append(frame, magic...)
	frame = append(frame, version)

	var count [countLen]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(entries)))
	frame = append(frame, count[:]...)

	for i, e := range entries {
		// The length fields are uint32, so a key or value beyond 4 GiB cannot
		// be represented. Refusing is better than truncating the length and
		// writing a file that reads back as something else. The check is
		// against MaxUint32 rather than MaxInt32 because the field really is
		// unsigned; a 3 GiB value would be perfectly legal here.
		if uint64(len(e.Key)) > math.MaxUint32 {
			return nil, fmt.Errorf("rdb: entry %d key is %d bytes, over the %d-byte limit", i, len(e.Key), uint64(math.MaxUint32))
		}
		if uint64(len(e.Value)) > math.MaxUint32 {
			return nil, fmt.Errorf("rdb: entry %d value is %d bytes, over the %d-byte limit", i, len(e.Value), uint64(math.MaxUint32))
		}

		var expiresAt [expiresAtLen]byte
		if !e.ExpiresAt.IsZero() {
			binary.BigEndian.PutUint64(expiresAt[:], uint64(e.ExpiresAt.UnixNano()))
		}
		frame = append(frame, expiresAt[:]...)

		var keyLen [lengthLen]byte
		binary.BigEndian.PutUint32(keyLen[:], uint32(len(e.Key)))
		frame = append(frame, keyLen[:]...)
		frame = append(frame, e.Key...)

		// nilFlag records the difference between a nil value and an empty
		// one. Both write a zero length, so without this byte the two are
		// indistinguishable on disk — and they are not interchangeable: the
		// RESP layer renders them as `$-1` (null) and `$0` (empty), which
		// clients branch on. The flag is what keeps `GET k` answering `(nil)`
		// rather than `""` after a restart.
		var nilFlag byte
		if e.Value == nil {
			nilFlag = 1
		}
		frame = append(frame, nilFlag)

		var valLen [lengthLen]byte
		binary.BigEndian.PutUint32(valLen[:], uint32(len(e.Value)))
		frame = append(frame, valLen[:]...)
		frame = append(frame, e.Value...)
	}

	// Hash everything written so far, then append the digest. Doing it in this
	// order means the CRC covers the header and every entry and excludes only
	// itself, which is what makes a corrupted count or magic detectable.
	_, _ = h.Write(frame)

	var crc [crcLen]byte
	binary.BigEndian.PutUint32(crc[:], h.Sum32())
	frame = append(frame, crc[:]...)

	return frame, nil
}

// Read loads entries from path. Returns ErrCorrupt if the magic, version,
// CRC, or per-entry lengths are invalid. Returns os.ErrNotExist if the
// file does not exist — that is not corruption, it is a first run.
func Read(path string) ([]Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		// Wrapped, not swallowed: the caller distinguishes a first run from a
		// broken file with errors.Is(err, os.ErrNotExist), and that works
		// through the wrap. Everything else — a permission error, a directory
		// where a file was expected — is also a real failure and must not be
		// mistaken for "no snapshot yet".
		return nil, fmt.Errorf("rdb: read %s: %w", path, err)
	}
	return decode(data)
}

// decode parses a complete file image.
//
// Every length read from the file is checked against the bytes that actually
// remain before it is used to slice or allocate. A crafted file declaring a
// four-gigabyte key must fail as corrupt without the process attempting to
// honour the declaration — the same class of bug the RESP decoder's fuzzer
// found in milestone 2, where an unchecked length wrapped a comparison and
// panicked.
func decode(data []byte) ([]Entry, error) {
	// The smallest possible valid file is a header and a CRC.
	if len(data) < headerLen+trailerLen {
		return nil, fmt.Errorf("%w: %d bytes is shorter than the %d-byte minimum", ErrCorrupt, len(data), headerLen+trailerLen)
	}

	if string(data[:magicLen]) != magic {
		return nil, fmt.Errorf("%w: bad magic %q", ErrCorrupt, data[:magicLen])
	}
	if data[magicLen] != version {
		return nil, fmt.Errorf("%w: version %d, this build writes and reads version %d", ErrCorrupt, data[magicLen], version)
	}

	// The CRC is checked before any entry is parsed, so a file that has been
	// altered anywhere — including in its count field — is rejected before the
	// count can be used to size anything.
	stored := binary.BigEndian.Uint32(data[len(data)-crcLen:])
	sum := crc32.ChecksumIEEE(data[:len(data)-crcLen])
	if stored != sum {
		return nil, fmt.Errorf("%w: checksum %08x does not match %08x", ErrCorrupt, stored, sum)
	}

	count := binary.BigEndian.Uint64(data[magicLen+versionLen : headerLen])

	// Every entry occupies at least entryFixedLen bytes plus its key and
	// value, so a count larger than the body can accommodate is impossible.
	// This is the count-field analogue of the per-entry bound below, and it is
	// what stops a four-byte edit from requesting a huge preallocation.
	body := uint64(len(data) - headerLen - trailerLen)
	if count > body/entryFixedLen {
		return nil, fmt.Errorf("%w: declares %d entries but only %d bytes of body", ErrCorrupt, count, body)
	}

	entries := make([]Entry, 0, count)
	pos := headerLen

	for i := uint64(0); i < count; i++ {
		// remaining is what is left between here and the CRC.
		remaining := len(data) - trailerLen - pos

		if remaining < entryFixedLen {
			return nil, fmt.Errorf("%w: entry %d: %d bytes left, need %d for its header", ErrCorrupt, i, remaining, entryFixedLen)
		}

		rawExpires := binary.BigEndian.Uint64(data[pos : pos+expiresAtLen])
		pos += expiresAtLen

		keyLen := binary.BigEndian.Uint32(data[pos : pos+lengthLen])
		pos += lengthLen

		// The bound is checked against the bytes that remain before the slice
		// is taken. Doing it the other way round — slicing and then checking —
		// is how the milestone 2 overflow happened.
		if uint64(keyLen) > uint64(len(data)-trailerLen-pos) {
			return nil, fmt.Errorf("%w: entry %d: key length %d exceeds the %d bytes remaining", ErrCorrupt, i, keyLen, len(data)-trailerLen-pos)
		}
		key := data[pos : pos+int(keyLen)]
		pos += int(keyLen)

		remaining = len(data) - trailerLen - pos
		if remaining < nilFlagLen+lengthLen {
			return nil, fmt.Errorf("%w: entry %d: %d bytes left, need %d for the nil flag and value length", ErrCorrupt, i, remaining, nilFlagLen+lengthLen)
		}
		valueWasNil := data[pos] == 1
		pos += nilFlagLen

		valLen := binary.BigEndian.Uint32(data[pos : pos+lengthLen])
		pos += lengthLen

		if uint64(valLen) > uint64(len(data)-trailerLen-pos) {
			return nil, fmt.Errorf("%w: entry %d: value length %d exceeds the %d bytes remaining", ErrCorrupt, i, valLen, len(data)-trailerLen-pos)
		}
		value := data[pos : pos+int(valLen)]
		pos += int(valLen)

		// Keys and values are copied out of data rather than aliasing it. The
		// caller receives entries it owns and may retain, and data is a
		// single buffer that would otherwise pin the whole file in memory for
		// as long as any one entry survived.
		e := Entry{
			Key: append([]byte(nil), key...),
		}
		switch {
		case valueWasNil:
			// Left nil, not converted to an empty slice. The two are
			// different values to every layer above this one.
			e.Value = nil
		case len(value) == 0:
			// An empty but non-nil value. make rather than append, because
			// append(nil) with an empty source returns nil and would collapse
			// this case back into the one above.
			e.Value = make([]byte, 0)
		default:
			e.Value = append([]byte(nil), value...)
		}
		if rawExpires != 0 {
			// Stored as uint64 in the file so that the zero value can mean
			// "no expiry"; the cast back to int64 recovers the nanoseconds,
			// including pre-1970 values if they ever appear.
			e.ExpiresAt = time.Unix(0, int64(rawExpires)).UTC()
		}

		entries = append(entries, e)
	}

	// The body must have been consumed exactly. Leftover bytes mean the count
	// disagreed with the contents, which is corruption even though the CRC
	// passed — a CRC over the wrong count is still a valid CRC.
	if pos != len(data)-trailerLen {
		return nil, fmt.Errorf("%w: %d bytes of body left after %d entries", ErrCorrupt, len(data)-trailerLen-pos, count)
	}

	return entries, nil
}

// encode builds the complete file image in memory.
