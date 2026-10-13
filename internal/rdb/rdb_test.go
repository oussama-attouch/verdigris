package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// writeTemp writes a file into the test's own temp directory and returns its
// path. Every test goes through this so that no test can accidentally write to
// the working directory — the same discipline the server needs, where the
// default is no persistence at all.
func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// encodeForTest builds a valid file image for the given entries, so a test can
// corrupt a specific byte of a known-good file rather than hand-assembling a
// whole frame.
func encodeForTest(t *testing.T, entries []Entry) []byte {
	t.Helper()

	data, err := encode(entries)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return data
}

// --- Round trips ------------------------------------------------------------

func TestRoundTripEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.rdb")

	if err := Write(path, nil); err != nil {
		t.Fatalf("Write(nil): %v", err)
	}

	entries, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Read returned %d entries, want 0", len(entries))
	}
}

func TestRoundTripSingleEntryNoExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "one.rdb")

	want := []Entry{{Key: []byte("foo"), Value: []byte("bar")}}
	if err := Write(path, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Read returned %d entries, want 1", len(got))
	}
	if !bytes.Equal(got[0].Key, want[0].Key) || !bytes.Equal(got[0].Value, want[0].Value) {
		t.Fatalf("round trip changed the entry: got %q=%q", got[0].Key, got[0].Value)
	}
	if !got[0].ExpiresAt.IsZero() {
		t.Fatalf("ExpiresAt = %v, want the zero time", got[0].ExpiresAt)
	}
}

func TestRoundTripSingleEntryWithExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "expiring.rdb")

	expiry := time.Date(2027, 3, 4, 5, 6, 7, 0, time.UTC)
	want := []Entry{{Key: []byte("k"), Value: []byte("v"), ExpiresAt: expiry}}

	if err := Write(path, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !got[0].ExpiresAt.Equal(expiry) {
		t.Fatalf("ExpiresAt = %v, want %v", got[0].ExpiresAt, expiry)
	}
}

func TestRoundTripManyEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "many.rdb")

	const count = 100
	want := make([]Entry, 0, count)
	for i := 0; i < count; i++ {
		e := Entry{
			Key:   []byte(fmt.Sprintf("key-%03d", i)),
			Value: []byte(fmt.Sprintf("value-%03d-%s", i, strings.Repeat("x", i%17))),
		}
		// Give a third of them an expiry, so the file exercises both entry
		// shapes in one round trip.
		if i%3 == 0 {
			e.ExpiresAt = time.Date(2027, 1, 1, 0, 0, 0, i, time.UTC)
		}
		want = append(want, e)
	}

	if err := Write(path, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != count {
		t.Fatalf("Read returned %d entries, want %d", len(got), count)
	}

	// Order is unspecified, so compare as a set keyed by key.
	byKey := make(map[string]Entry, len(got))
	for _, e := range got {
		byKey[string(e.Key)] = e
	}
	for _, w := range want {
		g, ok := byKey[string(w.Key)]
		if !ok {
			t.Fatalf("entry %q is missing", w.Key)
		}
		if !bytes.Equal(g.Value, w.Value) {
			t.Fatalf("entry %q value = %q, want %q", w.Key, g.Value, w.Value)
		}
		if !g.ExpiresAt.Equal(w.ExpiresAt) {
			t.Fatalf("entry %q expiry = %v, want %v", w.Key, g.ExpiresAt, w.ExpiresAt)
		}
	}
}

// TestRoundTripBinarySafe covers the bytes that a text format would corrupt:
// NUL, invalid UTF-8, CRLF, and — deliberately — a key that is itself a valid
// RDB header. A serializer that interpreted any of them would fail here.
func TestRoundTripBinarySafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binary.rdb")

	want := []Entry{
		{Key: []byte{0x00}, Value: []byte{0x00}},
		{Key: []byte{0xff}, Value: []byte{0xff, 0xfe, 0xfd}},
		{Key: []byte("\r\n"), Value: []byte("line1\r\nline2\r\n")},
		{Key: []byte(""), Value: []byte("empty key")},
		{Key: []byte("empty value"), Value: []byte{}},
		{Key: []byte("nil value"), Value: nil},
		{Key: []byte(magic), Value: []byte(magic + "\x01\x02\x03")},
		{Key: []byte{0x00, 0xff, 0x0d, 0x0a}, Value: []byte{0x0a, 0x0d, 0xff, 0x00}},
	}

	if err := Write(path, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Read returned %d entries, want %d", len(got), len(want))
	}

	byKey := make(map[string]Entry, len(got))
	for _, e := range got {
		byKey[string(e.Key)] = e
	}
	for _, w := range want {
		g, ok := byKey[string(w.Key)]
		if !ok {
			t.Fatalf("entry % x is missing", w.Key)
		}
		if !bytes.Equal(g.Value, w.Value) {
			t.Fatalf("entry % x value = % x, want % x", w.Key, g.Value, w.Value)
		}
		// Nil and empty must stay distinguishable through the file, because
		// the RESP layer renders them as different frames.
		if (g.Value == nil) != (w.Value == nil) {
			t.Fatalf("entry % x value nil-ness changed: got nil=%v, want nil=%v", w.Key, g.Value == nil, w.Value == nil)
		}
	}
}

// TestRoundTripExpiryPrecision checks that the expiry is stored as exact
// nanoseconds rather than being rounded to seconds somewhere in the encoding.
func TestRoundTripExpiryPrecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "precision.rdb")

	// A time with non-zero nanoseconds and a non-UTC zone, to catch a format
	// that stored a wall-clock rendering rather than an instant.
	zone := time.FixedZone("test", 5*3600+30*60)
	expiry := time.Date(2027, 6, 5, 4, 3, 2, 123456789, zone)

	want := []Entry{{Key: []byte("k"), Value: []byte("v"), ExpiresAt: expiry}}
	if err := Write(path, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got[0].ExpiresAt.Nanosecond() != 123456789 {
		t.Fatalf("nanoseconds = %d, want 123456789", got[0].ExpiresAt.Nanosecond())
	}
	if !got[0].ExpiresAt.Equal(expiry) {
		t.Fatalf("expiry = %v, want the same instant as %v", got[0].ExpiresAt, expiry)
	}
}

// --- Read failures ----------------------------------------------------------

func TestReadMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.rdb")

	_, err := Read(path)
	if err == nil {
		t.Fatal("Read of a missing file returned no error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v, want it to satisfy errors.Is(err, os.ErrNotExist)", err)
	}
	// A missing file is not corruption, and a caller distinguishing the two
	// must not be told otherwise.
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a missing file reported ErrCorrupt: %v", err)
	}
}

func TestReadBadMagic(t *testing.T) {
	entries := []Entry{{Key: []byte("k"), Value: []byte("v")}}
	data := encodeForTest(t, entries)

	// Overwrite the first byte, then fix the CRC so that corruption of the
	// magic is what fails rather than the checksum. Without this the test
	// would pass for the wrong reason.
	data[0] = 'X'
	fixCRC(data)

	path := writeTemp(t, "badmagic.rdb", data)

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), "magic") {
		t.Fatalf("error %q does not mention the magic", err)
	}
}

func TestReadBadVersion(t *testing.T) {
	entries := []Entry{{Key: []byte("k"), Value: []byte("v")}}
	data := encodeForTest(t, entries)

	data[magicLen] = version + 1
	fixCRC(data)

	path := writeTemp(t, "badversion.rdb", data)

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), "version") {
		t.Fatalf("error %q does not mention the version", err)
	}
}

func TestReadBadCRC(t *testing.T) {
	entries := []Entry{{Key: []byte("k"), Value: []byte("v")}}
	data := encodeForTest(t, entries)

	// Flip a byte in the middle of the body without touching the CRC.
	mid := headerLen + (len(data)-headerLen-trailerLen)/2
	data[mid] ^= 0xff

	path := writeTemp(t, "badcrc.rdb", data)

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("error %q does not mention the checksum", err)
	}
}

func TestReadTruncated(t *testing.T) {
	entries := []Entry{{Key: []byte("k"), Value: []byte("v")}}
	data := encodeForTest(t, entries)

	path := writeTemp(t, "truncated.rdb", data[:len(data)-trailerLen])

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
}

// TestReadDeclaredKeyLenTooLarge is the gate for the length-validation class
// of bug.
//
// A twenty-byte file claims a four-gigabyte key. The reader must reject it
// against the bytes actually remaining rather than attempting to allocate or
// slice what the declaration asks for. The failure mode this guards against is
// the one milestone 2's fuzzer found in the RESP decoder, where an unchecked
// length wrapped a comparison and panicked a connection goroutine.
func TestReadDeclaredKeyLenTooLarge(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.WriteByte(version)

	var count [countLen]byte
	binary.BigEndian.PutUint64(count[:], 1)
	buf.Write(count[:])

	var expiry [expiresAtLen]byte
	buf.Write(expiry[:])

	var keyLen [lengthLen]byte
	binary.BigEndian.PutUint32(keyLen[:], math.MaxUint32)
	buf.Write(keyLen[:])

	// Eight bytes of body where four billion are claimed.
	buf.Write([]byte("short!!"))

	data := buf.Bytes()

	var crc [crcLen]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(data))
	data = append(data, crc[:]...)

	path := writeTemp(t, "hugekeylen.rdb", data)

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), "key length") {
		t.Fatalf("error %q does not mention the key length", err)
	}
}

// TestReadDeclaredValueLenTooLarge is the same check for the value field,
// which is reached only after a valid key has been read.
func TestReadDeclaredValueLenTooLarge(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.WriteByte(version)

	var count [countLen]byte
	binary.BigEndian.PutUint64(count[:], 1)
	buf.Write(count[:])

	var expiry [expiresAtLen]byte
	buf.Write(expiry[:])

	var keyLen [lengthLen]byte
	binary.BigEndian.PutUint32(keyLen[:], 1)
	buf.Write(keyLen[:])
	buf.WriteByte('k')

	var valLen [lengthLen]byte
	binary.BigEndian.PutUint32(valLen[:], math.MaxUint32)
	buf.Write(valLen[:])

	data := buf.Bytes()

	var crc [crcLen]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(data))
	data = append(data, crc[:]...)

	path := writeTemp(t, "hugevallen.rdb", data)

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), "value length") {
		t.Fatalf("error %q does not mention the value length", err)
	}
}

// TestReadDeclaredCountTooLarge covers the count field, which is checked
// before any entry is parsed so that a four-byte edit cannot request a huge
// preallocation.
func TestReadDeclaredCountTooLarge(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.WriteByte(version)

	var count [countLen]byte
	binary.BigEndian.PutUint64(count[:], math.MaxUint64)
	buf.Write(count[:])

	data := buf.Bytes()

	var crc [crcLen]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(data))
	data = append(data, crc[:]...)

	path := writeTemp(t, "hugecount.rdb", data)

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), "declares") {
		t.Fatalf("error %q does not mention the declared count", err)
	}
}

// TestReadTooShort covers every length below the minimum valid file, which is
// the boundary a truncated or empty file lands on.
func TestReadTooShort(t *testing.T) {
	for _, n := range []int{0, 1, magicLen - 1, magicLen, headerLen - 1, headerLen, headerLen + trailerLen - 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			path := writeTemp(t, "short.rdb", bytes.Repeat([]byte{0}, n))

			_, err := Read(path)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Read of %d bytes returned %v, want ErrCorrupt", n, err)
			}
		})
	}
}

// TestReadCountDisagreesWithBody covers the case a CRC cannot catch: a file
// whose checksum is valid but whose count is smaller than its contents, so
// there are leftover bytes after the last entry.
func TestReadCountDisagreesWithBody(t *testing.T) {
	entries := []Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}
	data := encodeForTest(t, entries)

	// Claim one entry where the body holds two.
	binary.BigEndian.PutUint64(data[magicLen+versionLen:headerLen], 1)
	fixCRC(data)

	path := writeTemp(t, "mismatch.rdb", data)

	_, err := Read(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), "left after") {
		t.Fatalf("error %q does not mention leftover bytes", err)
	}
}

// --- Atomicity --------------------------------------------------------------

func TestWriteReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replace.rdb")

	first := []Entry{{Key: []byte("first"), Value: []byte("1")}}
	if err := Write(path, first); err != nil {
		t.Fatalf("first Write: %v", err)
	}

	second := []Entry{{Key: []byte("second"), Value: []byte("2")}}
	if err := Write(path, second); err != nil {
		t.Fatalf("second Write: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 || string(got[0].Key) != "second" {
		t.Fatalf("Read returned %#v, want only the second entry", got)
	}
}

// TestNoTempFileLeftBehindOnSuccess checks the temp file is renamed away
// rather than copied, so a successful save leaves exactly one file.
func TestNoTempFileLeftBehindOnSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.rdb")

	if err := Write(path, []Entry{{Key: []byte("k"), Value: []byte("v")}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	names := dirNames(t, dir)
	if len(names) != 1 || names[0] != "snapshot.rdb" {
		t.Fatalf("directory holds %v, want exactly [snapshot.rdb]", names)
	}
}

// TestNoTempFileLeftBehindOnFailure is the gate for the atomic-write
// requirement.
//
// The write is made to fail by pointing it at a directory that does not exist,
// which fails at os.Create before any frame is written. The assertions are the
// two that matter: the previous snapshot is byte-for-byte unchanged, and no
// .tmp file was left in the directory.
func TestNoTempFileLeftBehindOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.rdb")

	original := []Entry{{Key: []byte("original"), Value: []byte("value")}}
	if err := Write(path, original); err != nil {
		t.Fatalf("initial Write: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	// A path whose directory does not exist. os.Create fails, so this exercises
	// the earliest failure branch — the one that must leave nothing behind.
	missing := filepath.Join(dir, "no-such-dir", "snapshot.rdb")
	if err := Write(missing, []Entry{{Key: []byte("new"), Value: []byte("data")}}); err == nil {
		t.Fatal("Write to a missing directory reported success")
	}

	// The original file is untouched...
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back after failure: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("a failed write modified the existing snapshot")
	}

	// ...and nothing was left beside it.
	for _, name := range dirNames(t, dir) {
		if strings.HasSuffix(name, ".tmp") {
			t.Fatalf("a failed write left a temp file behind: %s", name)
		}
	}

	// The snapshot is still readable, which is the property the atomicity
	// exists to provide.
	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read after a failed write: %v", err)
	}
	if len(got) != 1 || string(got[0].Key) != "original" {
		t.Fatalf("Read returned %#v, want the original entry", got)
	}
}

// TestWriteFailsOnReadOnlyDirectory is a second failure injection, this time
// after the temp file has been created, so it exercises the cleanup path
// rather than the create path.
//
// It is skipped on Windows, where the read-only attribute on a directory does
// not prevent creating files inside it, and when running as root, where
// directory permissions are not enforced at all. On those platforms the test
// would not be measuring what it claims to.
func TestWriteFailsOnReadOnlyDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions do not block file creation on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root, which ignores directory permissions")
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	path := filepath.Join(dir, "snapshot.rdb")
	if err := Write(path, []Entry{{Key: []byte("k"), Value: []byte("v")}}); err == nil {
		t.Fatal("Write into a read-only directory reported success")
	}

	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file state after failure: %v, want it to not exist", err)
	}
}

// --- helpers ----------------------------------------------------------------

// fixCRC recomputes and rewrites the trailing checksum.
//
// Several tests corrupt a byte deliberately and then need the CRC to still
// match, so that the specific validation under test is what fails. Without
// this they would all pass by failing the checksum first, proving nothing
// about the magic, the version, or the count.
func fixCRC(data []byte) {
	sum := crc32.ChecksumIEEE(data[:len(data)-crcLen])
	binary.BigEndian.PutUint32(data[len(data)-crcLen:], sum)
}

// dirNames returns the sorted names of the entries in dir.
func dirNames(t *testing.T, dir string) []string {
	t.Helper()

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	names := make([]string, 0, len(dirEntries))
	for _, e := range dirEntries {
		names = append(names, e.Name())
	}
	return names
}
