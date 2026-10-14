package aof

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// FuzzReplay asserts the property that matters about reading a log nobody
// controls: Replay either succeeds, reports ErrCorrupt, or reports a missing
// file. It must never panic, and it must never invent a third kind of failure
// that a caller has no branch for.
//
// The seeds are the shapes a real log takes plus the ones a hostile file
// would: empty, one whole command, a command whose tail was cut off mid-write,
// arbitrary bytes, and a valid command followed by garbage. The last two are
// what drive the fuzzer into the truncate-or-refuse decision, which is the
// only branch in Replay that rewrites the file.
func FuzzReplay(f *testing.F) {
	valid := encodeCmd("SET", "foo", "bar")

	f.Add([]byte{})
	f.Add(valid)
	f.Add(valid[:len(valid)-3])
	f.Add([]byte{0x00, 0x01, 0x02, 0x03, 0xff, 0xfe})
	f.Add(append(append([]byte{}, valid...), 0xff, 0xff))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "f")

		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}

		cmds, err := Replay(path)
		// The only acceptable outcomes: success, ErrCorrupt, or an
		// os.ErrNotExist wrap (impossible here but the contract allows it).
		// Panics fail the test.
		if err != nil && !errors.Is(err, ErrCorrupt) && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected error: %v", err)
		}
		_ = cmds
	})
}
