package aof

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
)

// bulks builds an argument list from strings, which is what a command is.
func bulks(args ...string) []protocol.Value {
	out := make([]protocol.Value, len(args))
	for i, a := range args {
		out[i] = protocol.BulkString(a)
	}
	return out
}

// encodeCmd returns the bytes Append would write for args.
func encodeCmd(args ...string) []byte {
	return protocol.Encode(protocol.Array(bulks(args...)))
}

// readAll replays path and fails the test on any error.
func readAll(t *testing.T, path string) [][]protocol.Value {
	t.Helper()
	cmds, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay(%s): %v", path, err)
	}
	return cmds
}

// assertArgs compares a replayed command against the strings it should hold.
func assertArgs(t *testing.T, got []protocol.Value, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("command has %d arguments, want %d: %#v", len(got), len(want), got)
	}
	for i, w := range want {
		b, ok := got[i].(protocol.BulkString)
		if !ok {
			t.Fatalf("argument %d is %T, want protocol.BulkString", i, got[i])
		}
		if string(b) != w {
			t.Fatalf("argument %d = %q, want %q", i, b, w)
		}
	}
}

// decodeAll decodes a byte slice as a standalone log image and returns the
// number of complete frames in it, requiring that they consume it exactly.
// It is how the truncation tests prove the file on disk holds whole commands
// and nothing else.
func decodeAll(t *testing.T, data []byte) int {
	t.Helper()
	n := 0
	for len(data) > 0 {
		v, used, err := protocol.Decode(data)
		if err != nil {
			t.Fatalf("frame %d: %v (%d bytes left)", n, err, len(data))
		}
		if _, ok := v.(protocol.Array); !ok {
			t.Fatalf("frame %d is %T, want an array", n, v)
		}
		n++
		data = data[used:]
	}
	return n
}

func TestAppendAndReplayEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.aof")

	a, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cmds := readAll(t, path)
	if len(cmds) != 0 {
		t.Fatalf("replayed %d commands from an empty log, want 0: %#v", len(cmds), cmds)
	}
}

func TestAppendAndReplaySingle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single.aof")

	a, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := a.Append(bulks("SET", "foo", "bar")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cmds := readAll(t, path)
	if len(cmds) != 1 {
		t.Fatalf("replayed %d commands, want 1", len(cmds))
	}
	assertArgs(t, cmds[0], "SET", "foo", "bar")
}

func TestAppendAndReplayMany(t *testing.T) {
	path := filepath.Join(t.TempDir(), "many.aof")

	// Mixed arities and command shapes, so the test would catch a decoder
	// that lost its place on any one of them rather than only on a
	// uniform run of three-argument SETs.
	want := [][]string{
		{"SET", "k0", "v0"},
		{"SET", "k1", "v1", "EX", "100"},
		{"DEL", "k0"},
		{"EXPIRE", "k1", "50"},
		{"PERSIST", "k1"},
		{"PING"},
		{"SET", "k2"},
	}

	a, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 100; i++ {
		cmd := want[i%len(want)]
		if err := a.Append(bulks(cmd...)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cmds := readAll(t, path)
	if len(cmds) != 100 {
		t.Fatalf("replayed %d commands, want 100", len(cmds))
	}
	for i, got := range cmds {
		assertArgs(t, got, want[i%len(want)]...)
	}
}

func TestAppendBinarySafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binary.aof")

	// Every byte that a text format would have to escape, plus a byte that is
	// not valid UTF-8 at all, plus the two bytes that would terminate a RESP
	// line if they were ever emitted raw.
	key := []byte("k\x00ey\xff\r\n")
	value := []byte("\x00\xff\r\n\x01binary\xfe")

	a, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := a.Append([]protocol.Value{
		protocol.BulkString("SET"),
		protocol.BulkString(key),
		protocol.BulkString(value),
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cmds := readAll(t, path)
	if len(cmds) != 1 {
		t.Fatalf("replayed %d commands, want 1", len(cmds))
	}
	if len(cmds[0]) != 3 {
		t.Fatalf("command has %d arguments, want 3", len(cmds[0]))
	}

	gotKey, ok := cmds[0][1].(protocol.BulkString)
	if !ok {
		t.Fatalf("key is %T, want protocol.BulkString", cmds[0][1])
	}
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("key = %q, want %q", gotKey, key)
	}

	gotVal, ok := cmds[0][2].(protocol.BulkString)
	if !ok {
		t.Fatalf("value is %T, want protocol.BulkString", cmds[0][2])
	}
	if !bytes.Equal(gotVal, value) {
		t.Fatalf("value = %q, want %q", gotVal, value)
	}
}

func TestReplayMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.aof")

	cmds, err := Replay(path)
	if err == nil {
		t.Fatalf("Replay of a missing file returned %d commands and no error", len(cmds))
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error is %v, want one wrapping os.ErrNotExist", err)
	}
}

func TestReplayTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.aof")

	var buf bytes.Buffer
	for _, cmd := range [][]string{
		{"SET", "a", "1"},
		{"SET", "b", "2"},
		{"SET", "c", "3"},
	} {
		buf.Write(encodeCmd(cmd...))
	}
	complete := buf.Len()

	// Half of a fourth command: a real crash leaves a prefix of a frame, not
	// a cleanly terminated one.
	fourth := encodeCmd("SET", "d", "4")
	buf.Write(fourth[:len(fourth)/2])

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cmds := readAll(t, path)
	if len(cmds) != 3 {
		t.Fatalf("replayed %d commands, want the 3 complete ones", len(cmds))
	}
	for i, want := range [][]string{{"SET", "a", "1"}, {"SET", "b", "2"}, {"SET", "c", "3"}} {
		assertArgs(t, cmds[i], want...)
	}

	// The partial command must be gone from disk, not merely ignored: a log
	// that kept it would append after the debris and grow a second frame
	// boundary that no replayer can cross.
	final, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(final) != complete {
		t.Fatalf("file is %d bytes after replay, want %d (the 3 complete commands)", len(final), complete)
	}
	if n := decodeAll(t, final); n != 3 {
		t.Fatalf("file decodes to %d commands, want exactly 3", n)
	}
}

func TestReplayTruncatedTailWithNoCompleteCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alltail.aof")

	// A log whose only frame was cut mid-write, so there is no valid prefix
	// to return and nothing to keep.
	cmd := encodeCmd("SET", "only", "value")
	if err := os.WriteFile(path, cmd[:len(cmd)/2], 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cmds := readAll(t, path)
	if len(cmds) != 0 {
		t.Fatalf("replayed %d commands from a partial first frame, want 0", len(cmds))
	}

	final, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(final) != 0 {
		t.Fatalf("file is %d bytes after replay, want empty: %q", len(final), final)
	}
}

func TestReplayCorruptMidFile(t *testing.T) {
	// 0xff is not a RESP marker, so a decoder meeting it reports a protocol
	// violation rather than an incomplete frame. The two cases below are the
	// two shapes that error can take, and only one of them is recoverable.
	garbage := []byte{0xff, 0xfe, 0xfd}

	t.Run("garbage before the tail is fatal", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "corrupt.aof")

		var buf bytes.Buffer
		buf.Write(encodeCmd("SET", "a", "1"))
		buf.Write(encodeCmd("SET", "b", "2"))
		buf.Write(garbage)
		// Valid bytes after the garbage. This is what makes it mid-file
		// damage: the file did not stop here, so it cannot be a torn tail.
		buf.Write(encodeCmd("SET", "c", "3"))

		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		cmds, err := Replay(path)
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Replay returned %d commands and error %v, want one wrapping ErrCorrupt", len(cmds), err)
		}
		if cmds != nil {
			t.Fatalf("Replay returned %d commands alongside ErrCorrupt, want none", len(cmds))
		}

		// Nothing was rewritten. A corrupt file left as it was found is what
		// lets an operator inspect it.
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if !bytes.Equal(after, buf.Bytes()) {
			t.Fatal("Replay modified a corrupt file; it must leave it untouched")
		}
	})

	t.Run("garbage at the tail is still fatal", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "corrupttail.aof")

		var buf bytes.Buffer
		buf.Write(encodeCmd("SET", "a", "1"))
		buf.Write(encodeCmd("SET", "b", "2"))
		buf.Write(garbage)

		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		// Bytes that violate the grammar are corruption even at the end of
		// the file. Only a frame cut short — which the decoder reports as an
		// incomplete frame, never as a syntax error — counts as a torn tail.
		// Treating trailing garbage as recoverable would discard a region
		// nobody could read and hand back a shorter log with no warning.
		//
		// This is stricter than "corruption anywhere before the tail" and it
		// is the rule Redis follows: a crash can shorten a frame, but it
		// cannot produce bytes that are present and wrong.
		cmds, err := Replay(path)
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Replay returned %d commands and error %v, want one wrapping ErrCorrupt", len(cmds), err)
		}

		after, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("ReadFile: %v", rerr)
		}
		if !bytes.Equal(after, buf.Bytes()) {
			t.Fatal("Replay modified a corrupt file; it must leave it untouched")
		}
	})
}

func TestParseFsyncPolicy(t *testing.T) {
	valid := []struct {
		in   string
		want FsyncPolicy
	}{
		{"always", FsyncAlways},
		{"everysec", FsyncEverySec},
		{"no", FsyncNo},
	}
	for _, tc := range valid {
		got, err := ParseFsyncPolicy(tc.in)
		if err != nil {
			t.Fatalf("ParseFsyncPolicy(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseFsyncPolicy(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}

	invalid := []string{"Always", "EVERYSEC", "never", "", "every sec", "0", "NO"}
	for _, in := range invalid {
		got, err := ParseFsyncPolicy(in)
		if err == nil {
			t.Fatalf("ParseFsyncPolicy(%q) = %v and no error, want an error", in, got)
		}
	}
}

func TestFsyncPolicyString(t *testing.T) {
	for _, p := range []FsyncPolicy{FsyncAlways, FsyncEverySec, FsyncNo} {
		s := p.String()

		back, err := ParseFsyncPolicy(s)
		if err != nil {
			t.Fatalf("ParseFsyncPolicy(%q.String() = %q): %v", p, s, err)
		}
		if back != p {
			t.Fatalf("%v round-tripped to %v through %q", p, back, s)
		}
	}

	// Pinned literally as well as round-tripped: a change to both String and
	// Parse together would satisfy the loop above while silently changing the
	// flag's accepted names.
	want := map[FsyncPolicy]string{
		FsyncAlways:   "always",
		FsyncEverySec: "everysec",
		FsyncNo:       "no",
	}
	for p, w := range want {
		if got := p.String(); got != w {
			t.Fatalf("FsyncPolicy(%d).String() = %q, want %q", int(p), got, w)
		}
	}

	// An out-of-range policy has no CLI name. It must render as something
	// diagnosable rather than silently claiming to be a real policy — the
	// zero value here is FsyncAlways, so a bare fallthrough would print
	// "always" for a value that is not one.
	out := FsyncPolicy(99).String()
	if out == "always" || out == "everysec" || out == "no" {
		t.Fatalf("FsyncPolicy(99).String() = %q, which names a real policy", out)
	}
	if out == "" {
		t.Fatal("FsyncPolicy(99).String() is empty; it must identify the value")
	}
}

func TestEverysecSyncsWithinOneSecond(t *testing.T) {
	path := filepath.Join(t.TempDir(), "everysec.aof")

	a, err := Open(path, FsyncEverySec)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := a.Append(bulks("SET", "k", "v")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Long enough for the ticker to have fired at least once. Append writes
	// synchronously, so the bytes are already in the file regardless; what
	// this test proves is that the ticker goroutine ran without deadlocking
	// and that Close terminates it rather than hanging.
	time.Sleep(1500 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return; the everysec ticker is not being stopped")
	}

	cmds := readAll(t, path)
	if len(cmds) != 1 {
		t.Fatalf("replayed %d commands, want 1", len(cmds))
	}
	assertArgs(t, cmds[0], "SET", "k", "v")
}

func TestConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.aof")

	a, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const goroutines = 8
	const perGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				cmd := bulks("SET", fmt.Sprintf("k-%d-%d", g, i), "v")
				if err := a.Append(cmd); err != nil {
					// t.Fatalf from a non-test goroutine is not allowed, so
					// the failure is recorded and the goroutine returns.
					t.Errorf("Append: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cmds := readAll(t, path)
	if len(cmds) != goroutines*perGoroutine {
		t.Fatalf("replayed %d commands, want %d", len(cmds), goroutines*perGoroutine)
	}

	// Every command must be intact and appear exactly once. A lock that let
	// two frames interleave would show up here as a missing key, a duplicate,
	// or a frame that fails to decode at all.
	seen := make(map[string]int, len(cmds))
	for _, cmd := range cmds {
		if len(cmd) != 3 {
			t.Fatalf("command has %d arguments, want 3: %#v", len(cmd), cmd)
		}
		key, ok := cmd[1].(protocol.BulkString)
		if !ok {
			t.Fatalf("key is %T, want protocol.BulkString", cmd[1])
		}
		seen[string(key)]++
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("replayed %d distinct keys, want %d", len(seen), goroutines*perGoroutine)
	}
	for key, n := range seen {
		if n != 1 {
			t.Fatalf("key %q appeared %d times, want once", key, n)
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idempotent.aof")

	a, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := a.Append(bulks("SET", "k", "v")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
}

func TestAppendErrors(t *testing.T) {
	t.Run("after Close", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "afterclose.aof")

		a, err := Open(path, FsyncNo)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := a.Append(bulks("SET", "before", "close")); err != nil {
			t.Fatalf("Append before Close: %v", err)
		}
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		err = a.Append(bulks("SET", "after", "close"))
		if err == nil {
			t.Fatal("Append after Close returned nil, want an error")
		}
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Append after Close returned %v, want one wrapping ErrClosed", err)
		}

		// The rejected command must not be in the log.
		cmds := readAll(t, path)
		if len(cmds) != 1 {
			t.Fatalf("replayed %d commands, want only the one written before Close", len(cmds))
		}
		assertArgs(t, cmds[0], "SET", "before", "close")
	})

	t.Run("empty command", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "emptycmd.aof")

		a, err := Open(path, FsyncNo)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer a.Close()

		// A zero-argument command is a valid frame that names no command, so
		// it is refused rather than logged as an unnameable no-op.
		if err := a.Append(nil); !errors.Is(err, ErrEmptyCommand) {
			t.Fatalf("Append(nil) = %v, want one wrapping ErrEmptyCommand", err)
		}
		if err := a.Append([]protocol.Value{}); !errors.Is(err, ErrEmptyCommand) {
			t.Fatalf("Append(empty) = %v, want one wrapping ErrEmptyCommand", err)
		}
	})

	t.Run("underlying file closed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "underlying.aof")

		a, err := Open(path, FsyncNo)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer a.Close()

		// Close the descriptor behind the AOF's back, which is what a
		// double-close bug elsewhere would look like from here. The write
		// must surface as an error rather than being reported as success.
		if err := a.f.Close(); err != nil {
			t.Fatalf("closing the underlying file: %v", err)
		}

		if err := a.Append(bulks("SET", "k", "v")); err == nil {
			t.Fatal("Append with a closed underlying file returned nil, want an error")
		}
	})
}

func TestSyncAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "syncafterclose.aof")

	a, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := a.Append(bulks("SET", "k", "v")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Sync before Close must succeed; it is the same call Close makes.
	if err := a.Sync(); err != nil {
		t.Fatalf("Sync before Close: %v", err)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After Close there is no descriptor to flush, and saying so is better
	// than returning nil for a flush that did not happen.
	if err := a.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close = %v, want one wrapping ErrClosed", err)
	}
}

func TestOpenError(t *testing.T) {
	// A path inside a directory that does not exist, so the create fails.
	// This is the shape of a mistyped -aof-path on the command line.
	path := filepath.Join(t.TempDir(), "no-such-dir", "x.aof")

	a, err := Open(path, FsyncNo)
	if err == nil {
		a.Close()
		t.Fatal("Open of an unopenable path returned nil error")
	}
}

func TestReplayReadError(t *testing.T) {
	// A directory where a log was expected: ReadFile opens it and then fails
	// to read it, which is neither corruption nor a missing file, so neither
	// sentinel may be reported.
	dir := t.TempDir()

	cmds, err := Replay(dir)
	if err == nil {
		t.Fatalf("Replay of a directory returned %d commands and no error", len(cmds))
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay of a directory reported ErrCorrupt; it is not corruption: %v", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Replay of a directory reported os.ErrNotExist; it exists: %v", err)
	}
}

func TestReplayTruncateError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly.aof")

	// One whole command and a torn one, so Replay finds a tail to cut and
	// tries to rewrite the file.
	full := encodeCmd("SET", "a", "1")
	partial := encodeCmd("SET", "b", "2")
	data := append(append([]byte{}, full...), partial[:len(partial)/2]...)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// A read-only log cannot be rewritten. This is a real situation — the
	// file was chmodded, or the directory is not writable — and the failure
	// must be reported rather than swallowed, because a caller that ignores
	// it will append after the debris on the next Open.
	if err := os.Chmod(path, 0444); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0644) })

	cmds, err := Replay(path)
	if err == nil {
		t.Skip("this platform permits truncating a read-only file; nothing to assert")
	}

	// Whatever went wrong, it is not corruption and not a missing file: the
	// command that was readable is real, and the file is there.
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a failed truncate was reported as corruption: %v", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a failed truncate was reported as a missing file: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("Replay returned %d commands alongside a truncate failure", len(cmds))
	}
}

// The two benchmarks differ in exactly one thing: whether each Append fsyncs.
// Run them together to price the durability of FsyncAlways against FsyncNo:
//
//	go test -bench=BenchmarkAppend -benchtime=200x ./internal/aof/
func BenchmarkAppendNoSync(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench-nosync.aof")

	a, err := Open(path, FsyncNo)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer a.Close()

	cmd := bulks("SET", "bench-key", "bench-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a.Append(cmd); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

func BenchmarkAppendAlways(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench-always.aof")

	a, err := Open(path, FsyncAlways)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer a.Close()

	cmd := bulks("SET", "bench-key", "bench-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a.Append(cmd); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}
