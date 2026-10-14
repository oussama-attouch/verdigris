package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/Oussama-Att/verdigris/internal/aof"
	"github.com/Oussama-Att/verdigris/internal/server"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// version is a placeholder. It will be populated from build metadata in a
// later milestone. For now it exists so the binary has something to print
// and so the build can be verified end to end.
const version = "0.6.0-milestone7"

// shutdownTimeout bounds how long a graceful shutdown may take before
// in-flight connections are cut off.
const shutdownTimeout = 5 * time.Second

func main() {
	port := flag.Int("port", server.DefaultPort, "TCP port to listen on (1-65535)")
	sweepInterval := flag.Duration("sweep-interval", store.DefaultSweepInterval,
		"how often the active expiration sweeper runs (e.g. 100ms, 1s)")
	rdbPath := flag.String("rdb-path", "",
		"path to the RDB snapshot file (empty disables snapshot persistence)")
	aofPath := flag.String("aof-path", "",
		"path to the append-only log (empty disables the log)")
	aofFsync := flag.String("aof-fsync", "everysec",
		"when to fsync the append-only log: always, everysec, or no")
	flag.Parse()

	// A port outside the range is a usage error, not a runtime failure, so it
	// gets exit code 2 — the conventional code for "you invoked this wrong" —
	// rather than the 1 used for things that went wrong after a valid start.
	if *port < 1 || *port > 65535 {
		fmt.Fprintf(os.Stderr, "verdigris: invalid -port %d: must be between 1 and 65535\n", *port)
		flag.Usage()
		os.Exit(2)
	}

	// A non-positive interval is also a usage error. The store would silently
	// substitute its default, but silently ignoring a flag the operator
	// explicitly set is worse than telling them it was wrong.
	if *sweepInterval <= 0 {
		fmt.Fprintf(os.Stderr, "verdigris: invalid -sweep-interval %s: must be positive\n", *sweepInterval)
		flag.Usage()
		os.Exit(2)
	}

	// An unrecognized policy is a usage error: the operator named something
	// the server cannot honour, and defaulting instead would leave them
	// believing a durability guarantee they do not have.
	aofPolicy, err := aof.ParseFsyncPolicy(*aofFsync)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verdigris: invalid -aof-fsync %q: must be always, everysec, or no\n", *aofFsync)
		flag.Usage()
		os.Exit(2)
	}

	// A path whose directory does not exist is a usage error too, and it is
	// worth catching here rather than at the first save for two reasons: the
	// first save might be hours into a run, and on shutdown there is nowhere
	// useful left to report it. The file itself is allowed not to exist —
	// that is a first run.
	if err := checkPersistenceParent("-rdb-path", *rdbPath); err != nil {
		fmt.Fprintf(os.Stderr, "verdigris: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}
	if err := checkPersistenceParent("-aof-path", *aofPath); err != nil {
		fmt.Fprintf(os.Stderr, "verdigris: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}

	// Printed before the listener starts, so an operator looking at a running
	// process can confirm which build it is.
	fmt.Fprintf(os.Stdout, "verdigris %s\n", version)

	// Which mechanism is in charge is decided by server.NewWithAOF, not here,
	// so the rule lives in one place: a non-empty -aof-path makes the log the
	// startup source of truth, and -rdb-path is then only ever written by
	// SAVE. Both may be set; neither means no persistence at all.
	//
	// ":" + a validated integer binds every interface on that port, which is
	// the same thing redis-server does by default. The clock is the real one:
	// expiry in production follows wall-clock time, and injecting a clock is
	// for the tests.
	//
	// Construction loads the log or the snapshot, so a corrupt or unreadable
	// file stops the process before a single connection is accepted. That
	// ordering is the point: a client must never observe a server that is up
	// but has silently lost its data.
	srv, err := server.NewWithAOF(":"+strconv.Itoa(*port), store.RealClock{}, *sweepInterval, *rdbPath, *aofPath, aofPolicy)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verdigris: %v\n", err)
		os.Exit(1)
	}
	defer srv.Close()

	if err := srv.Listen(); err != nil {
		fmt.Fprintf(os.Stderr, "verdigris: %v\n", err)
		os.Exit(1)
	}

	// Buffered so the goroutine below never blocks on the send, even on the
	// path where a signal arrives and main stops listening to this channel.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	// Interrupt covers Ctrl-C; SIGTERM covers a service manager asking the
	// process to stop. Both are handled the same way.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serveErr:
		// Serve returned on its own, which means the accept loop failed
		// rather than being shut down. Nothing is in flight to drain.
		if err != nil {
			log.Printf("server stopped: %v", err)
			os.Exit(1)
		}

	case sig := <-signals:
		log.Printf("shutting down after %s", sig)

		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		if err := srv.Shutdown(ctx); err != nil {
			// The listener is already closed at this point, so the server has
			// stopped accepting either way. Shutdown has also signalled the
			// remaining handlers, so exiting here abandons nothing that is
			// still running.
			log.Printf("shutdown: %v", err)
			cancel()
			os.Exit(1)
		}
		cancel()

		// Collect Serve's result now that the accept loop has ended. It
		// reports nil for a clean shutdown; anything else is a genuine accept
		// error that the exit code should reflect.
		if err := <-serveErr; err != nil {
			log.Printf("server stopped: %v", err)
			os.Exit(1)
		}
	}
}

// checkPersistenceParent reports whether a persistence file's directory
// exists, so that a typo in -rdb-path or -aof-path is caught at startup rather
// than at the first write.
//
// An empty path disables that mechanism and has no directory to check. The
// file itself is deliberately not required to exist: a missing snapshot or log
// is a first run, and both loaders treat it as one.
//
// flagName is passed in so the message names the flag the operator actually
// typed, which is the difference between a fixable error and a puzzling one
// when both flags are on the command line.
func checkPersistenceParent(flagName, path string) error {
	if path == "" {
		return nil
	}

	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("invalid %s %s: directory %s does not exist", flagName, path, dir)
		}
		return fmt.Errorf("invalid %s %s: %w", flagName, path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("invalid %s %s: %s is not a directory", flagName, path, dir)
	}
	return nil
}
