package commands

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/store"
)

// ErrUnknownCommand is returned by Registry.Dispatch when args[0] does not
// name a registered command.
var ErrUnknownCommand = errors.New("unknown command")

// Registry maps command names to handlers. Names are case-insensitive on
// lookup, matching Redis's behavior.
type Registry struct {
	// mu guards handlers. The server dispatches from one goroutine per
	// connection, so several goroutines read the map concurrently. Reads
	// vastly outnumber writes — registration happens at startup — which is
	// what makes an RWMutex the right choice over a plain Mutex.
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry with no commands registered.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds a handler under name. Names are uppercased internally.
// A duplicate registration panics, because it can only happen at init time.
func (r *Registry) Register(name string, h Handler) {
	upper := strings.ToUpper(name)

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[upper]; exists {
		// A duplicate is a programming error with no sensible runtime
		// recovery: one of the two handlers would silently shadow the other,
		// and which one wins would depend on init order. Failing loudly at
		// startup is far better than a command that quietly does the wrong
		// thing later.
		panic(fmt.Sprintf("commands: duplicate registration of %q", upper))
	}
	r.handlers[upper] = h
}

// Dispatch takes a decoded RESP value and routes it to a handler.
//
// The Value return is what the client should see. The error return is an
// internal fault — nil for a successful dispatch including domain errors,
// non-nil only when a handler has hit a bug. The caller must log the error
// and substitute a generic reply; it must not forward the error text.
//
// Note the asymmetry, which is deliberate: every check Dispatch performs
// itself (not an array, empty array, non-bulk command name, unknown command)
// is a client error and returns (value, nil). Only a handler's own error
// return reaches the client as a fault, because only a handler can know that
// something went wrong on our side rather than theirs.
func (r *Registry) Dispatch(v protocol.Value) (protocol.Value, error) {
	args, ok := v.(protocol.Array)
	if !ok {
		// Real clients never send a bare bulk string as a command. Reporting
		// "unknown command" rather than a parse error matches what Redis does
		// and keeps the reply shape uniform.
		return protocol.Error("ERR unknown command"), nil
	}
	if len(args) == 0 {
		return protocol.Error("ERR empty command"), nil
	}

	name, ok := args[0].(protocol.BulkString)
	if !ok {
		// The command name must be a bulk string. A simple string here is a
		// malformed request rather than an unknown one, and saying so makes
		// the client's bug obvious.
		return protocol.Error("ERR invalid command name"), nil
	}

	upper := strings.ToUpper(string(name))

	r.mu.RLock()
	h, found := r.handlers[upper]
	r.mu.RUnlock()

	if !found {
		return protocol.Error(fmt.Sprintf("ERR unknown command '%s'", upper)), nil
	}

	// The handler is called with the lock released. A handler that registered
	// another command — or that one day calls back into the registry — would
	// deadlock against a write lock held across the call.
	return h(args)
}

// Names returns the registered command names, sorted. Useful for a
// COMMAND-like introspection command in a later milestone.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// DefaultRegistry returns a Registry with the built-in commands registered.
// The server uses this; tests construct their own registries with only the
// handlers they need.
//
// The store is a parameter rather than a field of Registry because the
// registry is a lookup table and knows nothing about data; only the handlers
// that need state capture it. PING, ECHO, CLIENT and COMMAND take the store
// and ignore it, which keeps the wiring uniform.
//
// save is the function SAVE invokes, and it is a parameter for the same
// reason: the registry has no business knowing that persistence exists. A nil
// save registers a handler that reports an internal fault if called, which is
// what a test that only cares about other commands wants — but production
// always passes a real one, and the server registers its own Save immediately
// after construction so that no reachable moment exists where SAVE is absent
// or broken.
func DefaultRegistry(s *store.Store, save SaveFunc) *Registry {
	r := NewRegistry()
	r.Register("PING", Ping)
	r.Register("ECHO", Echo)
	r.Register("SET", SetHandler(s))
	r.Register("GET", GetHandler(s))
	r.Register("DEL", DelHandler(s))
	r.Register("EXISTS", ExistsHandler(s))
	r.Register("EXPIRE", ExpireHandler(s))
	r.Register("TTL", TTLHandler(s))
	r.Register("PERSIST", PersistHandler(s))
	r.Register("CLIENT", ClientHandler())
	r.Register("COMMAND", CommandHandler())

	if save == nil {
		// The handler still exists, so PING and every other command behave
		// identically whether or not persistence is wired. Only a SAVE that
		// nobody can service fails, and it fails as an internal fault rather
		// than as an unknown command — the command is understood, the server
		// simply has nothing to save to.
		save = func() error { return errNoSaveFunc }
	}
	r.Register("SAVE", SaveHandler(save))

	return r
}

// errNoSaveFunc reports a SAVE reaching a registry that was built without a
// save function. It is a programming error in the caller, not a client error,
// so it travels the internal-fault channel.
var errNoSaveFunc = errors.New("commands: no save function registered")
