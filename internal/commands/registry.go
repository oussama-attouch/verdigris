package commands

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Oussama-Att/verdigris/internal/protocol"
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
// The value must be an Array (RESP commands are always sent as arrays of
// bulk strings). Anything else returns an error value the caller can send
// to the client.
func (r *Registry) Dispatch(v protocol.Value) protocol.Value {
	args, ok := v.(protocol.Array)
	if !ok {
		// Real clients never send a bare bulk string as a command. Reporting
		// "unknown command" rather than a parse error matches what Redis does
		// and keeps the reply shape uniform.
		return protocol.Error("ERR unknown command")
	}
	if len(args) == 0 {
		return protocol.Error("ERR empty command")
	}

	name, ok := args[0].(protocol.BulkString)
	if !ok {
		// The command name must be a bulk string. A simple string here is a
		// malformed request rather than an unknown one, and saying so makes
		// the client's bug obvious.
		return protocol.Error("ERR invalid command name")
	}

	upper := strings.ToUpper(string(name))

	r.mu.RLock()
	h, found := r.handlers[upper]
	r.mu.RUnlock()

	if !found {
		return protocol.Error(fmt.Sprintf("ERR unknown command '%s'", upper))
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
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register("PING", Ping)
	r.Register("ECHO", Echo)
	return r
}
