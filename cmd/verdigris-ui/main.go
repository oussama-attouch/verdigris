// Command verdigris-ui serves a web interface to a Verdigris server.
//
// It is a separate process from Verdigris on purpose. This program is an HTTP
// server that happens to speak RESP as a client; Verdigris is a RESP server
// that knows nothing about HTTP. Keeping them apart means the UI can be
// restarted, killed, or replaced without touching the keyspace, and — the part
// that matters for correctness — every effect this program has on the store it
// reaches through the same protocol a redis-cli user uses. There is no private
// path into internal/store, so a bug here can send a wrong command but cannot
// corrupt data in a way a client could not.
//
// The bridge is internal/respclient: one shared connection, mutex-protected,
// reconnecting when it dies. One client rather than one per request because the
// UI issues a handful of commands per page view and a connection pool would add
// state to manage for throughput nothing here needs.
//
// # Endpoints
//
//	GET    /api/health        PING
//	GET    /api/keys          KEYS *
//	GET    /api/dump          KEYS *, then GET and TTL for every key
//	GET    /api/get?key=      GET
//	POST   /api/set           {"key","value","ttl"} — ttl optional
//	DELETE /api/del?key=      DEL
//	POST   /api/expire        {"key","ttl"} — EXPIRE
//	GET    /api/ttl?key=      TTL
//	POST   /api/save          SAVE
//
// Every response is JSON, including every error: {"error": "..."} with 400 for
// a malformed request and 502 when Verdigris is unreachable or answers with a
// RESP error frame. The distinction is the point — a 400 means the browser sent
// something wrong, a 502 means the server behind this one did, and a UI that
// cannot tell those apart shows the wrong message to the wrong person.
//
// Static files are served from -web at /, so the page and the API share an
// origin and the front-end needs no CORS configuration.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Oussama-Att/verdigris/internal/protocol"
	"github.com/Oussama-Att/verdigris/internal/respclient"
)

// maxBodyBytes bounds a request body. The UI sends a key and a value; anything
// larger than this is not a request this program should try to parse, and
// reading it would let one client exhaust memory.
const maxBodyBytes = 1 << 20 // 1 MiB

// endpoint is one HTTP route: a method, a path, and the handler for it.
type endpoint struct {
	method  string
	path    string
	handler func(*app, http.ResponseWriter, *http.Request)
}

// routes is the routing table. It is a table rather than a switch inside one
// handler so that the method for each path is data — which is what lets
// registerRoutes answer 405 with the right Allow header instead of treating a
// wrong method as an unknown path.
var routes = []endpoint{
	{http.MethodGet, "/api/health", handleHealth},
	{http.MethodGet, "/api/keys", handleKeys},
	{http.MethodGet, "/api/dump", handleDump},
	{http.MethodGet, "/api/get", handleGet},
	{http.MethodPost, "/api/set", handleSet},
	{http.MethodDelete, "/api/del", handleDel},
	{http.MethodPost, "/api/expire", handleExpire},
	{http.MethodGet, "/api/ttl", handleTTL},
	{http.MethodPost, "/api/save", handleSave},
}

// app carries what the handlers share. It is one struct rather than a set of
// closures so a handler can be a plain function with a testable signature.
type app struct {
	client *respclient.Client
}

func main() {
	verdigrisAddr := flag.String("verdigris", "127.0.0.1:6379", "host:port of the Verdigris server to talk to")
	httpAddr := flag.String("http", "127.0.0.1:8080", "host:port for the UI's HTTP listener")
	webDir := flag.String("web", "./web", "directory of static files to serve at /")
	flag.Parse()

	// A failed initial dial is a warning, not a fatal error. The UI is more
	// useful up than down: the page loads, /api/health reports the problem
	// honestly, and the first successful command reconnects on its own. Exiting
	// here would mean a UI that cannot start before the server it monitors,
	// which is exactly backwards for a monitoring UI.
	client, err := respclient.Dial(*verdigrisAddr)
	if err != nil {
		log.Printf("warning: cannot reach verdigris at %s yet: %v", *verdigrisAddr, err)
		log.Printf("warning: starting anyway; the UI will reconnect on the first successful command")
	} else {
		log.Printf("connected to verdigris at %s", *verdigrisAddr)
	}
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	// A client is needed even when the dial failed, because Do is what
	// reconnects. Constructing one for an unreachable address is deliberate:
	// it holds no connection and dials on first use.
	if client == nil {
		client = respclient.NewUnconnected(*verdigrisAddr)
	}

	a := &app{client: client}

	mux := http.NewServeMux()
	registerRoutes(mux, a)

	// Static files last, so /api/... wins. Patterns are matched by specificity
	// in Go's ServeMux, but registering the API first also keeps the reading
	// order the same as the precedence.
	//
	// http.Dir takes the flag rather than a hardcoded "./web" so the directory
	// can be pointed elsewhere without a rebuild.
	mux.Handle("/", http.FileServer(http.Dir(*webDir)))

	srv := &http.Server{
		Addr: *httpAddr,
		// Both timeouts are short on purpose. This program serves small JSON
		// documents and one static page; a request that has not been read or
		// answered in five seconds is stuck, and holding the connection open
		// would only delay noticing. Without a write timeout a client that
		// stops reading can hold a handler goroutine forever.
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		Handler:      mux,
	}

	log.Printf("serving UI on http://%s (static files from %s)", *httpAddr, *webDir)
	if err := srv.ListenAndServe(); err != nil {
		// ListenAndServe only returns on failure: a bind error, or the server
		// being shut down. Either way nothing is left to serve, so this is a
		// fatal condition for the process.
		fmt.Fprintf(os.Stderr, "verdigris-ui: %v\n", err)
		os.Exit(1)
	}
}

// registerRoutes wires the routing table into a mux.
//
// The 405 case is handled by hand because net/http has no notion of "this path
// exists but not for that method": a POST to /api/keys would otherwise fall
// through to the file server and answer 404, which tells the client the wrong
// thing. The Allow header names the methods that do work, which is what a
// client needs to recover.
func registerRoutes(mux *http.ServeMux, a *app) {
	for _, ep := range routes {
		ep := ep
		mux.HandleFunc(ep.path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != ep.method {
				w.Header().Set("Allow", allowedMethods(ep.path))
				writeError(w, http.StatusMethodNotAllowed,
					fmt.Sprintf("%s is not allowed on %s; use %s", r.Method, ep.path, allowedMethods(ep.path)))
				return
			}
			ep.handler(a, w, r)
		})
	}
}

// allowedMethods lists the methods registered for a path, for the Allow header.
func allowedMethods(path string) string {
	var methods []string
	for _, ep := range routes {
		if ep.path == path {
			methods = append(methods, ep.method)
		}
	}
	return strings.Join(methods, ", ")
}

// --- responses --------------------------------------------------------------

// writeJSON encodes v as the response body.
//
// The Content-Type is set before the status so that a handler which forgets to
// call this first still cannot emit a body with the wrong type; and the write
// error is logged rather than reported, because by the time it happens the
// status line is already on the wire and there is nothing left to say.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	enc := json.NewEncoder(w)
	if err := enc.Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

// writeError answers with the one error shape this API has.
//
// Every failure path goes through here, so a client never has to parse a plain
// text body or guess at a status code's meaning.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeUpstreamError answers 502 for a failure on Verdigris's side.
//
// A RESP error frame and an unreachable server are both 502 rather than 500:
// this program worked, the thing it depends on did not. The distinction from
// 400 is the one the UI shows to a user — "your request was wrong" versus "the
// server is unhappy" — so the error frame's own text is passed through, where a
// 500 would have to invent a message.
func writeUpstreamError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusBadGateway, err.Error())
}

// do runs one command and converts a failure into the right HTTP response.
//
// It returns ok=false when it has already written a response, which is the
// signal for the caller to stop. Every handler goes through here, so the
// 400-versus-502 decision is made in exactly one place.
func (a *app) do(w http.ResponseWriter, args ...string) (protocol.Value, bool) {
	reply, err := a.client.Do(args...)
	if err != nil {
		// A transport failure: no reply at all, so the server is unreachable or
		// died mid-command. The client has already dropped the connection and
		// will dial again next time.
		writeUpstreamError(w, err)
		return nil, false
	}

	// A RESP error frame is a reply, not a transport failure, and it belongs to
	// the server's judgement of the command — "unknown command", "wrong number
	// of arguments". Reported as 502 so the client can tell it apart from its
	// own malformed request.
	if errValue, isErr := reply.(protocol.Error); isErr {
		writeError(w, http.StatusBadGateway, string(errValue))
		return nil, false
	}

	return reply, true
}

// --- RESP to JSON -----------------------------------------------------------

// stringValue converts a bulk-string reply to a Go string.
//
// A null bulk string is not a value: it means the key is absent, and the
// callers that can see one handle it before asking. Reaching here with a null
// means the command answered something the handler did not expect.
func stringValue(v protocol.Value) (string, error) {
	bulk, ok := v.(protocol.BulkString)
	if !ok {
		return "", fmt.Errorf("expected a bulk string, got %s", typeName(v))
	}
	if bulk == nil {
		return "", errors.New("expected a bulk string, got null")
	}
	return string(bulk), nil
}

// intValue converts an integer reply to an int64.
func intValue(v protocol.Value) (int64, error) {
	n, ok := v.(protocol.Integer)
	if !ok {
		return 0, fmt.Errorf("expected an integer, got %s", typeName(v))
	}
	return int64(n), nil
}

// keyList converts a KEYS * reply to a slice of strings.
//
// Every element must be a bulk string. One that is not is a protocol
// violation rather than a value to interpret: KEYS is defined to answer with
// strings, so a different frame type means this program and the server
// disagree about the protocol, and guessing would hide that. The error names
// the offending element so the disagreement is diagnosable.
func keyList(v protocol.Value) ([]string, error) {
	arr, ok := v.(protocol.Array)
	if !ok {
		return nil, fmt.Errorf("expected an array from KEYS, got %s", typeName(v))
	}

	keys := make([]string, 0, len(arr))
	for i, elem := range arr {
		key, err := stringValue(elem)
		if err != nil {
			return nil, fmt.Errorf("KEYS element %d is not a bulk string: %w", i, err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// typeName names a value's type for an error message. It reports "null" rather
// than "protocol.Null" because the message is read by whoever is debugging the
// UI, and the wire concept is the useful one.
func typeName(v protocol.Value) string {
	switch v.(type) {
	case protocol.Null:
		return "null"
	case protocol.BulkString:
		return "a bulk string"
	case protocol.SimpleString:
		return "a simple string"
	case protocol.Integer:
		return "an integer"
	case protocol.Array:
		return "an array"
	case protocol.Error:
		return "an error frame"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// --- request helpers --------------------------------------------------------

// queryKey reads the ?key= parameter.
//
// An empty key is refused rather than passed on. It is not a valid key to
// create or fetch, and sending it would make the server answer "wrong number of
// arguments" for GET and store a zero-length name for SET — a client error
// dressed up as a server error.
func queryKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing required query parameter: key")
		return "", false
	}
	return key, true
}

// decodeBody reads and parses a JSON request body.
//
// The body is limited and unknown fields are refused, so a front-end typo
// ("ttl" spelled "tll") is reported instead of silently becoming a write with
// no expiry — which is the kind of bug that looks like the server ignoring the
// request.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// --- handlers ---------------------------------------------------------------

// handleHealth reports whether Verdigris is answering.
//
// It answers 200 either way, with the status in the body: "the UI is up and the
// server is down" is a successful answer to the question this endpoint asks,
// and a load balancer or a browser checking on the UI should not treat it as
// the UI being broken.
func handleHealth(a *app, w http.ResponseWriter, _ *http.Request) {
	reply, err := a.client.Do("PING")
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{
			"verdigris": "down",
			"error":     err.Error(),
		})
		return
	}
	if errValue, isErr := reply.(protocol.Error); isErr {
		writeJSON(w, http.StatusOK, map[string]string{
			"verdigris": "down",
			"error":     string(errValue),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"verdigris": "up"})
}

// handleKeys lists the keyspace. KEYS * answers with an empty array for an
// empty store, so the reply is a JSON array and never null.
func handleKeys(a *app, w http.ResponseWriter, _ *http.Request) {
	reply, ok := a.do(w, "KEYS", "*")
	if !ok {
		return
	}

	keys, err := keyList(reply)
	if err != nil {
		writeUpstreamError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, keys)
}

// entry is one row of the /api/dump reply.
//
// Value and TTL are pointers so that "absent" is distinguishable from "empty"
// and "no expiry" in the JSON. A key holding an empty string has exists=true
// and value="", while a key that vanished between the KEYS and the GET has
// exists=false and value=null; collapsing those would make the UI show a
// missing key as an empty one.
type entry struct {
	Key    string  `json:"key"`
	Value  *string `json:"value"`
	TTL    int64   `json:"ttl"`
	Exists bool    `json:"exists"`
}

// handleDump returns every key with its value and TTL.
//
// This exists so the front-end makes one request instead of 1 + 2N. It is
// deliberately sequential rather than concurrent: the shared client serializes
// commands anyway, so parallel goroutines would add coordination without adding
// throughput, and a bounded keyspace makes the round trips cheap.
func handleDump(a *app, w http.ResponseWriter, _ *http.Request) {
	reply, ok := a.do(w, "KEYS", "*")
	if !ok {
		return
	}

	keys, err := keyList(reply)
	if err != nil {
		writeUpstreamError(w, err)
		return
	}

	entries := make([]entry, 0, len(keys))
	for _, key := range keys {
		// The TTL is asked for first so that the value read is the later of the
		// two observations. A key that expires in between then shows as
		// exists=false with whatever TTL was true a moment earlier, rather than
		// as a value the store no longer has.
		ttlReply, ok := a.do(w, "TTL", key)
		if !ok {
			return
		}
		ttl, err := intValue(ttlReply)
		if err != nil {
			writeUpstreamError(w, fmt.Errorf("TTL %s: %w", key, err))
			return
		}

		valueReply, ok := a.do(w, "GET", key)
		if !ok {
			return
		}

		e := entry{Key: key, TTL: ttl}
		switch v := valueReply.(type) {
		case protocol.Null:
			// The key expired or was deleted between KEYS and GET.
			e.Exists = false
		case protocol.BulkString:
			s := string(v)
			e.Value = &s
			e.Exists = true
		default:
			writeUpstreamError(w, fmt.Errorf("GET %s: expected a bulk string, got %s", key, typeName(valueReply)))
			return
		}

		entries = append(entries, e)
	}

	writeJSON(w, http.StatusOK, entries)
}

// handleGet reads one key.
//
// A missing key is not an error: it is exists=false with a null value, because
// "this key is not here" is an answer the UI renders, not a failure it reports.
func handleGet(a *app, w http.ResponseWriter, r *http.Request) {
	key, ok := queryKey(w, r)
	if !ok {
		return
	}

	reply, ok := a.do(w, "GET", key)
	if !ok {
		return
	}

	out := struct {
		Key    string  `json:"key"`
		Value  *string `json:"value"`
		Exists bool    `json:"exists"`
	}{Key: key}

	switch v := reply.(type) {
	case protocol.Null:
		out.Exists = false
	case protocol.BulkString:
		s := string(v)
		out.Value = &s
		out.Exists = true
	default:
		writeUpstreamError(w, fmt.Errorf("GET %s: expected a bulk string, got %s", key, typeName(reply)))
		return
	}

	writeJSON(w, http.StatusOK, out)
}

// setRequest is the POST /api/set body. TTL is a pointer so that "no ttl field"
// and "ttl of zero" are different requests: the first is a plain SET, the second
// is a SET that expires immediately.
type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTL   *int64 `json:"ttl"`
}

func handleSet(a *app, w http.ResponseWriter, r *http.Request) {
	var req setRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key must not be empty")
		return
	}

	// The value is sent as given, including an empty string: a key holding ""
	// is a real key, and it is distinct from an absent one.
	if req.TTL != nil {
		// A non-positive TTL is refused rather than passed to SET EX, which
		// would answer an error about a syntax the caller never wrote. Saying
		// what is wrong with the request is this layer's job.
		if *req.TTL <= 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("ttl must be positive, got %d", *req.TTL))
			return
		}
		// The TTL is formatted here rather than sent as an integer frame
		// because every argument on this path is a bulk string, which is what
		// redis-cli sends and what the server is tested against.
		if _, ok := a.do(w, "SET", req.Key, req.Value, "EX", fmt.Sprintf("%d", *req.TTL)); !ok {
			return
		}
	} else {
		if _, ok := a.do(w, "SET", req.Key, req.Value); !ok {
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleDel deletes one key and reports whether it was there. A key that was
// already absent is deleted=0, not an error.
func handleDel(a *app, w http.ResponseWriter, r *http.Request) {
	key, ok := queryKey(w, r)
	if !ok {
		return
	}

	reply, ok := a.do(w, "DEL", key)
	if !ok {
		return
	}

	deleted, err := intValue(reply)
	if err != nil {
		writeUpstreamError(w, fmt.Errorf("DEL %s: %w", key, err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]int64{"deleted": deleted})
}

// expireRequest is the POST /api/expire body.
type expireRequest struct {
	Key string `json:"key"`
	TTL int64  `json:"ttl"`
}

func handleExpire(a *app, w http.ResponseWriter, r *http.Request) {
	var req expireRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key must not be empty")
		return
	}
	// EXPIRE accepts zero and negative timeouts and deletes the key, which is
	// Redis's documented behaviour. Passed through rather than refused, because
	// a UI that offers "delete via expire" needs it, and second-guessing the
	// server's semantics here would be this program inventing policy.
	reply, ok := a.do(w, "EXPIRE", req.Key, fmt.Sprintf("%d", req.TTL))
	if !ok {
		return
	}

	set, err := intValue(reply)
	if err != nil {
		writeUpstreamError(w, fmt.Errorf("EXPIRE %s: %w", req.Key, err))
		return
	}

	// set is 1 when the key existed and the timeout was applied, 0 when there
	// was no such key. Reported alongside ok so the UI can distinguish "expiry
	// set" from "no such key".
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "set": set})
}

// handleTTL reports a key's remaining time.
//
// The sentinels are passed through unchanged: -1 means the key exists with no
// expiry and -2 means it is absent. Translating them here would move a decision
// the front-end is better placed to make into the bridge.
func handleTTL(a *app, w http.ResponseWriter, r *http.Request) {
	key, ok := queryKey(w, r)
	if !ok {
		return
	}

	reply, ok := a.do(w, "TTL", key)
	if !ok {
		return
	}

	ttl, err := intValue(reply)
	if err != nil {
		writeUpstreamError(w, fmt.Errorf("TTL %s: %w", key, err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"key": key, "ttl": ttl})
}

// handleSave asks Verdigris to write a snapshot.
//
// SAVE is a read command in the registry — it does not change the keyspace — so
// it is not logged to the AOF, but it is still exposed because an operator
// looking at the UI wants a way to take a snapshot without a terminal.
func handleSave(a *app, w http.ResponseWriter, _ *http.Request) {
	if _, ok := a.do(w, "SAVE"); !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
