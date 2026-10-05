package protocol

import "strconv"

// Value is a decoded RESP frame. The set of types that implement it is
// closed: nothing outside this package can meaningfully implement Value,
// because the unexported method is the seal.
//
// A sealed interface is what lets the command layer write an exhaustive type
// switch and lets the compiler tell it when a new frame type is added. A
// caller can hold a Value, inspect it, and pass it on, but cannot invent a
// seventh kind of frame — which is the property that makes the encoder in
// milestone 3 a finite, provable job.
type Value interface {
	value() // unexported method; makes Value a sealed interface
}

// SimpleString is a non-binary string. By protocol, it contains no CRLF.
//
// The CRLF-free guarantee is enforced at decode time rather than assumed:
// Decode refuses a frame whose payload contains a bare CR or LF. That is what
// makes representing it as a Go string safe.
type SimpleString string

// Error is a RESP error frame. The wire form is identical to SimpleString
// with a different marker; the distinction is semantic.
//
// Like SimpleString it is a string, and for the same reason: the protocol
// forbids CRLF inside it, and Decode enforces that.
type Error string

// Integer is a signed 64-bit RESP integer.
//
// int64 rather than int: RESP defines the integer frame as exactly 64-bit
// signed, so an int on a 32-bit platform would silently truncate a valid
// frame instead of failing.
type Integer int64

// BulkString is a binary-safe byte sequence. len may be zero.
//
// []byte rather than string, and this is the core correctness property of the
// whole package: a bulk string may contain any byte value, including invalid
// UTF-8 and embedded CRLF. Making it a string would impose UTF-8 semantics on
// data that has none.
type BulkString []byte

// Array is an ordered list of Values. len may be zero.
type Array []Value

// Null is the empty RESP value. It represents both `$-1\r\n` (null bulk
// string) and `*-1\r\n` (null array). The two are indistinguishable at the
// semantic level in RESP2; a caller that needs to know which arrived does
// not have a use case that RESP2 supports.
//
// A struct rather than a nil Value: a nil interface means "no value was
// produced", while Null means "the peer sent the null value". Those are
// different facts, and a caller checking one must not accidentally match the
// other.
type Null struct{}

func (SimpleString) value() {}
func (Error) value()        {}
func (Integer) value()      {}
func (BulkString) value()   {}
func (Array) value()        {}
func (Null) value()         {}

// String returns a human-readable rendering of a value, for log lines, error
// messages, and test failures.
//
// It is deliberately NOT the wire form, and it is lossy: a bulk string is
// shown in quotes, a null bulk string and a null array are both rendered
// "null" because RESP2 cannot tell them apart, and a nil slice and an empty
// slice both render as an empty payload. Use Encode when the bytes matter.
//
// The command layer needs this because a client sends a command name as a
// bulk string — a []byte — and building "ERR unknown command 'FOO'" requires
// turning that []byte back into text.
func (v SimpleString) String() string { return string(v) }
func (v Error) String() string        { return string(v) }
func (v Integer) String() string      { return strconv.FormatInt(int64(v), 10) }
func (v BulkString) String() string   { return string(v) }
func (Array) String() string          { return "[array]" }
func (Null) String() string           { return "null" }
