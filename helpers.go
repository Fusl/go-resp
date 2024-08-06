package resp

import (
	"github.com/Fusl/go-resp/static"
	"strconv"
	"unsafe"
)

// SanitizeSimpleString replaces all '\r' and '\n' characters with spaces.
// Useful when writing untrusted error messages to the client.
func SanitizeSimpleString(buf []byte) []byte {
	for i := 0; i < len(buf); i++ {
		if buf[i] == '\r' || buf[i] == '\n' {
			buf[i] = ' '
		}
	}
	return buf
}

// bstring converts a byte slice to a string without copying.
func bstring(bs []byte) string {
	return unsafe.String(&bs[0], len(bs))
}

// sbytes converts a string to a byte slice without copying.
func sbytes(s string) []byte {
	if s == "" {
		return static.NullBytes
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// Expand works similarly to slices.Grow but returns the expanded slice rather than the capped slice.
func Expand[S ~[]E, E any](s S, n int) S {
	if n < 0 {
		panic("cannot be negative")
	}
	if n -= cap(s) - len(s); n > 0 {
		s = append(s[:cap(s)], make([]E, n)...)
	}
	return s
}

func ParseUInt(b []byte) (int, error) {
	l := len(b)
	if !(strconv.IntSize == 32 && (0 < l && l < 10) || strconv.IntSize == 64 && (0 < l && l < 19)) {
		return 0, strconv.ErrSyntax
	}

	n := 0
	for _, c := range b {
		c -= '0'
		if c > 9 {
			return 0, strconv.ErrSyntax
		}
		n = n*10 + int(c)
	}

	return n, nil
}
