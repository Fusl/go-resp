package resp

import (
	"github.com/Fusl/go-resp/static"
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
