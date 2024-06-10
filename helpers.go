package resp

import (
	"reflect"
	"resp/static"
	"unsafe"
)

// SanitizeSimpleString replaces all '\r' and '\n' characters with spaces.
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
	return *(*string)(unsafe.Pointer(&bs))
}

// unsafeGetBytes converts a string to a byte slice without copying.
// https://stackoverflow.com/a/59210739
func unsafeGetBytes(s string) []byte {
	if s == "" {
		return static.NullBytes
	}
	return (*[0x7fff0000]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}
