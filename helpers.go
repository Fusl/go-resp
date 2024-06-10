package resp

import (
	"reflect"
	"unsafe"
)

func BytesToLower(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return b
}

func BytesToUpper(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return b
}

// SanitizeSimpleString replaces all '\r' and '\n' characters with spaces.
func SanitizeSimpleString(buf []byte) []byte {
	for i := 0; i < len(buf); i++ {
		if buf[i] == '\r' || buf[i] == '\n' {
			buf[i] = ' '
		}
	}
	return buf
}

func bstring(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}

func bstrings(bss [][]byte) []string {
	ss := make([]string, len(bss))
	for i, bs := range bss {
		ss[i] = bstring(bs)
	}
	return ss
}

// https://stackoverflow.com/a/59210739
func unsafeGetBytes(s string) []byte {
	if s == "" {
		return nil // or []byte{}
	}
	return (*[0x7fff0000]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}
