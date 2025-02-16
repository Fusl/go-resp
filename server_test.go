package resp

import (
	"bufio"
	"bytes"
	"io"
	"math/rand/v2"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func randomString(l int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, l)
	for i := 0; i < l; i++ {
		b[i] = chars[rand.IntN(len(chars))]
	}
	return string(b)
}

type Conn struct {
	r io.Reader
	w io.Writer
}

func (c *Conn) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

func (c *Conn) Write(p []byte) (n int, err error) {
	return c.w.Write(p)
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) LocalAddr() net.Addr {
	return nil
}

func (c *Conn) RemoteAddr() net.Addr {
	return nil
}

func (c *Conn) SetDeadline(t time.Time) error {
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return nil
}

func GetTestingClientConnParser(b []byte) *Server {
	bytesReader := bytes.NewReader(b)
	bufioReader := bufio.NewReader(bytesReader)
	bufioWriter := bufio.NewWriter(io.Discard)

	conn := &Conn{
		r: bufioReader,
		w: bufioWriter,
	}

	return NewServer(conn)
}

var parserTestCases = map[string][][]string{
	// valid test cases
	"*1\r\n$3\r\nfoo\r\n":                             {{"foo"}},
	"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n":                {{"foo", "bar"}},
	"*0\r\n*1\r\n$3\r\nfoo\r\n":                       {{"foo"}},
	"*3\r\n$3\r\nfoo\r\n$0\r\n\r\n$3\r\nbar\r\n":      {{"foo", "", "bar"}},
	"\"foo\" \"bar\" \"baz\"\r\n":                     {{"foo", "bar", "baz"}},
	"foo bar baz\r\n":                                 {{"foo", "bar", "baz"}},
	"foo bar baz\n":                                   {{"foo", "bar", "baz"}},
	"\"foo\" bar \"baz\"\r\n":                         {{"foo", "bar", "baz"}},
	"foo \"bar baz\"\r\n":                             {{"foo", "bar baz"}},
	" *1\r\n":                                         {{"*1"}},
	"$3\r\n":                                          {{"$3"}},
	"*1\r\n$3\r\nfooxxbar\r\n":                        {{"foo"}, {"bar"}},
	"*1\r\n$3\r\nfoo\r\n*1\r\n$3\r\nbar\r\n":          {{"foo"}, {"bar"}},
	"\"\\x66\\x6F\\x6F\\n\\r\\t\\b\\a\\x\\y\\z\"\r\n": {{"foo\n\r\t\b\axyz"}},
	"'foo' \"bar\" baz\r\n":                           {{"foo", "bar", "baz"}},
	"'\"foo\"' \"'bar'\"\r\n":                         {{"\"foo\"", "'bar'"}},

	// invalid test cases
	"":                         {nil},
	"*-1\r\n":                  {nil},
	"*1\r\n$3\r\nfoo\r":        {nil},
	"*1\r\n$3\r\nfoo":          {nil},
	"*1\r\n$3\rfoo\r\n":        {nil},
	"*a\r\n$3\r\nfoo\r\n":      {nil},
	"*1\r\n$":                  {nil},
	"*1\r\n$-3\r\nfoo\r\n":     {nil},
	"* 1\r\n$3\r\nfoo\r\n":     {nil},
	"*1 \r\n$3\r\nfoo\r\n":     {nil},
	"*1\r\n$ 3\r\nfoo\r\n":     {nil},
	"*1\r\n$3 \r\nfoo\r\n":     {nil},
	"*1\r\n*3\r\nfoo\r\n":      {nil},
	"\r\n":                     {nil},
	"*1\r\n$30\r\nfoo\r\n":     {nil},
	"*3.1415\r\n$3\r\nfoo\r\n": {nil},
	"*11111111111111111111111111111111\r\n$3\r\nfoo\r\n": {nil},
	randomString(65537) + "\r\n":                         {nil},
	"\"foo\r\n":                                          {nil},
	"'foo\r\n":                                           {nil},
	"\"foo\"\"bar\"\r\n":                                 {nil},
}

func TestServerParser(t *testing.T) {
	bytesRd := bytes.NewReader(nil)
	bufRd := bufio.NewReader(bytesRd)
	server := &Server{
		rd:                 bufRd,
		maxMultiBulkLength: 16,
		maxBulkLength:      1024,
		maxBufferSize:      65536,
	}
	for input, expected := range parserTestCases {
		t.Run(strings.ReplaceAll(input[:min(len(input), 32)], "\r\n", ","), func(t *testing.T) {
			server.err = nil
			bytesRd.Reset([]byte(input))
			bufRd.Reset(bytesRd)
			for _, expected := range expected {
				args, err := server.Next()
				if expected == nil {
					if err == nil {
						t.Fatalf("processing %q, expected error, got %q", input, args)
					}
					break
				}
				if err != nil {
					t.Fatalf("processing %q, unexpected error: %v", input, err)
				}
				argStrings := make([]string, len(args))
				for i, arg := range args {
					argStrings[i] = string(arg)
				}
				if !reflect.DeepEqual(argStrings, expected) {
					t.Fatalf("processing %q, expected %q, got %q", input, expected, argStrings)
				}
			}
			args, err := server.Next()
			if err == nil {
				t.Fatalf("processing %q, expected EOF, got %s", input, args)
			}
		})
	}
}

func BenchmarkServerParser(b *testing.B) {
	bytesRd := bytes.NewReader(nil)
	bufRd := bufio.NewReader(bytesRd)
	server := &Server{
		rd:                 bufRd,
		maxMultiBulkLength: 16,
		maxBulkLength:      1024,
		maxBufferSize:      65536,
	}
	for input := range parserTestCases {
		b.Run(strings.ReplaceAll(input[:min(len(input), 32)], "\r\n", ","), func(b *testing.B) {
			bytesRd.Reset([]byte(input))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				server.err = nil
				bytesRd.Seek(0, io.SeekStart)
				bufRd.Reset(bytesRd)
				for {
					args, err := server.Next()
					if err != nil {
						break
					}
					_ = args
				}
			}
		})
	}
}

func TestServerLimits(t *testing.T) {
	t.Run("Baseline_MultiBulk", func(t *testing.T) {
		rconn := GetTestingClientConnParser([]byte("*1\r\n$3\r\nfoo\r\n"))
		defer rconn.Close()
		rconn.SetOptions(ServerOptions{
			MaxMultiBulkLength: Pointer(1),
			MaxBulkLength:      Pointer(3),
			MaxBufferSize:      Pointer(5),
		})
		args, err := rconn.Next()
		if err != nil {
			t.Fatalf("unexpected error %q", err)
		}
		if len(args) != 1 || string(args[0]) != "foo" {
			t.Fatalf("unexpected args %q", args)
		}
	})
	t.Run("Baseline_Simple", func(t *testing.T) {
		rconn := GetTestingClientConnParser([]byte("foo\r\n"))
		defer rconn.Close()
		rconn.SetOptions(ServerOptions{
			MaxMultiBulkLength: Pointer(1),
			MaxBulkLength:      Pointer(3),
			MaxBufferSize:      Pointer(5),
		})
		args, err := rconn.Next()
		if err != nil {
			t.Fatalf("unexpected error %q", err)
		}
		if len(args) != 1 || string(args[0]) != "foo" {
			t.Fatalf("unexpected args %q", args)
		}
	})
	t.Run("MaxMultiBulkLength", func(t *testing.T) {
		rconn := GetTestingClientConnParser([]byte("*5\r\n$0\r\n\r\n$0\r\n\r\n$0\r\n\r\n$0\r\n\r\n$0\r\n\r\n"))
		defer rconn.Close()
		rconn.SetOptions(ServerOptions{
			MaxMultiBulkLength: Pointer(4),
		})
		args, err := rconn.Next()
		if err == nil {
			t.Fatalf("expected error, got %q", args)
		}
		if err.Error() != "Protocol error: invalid multibulk length" {
			t.Fatalf("unexpected error %q", err)
		}
	})
	t.Run("MaxBulkLength", func(t *testing.T) {
		rconn := GetTestingClientConnParser([]byte("*1\r\n$1025\r\n" + strings.Repeat("a", 1025) + "\r\n"))
		defer rconn.Close()
		rconn.SetOptions(ServerOptions{
			MaxBulkLength: Pointer(1024),
		})
		args, err := rconn.Next()
		if err == nil {
			t.Fatalf("expected error, got %q", args)
		}
		if err.Error() != "Protocol error: invalid bulk length" {
			t.Fatalf("unexpected error %q", err)
		}
	})
	t.Run("MaxBufferSize_MultiBulk", func(t *testing.T) {
		rconn := GetTestingClientConnParser([]byte("*1\r\n$3\r\nfoo\r\n"))
		defer rconn.Close()
		rconn.SetOptions(ServerOptions{
			MaxBufferSize: Pointer(4),
		})
		args, err := rconn.Next()
		if err == nil {
			t.Fatalf("expected error, got %q", args)
		}
		if err.Error() != bufio.ErrBufferFull.Error() {
			t.Fatalf("unexpected error %q", err)
		}
	})
	t.Run("MaxBufferSize_SimpleString", func(t *testing.T) {
		rconn := GetTestingClientConnParser([]byte("foo\r\n"))
		defer rconn.Close()
		rconn.SetOptions(ServerOptions{
			MaxBufferSize: Pointer(2),
		})
		args, err := rconn.Next()
		if err == nil {
			t.Fatalf("expected error, got %q", args)
		}
		if err.Error() != bufio.ErrBufferFull.Error() {
			t.Fatalf("unexpected error %q", err)
		}
	})
}

func FuzzServerParser(f *testing.F) {
	for input := range parserTestCases {
		f.Add([]byte(input))
	}

	bytesRd := bytes.NewReader(nil)
	bufRd := bufio.NewReader(bytesRd)
	server := &Server{
		rd:                 bufRd,
		maxMultiBulkLength: 64,
		maxBulkLength:      65536,
		maxBufferSize:      1048576,
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		server.err = nil
		bytesRd.Reset(data)
		bufRd.Reset(bytesRd)
		_, err := server.Next()
		switch err {
		case nil, io.EOF, io.ErrUnexpectedEOF, ErrProtoInvalidMultiBulkLength, ErrProtoInvalidBulkLength, ErrProtoUnbalancedQuotes, ErrProtoExpectedString, bufio.ErrBufferFull:
		default:
			panic(string(data) + " " + err.Error())
		}
	})
}
