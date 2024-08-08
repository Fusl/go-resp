package tests

import (
	"bufio"
	"bytes"
	"github.com/Fusl/go-resp"
	"io"
	"net"
	_ "net/http/pprof"
	"reflect"
	"strings"
	"testing"
	"time"
)

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

func GetTestingClientConnParser(b []byte) *resp.ClientConn {
	bytesReader := bytes.NewReader(b)
	bufioReader := bufio.NewReader(bytesReader)
	bufioWriter := bufio.NewWriter(io.Discard)

	conn := &Conn{
		r: bufioReader,
		w: bufioWriter,
	}

	return resp.NewClientConn(conn)
}

func TestParser(t *testing.T) {
	testCases := map[string][][]string{
		// valid test cases
		"*1\r\n$3\r\nfoo\r\n":                        {{"foo"}},
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n":           {{"foo", "bar"}},
		"*0\r\n*1\r\n$3\r\nfoo\r\n":                  {{"foo"}},
		"*3\r\n$3\r\nfoo\r\n$0\r\n\r\n$3\r\nbar\r\n": {{"foo", "", "bar"}},
		"\"foo\" \"bar\" \"baz\"\r\n":                {{"foo", "bar", "baz"}},
		"foo bar baz\r\n":                            {{"foo", "bar", "baz"}},
		"\"foo\" bar \"baz\"\r\n":                    {{"foo", "bar", "baz"}},
		"foo \"bar baz\"\r\n":                        {{"foo", "bar baz"}},
		" *1\r\n":                                    {{"*1"}},
		"$3\r\n":                                     {{"$3"}},
		"*1\r\n$3\r\nfooxxbar\r\n":                   {{"foo"}, {"bar"}},
		"*1\r\n$3\r\nfoo\r\n*1\r\n$3\r\nbar\r\n":     {{"foo"}, {"bar"}},

		// invalid test cases
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
		"\r\n":                     {nil},
		"*1\r\n$30\r\nfoo\r\n":     {nil},
		"*3.1415\r\n$3\r\nfoo\r\n": {nil},
		"*11111111111111111111111111111111\r\n$3\r\nfoo\r\n": {nil},
	}

	for input, expected := range testCases {
		t.Run(strings.ReplaceAll(input, "\r\n", ","), func(t *testing.T) {
			rconn := GetTestingClientConnParser([]byte(input))
			rconn.SetOptions(resp.ClientConnOptions{
				MaxMultiBulkLength: resp.Pointer(16),
				MaxBulkLength:      resp.Pointer(1024),
				MaxBufferSize:      resp.Pointer(65536),
			})
			for _, expected := range expected {
				args, err := rconn.Next()
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
					t.Fatalf("processing %q, expected %v, got %q", input, expected, argStrings)
				}
			}
			args, err := rconn.Next()
			if err == nil {
				t.Fatalf("processing %q, expected EOF, got %s", input, args)
			}
		})
	}
}

func TestLimits(t *testing.T) {
	t.Run("Baseline_MultiBulk", func(t *testing.T) {
		rconn := GetTestingClientConnParser([]byte("*1\r\n$3\r\nfoo\r\n"))
		rconn.SetOptions(resp.ClientConnOptions{
			MaxMultiBulkLength: resp.Pointer(1),
			MaxBulkLength:      resp.Pointer(3),
			MaxBufferSize:      resp.Pointer(5),
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
		rconn.SetOptions(resp.ClientConnOptions{
			MaxMultiBulkLength: resp.Pointer(0),
			MaxBulkLength:      resp.Pointer(0),
			MaxBufferSize:      resp.Pointer(5),
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
		rconn := GetTestingClientConnParser([]byte("*5\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n"))
		rconn.SetOptions(resp.ClientConnOptions{
			MaxMultiBulkLength: resp.Pointer(4),
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
		rconn.SetOptions(resp.ClientConnOptions{
			MaxBulkLength: resp.Pointer(1024),
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
		rconn.SetOptions(resp.ClientConnOptions{
			MaxBufferSize: resp.Pointer(4),
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
		rconn.SetOptions(resp.ClientConnOptions{
			MaxBufferSize: resp.Pointer(2),
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
