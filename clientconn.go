package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/Fusl/go-resp/static"
	"github.com/Fusl/go-resp/types"
	"io"
	"math/big"
	"net"
	"slices"
	"strconv"
)

type ClientConn struct {
	rd          *bufio.Reader
	wr          *bufio.Writer
	conn        net.Conn
	writeBuf    []byte
	resp2compat bool
	args        [][]byte
	doflush     bool
	forceflush  bool
}

// NewClientConn returns a new wrapped client connection with sane default parameters set.
func NewClientConn(conn net.Conn) *ClientConn {
	c := &ClientConn{
		rd:      bufio.NewReader(conn),
		wr:      bufio.NewWriter(conn),
		conn:    conn,
		doflush: true,
	}
	return c
}

// SetFlush sets the flush behavior of the client connection. If v is true, the connection will be flushed after every write.
// If v is false, the connection will only be flushed when the buffer is full, when the connection is closed, or when flushing is turned back on again.
// The return value is the previous flush behavior. It is recommended to call this function as `defer c.SetFlush(c.SetFlush(false))` to ensure that the flush behavior is reset to its previous value.
func (c *ClientConn) SetFlush(v bool) bool {
	prev := c.doflush
	c.doflush = v
	if !prev && v {
		c.Flush()
	}
	return prev
}

// SetForceFlush sets the force flush behavior of the client connection.
// Normally we only flush the connection when the read buffer is empty to avoid unnecessary flushes when the client is pipelineing commands.
// If v is true, the write buffer will be flushed after every write as long as SetFlush is true.
// You should only set this to true if you are reading and writing to the connection from multiple goroutines (such as when handling Pub/Sub).
func (c *ClientConn) SetForceFlush(v bool) bool {
	prev := c.forceflush
	c.forceflush = v
	return prev
}

// SetRESP2Compat sets the RESP2 compatibility mode of the client connection.
// This mode is useful when talking with a RESP2 client that does not support the new RESP3 types.
// In this mode, Write functions will convert RESP3 types to RESP2 types where possible.
func (c *ClientConn) SetRESP2Compat(v bool) {
	c.resp2compat = v
}

// Next reads the next command from the client connection and returns it as a slice of byte slices.
// The returned slice is only valid until the next call to Next as it is reused for each command.
// If in doubt, copy the slice and every byte slice it contains to a newly allocated slice and byte slices.
func (c *ClientConn) Next() ([][]byte, error) {
	for {
		if args, err := c.next(); err != nil {
			c.CloseWithError(err)
			return args, err
		} else if len(args) > 0 {
			return args, err
		}
	}
}

func (c *ClientConn) collectFragmentsSize(delim byte, maxLen int) (full []byte, err error) {
	var frag []byte
	for {
		var e error
		frag, e = c.rd.ReadSlice(delim)

		// if read returns no error, we have a full line, return it
		if e == nil {
			break
		}

		// if read returns an error other than bufio.ErrBufferFull, return the error
		if !errors.Is(e, bufio.ErrBufferFull) {
			err = e
			break
		}

		// if the full + fragment buffer has grown too large, return an error
		if len(full)+len(frag) > maxLen {
			err = bufio.ErrBufferFull
			break
		}

		// copy the fragment to the full buffer so we can reuse the fragment buffer
		full = append(full, bytes.Clone(frag)...)
	}

	if len(full) <= 0 {
		return frag, err
	}
	if len(frag) > 0 {
		full = append(full, frag...)
	}
	return full, err
}

func (c *ClientConn) readLine() ([]byte, error) {
	b, err := c.collectFragmentsSize('\n', MaxInlineSize)
	if err != nil {
		return b, err
	}
	l := len(b)
	if l > 1 && b[l-2] == '\r' {
		return b[:l-2], nil
	}
	return b[:l-1], nil
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func hexDigitToInt(c byte) int {
	if c >= '0' && c <= '9' {
		return int(c - '0')
	}
	if c >= 'a' && c <= 'f' {
		return int(10 + c - 'a')
	}
	if c >= 'A' && c <= 'F' {
		return int(10 + c - 'A')
	}
	return 0
}

func (c *ClientConn) splitArgs(line []byte) ([][]byte, error) {
	p := 0
	var current []byte
	vector := c.args
	argc := 0
	ll := len(line)

	for {
		// skip blanks
		for p < ll && line[p] == ' ' {
			p++
		}
		if ll > p {
			// get a token
			indq := false // set to true if we are in "double quotes"
			insq := false // set to true if we are in 'single quotes'
			done := false

			if current == nil {
				if argc >= len(vector) {
					current = []byte{}
					vector = append(vector, current)
				} else {
					current = vector[argc]
					current = current[:0]
				}
			}
			for !done {
				if indq {
					if p >= ll {
						goto err // unterminated quotes
					}
					if line[p] == '\\' && p+3 < ll && line[p+1] == 'x' && isHexDigit(line[p+2]) && isHexDigit(line[p+3]) {
						b := byte(hexDigitToInt(line[p+2])<<4 | hexDigitToInt(line[p+3]))
						current = append(current, b)
						p += 3
					} else if line[p] == '\\' && p+1 < ll {
						var c byte
						p++
						switch line[p] {
						case 'n':
							c = '\n'
						case 'r':
							c = '\r'
						case 't':
							c = '\t'
						case 'b':
							c = '\b'
						case 'a':
							c = '\a'
						default:
							c = line[p]
						}
						current = append(current, c)
					} else if line[p] == '"' {
						// closing quote must be followed by a space or nothing at all
						if p+1 < ll && line[p+1] != ' ' {
							goto err
						}
						indq = false
					} else {
						current = append(current, line[p])
					}
				} else if insq {
					if p >= ll {
						goto err // unterminated quotes
					}
					if line[p] == '\\' && p+1 < ll && line[p+1] == '\'' {
						current = append(current, '\'')
						p++
					} else if line[p] == '\'' {
						// closing quote must be followed by a space or nothing at all
						if p+1 < ll && line[p+1] != ' ' {
							goto err
						}
						insq = false
					} else {
						current = append(current, line[p])
					}
				} else {
					if p >= ll {
						break
					}
					switch line[p] {
					case ' ', '\n', '\r', '\t', '\x00':
						done = true
					case '"':
						indq = true
					case '\'':
						insq = true
					default:
						current = append(current, line[p])
					}
				}
				if ll > p {
					p++
				}
			}
			// add the token to the vector
			vector[argc] = current
			current = nil
			argc++
		} else {
			c.args = vector
			return vector[:argc], nil
		}
	}

err:
	c.args = vector
	return nil, fmt.Errorf("Protocol error: unbalanced quotes in request")
}

func (c *ClientConn) next() ([][]byte, error) {
	t, err := c.rd.Peek(1)
	if err != nil {
		return nil, err
	}
	if len(t) == 0 {
		return nil, nil
	}
	if t[0] != types.RespArray {

	}

	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != types.RespArray {
		return c.splitArgs(line)
	}
	n, err := strconv.Atoi(bstring(line[1:]))
	if err != nil || n > MaxMultiBulkLength {
		return nil, fmt.Errorf("Protocol error: invalid multibulk length")
	}
	if n <= 0 {
		return nil, nil
	}
	args := c.args
	if cap(args) < n {
		args = slices.Grow(args, n)
	}
	for len(args) < cap(args) {
		args = append(args, nil)
	}
	for i := 0; i < n; i++ {
		line, err := c.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			return nil, fmt.Errorf("Protocol error: expected '%c', got empty string", types.RespString)
		}
		if line[0] != types.RespString {
			return nil, fmt.Errorf("Protocol error: expected '%c', got '%c'", types.RespString, line[0])
		}
		l, err := strconv.Atoi(bstring(line[1:]))
		if err != nil || l < 0 || l > MaxBulkLength {
			return nil, fmt.Errorf("Protocol error: invalid bulk length")
		}
		buf := args[i]
		if cap(buf) < l+2 {
			buf = slices.Grow(buf, l+2)
		}
		buf = buf[:l+2]
		if _, err := io.ReadFull(c.rd, buf); err != nil {
			return nil, err
		}
		args[i] = buf[:l]
	}
	c.args = args
	return args[:n], nil
}

// Close gracefully closes the client connection after flushing any pending writes.
func (c *ClientConn) Close() error {
	flushErr := c.wr.Flush()
	closeErr := c.conn.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// CloseWithError closes the client connection after writing an error response.
// This is a convenience function that combines WriteError and Close.
func (c *ClientConn) CloseWithError(err error) error {
	if err != nil {
		c.WriteError(err)
	}
	return c.Close()
}

// Flush flushes the write buffer of the client connection.
// It is ignored if the connection is set to not flush after every write (see SetFlush) or if the read buffer is not empty and SetForceFlush is not set to true.
func (c *ClientConn) Flush() error {
	if c.doflush && (c.forceflush || c.rd.Buffered() == 0) {
		flushErr := c.wr.Flush()
		if flushErr != nil {
			c.Close()
			return flushErr
		}
	}
	return nil
}

func (c *ClientConn) write(b []byte) error {
	_, err := c.wr.Write(b)
	if err != nil {
		c.Close()
		return err
	}
	if err := c.Flush(); err != nil {
		return err
	}
	return err
}

func (c *ClientConn) writeWithType(typeByte byte, head []byte, body []byte) error {
	replySize := 1 + len(head) + len(types.CRLF)
	if body != nil {
		replySize += len(body) + len(types.CRLF)
	}
	replyBuf := slices.Grow(c.writeBuf, replySize)
	replyBuf = append(replyBuf, typeByte)
	if len(head) > 0 {
		replyBuf = append(replyBuf, SanitizeSimpleString(head)...)
	}
	replyBuf = append(replyBuf, types.CRLF...)
	if body != nil {
		replyBuf = append(replyBuf, body...)
		replyBuf = append(replyBuf, types.CRLF...)
	}
	c.writeBuf = replyBuf[:0]
	return c.write(replyBuf)
}

func (c *ClientConn) writeWithPrefix(prefix []byte, head []byte, body []byte) error {
	replySize := len(prefix) + len(head) + len(types.CRLF)
	if body != nil {
		replySize += len(body) + len(types.CRLF)
	}
	replyBuf := slices.Grow(c.writeBuf, replySize)
	replyBuf = append(replyBuf, prefix...)
	if len(head) > 0 {
		replyBuf = append(replyBuf, SanitizeSimpleString(head)...)
	}
	replyBuf = append(replyBuf, types.CRLF...)
	if body != nil {
		replyBuf = append(replyBuf, body...)
		replyBuf = append(replyBuf, types.CRLF...)
	}
	c.writeBuf = replyBuf[:0]
	return c.write(replyBuf)
}

// WriteSimpleString writes a [Simple string](https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-string-reply).
func (c *ClientConn) WriteStatusBytes(v []byte) error {
	return c.writeWithType(types.RespStatus, v, nil)
}

// WriteSimpleString writes a [Simple string](https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-string-reply).
func (c *ClientConn) WriteStatusString(v string) error {
	return c.WriteStatusBytes(unsafeGetBytes(v))
}

// WriteError writes a sanitized [Simple error](https://redis.io/docs/latest/develop/reference/protocol-spec/#error-reply).
func (c *ClientConn) WriteError(e error) error {
	return c.writeWithPrefix(static.RespPrefixedErrorBytes, unsafeGetBytes(e.Error()), nil)
}

// WriteOK is a convenience method for calling WriteStatusBytes with "OK".
func (c *ClientConn) WriteOK() error {
	return c.writeWithPrefix(static.RespPrefixedOKBytes, nil, nil)
}

// WriteBytes writes a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply).
func (c *ClientConn) WriteBytes(v []byte) error {
	if v == nil {
		v = static.NullBytes
	}
	return c.writeWithType(types.RespString, strconv.AppendInt(nil, int64(len(v)), 10), v)
}

// WriteString writes a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply).
func (c *ClientConn) WriteString(v string) error {
	return c.WriteBytes(unsafeGetBytes(v))
}

// WriteInt writes an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply).
func (c *ClientConn) WriteInt(v int) error {
	return c.WriteInt64(int64(v))
}

// WriteInt64 writes an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply).
func (c *ClientConn) WriteInt64(v int64) error {
	return c.writeWithType(types.RespInt, strconv.AppendInt(nil, v, 10), nil)
}

// WriteExplicitNullString writes a [Null Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-reply).
func (c *ClientConn) WriteExplicitNullString() error {
	return c.writeWithPrefix(static.RespPrefixedNullStringBytes, nil, nil)
}

// WriteExplicitNullArray writes a [Null Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-array-reply).
func (c *ClientConn) WriteExplicitNullArray() error {
	return c.writeWithPrefix(static.RespPrefixedNullArrayBytes, nil, nil)
}

// WriteNullString writes a [Null](https://redis.io/docs/latest/develop/reference/protocol-spec/#null-reply) for RESP3 connections or a [Null Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-reply) for RESP2 connections.
func (c *ClientConn) WriteNullString() error {
	if c.resp2compat {
		return c.WriteExplicitNullString()
	}
	return c.writeWithType(types.RespNil, nil, nil)
}

// WriteNullArray writes a [Null](https://redis.io/docs/latest/develop/reference/protocol-spec/#null-reply) for RESP3 connections or a [Null Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-array-reply) for RESP2 connections.
func (c *ClientConn) WriteNullArray() error {
	if c.resp2compat {
		return c.WriteExplicitNullArray()
	}
	return c.writeWithType(types.RespNil, nil, nil)
}

// WriteFloat writes a [Double](https://redis.io/docs/latest/develop/reference/protocol-spec/#double-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) containing the string representation of the float for RESP2 connections.
func (c *ClientConn) WriteFloat(v float64) error {
	if c.resp2compat {
		return c.WriteBytes(strconv.AppendFloat(nil, v, 'g', -1, 64))
	}
	return c.writeWithType(types.RespFloat, strconv.AppendFloat(nil, v, 'g', -1, 64), nil)
}

// WriteBool writes a [Boolean](https://redis.io/docs/latest/develop/reference/protocol-spec/#boolean-reply) for RESP3 connections or an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply) of `0` or `1` for RESP2 connections.
func (c *ClientConn) WriteBool(v bool) error {
	if c.resp2compat {
		if v {
			return c.WriteInt(1)
		} else {
			return c.WriteInt(0)
		}
	}
	if v {
		return c.writeWithPrefix(static.RespPrefixedBoolTrueBytes, nil, nil)
	}
	return c.writeWithPrefix(static.RespPrefixedBoolFalseBytes, nil, nil)
}

// WriteBlobError writes a [Bulk error](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-error-reply) for RESP3 connections or a sanitized [Simple error](https://redis.io/docs/latest/develop/reference/protocol-spec/#error-reply) for RESP2 connections.
func (c *ClientConn) WriteBlobError(e error) error {
	if c.resp2compat {
		return c.WriteError(e)
	}
	v := unsafeGetBytes(e.Error())
	return c.writeWithType(types.RespBlobError, strconv.AppendInt(nil, int64(len(v)), 10), v)
}

// WriteBlobString writes a [Verbatim string](https://redis.io/docs/latest/develop/reference/protocol-spec/#verbatim-string-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) for RESP2 connections.
// The verbatim string needs to contain the data encoding part and the data itself. Example: `txt:Arbitrary text data`. The data encoding part is stripped when sending the data as Bulk string to RESP2 clients.
func (c *ClientConn) WriteVerbatimBytes(v []byte) error {
	if c.resp2compat {
		if len(v) >= 4 {
			return c.WriteBytes(v[4:])
		}
		return c.WriteBytes(v)
	}
	if v == nil {
		v = static.NullBytes
	}
	return c.writeWithType(types.RespVerbatim, strconv.AppendInt(nil, int64(len(v)), 10), v)
}

// WriteBlobString writes a [Verbatim string](https://redis.io/docs/latest/develop/reference/protocol-spec/#verbatim-string-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) for RESP2 connections.
// The verbatim string needs to contain the data encoding part and the data itself. Example: `txt:Arbitrary text data`. The data encoding part is stripped when sending the data as Bulk string to RESP2 clients.
func (c *ClientConn) WriteVerbatimString(v string) error {
	return c.WriteVerbatimBytes(unsafeGetBytes(v))
}

// WriteBigInt writes a [Big number](https://redis.io/docs/latest/develop/reference/protocol-spec/#big-number-reply) for RESP3 connections or an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply) for RESP2 connections.
// If v cannot be represented in an int64, the result is undefined when sending to a RESP2 client.
func (c *ClientConn) WriteBigInt(v big.Int) error {
	if c.resp2compat {
		return c.WriteInt64(v.Int64())
	}
	return c.writeWithType(types.RespBigInt, v.Append(nil, 10), nil)
}

// WriteArrayHeader writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header.
func (c *ClientConn) WriteArrayHeader(l int) error {
	return c.writeWithType(types.RespArray, strconv.AppendInt(nil, int64(l), 10), nil)
}

// WriteArrayBytes is a convenience method to write an array of Bulk strings. It is recommended to use WriteArrayHeader in combination with WriteBytes and SetFlush for better performance.
func (c *ClientConn) WriteArrayBytes(v [][]byte) error {
	defer c.SetFlush(c.SetFlush(false))
	if err := c.WriteArrayHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteArrayString is a convenience method to write an array of Bulk strings. It is recommended to use WriteArrayHeader in combination with WriteString and SetFlush for better performance.
func (c *ClientConn) WriteArrayString(v []string) error {
	defer c.SetFlush(c.SetFlush(false))
	if err := c.WriteArrayHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteMapHeader writes a [Map](https://redis.io/docs/latest/develop/reference/protocol-spec/#map-reply) header with the specified length. For RESP2 clients, this writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header with twice the specified length.
func (c *ClientConn) WriteMapHeader(l int) error {
	if c.resp2compat {
		return c.WriteArrayHeader(l * 2)
	}
	return c.writeWithType(types.RespMap, strconv.AppendInt(nil, int64(l), 10), nil)
}

// WriteMapBytes is a convenience method to write a map of Bulk strings. It is recommended to use WriteMapHeader in combination with WriteBytes and SetFlush for better performance.
func (c *ClientConn) WriteMapBytes(v map[string][]byte) error {
	defer c.SetFlush(c.SetFlush(false))
	if err := c.WriteMapHeader(len(v)); err != nil {
		return err
	}
	for k, arg := range v {
		if err := c.WriteString(k); err != nil {
			return err
		}
		if err := c.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteMapString is a convenience method to write a map of Bulk strings. It is recommended to use WriteMapHeader in combination with WriteString and SetFlush for better performance.
func (c *ClientConn) WriteMapString(v map[string]string) error {
	defer c.SetFlush(c.SetFlush(false))
	if err := c.WriteMapHeader(len(v)); err != nil {
		return err
	}
	for k, arg := range v {
		if err := c.WriteString(k); err != nil {
			return err
		}
		if err := c.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteSetHeader writes a [Set](https://redis.io/docs/latest/develop/reference/protocol-spec/#set-reply) header with the specified length. For RESP2 clients, this writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header with twice the specified length.
func (c *ClientConn) WriteSetHeader(l int) error {
	if c.resp2compat {
		return c.WriteArrayHeader(l)
	}
	return c.writeWithType(types.RespSet, strconv.AppendInt(nil, int64(l), 10), nil)
}

// WriteSetBytes is a convenience method to write a set of Bulk strings. It is recommended to use WriteSetHeader in combination with WriteBytes and SetFlush for better performance.
func (c *ClientConn) WriteSetBytes(v [][]byte) error {
	defer c.SetFlush(c.SetFlush(false))
	if err := c.WriteSetHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteSetString is a convenience method to write a set of Bulk strings. It is recommended to use WriteSetHeader in combination with WriteString and SetFlush for better performance.
func (c *ClientConn) WriteSetString(v []string) error {
	defer c.SetFlush(c.SetFlush(false))
	if err := c.WriteSetHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteString(arg); err != nil {
			return err
		}
	}
	return nil

}

// WriteAttrHeader writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) header with the specified length. For RESP2 clients, function calls are silently discarded and no data is written.
func (c *ClientConn) WriteAttrBytes(v map[string][]byte) error {
	if c.resp2compat {
		return nil
	}
	defer c.SetFlush(c.SetFlush(false))
	if err := c.writeWithType(types.RespAttr, strconv.AppendInt(nil, int64(len(v)), 10), nil); err != nil {
		return err
	}
	for k, arg := range v {
		if err := c.WriteString(k); err != nil {
			return err
		}
		if err := c.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteAttrString writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) header with the specified length. For RESP2 clients, function calls are silently discarded and no data is written.
func (c *ClientConn) WriteAttrString(v map[string]string) error {
	if c.resp2compat {
		return nil
	}
	defer c.SetFlush(c.SetFlush(false))
	if err := c.writeWithType(types.RespAttr, strconv.AppendInt(nil, int64(len(v)), 10), nil); err != nil {
		return err
	}
	for k, arg := range v {
		if err := c.WriteString(k); err != nil {
			return err
		}
		if err := c.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 clients, function calls are silently discarded and no data is written.
// Note that care must be taken when concurrently reading and writing data on the same connection. See SetForceFlush for more information.
func (c *ClientConn) WritePushBytes(v [][]byte) error {
	if c.resp2compat {
		return nil
	}
	defer c.SetFlush(c.SetFlush(false))
	if err := c.writeWithType(types.RespPush, strconv.AppendInt(nil, int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 clients, function calls are silently discarded and no data is written.
// Note that care must be taken when concurrently reading and writing data on the same connection. See SetForceFlush for more information.
func (c *ClientConn) WritePushString(v []string) error {
	if c.resp2compat {
		return nil
	}
	defer c.SetFlush(c.SetFlush(false))
	if err := c.writeWithType(types.RespPush, strconv.AppendInt(nil, int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}
