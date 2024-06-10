package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"resp/static"
	"resp/types"
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

func NewClientConn(conn net.Conn) *ClientConn {
	c := &ClientConn{
		rd:      bufio.NewReader(conn),
		wr:      bufio.NewWriter(conn),
		conn:    conn,
		doflush: true,
	}
	return c
}

func (c *ClientConn) SetFlush(v bool) bool {
	prev := c.doflush
	c.doflush = v
	if !prev && v {
		c.Flush()
	}
	return prev
}

func (c *ClientConn) SetForceFlush(v bool) bool {
	prev := c.forceflush
	c.forceflush = v
	return prev
}

func (c *ClientConn) SetRESP2Compat(v bool) {
	c.resp2compat = v
}

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

func (c *ClientConn) Close() error {
	flushErr := c.wr.Flush()
	closeErr := c.conn.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

func (c *ClientConn) CloseWithError(err error) error {
	if err != nil {
		c.WriteError(err)
	}
	return c.Close()
}

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

func (c *ClientConn) WriteStatusBytes(v []byte) error {
	return c.writeWithType(types.RespStatus, v, nil)
}

func (c *ClientConn) WriteStatusString(v string) error {
	return c.WriteStatusBytes(unsafeGetBytes(v))
}

func (c *ClientConn) WriteError(e error) error {
	return c.writeWithPrefix(static.RespPrefixedErrorBytes, unsafeGetBytes(e.Error()), nil)
}

func (c *ClientConn) WriteOK() error {
	return c.writeWithPrefix(static.RespPrefixedOKBytes, nil, nil)
}

func (c *ClientConn) WriteBytes(v []byte) error {
	if v == nil {
		v = static.NullBytes
	}
	return c.writeWithType(types.RespString, strconv.AppendInt(nil, int64(len(v)), 10), v)
}

func (c *ClientConn) WriteString(v string) error {
	return c.WriteBytes(unsafeGetBytes(v))
}

func (c *ClientConn) WriteInt(v int) error {
	return c.WriteInt64(int64(v))
}

func (c *ClientConn) WriteInt64(v int64) error {
	return c.writeWithType(types.RespInt, strconv.AppendInt(nil, v, 10), nil)
}

func (c *ClientConn) WriteExplicitNullString() error {
	return c.writeWithPrefix(static.RespPrefixedNullStringBytes, nil, nil)
}

func (c *ClientConn) WriteExplicitNullArray() error {
	return c.writeWithPrefix(static.RespPrefixedNullArrayBytes, nil, nil)
}

func (c *ClientConn) WriteNullString() error {
	if c.resp2compat {
		return c.WriteExplicitNullString()
	}
	return c.writeWithType(types.RespNil, nil, nil)
}

func (c *ClientConn) WriteNullArray() error {
	if c.resp2compat {
		return c.WriteExplicitNullArray()
	}
	return c.writeWithType(types.RespNil, nil, nil)
}

func (c *ClientConn) WriteFloat(v float64) error {
	if c.resp2compat {
		return c.WriteBytes(strconv.AppendFloat(nil, v, 'g', -1, 64))
	}
	return c.writeWithType(types.RespFloat, strconv.AppendFloat(nil, v, 'g', -1, 64), nil)
}

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

func (c *ClientConn) WriteBlobError(e error) error {
	if c.resp2compat {
		return c.WriteError(e)
	}
	v := unsafeGetBytes(e.Error())
	return c.writeWithType(types.RespBlobError, strconv.AppendInt(nil, int64(len(v)), 10), v)
}

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

func (c *ClientConn) WriteVerbatimString(v string) error {
	return c.WriteVerbatimBytes(unsafeGetBytes(v))
}

func (c *ClientConn) WriteBigInt(v big.Int) error {
	if c.resp2compat {
		return c.WriteInt64(v.Int64())
	}
	return c.writeWithType(types.RespBigInt, v.Append(nil, 10), nil)
}

func (c *ClientConn) WriteArrayHeader(l int) error {
	return c.writeWithType(types.RespArray, strconv.AppendInt(nil, int64(l), 10), nil)
}

// WriteArrayBytes is a convenience function to write an array of byte slices.
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

// WriteArrayString is a convenience function to write an array of strings.
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

func (c *ClientConn) WriteMapHeader(l int) error {
	if c.resp2compat {
		return c.WriteArrayHeader(l * 2)
	}
	return c.writeWithType(types.RespMap, strconv.AppendInt(nil, int64(l), 10), nil)
}

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

func (c *ClientConn) WriteSetHeader(l int) error {
	if c.resp2compat {
		return c.WriteArrayHeader(l)
	}
	return c.writeWithType(types.RespSet, strconv.AppendInt(nil, int64(l), 10), nil)
}

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
