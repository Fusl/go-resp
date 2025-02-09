package resp

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/Fusl/go-resp/doublebuffer"
	"github.com/Fusl/go-resp/static"
	"github.com/Fusl/go-resp/types"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync/atomic"
)

type Server struct {
	rd                 *bufio.Reader
	wr                 *doublebuffer.DoubleBuffer
	conn               net.Conn
	writeBuf           []byte
	readBuffer         []byte
	args               [][]byte
	argRefs            []int
	resp2compat        bool
	hasBuffered        atomic.Bool
	maxMultiBulkLength int
	maxBulkLength      int
	maxBufferSize      int
	err                error
	appendIntBuf       []byte
}

type ServerOptions struct {
	// MaxMultiBulkLength sets the maximum number of elements in a multi-bulk command request.
	MaxMultiBulkLength *int

	// MaxBulkLength sets the maximum length of a bulk string in bytes.
	MaxBulkLength *int

	// MaxBufferSize sets the maximum size of the buffer used to read full commands from the client.
	MaxBufferSize *int

	// RESP2Compat sets the RESP2 compatibility mode of the incoming connection.
	// This mode is useful when talking with a RESP2 client that does not support the new RESP3 types.
	// In this mode, Write functions will convert RESP3 types to RESP2 types where possible.
	RESP2Compat *bool
}

// NewServer returns a new wrapped incoming connection with sane default parameters set.
func NewServer(conn net.Conn) *Server {
	c := &Server{
		rd:                 bufio.NewReaderSize(conn, 65536),
		wr:                 doublebuffer.NewWriterSize(conn, 65536),
		maxMultiBulkLength: MaxMultiBulkLength,
		maxBulkLength:      MaxBulkLength,
		maxBufferSize:      MaxMultiBulkLength * MaxBulkLength,
		conn:               conn,
		appendIntBuf:       make([]byte, 0, 20),
	}
	return c
}

// SetOptions sets the options for the incoming connection.
func (c *Server) SetOptions(opts ServerOptions) error {
	if opts.MaxMultiBulkLength != nil {
		c.maxMultiBulkLength = *opts.MaxMultiBulkLength
	}
	if opts.MaxBulkLength != nil {
		c.maxBulkLength = *opts.MaxBulkLength
	}
	if opts.MaxBufferSize != nil {
		c.maxBufferSize = *opts.MaxBufferSize
	}
	if opts.RESP2Compat != nil {
		c.resp2compat = *opts.RESP2Compat
	}
	return nil
}

// SetRESP2Compat sets the RESP2 compatibility mode of the incoming connection.
// This mode is useful when talking with a RESP2 client that does not support the new RESP3 types.
// In this mode, Write functions will convert RESP3 types to RESP2 types where possible.
func (c *Server) SetRESP2Compat(v bool) {
	c.resp2compat = v
}

// Next reads the next command from the incoming connection and returns it as a slice of byte slices.
// The returned slice is only valid until the next call to Next as it is reused for each command.
// If in doubt, copy the slice and every byte slice it contains to a newly allocated slice and byte slices.
func (c *Server) Next() ([][]byte, error) {
	if err := c.err; err != nil {
		return nil, err
	}
	for {
		if args, err := c.next(); err != nil {
			c.err = err
			return args, err
		} else if len(args) > 0 {
			return args, err
		}
	}
}

func (c *Server) collectFragmentsSize(delim byte, maxLen int) (full []byte, err error) {
	var frag []byte
	fullLen := 0
	fragLen := 0
	for {
		var e error
		frag, e = c.rd.ReadSlice(delim)
		fragLen = len(frag)

		// if the full + fragment buffer has grown too large, return an error
		if fullLen+fragLen > maxLen {
			err = bufio.ErrBufferFull
			break
		}

		// if read returns no error, we have a full line, return it
		if e == nil {
			break
		}

		// if read returns an error other than bufio.ErrBufferFull, return the error
		if !errors.Is(e, bufio.ErrBufferFull) {
			err = e
			break
		}

		// copy the fragment to the full buffer so we can reuse the fragment buffer
		full = append(full, frag...)
		fullLen += fragLen
	}

	if fullLen <= 0 {
		return frag, err
	}
	if fragLen > 0 {
		full = append(full, frag...)
	}
	return full, err
}

func (c *Server) readLine() ([]byte, error) {
	b, err := c.collectFragmentsSize('\n', c.maxBufferSize)
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

func (c *Server) splitArgs(line []byte) ([][]byte, error) {
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
	return nil, ErrProtoUnbalancedQuotes
}

func (c *Server) setBuffered() {
	c.hasBuffered.Store(c.rd.Buffered() > 0)
}

func (c *Server) next() ([][]byte, error) {
	defer c.setBuffered()
	t, err := c.rd.Peek(1)
	if err != nil {
		return nil, err
	}
	if len(t) == 0 {
		return nil, nil
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
	n32, err := ParseUInt32(line[1:])
	n := int(n32)
	if err != nil || n > c.maxMultiBulkLength {
		return nil, ErrProtoInvalidMultiBulkLength
	}
	if n <= 0 {
		return nil, nil
	}
	args := Expand(c.args, n)
	argRefs := Expand(c.argRefs, int(n)*2)
	c.args = args
	c.argRefs = argRefs
	readBuffer := c.readBuffer
	p := 0
	for i := 0; i < n; i++ {
		line, err := c.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != types.RespString {
			return nil, ErrProtoExpectedString
		}
		l32, err := ParseUInt32(line[1:])
		l := int(l32)
		if err != nil || l < 0 || l > c.maxBulkLength {
			return nil, ErrProtoInvalidBulkLength
		}
		if p+l+2 > c.maxBufferSize {
			return nil, bufio.ErrBufferFull
		}
		readBuffer = Expand(readBuffer, p+l+2)
		readBufferChunk := readBuffer[p : p+l+2]
		argRefs[i*2] = p
		argRefs[i*2+1] = l
		if _, err := io.ReadFull(c.rd, readBufferChunk); err != nil {
			return nil, err
		}
		p += l
	}
	c.readBuffer = readBuffer
	for i := 0; i < n; i++ {
		args[i] = readBuffer[argRefs[i*2] : argRefs[i*2]+argRefs[i*2+1]]
	}
	return args[:n], nil
}

// Close gracefully closes the incoming connection after flushing any pending writes.
func (c *Server) Close() error {
	return c.conn.Close()
}

// CloseWithError closes the incoming connection after writing an error response.
// This is a convenience function that combines WriteError and Close.
func (c *Server) CloseWithError(err error) error {
	if err != nil {
		c.WriteError(err)
	}
	return c.Close()
}

func (c *Server) write(b []byte) error {
	_, err := c.wr.Write(b)
	return err
}

var crlfLen = len(types.CRLF)

func (c *Server) writeWithType(typeByte byte, head []byte, body []byte) error {
	headLen := len(head)
	bodyLen := len(body)
	replySize := 1 + headLen + crlfLen
	if body != nil {
		replySize += bodyLen + crlfLen
	}
	replyBuf := Expand(c.writeBuf, replySize)
	replyBuf[0] = typeByte
	p := 1
	if headLen > 0 {
		copy(replyBuf[p:], SanitizeSimpleString(head))
		p += headLen
	}
	copy(replyBuf[p:], types.CRLF)
	p += crlfLen
	if body != nil {
		copy(replyBuf[p:], body)
		p += bodyLen
		copy(replyBuf[p:], types.CRLF)
		p += crlfLen
	}
	c.writeBuf = replyBuf
	return c.write(replyBuf[:p])
}

func (c *Server) writeWithPrefix(prefix []byte, head []byte, body []byte) error {
	prefixLen := len(prefix)
	headLen := len(head)
	bodyLen := len(body)
	replySize := prefixLen + headLen + crlfLen
	if body != nil {
		replySize += bodyLen + crlfLen
	}
	replyBuf := Expand(c.writeBuf, replySize)
	copy(replyBuf, prefix)
	p := prefixLen
	if headLen > 0 {
		copy(replyBuf[p:], SanitizeSimpleString(head))
		p += headLen
	}
	copy(replyBuf[p:], types.CRLF)
	p += crlfLen
	if body != nil {
		copy(replyBuf[p:], body)
		p += bodyLen
		copy(replyBuf[p:], types.CRLF)
		p += crlfLen
	}
	c.writeBuf = replyBuf
	return c.write(replyBuf[:p])
}

// WriteSimpleString writes a [Simple string](https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-string-reply).
func (c *Server) WriteStatusBytes(v []byte) error {
	return c.writeWithType(types.RespStatus, v, nil)
}

// WriteSimpleString writes a [Simple string](https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-string-reply).
func (c *Server) WriteStatusString(v string) error {
	return c.WriteStatusBytes(sbytes(v))
}

// WriteError writes a sanitized [Simple error](https://redis.io/docs/latest/develop/reference/protocol-spec/#error-reply).
func (c *Server) WriteError(e error) error {
	return c.writeWithPrefix(static.RespPrefixedErrorBytes, sbytes(e.Error()), nil)
}

// WriteOK is a convenience method for calling WriteStatusBytes with "OK".
func (c *Server) WriteOK() error {
	return c.writeWithPrefix(static.RespPrefixedOKBytes, nil, nil)
}

// WriteBytes writes a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply).
func (c *Server) WriteBytes(v []byte) error {
	if v == nil {
		v = static.NullBytes
	}
	return c.writeWithType(types.RespString, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), v)
}

// WriteString writes a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply).
func (c *Server) WriteString(v string) error {
	return c.WriteBytes(sbytes(v))
}

// WriteInt writes an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply).
func (c *Server) WriteInt(v int) error {
	return c.WriteInt64(int64(v))
}

// WriteInt64 writes an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply).
func (c *Server) WriteInt64(v int64) error {
	return c.writeWithType(types.RespInt, strconv.AppendInt(c.appendIntBuf[:0], v, 10), nil)
}

// WriteExplicitNullString writes a [Null Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-reply).
func (c *Server) WriteExplicitNullString() error {
	return c.writeWithPrefix(static.RespPrefixedNullStringBytes, nil, nil)
}

// WriteExplicitNullArray writes a [Null Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-array-reply).
func (c *Server) WriteExplicitNullArray() error {
	return c.writeWithPrefix(static.RespPrefixedNullArrayBytes, nil, nil)
}

// WriteNullString writes a [Null](https://redis.io/docs/latest/develop/reference/protocol-spec/#null-reply) for RESP3 connections or a [Null Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-reply) for RESP2 connections.
func (c *Server) WriteNullString() error {
	if c.resp2compat {
		return c.WriteExplicitNullString()
	}
	return c.writeWithType(types.RespNil, nil, nil)
}

// WriteNullArray writes a [Null](https://redis.io/docs/latest/develop/reference/protocol-spec/#null-reply) for RESP3 connections or a [Null Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-array-reply) for RESP2 connections.
func (c *Server) WriteNullArray() error {
	if c.resp2compat {
		return c.WriteExplicitNullArray()
	}
	return c.writeWithType(types.RespNil, nil, nil)
}

// WriteFloat writes a [Double](https://redis.io/docs/latest/develop/reference/protocol-spec/#double-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) containing the string representation of the float for RESP2 connections.
func (c *Server) WriteFloat(v float64) error {
	if c.resp2compat {
		return c.WriteBytes(strconv.AppendFloat(nil, v, 'g', -1, 64))
	}
	return c.writeWithType(types.RespFloat, strconv.AppendFloat(nil, v, 'g', -1, 64), nil)
}

// WriteBool writes a [Boolean](https://redis.io/docs/latest/develop/reference/protocol-spec/#boolean-reply) for RESP3 connections or an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply) of `0` or `1` for RESP2 connections.
func (c *Server) WriteBool(v bool) error {
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
func (c *Server) WriteBlobError(e error) error {
	if c.resp2compat {
		return c.WriteError(e)
	}
	v := sbytes(e.Error())
	return c.writeWithType(types.RespBlobError, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), v)
}

// WriteBlobString writes a [Verbatim string](https://redis.io/docs/latest/develop/reference/protocol-spec/#verbatim-string-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) for RESP2 connections.
// The verbatim string needs to contain the data encoding part and the data itself. Example: `txt:Arbitrary text data`. The data encoding part is stripped when sending the data as Bulk string to RESP2 clients.
func (c *Server) WriteVerbatimBytes(v []byte) error {
	vLen := len(v)
	if c.resp2compat {
		if vLen >= 4 {
			return c.WriteBytes(v[4:])
		}
		return c.WriteBytes(v)
	}
	if v == nil {
		v = static.NullBytes
	}
	return c.writeWithType(types.RespVerbatim, strconv.AppendInt(c.appendIntBuf[:0], int64(vLen), 10), v)
}

// WriteBlobString writes a [Verbatim string](https://redis.io/docs/latest/develop/reference/protocol-spec/#verbatim-string-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) for RESP2 connections.
// The verbatim string needs to contain the data encoding part and the data itself. Example: `txt:Arbitrary text data`. The data encoding part is stripped when sending the data as Bulk string to RESP2 clients.
func (c *Server) WriteVerbatimString(v string) error {
	return c.WriteVerbatimBytes(sbytes(v))
}

// WriteBigInt writes a [Big number](https://redis.io/docs/latest/develop/reference/protocol-spec/#big-number-reply) for RESP3 connections or an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply) for RESP2 connections.
// If v cannot be represented in an int64, the result is undefined when sending to a RESP2 client.
func (c *Server) WriteBigInt(v big.Int) error {
	if c.resp2compat {
		return c.WriteInt64(v.Int64())
	}
	return c.writeWithType(types.RespBigInt, v.Append(nil, 10), nil)
}

// WriteArrayHeader writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header.
func (c *Server) WriteArrayHeader(l int) error {
	return c.writeWithType(types.RespArray, strconv.AppendInt(c.appendIntBuf[:0], int64(l), 10), nil)
}

// WriteArrayBytes is a convenience method to write an array of Bulk strings. It is recommended to use WriteArrayHeader in combination with WriteBytes and SetFlush for better performance.
func (c *Server) WriteArrayBytes(v [][]byte) error {
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
func (c *Server) WriteArrayString(v []string) error {
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

// WriteArray writes an array of any values that can be converted to a RESP type.
func (c *Server) WriteArray(v []any) error {
	if err := c.WriteArrayHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteMapHeader writes a [Map](https://redis.io/docs/latest/develop/reference/protocol-spec/#map-reply) header with the specified length. For RESP2 connections, this writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header with twice the specified length.
func (c *Server) WriteMapHeader(l int) error {
	if c.resp2compat {
		return c.WriteArrayHeader(l * 2)
	}
	return c.writeWithType(types.RespMap, strconv.AppendInt(c.appendIntBuf[:0], int64(l), 10), nil)
}

// WriteMapBytes is a convenience method to write a map of Bulk strings. It is recommended to use WriteMapHeader in combination with WriteBytes and SetFlush for better performance.
func (c *Server) WriteMapBytes(v map[string][]byte) error {
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
func (c *Server) WriteMapString(v map[string]string) error {
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

// WriteMap writes a map of any values that can be converted to a RESP type.
func (c *Server) WriteMap(v map[string]any) error {
	if err := c.WriteMapHeader(len(v)); err != nil {
		return err
	}
	for k, arg := range v {
		if err := c.WriteString(k); err != nil {
			return err
		}
		if err := c.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteSetHeader writes a [Set](https://redis.io/docs/latest/develop/reference/protocol-spec/#set-reply) header with the specified length. For RESP2 connections, this writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header with twice the specified length.
func (c *Server) WriteSetHeader(l int) error {
	if c.resp2compat {
		return c.WriteArrayHeader(l)
	}
	return c.writeWithType(types.RespSet, strconv.AppendInt(c.appendIntBuf[:0], int64(l), 10), nil)
}

// WriteSetBytes is a convenience method to write a set of Bulk strings. It is recommended to use WriteSetHeader in combination with WriteBytes and SetFlush for better performance.
func (c *Server) WriteSetBytes(v [][]byte) error {
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
func (c *Server) WriteSetString(v []string) error {
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

// WriteSet writes a set of any values that can be converted to a RESP type.
func (c *Server) WriteSet(v []any) error {
	if err := c.WriteSetHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteAttrBytes writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) with the given data. For RESP2 connections, function calls are silently discarded and no data is written
func (c *Server) WriteAttrBytes(v map[string][]byte) error {
	if c.resp2compat {
		return nil
	}
	if err := c.writeWithType(types.RespAttr, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
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

// WriteAttrString writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) with the given data. For RESP2 connections, function calls are silently discarded and no data is written
func (c *Server) WriteAttrString(v map[string]string) error {
	if c.resp2compat {
		return nil
	}
	if err := c.writeWithType(types.RespAttr, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
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

// WriteAttr writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) with the given data. For RESP2 connections, function calls are silently discarded and no data is written
func (c *Server) WriteAttr(v map[string]any) error {
	if c.resp2compat {
		return nil
	}
	if err := c.writeWithType(types.RespAttr, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for k, arg := range v {
		if err := c.WriteString(k); err != nil {
			return err
		}
		if err := c.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 connections, function calls are silently discarded and no data is written.
// Note that care must be taken when concurrently reading and writing data on the same connection. See SetForceFlush for more information.
func (c *Server) WritePushBytes(v [][]byte) error {
	if c.resp2compat {
		return nil
	}
	if err := c.writeWithType(types.RespPush, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 connections, function calls are silently discarded and no data is written.
// Note that care must be taken when concurrently reading and writing data on the same connection. See SetForceFlush for more information.
func (c *Server) WritePushString(v []string) error {
	if c.resp2compat {
		return nil
	}
	if err := c.writeWithType(types.RespPush, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 connections, function calls are silently discarded and no data is written.
func (c *Server) WritePush(v []any) error {
	if c.resp2compat {
		return nil
	}
	if err := c.writeWithType(types.RespPush, strconv.AppendInt(c.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := c.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// Write writes any value that can be converted to a RESP type.
func (c *Server) Write(v any) error {
	switch v := v.(type) {
	case []byte:
		return c.WriteBytes(v)
	case string:
		return c.WriteString(v)
	case int:
		return c.WriteInt(v)
	case int32:
		return c.WriteInt(int(v))
	case int64:
		return c.WriteInt64(v)
	case float64:
		return c.WriteFloat(v)
	case bool:
		return c.WriteBool(v)
	case error:
		return c.WriteError(v)
	case nil:
		return c.WriteNullString()
	case []any:
		return c.WriteArray(v)
	case map[string]any:
		return c.WriteMap(v)
	case big.Int:
		return c.WriteBigInt(v)
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
}
