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
	"strconv"
)

const ReaderBufferSize = 65536
const WriterBufferSize = 65536

type Server struct {
	rd   *bufio.Reader
	wr   *doublebuffer.DoubleBuffer
	rerr error
	werr error

	// buffers
	writeBuf     []byte   // holds temporary data used to merge write calls
	appendIntBuf []byte   // holds temporary data used to append integers
	argsBuffer   []byte   // holds all the arguments back to back
	argsRefs     [][]byte // holds slice references to the arguments in argsBuffer
	argsBounds   []int    // holds the start pos and len of each argument in argsBuffer

	// limits
	maxMultiBulkLength int
	maxBulkLength      int
	maxBufferSize      int

	resp2compat bool
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
func NewServer(rw io.ReadWriter) *Server {
	s := &Server{
		rd:                 bufio.NewReaderSize(rw, ReaderBufferSize),
		wr:                 doublebuffer.NewWriterSize(rw, WriterBufferSize),
		maxMultiBulkLength: MaxMultiBulkLength,
		maxBulkLength:      MaxBulkLength,
		maxBufferSize:      MaxMultiBulkLength * MaxBulkLength,
		appendIntBuf:       make([]byte, 0, 20),
	}
	return s
}

// ResetReader starts reading commands from the newly passed reader. Mostly used for testing purposes.
func (s *Server) ResetReader(r io.Reader) {
	s.rd.Reset(r)
	s.rerr = nil
}

// Reset starts reading and writing commands from the newly passed reader and writer. Mostly used for testing purposes.
func (s *Server) Reset(rw io.ReadWriter) {
	s.rd.Reset(rw)
	s.wr.Reset(rw)
	s.rerr = nil
	s.werr = nil
}

// SetOptions sets the options for the incoming connection.
func (s *Server) SetOptions(opts ServerOptions) error {
	if opts.MaxMultiBulkLength != nil {
		s.maxMultiBulkLength = *opts.MaxMultiBulkLength
	}
	if opts.MaxBulkLength != nil {
		s.maxBulkLength = *opts.MaxBulkLength
	}
	if opts.MaxBufferSize != nil {
		s.maxBufferSize = *opts.MaxBufferSize
	}
	if opts.RESP2Compat != nil {
		s.resp2compat = *opts.RESP2Compat
	}
	return nil
}

// SetRESP2Compat sets the RESP2 compatibility mode of the incoming connection.
// This mode is useful when talking with a RESP2 client that does not support the new RESP3 types.
// In this mode, Write functions will convert RESP3 types to RESP2 types where possible.
func (s *Server) SetRESP2Compat(v bool) {
	s.resp2compat = v
}

// Next reads the next command from the incoming connection and returns it as a slice of byte slices.
// The returned slice is only valid until the next call to Next as it is reused for each command.
// If in doubt, copy the slice and every byte slice it contains to a newly allocated slice and byte slices.
func (s *Server) Next() ([][]byte, error) {
	if err := s.rerr; err != nil {
		return nil, err
	}
	for {
		if args, err := s.next(); err != nil {
			s.rerr = err
			return args, err
		} else if len(args) > 0 {
			return args, err
		}
	}
}

func (s *Server) collectFragmentsSize(delim byte, maxLen int) (full []byte, err error) {
	var frag []byte
	fullLen := 0
	fragLen := 0
	for {
		var e error
		frag, e = s.rd.ReadSlice(delim)
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

func (s *Server) readLine() ([]byte, error) {
	b, err := s.collectFragmentsSize('\n', s.maxBufferSize)
	if err != nil {
		return b, err
	}
	l := len(b)
	if l > 1 && b[l-2] == '\r' {
		return b[:l-2], nil
	}
	return b[:l-1], nil
}

func (s *Server) readShortLine() ([]byte, error) {
	b, err := s.collectFragmentsSize('\n', s.rd.Size())
	if err != nil {
		return b, err
	}
	l := len(b)
	if l > 1 && b[l-2] == '\r' {
		return b[:l-2], nil
	}
	return b[:l-1], nil
}

func hexDigitToInt(c byte) int {
	if c >= '0' && c <= '9' {
		return int(c - '0')
	}
	c |= 'x' - 'X'
	if c >= 'a' && c <= 'f' {
		return 10 + int(c-'a')
	}
	return -1
}

var readArgsSpaceChars = [256]uint8{' ': 1, '\r': 1, '\n': 2, '\v': 3, '\f': 3, '\t': 1, '\x00': 1}

func (s *Server) readArgs() ([][]byte, error) {
	argsRefs := s.argsRefs
	argsBounds := s.argsBounds[:0]
	argsBuffer := s.argsBuffer[:0]
	defer func() {
		s.argsRefs = argsRefs
		s.argsBounds = argsBounds
		s.argsBuffer = argsBuffer
	}()
	argc := 0
	barrier := 0

	indq := false // set to true if we are in "double quotes"
	insq := false // set to true if we are in 'single quotes'

	for {
		for {
			c, err := s.rd.ReadByte()
			if err != nil {
				return nil, err
			}
			switch readArgsSpaceChars[c] {
			case 1, 3: // spaces
				continue
			case 2: // newline (\n)
				if insq || indq {
					return nil, ErrProtoUnbalancedQuotes
				}
				argsRefs = Expand(argsRefs, argc)
				for i := 0; i < argc; i++ {
					argsRefs[i] = argsBuffer[argsBounds[i*2] : argsBounds[i*2]+argsBounds[i*2+1]]
				}
				return argsRefs[:argc], nil
			}
			s.rd.UnreadByte()
			break
		}

	inner:
		for {
			c, err := s.rd.ReadByte()
			if err != nil {
				break // we handle read errors at the start of the outer loop
			}

			if indq {
				if c == '\n' {
					return nil, ErrProtoUnbalancedQuotes
				}
				if c == '\\' {
					peek, err := s.rd.Peek(3)
					if err != nil && !errors.Is(err, io.EOF) {
						return nil, err
					}
					if len(peek) >= 3 && peek[0] == 'x' {
						i0 := hexDigitToInt(peek[1])
						i1 := hexDigitToInt(peek[2])
						if i0 >= 0 && i1 >= 0 {
							c = byte(i0<<4 | i1)
							if len(argsBuffer)+1 > s.maxBufferSize {
								return nil, bufio.ErrBufferFull
							}
							argsBuffer = append(argsBuffer, c)
							s.rd.Discard(3)
							continue
						}
					}
					if len(peek) >= 1 {
						switch peek[0] {
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
							c = peek[0]
						}
						if len(argsBuffer)+1 > s.maxBufferSize {
							return nil, bufio.ErrBufferFull
						}
						argsBuffer = append(argsBuffer, c)
						s.rd.Discard(1)
						continue
					}
				}
				if c == '"' {
					peek, err := s.rd.Peek(1)
					if err != nil && !errors.Is(err, io.EOF) {
						return nil, err
					}
					if len(peek) > 0 && readArgsSpaceChars[peek[0]] == 0 {
						return nil, ErrProtoUnbalancedQuotes
					}
					indq = false
					break
				}
				if len(argsBuffer)+1 > s.maxBufferSize {
					return nil, bufio.ErrBufferFull
				}
				argsBuffer = append(argsBuffer, c)
				continue
			}
			if insq {
				if c == '\n' {
					return nil, ErrProtoUnbalancedQuotes
				}
				if c == '\\' {
					peek, err := s.rd.Peek(1)
					if err != nil && !errors.Is(err, io.EOF) {
						return nil, err
					}
					if len(peek) >= 1 && peek[0] == '\'' {
						c = '\''
						if len(argsBuffer)+1 > s.maxBufferSize {
							return nil, bufio.ErrBufferFull
						}
						argsBuffer = append(argsBuffer, c)
						s.rd.Discard(1)
						continue
					}
				}
				if c == '\'' {
					peek, err := s.rd.Peek(1)
					if err != nil && !errors.Is(err, io.EOF) {
						return nil, err
					}
					if len(peek) > 0 && readArgsSpaceChars[peek[0]] == 0 {
						return nil, ErrProtoUnbalancedQuotes
					}
					insq = false
					break
				}
				if len(argsBuffer)+1 > s.maxBufferSize {
					return nil, bufio.ErrBufferFull
				}
				argsBuffer = append(argsBuffer, c)
				continue
			}
			switch c {
			case '"':
				indq = true
			case '\'':
				insq = true
			default:
				switch readArgsSpaceChars[c] {
				case 1, 2:
					s.rd.UnreadByte()
					break inner
				}
				if len(argsBuffer)+1 > s.maxBufferSize {
					return nil, bufio.ErrBufferFull
				}
				argsBuffer = append(argsBuffer, c)
			}
		}
		size := len(argsBuffer) - barrier
		if size > s.maxBulkLength {
			return nil, ErrProtoInvalidBulkLength
		}
		argc++
		if argc > s.maxMultiBulkLength {
			return nil, ErrProtoInvalidMultiBulkLength
		}
		argsBounds = append(argsBounds, barrier, size)
		barrier = len(argsBuffer)
	}
}

func (s *Server) next() ([][]byte, error) {
	t, err := s.rd.Peek(1)
	if err != nil {
		return nil, err
	}
	if len(t) == 0 {
		return nil, nil
	}
	switch t[0] {
	case types.RespArray:
	default:
		return s.readArgs()
	}
	line, err := s.readShortLine()
	if err != nil {
		return nil, err
	}
	if len(line) <= 1 {
		return nil, nil
	}
	n32, err := ParseInt32(line[1:])
	n := int(n32)
	if err != nil || n > s.maxMultiBulkLength {
		return nil, ErrProtoInvalidMultiBulkLength
	}
	if n <= 0 {
		return nil, nil
	}

	argsRefs := Expand(s.argsRefs, n)
	argsBounds := Expand(s.argsBounds, int(n)*2)
	argsBuffer := s.argsBuffer
	defer func() {
		s.argsRefs = argsRefs
		s.argsBounds = argsBounds
		s.argsBuffer = argsBuffer
	}()

	p := 0
	for i := 0; i < n; i++ {
		line, err := s.readShortLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != types.RespString {
			return nil, ErrProtoExpectedString
		}
		l32, err := ParseUInt32(line[1:])
		l := int(l32)
		if err != nil || l < 0 || l > s.maxBulkLength {
			return nil, ErrProtoInvalidBulkLength
		}
		if p+l+2 > s.maxBufferSize {
			return nil, bufio.ErrBufferFull
		}
		argsBuffer = Expand(argsBuffer, p+l+2)
		readBufferChunk := argsBuffer[p : p+l+2]
		argsBounds[i*2] = p
		argsBounds[i*2+1] = l
		if _, err := io.ReadFull(s.rd, readBufferChunk); err != nil {
			return nil, err
		}
		p += l
	}
	for i := 0; i < n; i++ {
		argsRefs[i] = argsBuffer[argsBounds[i*2] : argsBounds[i*2]+argsBounds[i*2+1]]
	}
	return argsRefs[:n], nil
}

// Close gracefully closes the incoming connection after flushing any pending writes.
func (s *Server) Close() error {
	return s.wr.Close()
}

// CloseWithError closes the incoming connection after writing an error response.
// This is a convenience function that combines WriteError and Close.
func (s *Server) CloseWithError(err error) error {
	if err != nil {
		s.WriteError(err)
	}
	return s.Close()
}

func (s *Server) write(b []byte) error {
	if s.werr != nil {
		return s.werr
	}
	if len(b) == 0 {
		return nil
	}
	_, err := s.wr.Write(b)
	if err != nil {
		s.werr = err
	}
	return err
}

var crlfLen = len(types.CRLF)

func (s *Server) writeWithType(typeByte byte, head []byte, body []byte) error {
	headLen := len(head)
	bodyLen := len(body)
	replySize := 1 + headLen + crlfLen
	if body != nil {
		replySize += bodyLen + crlfLen
	}
	replyBuf := Expand(s.writeBuf, replySize)
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
	s.writeBuf = replyBuf
	return s.write(replyBuf[:p])
}

func (s *Server) writeWithPrefix(prefix []byte, head []byte, body []byte) error {
	prefixLen := len(prefix)
	headLen := len(head)
	bodyLen := len(body)
	replySize := prefixLen + headLen + crlfLen
	if body != nil {
		replySize += bodyLen + crlfLen
	}
	replyBuf := Expand(s.writeBuf, replySize)
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
	s.writeBuf = replyBuf
	return s.write(replyBuf[:p])
}

// WriteSimpleString writes a [Simple string](https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-string-reply).
func (s *Server) WriteStatusBytes(v []byte) error {
	return s.writeWithType(types.RespStatus, v, nil)
}

// WriteSimpleString writes a [Simple string](https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-string-reply).
func (s *Server) WriteStatusString(v string) error {
	return s.WriteStatusBytes(sbytes(v))
}

// WriteError writes a sanitized [Simple error](https://redis.io/docs/latest/develop/reference/protocol-spec/#error-reply).
func (s *Server) WriteError(e error) error {
	return s.writeWithPrefix(static.RespPrefixedErrorBytes, sbytes(e.Error()), nil)
}

// WriteOK is a convenience method for calling WriteStatusBytes with "OK".
func (s *Server) WriteOK() error {
	return s.writeWithPrefix(static.RespPrefixedOKBytes, nil, nil)
}

// WriteBytes writes a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply).
func (s *Server) WriteBytes(v []byte) error {
	if v == nil {
		v = static.NullBytes
	}
	return s.writeWithType(types.RespString, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), v)
}

// WriteString writes a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply).
func (s *Server) WriteString(v string) error {
	return s.WriteBytes(sbytes(v))
}

// WriteInt writes an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply).
func (s *Server) WriteInt(v int) error {
	return s.WriteInt64(int64(v))
}

// WriteInt64 writes an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply).
func (s *Server) WriteInt64(v int64) error {
	return s.writeWithType(types.RespInt, strconv.AppendInt(s.appendIntBuf[:0], v, 10), nil)
}

// WriteExplicitNullString writes a [Null Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-reply).
func (s *Server) WriteExplicitNullString() error {
	return s.writeWithPrefix(static.RespPrefixedNullStringBytes, nil, nil)
}

// WriteExplicitNullArray writes a [Null Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-array-reply).
func (s *Server) WriteExplicitNullArray() error {
	return s.writeWithPrefix(static.RespPrefixedNullArrayBytes, nil, nil)
}

// WriteNullString writes a [Null](https://redis.io/docs/latest/develop/reference/protocol-spec/#null-reply) for RESP3 connections or a [Null Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-reply) for RESP2 connections.
func (s *Server) WriteNullString() error {
	if s.resp2compat {
		return s.WriteExplicitNullString()
	}
	return s.writeWithType(types.RespNil, nil, nil)
}

// WriteNullArray writes a [Null](https://redis.io/docs/latest/develop/reference/protocol-spec/#null-reply) for RESP3 connections or a [Null Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#nil-array-reply) for RESP2 connections.
func (s *Server) WriteNullArray() error {
	if s.resp2compat {
		return s.WriteExplicitNullArray()
	}
	return s.writeWithType(types.RespNil, nil, nil)
}

// WriteFloat writes a [Double](https://redis.io/docs/latest/develop/reference/protocol-spec/#double-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) containing the string representation of the float for RESP2 connections.
func (s *Server) WriteFloat(v float64) error {
	if s.resp2compat {
		return s.WriteBytes(strconv.AppendFloat(nil, v, 'g', -1, 64))
	}
	return s.writeWithType(types.RespFloat, strconv.AppendFloat(nil, v, 'g', -1, 64), nil)
}

// WriteBool writes a [Boolean](https://redis.io/docs/latest/develop/reference/protocol-spec/#boolean-reply) for RESP3 connections or an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply) of `0` or `1` for RESP2 connections.
func (s *Server) WriteBool(v bool) error {
	if s.resp2compat {
		if v {
			return s.WriteInt(1)
		} else {
			return s.WriteInt(0)
		}
	}
	if v {
		return s.writeWithPrefix(static.RespPrefixedBoolTrueBytes, nil, nil)
	}
	return s.writeWithPrefix(static.RespPrefixedBoolFalseBytes, nil, nil)
}

// WriteBlobError writes a [Bulk error](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-error-reply) for RESP3 connections or a sanitized [Simple error](https://redis.io/docs/latest/develop/reference/protocol-spec/#error-reply) for RESP2 connections.
func (s *Server) WriteBlobError(e error) error {
	if s.resp2compat {
		return s.WriteError(e)
	}
	v := sbytes(e.Error())
	return s.writeWithType(types.RespBlobError, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), v)
}

// WriteBlobString writes a [Verbatim string](https://redis.io/docs/latest/develop/reference/protocol-spec/#verbatim-string-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) for RESP2 connections.
// The verbatim string needs to contain the data encoding part and the data itself. Example: `txt:Arbitrary text data`. The data encoding part is stripped when sending the data as Bulk string to RESP2 clients.
func (s *Server) WriteVerbatimBytes(v []byte) error {
	vLen := len(v)
	if s.resp2compat {
		if vLen >= 4 {
			return s.WriteBytes(v[4:])
		}
		return s.WriteBytes(v)
	}
	if v == nil {
		v = static.NullBytes
	}
	return s.writeWithType(types.RespVerbatim, strconv.AppendInt(s.appendIntBuf[:0], int64(vLen), 10), v)
}

// WriteBlobString writes a [Verbatim string](https://redis.io/docs/latest/develop/reference/protocol-spec/#verbatim-string-reply) for RESP3 connections or a [Bulk string](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-string-reply) for RESP2 connections.
// The verbatim string needs to contain the data encoding part and the data itself. Example: `txt:Arbitrary text data`. The data encoding part is stripped when sending the data as Bulk string to RESP2 clients.
func (s *Server) WriteVerbatimString(v string) error {
	return s.WriteVerbatimBytes(sbytes(v))
}

// WriteBigInt writes a [Big number](https://redis.io/docs/latest/develop/reference/protocol-spec/#big-number-reply) for RESP3 connections or an [Integer](https://redis.io/docs/latest/develop/reference/protocol-spec/#integer-reply) for RESP2 connections.
// If v cannot be represented in an int64, the result is undefined when sending to a RESP2 client.
func (s *Server) WriteBigInt(v big.Int) error {
	if s.resp2compat {
		return s.WriteInt64(v.Int64())
	}
	return s.writeWithType(types.RespBigInt, v.Append(nil, 10), nil)
}

// WriteArrayHeader writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header.
func (s *Server) WriteArrayHeader(l int) error {
	return s.writeWithType(types.RespArray, strconv.AppendInt(s.appendIntBuf[:0], int64(l), 10), nil)
}

// WriteArrayBytes is a convenience method to write an array of Bulk strings.
func (s *Server) WriteArrayBytes(v [][]byte) error {
	if err := s.WriteArrayHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteArrayString is a convenience method to write an array of Bulk strings.
func (s *Server) WriteArrayString(v []string) error {
	if err := s.WriteArrayHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteArray writes an array of any values that can be converted to a RESP type.
func (s *Server) WriteArray(v []any) error {
	if err := s.WriteArrayHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteMapHeader writes a [Map](https://redis.io/docs/latest/develop/reference/protocol-spec/#map-reply) header with the specified length. For RESP2 connections, this writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header with twice the specified length.
func (s *Server) WriteMapHeader(l int) error {
	if s.resp2compat {
		return s.WriteArrayHeader(l * 2)
	}
	return s.writeWithType(types.RespMap, strconv.AppendInt(s.appendIntBuf[:0], int64(l), 10), nil)
}

// WriteMapBytes is a convenience method to write a map of Bulk strings.
func (s *Server) WriteMapBytes(v map[string][]byte) error {
	if err := s.WriteMapHeader(len(v)); err != nil {
		return err
	}
	for k, arg := range v {
		if err := s.WriteString(k); err != nil {
			return err
		}
		if err := s.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteMapString is a convenience method to write a map of Bulk strings.
func (s *Server) WriteMapString(v map[string]string) error {
	if err := s.WriteMapHeader(len(v)); err != nil {
		return err
	}
	for k, arg := range v {
		if err := s.WriteString(k); err != nil {
			return err
		}
		if err := s.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteMap writes a map of any values that can be converted to a RESP type.
func (s *Server) WriteMap(v map[string]any) error {
	if err := s.WriteMapHeader(len(v)); err != nil {
		return err
	}
	for k, arg := range v {
		if err := s.WriteString(k); err != nil {
			return err
		}
		if err := s.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteSetHeader writes a [Set](https://redis.io/docs/latest/develop/reference/protocol-spec/#set-reply) header with the specified length. For RESP2 connections, this writes an [Array](https://redis.io/docs/latest/develop/reference/protocol-spec/#array-reply) header with twice the specified length.
func (s *Server) WriteSetHeader(l int) error {
	if s.resp2compat {
		return s.WriteArrayHeader(l)
	}
	return s.writeWithType(types.RespSet, strconv.AppendInt(s.appendIntBuf[:0], int64(l), 10), nil)
}

// WriteSetBytes is a convenience method to write a set of Bulk strings.
func (s *Server) WriteSetBytes(v [][]byte) error {
	if err := s.WriteSetHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteSetString is a convenience method to write a set of Bulk strings.
func (s *Server) WriteSetString(v []string) error {
	if err := s.WriteSetHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteSet writes a set of any values that can be converted to a RESP type.
func (s *Server) WriteSet(v []any) error {
	if err := s.WriteSetHeader(len(v)); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteAttrBytes writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) with the given data. For RESP2 connections, function calls are silently discarded and no data is written
func (s *Server) WriteAttrBytes(v map[string][]byte) error {
	if s.resp2compat {
		return nil
	}
	if err := s.writeWithType(types.RespAttr, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for k, arg := range v {
		if err := s.WriteString(k); err != nil {
			return err
		}
		if err := s.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteAttrString writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) with the given data. For RESP2 connections, function calls are silently discarded and no data is written
func (s *Server) WriteAttrString(v map[string]string) error {
	if s.resp2compat {
		return nil
	}
	if err := s.writeWithType(types.RespAttr, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for k, arg := range v {
		if err := s.WriteString(k); err != nil {
			return err
		}
		if err := s.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteAttr writes an [Attribute](https://github.com/antirez/RESP3/blob/master/spec.md#attribute-type) with the given data. For RESP2 connections, function calls are silently discarded and no data is written
func (s *Server) WriteAttr(v map[string]any) error {
	if s.resp2compat {
		return nil
	}
	if err := s.writeWithType(types.RespAttr, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for k, arg := range v {
		if err := s.WriteString(k); err != nil {
			return err
		}
		if err := s.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 connections, function calls are silently discarded and no data is written.
func (s *Server) WritePushBytes(v [][]byte) error {
	if s.resp2compat {
		return nil
	}
	if err := s.writeWithType(types.RespPush, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.WriteBytes(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 connections, function calls are silently discarded and no data is written.
func (s *Server) WritePushString(v []string) error {
	if s.resp2compat {
		return nil
	}
	if err := s.writeWithType(types.RespPush, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.WriteString(arg); err != nil {
			return err
		}
	}
	return nil
}

// WritePushHeader writes a [Push](https://redis.io/docs/latest/develop/reference/protocol-spec/#push-event) event with the given data. For RESP2 connections, function calls are silently discarded and no data is written.
func (s *Server) WritePush(v []any) error {
	if s.resp2compat {
		return nil
	}
	if err := s.writeWithType(types.RespPush, strconv.AppendInt(s.appendIntBuf[:0], int64(len(v)), 10), nil); err != nil {
		return err
	}
	for _, arg := range v {
		if err := s.Write(arg); err != nil {
			return err
		}
	}
	return nil
}

// Write writes any value that can be converted to a RESP type.
func (s *Server) Write(v any) error {
	switch v := v.(type) {
	case []byte:
		return s.WriteBytes(v)
	case string:
		return s.WriteString(v)
	case int:
		return s.WriteInt(v)
	case int32:
		return s.WriteInt(int(v))
	case int64:
		return s.WriteInt64(v)
	case float64:
		return s.WriteFloat(v)
	case bool:
		return s.WriteBool(v)
	case error:
		return s.WriteError(v)
	case nil:
		return s.WriteNullString()
	case []any:
		return s.WriteArray(v)
	case map[string]any:
		return s.WriteMap(v)
	case big.Int:
		return s.WriteBigInt(v)
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
}

// WriteRaw writes raw RESP data to the connection.
// You can also use this function to check for any previous write errors by writing a nil or empty value.
func (s *Server) WriteRaw(v []byte) error {
	return s.write(v)
}
