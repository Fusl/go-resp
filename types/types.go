package types

var CRLF = []byte("\r\n")

// https://github.com/redis/go-redis/blob/f752b9a9d5cc158381c2ffe5b13c531037426b39/internal/proto/reader.go#L16-L32
const (
	RespStatus    = byte('+') // +<string>\r\n
	RespError     = byte('-') // -<string>\r\n
	RespString    = byte('$') // $<length>\r\n<bytes>\r\n
	RespInt       = byte(':') // :<number>\r\n
	RespNil       = byte('_') // _\r\n
	RespFloat     = byte(',') // ,<floating-point-number>\r\n (golang float)
	RespBool      = byte('#') // true: #t\r\n false: #f\r\n
	RespBlobError = byte('!') // !<length>\r\n<bytes>\r\n
	RespVerbatim  = byte('=') // =<length>\r\nFORMAT:<bytes>\r\n
	RespBigInt    = byte('(') // (<big number>\r\n
	RespArray     = byte('*') // *<len>\r\n... (same as resp2)
	RespMap       = byte('%') // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
	RespSet       = byte('~') // ~<len>\r\n... (same as Array)
	RespAttr      = byte('|') // |<len>\r\n(key)\r\n(value)\r\n... + command reply
	RespPush      = byte('>') // ><len>\r\n... (same as Array)
)
