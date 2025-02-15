package resp

import "errors"

var (
	ErrProtoUnbalancedQuotes       = errors.New("Protocol error: unbalanced quotes in request")
	ErrProtoInvalidMultiBulkLength = errors.New("Protocol error: invalid multibulk length")
	ErrProtoInvalidBulkLength      = errors.New("Protocol error: invalid bulk length")
	ErrProtoExpectedString         = errors.New("Protocol error: expected '$'")
)
