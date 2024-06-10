package resp

import "math"

const (
	MaxInlineSize      = 64 * 1024 // 64 KiB
	MaxMultiBulkLength = math.MaxInt32
	MaxBulkLength      = 128 * 1024 * 1024 // 128 MiB
)
