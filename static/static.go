package static

var (
	TxtBytes                    = []byte("txt")
	NullBytes                   = []byte("")
	RespPrefixedErrorBytes      = []byte("-ERR ")
	RespPrefixedOKBytes         = []byte("+OK")
	RespOKBytes                 = []byte("OK")
	RespPrefixedNullStringBytes = []byte("$-1")
	RespPrefixedNullArrayBytes  = []byte("*-1")
	RespPrefixedBoolTrueBytes   = []byte("#t")
	RespPrefixedBoolFalseBytes  = []byte("#f")
)
