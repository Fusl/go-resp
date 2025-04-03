package resp

import (
	"bufio"
	"bytes"
	"math"
	"testing"
)

//https://github.com/redis/redis/blob/cb2782c314f0af3df56853974f7ba5541c095eeb/src/sds.c#L932-L1040
//sds *sdssplitargs(const char *line, int *argc) {
//    const char *p = line;
//    char *current = NULL;
//    char **vector = NULL;
//
//    *argc = 0;
//    while(1) {
//        /* skip blanks */
//        while(*p && isspace(*p)) p++;
//        if (*p) {
//            /* get a token */
//            int inq=0;  /* set to 1 if we are in "quotes" */
//            int insq=0; /* set to 1 if we are in 'single quotes' */
//            int done=0;
//
//            if (current == NULL) current = sdsempty();
//            while(!done) {
//                if (inq) {
//                    if (*p == '\\' && *(p+1) == 'x' &&
//                                             is_hex_digit(*(p+2)) &&
//                                             is_hex_digit(*(p+3)))
//                    {
//                        unsigned char byte;
//
//                        byte = (hex_digit_to_int(*(p+2))*16)+
//                                hex_digit_to_int(*(p+3));
//                        current = sdscatlen(current,(char*)&byte,1);
//                        p += 3;
//                    } else if (*p == '\\' && *(p+1)) {
//                        char c;
//
//                        p++;
//                        switch(*p) {
//                        case 'n': c = '\n'; break;
//                        case 'r': c = '\r'; break;
//                        case 't': c = '\t'; break;
//                        case 'b': c = '\b'; break;
//                        case 'a': c = '\a'; break;
//                        default: c = *p; break;
//                        }
//                        current = sdscatlen(current,&c,1);
//                    } else if (*p == '"') {
//                        /* closing quote must be followed by a space or
//                         * nothing at all. */
//                        if (*(p+1) && !isspace(*(p+1))) goto err;
//                        done=1;
//                    } else if (!*p) {
//                        /* unterminated quotes */
//                        goto err;
//                    } else {
//                        current = sdscatlen(current,p,1);
//                    }
//                } else if (insq) {
//                    if (*p == '\\' && *(p+1) == '\'') {
//                        p++;
//                        current = sdscatlen(current,"'",1);
//                    } else if (*p == '\'') {
//                        /* closing quote must be followed by a space or
//                         * nothing at all. */
//                        if (*(p+1) && !isspace(*(p+1))) goto err;
//                        done=1;
//                    } else if (!*p) {
//                        /* unterminated quotes */
//                        goto err;
//                    } else {
//                        current = sdscatlen(current,p,1);
//                    }
//                } else {
//                    switch(*p) {
//                    case ' ':
//                    case '\n':
//                    case '\r':
//                    case '\t':
//                    case '\0':
//                        done=1;
//                        break;
//                    case '"':
//                        inq=1;
//                        break;
//                    case '\'':
//                        insq=1;
//                        break;
//                    default:
//                        current = sdscatlen(current,p,1);
//                        break;
//                    }
//                }
//                if (*p) p++;
//            }
//            /* add the token to the vector */
//            vector = s_realloc(vector,((*argc)+1)*sizeof(char*));
//            vector[*argc] = current;
//            (*argc)++;
//            current = NULL;
//        } else {
//            /* Even on empty input string return something not NULL. */
//            if (vector == NULL) vector = s_malloc(sizeof(void*));
//            return vector;
//        }
//    }
//
//err:
//    while((*argc)--)
//        sdsfree(vector[*argc]);
//    s_free(vector);
//    if (current) sdsfree(current);
//    *argc = 0;
//    return NULL;
//}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func sdssplitargs(line []byte) [][]byte {
	go func() {
		if err := recover(); err != nil {
			//panic(fmt.Sprintf("%q", line))
		}
	}()

	p := 0
	var current []byte
	var vector [][]byte
	argc := 0
	for {
		// skip blanks
		for p < len(line) && readArgsSpaceChars[line[p]] > 0 {
			p++
		}

		if p < len(line) {
			// get a token
			inq := false  // set to true if we are in "quotes"
			insq := false // set to true if we are in 'single quotes'
			done := false

			if current == nil {
				current = []byte{}
			}

			for !done {
				if inq {
					if p >= len(line) {
						goto err
					}
					if line[p] == '\\' && p+3 < len(line) && line[p+1] == 'x' && isHexDigit(line[p+2]) && isHexDigit(line[p+3]) {
						current = append(current, byte((hexDigitToInt(line[p+2])*16)+hexDigitToInt(line[p+3])))
						p += 3
					} else if line[p] == '\\' && p+1 < len(line) {
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
						if p+1 < len(line) && readArgsSpaceChars[line[p+1]] == 0 {
							goto err
						}
						done = true
					} else if p >= len(line) {
						// unterminated quotes
						goto err
					} else {
						current = append(current, line[p])
					}
				} else if insq {
					if p >= len(line) {
						goto err
					}
					if line[p] == '\\' && p+1 < len(line) && line[p+1] == '\'' {
						p++
						current = append(current, '\'')
					} else if line[p] == '\'' {
						// closing quote must be followed by a space or nothing at all
						if p+1 < len(line) && readArgsSpaceChars[line[p+1]] == 0 {
							goto err
						}
						done = true
					} else if p >= len(line) {
						// unterminated quotes
						goto err
					} else {
						current = append(current, line[p])
					}
				} else {
					if p >= len(line) {
						break
					}
					switch line[p] {
					case ' ', '\n', '\r', '\t', '\x00':
						done = true
					case '"':
						inq = true
					case '\'':
						insq = true
					default:
						current = append(current, line[p])
					}
				}
				if p < len(line) {
					p++
				}
			}
			// add the token to the vector
			vector = append(vector, current)
			argc++
			current = nil
		} else {
			// Even on empty input string return something not nil
			if vector == nil {
				vector = [][]byte{}
			}
			return vector
		}
	}

err:
	return nil
}

func TestSplitArgs(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	bufRd := bufio.NewReader(buf)
	server := &Server{
		rd:                 bufRd,
		maxMultiBulkLength: math.MaxInt64,
		maxBulkLength:      1024,
		maxBufferSize:      1048576,
	}

	for stringData := range parserTestCases {
		data := []byte(stringData)
		parts := bytes.Split(data, []byte("\n"))

		for _, part := range parts {
			if len(part) > 1024 {
				part = part[:1024]
			}
			originalResult := sdssplitargs(part)
			server.rerr = nil
			server.werr = nil
			buf.Reset()
			buf.Write(part)
			buf.WriteByte('\n')
			bufRd.Reset(buf)
			ourResult, err := server.readArgs()

			if (originalResult == nil && err == nil) || (originalResult != nil && err != nil) {
				t.Fatalf("data %q, expected %q(%t), got %q(%t)", part, originalResult, originalResult == nil, ourResult, ourResult == nil)
			}

			if len(originalResult) != len(ourResult) {
				t.Fatalf("data %q, expected %q, got %q", part, originalResult, ourResult)
			}

			for i := range originalResult {
				if string(originalResult[i]) != string(ourResult[i]) {
					t.Fatalf("data %q, expected %q, got %q", part, originalResult, ourResult)
				}
			}
		}
	}
}

func FuzzSplitArgs(f *testing.F) {
	for input := range parserTestCases {
		f.Add([]byte(input))
	}
	//f.Add([]byte(""))

	buf := bytes.NewBuffer(nil)
	bufRd := bufio.NewReader(buf)
	server := &Server{
		rd:                 bufRd,
		maxMultiBulkLength: math.MaxInt64,
		maxBulkLength:      1024,
		maxBufferSize:      1048576,
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		parts := bytes.Split(data, []byte("\n"))

		for _, part := range parts {
			if len(part) > 1024 {
				part = part[:1024]
			}
			originalResult := sdssplitargs(part)
			server.rerr = nil
			server.werr = nil
			buf.Reset()
			buf.Write(part)
			buf.WriteByte('\n')
			bufRd.Reset(buf)
			ourResult, err := server.readArgs()

			if (originalResult == nil && err == nil) || (originalResult != nil && err != nil) {
				t.Fatalf("data %q, expected %q(%t), got %q(%v)", part, originalResult, originalResult == nil, ourResult, err)
			}

			if len(originalResult) != len(ourResult) {
				t.Fatalf("data %q, expected %q, got %q", part, originalResult, ourResult)
			}

			for i := range originalResult {
				if string(originalResult[i]) != string(ourResult[i]) {
					t.Fatalf("data %q, expected %q, got %q", part, originalResult, ourResult)
				}
			}
		}
	})
}
