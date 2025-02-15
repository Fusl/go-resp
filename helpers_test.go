package resp

import (
	"strconv"
	"testing"
)

func BenchmarkInt64(b *testing.B) {
	//f, err := os.Create("prof")
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer f.Close()
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()
	testCases := [][]byte{
		// normal test cases
		//[]byte("0"),
		[]byte("1"),
		//[]byte("99"),
		//[]byte("-1"),
		//[]byte("-99"),
		//[]byte("-9223372036854775808"),
		//[]byte("9223372036854775807"),
		//
		//// failing test cases
		//[]byte("+1"),
		//[]byte(" 1"),
		//[]byte("1 "),
		//[]byte("01"),
		//[]byte("-9223372036854775809"),
		//[]byte("9223372036854775808"),
	}
	b.Run("ParseInt64", func(b *testing.B) {
		for _, tc := range testCases {
			for i := 0; i < b.N; i++ {
				ParseInt64(tc)
			}
		}
	})
	b.Run("ParseUint", func(b *testing.B) {
		for _, tc := range testCases {
			for i := 0; i < b.N; i++ {
				ParseUInt32(tc)
			}
		}
	})
	b.Run("strconv.Atoi", func(b *testing.B) {
		for _, tc := range testCases {
			for i := 0; i < b.N; i++ {
				strconv.Atoi(bstring(tc))
			}
		}
	})
	b.Run("strconv.ParseInt", func(b *testing.B) {
		for _, tc := range testCases {
			for i := 0; i < b.N; i++ {
				strconv.ParseInt(bstring(tc), 10, 64)
			}
		}
	})
}

func TestParseInt64(t *testing.T) {
	// May not start with +.
	if i, err := ParseInt64([]byte("+1")); err == nil {
		t.Fatalf("expected error, got %d", i)
	}

	// Leading space.
	if i, err := ParseInt64([]byte(" 1")); err == nil {
		t.Fatalf("expected error, got %d", i)
	}

	// Trailing space.
	if i, err := ParseInt64([]byte("1 ")); err == nil {
		t.Fatalf("expected error, got %d", i)
	}

	// May not start with 0.
	if i, err := ParseInt64([]byte("01")); err == nil {
		t.Fatalf("expected error, got %d", i)
	}

	// -1
	if i, err := ParseInt64([]byte("-1")); err != nil {
		t.Fatalf("expected -1, got error %v", err)
	} else if i != -1 {
		t.Fatalf("expected -1, got %d", i)
	}

	// 0
	if i, err := ParseInt64([]byte("0")); err != nil {
		t.Fatalf("expected 0, got error %v", err)
	} else if i != 0 {
		t.Fatalf("expected 0, got %d", i)
	}

	// 1
	if i, err := ParseInt64([]byte("1")); err != nil {
		t.Fatalf("expected 1, got error %v", err)
	} else if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}

	// 99
	if i, err := ParseInt64([]byte("99")); err != nil {
		t.Fatalf("expected 99, got error %v", err)
	} else if i != 99 {
		t.Fatalf("expected 99, got %d", i)
	}

	// -99
	if i, err := ParseInt64([]byte("-99")); err != nil {
		t.Fatalf("expected -99, got error %v", err)
	} else if i != -99 {
		t.Fatalf("expected -99, got %d", i)
	}

	// -9223372036854775808
	if i, err := ParseInt64([]byte("-9223372036854775808")); err != nil {
		t.Fatalf("expected -9223372036854775808, got error %v", err)
	} else if i != -9223372036854775808 {
		t.Fatalf("expected -9223372036854775808, got %d", i)
	}

	// -9223372036854775809
	if i, err := ParseInt64([]byte("-9223372036854775809")); err == nil {
		t.Fatalf("expected error, got %d", i)
	}

	// 9223372036854775807
	if i, err := ParseInt64([]byte("9223372036854775807")); err != nil {
		t.Fatalf("expected 9223372036854775807, got error %v", err)
	} else if i != 9223372036854775807 {
		t.Fatalf("expected 9223372036854775807, got %d", i)
	}

	// 9223372036854775808
	if i, err := ParseInt64([]byte("9223372036854775808")); err == nil {
		t.Fatalf("expected error, got %d", i)
	}
}
