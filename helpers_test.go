package resp

import (
	"errors"
	"strconv"
	"testing"
)

func BenchmarkIntParsing(b *testing.B) {
	//f, err := os.Create("prof")
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer f.Close()
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()
	testCases := [][]byte{
		// normal test cases
		[]byte("0"),
		[]byte("1"),
		[]byte("99"),
		[]byte("-1"),
		[]byte("-99"),
		[]byte("-2147483648"),
		[]byte("2147483647"),

		// failing test cases
		[]byte("+1"),
		[]byte(" 1"),
		[]byte("1 "),
		[]byte("01"),
		[]byte("-9223372036854775809"),
		[]byte("9223372036854775808"),
	}
	b.Run("ParseInt64", func(b *testing.B) {
		for _, tc := range testCases {
			for i := 0; i < b.N; i++ {
				ParseInt64(tc)
			}
		}
	})
	b.Run("ParseInt32", func(b *testing.B) {
		for _, tc := range testCases {
			for i := 0; i < b.N; i++ {
				ParseInt32(tc)
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
			v := bstring(tc)
			for i := 0; i < b.N; i++ {
				strconv.Atoi(v)
			}
		}
	})
	b.Run("strconv.ParseInt(64)", func(b *testing.B) {
		for _, tc := range testCases {
			v := bstring(tc)
			for i := 0; i < b.N; i++ {
				strconv.ParseInt(v, 10, 64)
			}
		}
	})
	b.Run("strconv.ParseInt(32)", func(b *testing.B) {
		for _, tc := range testCases {
			v := bstring(tc)
			for i := 0; i < b.N; i++ {
				strconv.ParseInt(v, 10, 32)
			}
		}
	})
}

func TestParseInt64(t *testing.T) {
	// May not start with +.
	if i, err := ParseInt64([]byte("+1")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// Leading space.
	if i, err := ParseInt64([]byte(" 1")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// Trailing space.
	if i, err := ParseInt64([]byte("1 ")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// May not start with 0.
	if i, err := ParseInt64([]byte("01")); !errors.Is(err, strconv.ErrSyntax) {
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
	if i, err := ParseInt64([]byte("-9223372036854775809")); !errors.Is(err, strconv.ErrRange) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 9223372036854775807
	if i, err := ParseInt64([]byte("9223372036854775807")); err != nil {
		t.Fatalf("expected 9223372036854775807, got error %v", err)
	} else if i != 9223372036854775807 {
		t.Fatalf("expected 9223372036854775807, got %d", i)
	}

	// 9223372036854775808
	if i, err := ParseInt64([]byte("9223372036854775808")); !errors.Is(err, strconv.ErrRange) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 1_234
	if i, err := ParseInt64([]byte("1_234")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 1.234
	if i, err := ParseInt64([]byte("1.234")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 1,234
	if i, err := ParseInt64([]byte("1,234")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}
}

func TestParseInt32(t *testing.T) {
	// May not start with +.
	if i, err := ParseInt32([]byte("+1")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// Leading space.
	if i, err := ParseInt32([]byte(" 1")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// Trailing space.
	if i, err := ParseInt32([]byte("1 ")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// May not start with 0.
	if i, err := ParseInt32([]byte("01")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// -1
	if i, err := ParseInt32([]byte("-1")); err != nil {
		t.Fatalf("expected -1, got error %v", err)
	} else if i != -1 {
		t.Fatalf("expected -1, got %d", i)
	}

	// 0
	if i, err := ParseInt32([]byte("0")); err != nil {
		t.Fatalf("expected 0, got error %v", err)
	} else if i != 0 {
		t.Fatalf("expected 0, got %d", i)
	}

	// 1
	if i, err := ParseInt32([]byte("1")); err != nil {
		t.Fatalf("expected 1, got error %v", err)
	} else if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}

	// 99
	if i, err := ParseInt32([]byte("99")); err != nil {
		t.Fatalf("expected 99, got error %v", err)
	} else if i != 99 {
		t.Fatalf("expected 99, got %d", i)
	}

	// -99
	if i, err := ParseInt32([]byte("-99")); err != nil {
		t.Fatalf("expected -99, got error %v", err)
	} else if i != -99 {
		t.Fatalf("expected -99, got %d", i)
	}

	// -2147483648
	if i, err := ParseInt32([]byte("-2147483648")); err != nil {
		t.Fatalf("expected -2147483648, got error %v", err)
	} else if i != -2147483648 {
		t.Fatalf("expected -2147483648, got %d", i)
	}

	// -2147483649
	if i, err := ParseInt32([]byte("-2147483649")); !errors.Is(err, strconv.ErrRange) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 2147483647
	if i, err := ParseInt32([]byte("2147483647")); err != nil {
		t.Fatalf("expected 2147483647, got error %v", err)
	} else if i != 2147483647 {
		t.Fatalf("expected 2147483647, got %d", i)
	}

	// 2147483648
	if i, err := ParseInt32([]byte("2147483648")); !errors.Is(err, strconv.ErrRange) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 1_234
	if i, err := ParseInt32([]byte("1_234")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 1.234
	if i, err := ParseInt32([]byte("1.234")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}

	// 1,234
	if i, err := ParseInt32([]byte("1,234")); !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("expected error, got %d, err=%v", i, err)
	}
}
