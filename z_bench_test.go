package goracle

import (
	"fmt"
	"strings"
	"testing"
)

func shortenFloat(s string) string {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return s
	}
	for j := i + 1; j < len(s); j++ {
		if s[j] != '0' {
			return s
		}
	}
	return s[:i]
}

const bFloat = 12345.6789

func BenchmarkSprintfFloat(b *testing.B) {
	var length int64
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf("%f", bFloat)
		s = shortenFloat(s)
		length += int64(len(s))
	}
}
func BenchmarkAppendFloat(b *testing.B) {
	var length int64
	for i := 0; i < b.N; i++ {
		s := printFloat(bFloat)
		length += int64(len(s))
	}
}
