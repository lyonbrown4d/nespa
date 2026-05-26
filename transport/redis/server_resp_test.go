package redis_test

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"testing"
)

func readBulk(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	header, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk header: %v", err)
	}
	header = strings.TrimSuffix(header, "\r\n")
	if !strings.HasPrefix(header, "$") {
		t.Fatalf("bulk header = %q", header)
	}
	size, err := strconv.Atoi(strings.TrimPrefix(header, "$"))
	if err != nil || size < 0 {
		t.Fatalf("bulk header = %q", header)
	}
	raw := make([]byte, size+2)
	if _, err := io.ReadFull(reader, raw); err != nil {
		t.Fatalf("read bulk body: %v", err)
	}
	if string(raw[size:]) != "\r\n" {
		t.Fatalf("bulk body suffix = %q", raw[size:])
	}
	return string(raw[:size])
}
