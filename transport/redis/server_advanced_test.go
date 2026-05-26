package redis_test

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestServerBitmapCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "SETBIT", "flags", "9", "1")
	requireLine(t, reader, ":0")
	writeCommand(t, conn, "GETBIT", "flags", "9")
	requireLine(t, reader, ":1")
	writeCommand(t, conn, "BITCOUNT", "flags")
	requireLine(t, reader, ":1")
	writeCommand(t, conn, "TYPE", "flags")
	requireLine(t, reader, "+bitmap")
}

func TestServerHyperLogLogCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "PFADD", "visitors:a", "alice", "bob", "alice")
	requireLine(t, reader, ":1")
	writeCommand(t, conn, "PFCOUNT", "visitors:a")
	requireLine(t, reader, ":2")
	writeCommand(t, conn, "PFADD", "visitors:b", "bob", "carol")
	requireLine(t, reader, ":1")
	writeCommand(t, conn, "PFMERGE", "visitors:all", "visitors:a", "visitors:b")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "PFCOUNT", "visitors:all")
	requireLine(t, reader, ":3")
	writeCommand(t, conn, "TYPE", "visitors:all")
	requireLine(t, reader, "+hll")
}

func TestServerGeoCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "GEOADD", "cities",
		"116.4074", "39.9042", "beijing",
		"117.3616", "39.3434", "tianjin",
		"121.4737", "31.2304", "shanghai")
	requireLine(t, reader, ":3")
	writeCommand(t, conn, "GEODIST", "cities", "beijing", "tianjin", "km")
	requireBulkWithPrefix(t, reader, "102.")
	writeCommand(t, conn, "GEORADIUS", "cities", "116.4074", "39.9042", "150", "km", "WITHDIST")
	requireArrayHeader(t, reader, 2)
	requireArrayHeader(t, reader, 2)
	requireBulk(t, reader, "beijing")
	requireBulkWithPrefix(t, reader, "0.0000")
	requireArrayHeader(t, reader, 2)
	requireBulk(t, reader, "tianjin")
	requireBulkWithPrefix(t, reader, "102.")
	writeCommand(t, conn, "TYPE", "cities")
	requireLine(t, reader, "+geo")
}

func requireBulkWithPrefix(t *testing.T, reader *bufio.Reader, prefix string) {
	t.Helper()
	value := readBulkString(t, reader)
	if !strings.HasPrefix(value, prefix) {
		t.Fatalf("bulk = %q, want prefix %q", value, prefix)
	}
}

func readBulkString(t *testing.T, reader *bufio.Reader) string {
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
	if err != nil {
		t.Fatalf("bulk size = %q: %v", header, err)
	}
	raw := make([]byte, size+2)
	if _, err := io.ReadFull(reader, raw); err != nil {
		t.Fatalf("read bulk body: %v", err)
	}
	return string(raw[:size])
}
