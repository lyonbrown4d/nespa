package redis_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	rediscompat "github.com/lyonbrown4d/nespa/transport/redis"
)

func TestServerRequiresAuthAndMapsUserAndDB(t *testing.T) {
	server, service := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	writeCommand(t, conn, "GET", "k")
	requireLine(t, reader, "-NOAUTH Authentication required.")

	writeCommand(t, conn, "AUTH", "alice", "secret")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SELECT", "2")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SET", "k", "value")
	requireLine(t, reader, "+OK")

	record, found, err := service.Get(context.Background(), cache.Key{
		Namespace: "alice",
		Space:     "2-space",
		Entity:    "redis:string",
		Key:       "k",
	}, cache.GetOptions{})
	if err != nil {
		t.Fatalf("get mapped record: %v", err)
	}
	if !found || string(record.Value) != "value" {
		t.Fatalf("mapped record = found %t value %q, want value", found, record.Value)
	}
}

func TestServerRejectsPasswordOnlyAuth(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	writeCommand(t, conn, "AUTH", "secret")
	requireLine(t, reader, "-ERR AUTH requires username and password")
	writeCommand(t, conn, "SET", "k", "value")
	requireLine(t, reader, "-NOAUTH Authentication required.")
}

func TestServerStringCounterBatchAndTTLCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	writeCommand(t, conn, "AUTH", "alice", "secret")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SET", "k", "value", "EX", "10")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "GET", "k")
	requireBulk(t, reader, "value")
	writeCommand(t, conn, "EXISTS", "k", "missing")
	requireLine(t, reader, ":1")
	writeCommand(t, conn, "TTL", "k")
	requirePositiveInteger(t, reader)

	writeCommand(t, conn, "INCRBY", "counter", "5")
	requireLine(t, reader, ":5")
	writeCommand(t, conn, "DECR", "counter")
	requireLine(t, reader, ":4")

	writeCommand(t, conn, "MSET", "a", "1", "b", "2")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "MGET", "a", "b", "missing")
	requireLine(t, reader, "*3")
	requireBulk(t, reader, "1")
	requireBulk(t, reader, "2")
	requireLine(t, reader, "$-1")
}

func TestServerTransactionQueuesAndExecutesCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "MULTI")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SET", "tx-key", "value")
	requireLine(t, reader, "+QUEUED")
	writeCommand(t, conn, "GET", "tx-key")
	requireLine(t, reader, "+QUEUED")
	writeCommand(t, conn, "INCRBY", "tx-counter", "2")
	requireLine(t, reader, "+QUEUED")
	writeCommand(t, conn, "EXEC")
	requireArrayHeader(t, reader, 3)
	requireLine(t, reader, "+OK")
	requireBulk(t, reader, "value")
	requireLine(t, reader, ":2")

	writeCommand(t, conn, "GET", "tx-key")
	requireBulk(t, reader, "value")
}

func TestServerTransactionDiscardDropsQueuedCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "MULTI")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SET", "discarded", "value")
	requireLine(t, reader, "+QUEUED")
	writeCommand(t, conn, "DISCARD")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "GET", "discarded")
	requireLine(t, reader, "$-1")
}

func TestServerHelloAuthenticates(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	writeCommand(t, conn, "HELLO", "2", "AUTH", "alice", "secret")
	requireLine(t, reader, "*12")
	requireBulk(t, reader, "server")
	requireBulk(t, reader, "nespa")
}

func authRedis(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	writeCommand(t, conn, "AUTH", "alice", "secret")
	requireLine(t, reader, "+OK")
}

func startRedisServer(t *testing.T) (*rediscompat.Server, cache.Service) {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	service := cache.NewService(eng)
	server := rediscompat.NewServer(rediscompat.Config{
		Addr:  "127.0.0.1:0",
		Users: []string{"alice=secret"},
	}, service)
	if err := server.Start(context.Background(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start redis server: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Fatalf("stop redis server: %v", err)
		}
		if err := eng.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	})
	return server, service
}

func dialRedisServer(t *testing.T, addr string) net.Conn {
	t.Helper()
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("dial redis server: %v", err)
	}
	return conn
}

func writeCommand(t *testing.T, conn net.Conn, args ...string) {
	t.Helper()
	raw := []byte("*")
	raw = strconv.AppendInt(raw, int64(len(args)), 10)
	raw = append(raw, '\r', '\n')
	for _, arg := range args {
		raw = append(raw, '$')
		raw = strconv.AppendInt(raw, int64(len(arg)), 10)
		raw = append(raw, '\r', '\n')
		raw = append(raw, arg...)
		raw = append(raw, '\r', '\n')
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("write command: %v", err)
	}
}

func requireLine(t *testing.T, reader *bufio.Reader, want string) {
	t.Helper()
	got, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	got = strings.TrimSuffix(got, "\r\n")
	if got != want {
		t.Fatalf("line = %q, want %q", got, want)
	}
}

func requireArrayHeader(t *testing.T, reader *bufio.Reader, want int) {
	t.Helper()
	requireLine(t, reader, "*"+strconv.Itoa(want))
}

func requireBulk(t *testing.T, reader *bufio.Reader, want string) {
	t.Helper()
	requireLine(t, reader, fmt.Sprintf("$%d", len(want)))
	raw := make([]byte, len(want)+2)
	if _, err := io.ReadFull(reader, raw); err != nil {
		t.Fatalf("read bulk body: %v", err)
	}
	if string(raw[:len(want)]) != want || string(raw[len(want):]) != "\r\n" {
		t.Fatalf("bulk body = %q, want %q", raw, want)
	}
}

func requirePositiveInteger(t *testing.T, reader *bufio.Reader) {
	t.Helper()
	got, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read integer: %v", err)
	}
	got = strings.TrimSuffix(got, "\r\n")
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("integer response = %q", got)
	}
	value, err := strconv.Atoi(strings.TrimPrefix(got, ":"))
	if err != nil || value <= 0 {
		t.Fatalf("integer response = %q, want positive", got)
	}
}

func closeTestConn(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}
}
