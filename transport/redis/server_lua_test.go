package redis_test

import (
	"bufio"
	"strings"
	"testing"
)

func TestServerLuaEvalUsesKeysArgsAndRedisCallCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	script := strings.Join([]string{
		"redis.call('SET', KEYS[1], ARGV[1])",
		"redis.call('INCRBY', KEYS[2], 3)",
		"return {",
		"  redis.call('GET', KEYS[1]),",
		"  redis.call('GET', KEYS[2]),",
		"  redis.call('DEL', KEYS[1]),",
		"  redis.call('EXISTS', KEYS[1], KEYS[2])",
		"}",
	}, "\n")
	writeCommand(t, conn, "EVAL", script, "2", "lua:value", "lua:counter", "from-argv")
	requireArrayHeader(t, reader, 4)
	requireBulk(t, reader, "from-argv")
	requireBulk(t, reader, "3")
	requireLine(t, reader, ":1")
	requireLine(t, reader, ":1")

	writeCommand(t, conn, "GET", "lua:value")
	requireLine(t, reader, "$-1")
	writeCommand(t, conn, "GET", "lua:counter")
	requireBulk(t, reader, "3")
}

func TestServerLuaScriptLoadExistsEvalShaAndFlush(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	script := "return redis.call('GET', KEYS[1])"
	writeCommand(t, conn, "SCRIPT", "LOAD", script)
	sha := readBulk(t, reader)

	writeCommand(t, conn, "SET", "loaded-key", "loaded-value")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SCRIPT", "EXISTS", sha, strings.Repeat("0", 40))
	requireArrayHeader(t, reader, 2)
	requireLine(t, reader, ":1")
	requireLine(t, reader, ":0")

	writeCommand(t, conn, "EVALSHA", sha, "1", "loaded-key")
	requireBulk(t, reader, "loaded-value")

	writeCommand(t, conn, "SCRIPT", "FLUSH")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SCRIPT", "EXISTS", sha)
	requireArrayHeader(t, reader, 1)
	requireLine(t, reader, ":0")
	writeCommand(t, conn, "EVALSHA", sha, "1", "loaded-key")
	requireErrorPrefix(t, reader, "-NOSCRIPT")
}

func TestServerLuaPCallReturnsErrorWithoutTerminatingScript(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)
	writeCommand(t, conn, "SET", "not-int", "abc")
	requireLine(t, reader, "+OK")

	script := strings.Join([]string{
		"local result = redis.pcall('INCRBY', KEYS[1], 1)",
		"redis.call('SET', KEYS[2], 'after-error')",
		"return {result, redis.call('GET', KEYS[2])}",
	}, "\n")
	writeCommand(t, conn, "EVAL", script, "2", "not-int", "pcall-marker")
	requireArrayHeader(t, reader, 2)
	requireErrorPrefix(t, reader, "-ERR value is not an integer")
	requireBulk(t, reader, "after-error")

	writeCommand(t, conn, "GET", "pcall-marker")
	requireBulk(t, reader, "after-error")
}

func TestServerLuaCallErrorReturnsRESPError(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)
	writeCommand(t, conn, "SET", "not-int", "abc")
	requireLine(t, reader, "+OK")

	writeCommand(t, conn, "EVAL", "return redis.call('INCRBY', KEYS[1], 1)", "1", "not-int")
	requireErrorPrefix(t, reader, "-ERR")
}

func TestServerLuaCommandsRequireAuth(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	writeCommand(t, conn, "EVAL", "return 1", "0")
	requireLine(t, reader, "-NOAUTH Authentication required.")
	writeCommand(t, conn, "SCRIPT", "LOAD", "return 1")
	requireLine(t, reader, "-NOAUTH Authentication required.")
}

func TestServerLuaCommandsQueueAndExecuteInTransaction(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	script := "return redis.call('SET', KEYS[1], ARGV[1])"
	sha := "d8f2fad9f8e86a53d2a6ebd960b33c4972cacc37"
	writeCommand(t, conn, "MULTI")
	requireLine(t, reader, "+OK")
	writeCommand(t, conn, "SCRIPT", "LOAD", script)
	requireLine(t, reader, "+QUEUED")
	writeCommand(t, conn, "EVALSHA", sha, "1", "tx-lua", "queued-value")
	requireLine(t, reader, "+QUEUED")
	writeCommand(t, conn, "EXEC")
	requireArrayHeader(t, reader, 2)
	requireBulk(t, reader, sha)
	requireLine(t, reader, "+OK")

	writeCommand(t, conn, "GET", "tx-lua")
	requireBulk(t, reader, "queued-value")
}

func requireErrorPrefix(t *testing.T, reader *bufio.Reader, wantPrefix string) {
	t.Helper()
	got, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error line: %v", err)
	}
	got = strings.TrimSuffix(got, "\r\n")
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("error line = %q, want prefix %q", got, wantPrefix)
	}
}
