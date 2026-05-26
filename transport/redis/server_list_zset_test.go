package redis_test

import (
	"bufio"
	"testing"
)

func TestServerListCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "RPUSH", "queue", "middle", "last")
	requireLine(t, reader, ":2")
	writeCommand(t, conn, "LPUSH", "queue", "first")
	requireLine(t, reader, ":3")
	writeCommand(t, conn, "TYPE", "queue")
	requireLine(t, reader, "+list")
	writeCommand(t, conn, "LLEN", "queue")
	requireLine(t, reader, ":3")
	writeCommand(t, conn, "LRANGE", "queue", "0", "-1")
	requireArrayHeader(t, reader, 3)
	requireBulk(t, reader, "first")
	requireBulk(t, reader, "middle")
	requireBulk(t, reader, "last")
	writeCommand(t, conn, "LPOP", "queue")
	requireBulk(t, reader, "first")
	writeCommand(t, conn, "RPOP", "queue")
	requireBulk(t, reader, "last")
}

func TestServerZSetCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "ZADD", "rank", "2", "alice", "1", "bob", "3", "cara")
	requireLine(t, reader, ":3")
	writeCommand(t, conn, "TYPE", "rank")
	requireLine(t, reader, "+zset")
	writeCommand(t, conn, "ZCARD", "rank")
	requireLine(t, reader, ":3")
	writeCommand(t, conn, "ZRANGE", "rank", "0", "-1")
	requireArrayHeader(t, reader, 3)
	requireBulk(t, reader, "bob")
	requireBulk(t, reader, "alice")
	requireBulk(t, reader, "cara")
	writeCommand(t, conn, "ZRANGE", "rank", "0", "1", "WITHSCORES")
	requireArrayHeader(t, reader, 4)
	requireBulk(t, reader, "bob")
	requireBulk(t, reader, "1")
	requireBulk(t, reader, "alice")
	requireBulk(t, reader, "2")
	writeCommand(t, conn, "ZREM", "rank", "bob", "missing")
	requireLine(t, reader, ":1")
}
