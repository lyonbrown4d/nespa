package redis_test

import (
	"bufio"
	"testing"
)

func TestServerHashCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "HSET", "profile", "name", "alice", "role", "admin")
	requireLine(t, reader, ":2")
	writeCommand(t, conn, "HSET", "profile", "role", "owner")
	requireLine(t, reader, ":0")
	writeCommand(t, conn, "TYPE", "profile")
	requireLine(t, reader, "+hash")
	writeCommand(t, conn, "HGET", "profile", "role")
	requireBulk(t, reader, "owner")
	writeCommand(t, conn, "HEXISTS", "profile", "name")
	requireLine(t, reader, ":1")
	writeCommand(t, conn, "HLEN", "profile")
	requireLine(t, reader, ":2")
	writeCommand(t, conn, "HGETALL", "profile")
	requireArrayHeader(t, reader, 4)
	requireBulk(t, reader, "name")
	requireBulk(t, reader, "alice")
	requireBulk(t, reader, "role")
	requireBulk(t, reader, "owner")
	writeCommand(t, conn, "HDEL", "profile", "name", "missing")
	requireLine(t, reader, ":1")
}

func TestServerSetCommands(t *testing.T) {
	server, _ := startRedisServer(t)
	conn := dialRedisServer(t, server.Addr())
	defer closeTestConn(t, conn)
	reader := bufio.NewReader(conn)

	authRedis(t, conn, reader)

	writeCommand(t, conn, "SADD", "tags", "blue", "red", "blue")
	requireLine(t, reader, ":2")
	writeCommand(t, conn, "TYPE", "tags")
	requireLine(t, reader, "+set")
	writeCommand(t, conn, "SISMEMBER", "tags", "red")
	requireLine(t, reader, ":1")
	writeCommand(t, conn, "SCARD", "tags")
	requireLine(t, reader, ":2")
	writeCommand(t, conn, "SMEMBERS", "tags")
	requireArrayHeader(t, reader, 2)
	requireBulk(t, reader, "blue")
	requireBulk(t, reader, "red")
	writeCommand(t, conn, "SREM", "tags", "blue", "missing")
	requireLine(t, reader, ":1")
}
