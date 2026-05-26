package redis

import (
	"context"
	"strconv"
	"strings"
	"time"
)

const (
	entityString    = "redis:string"
	entityHash      = "redis:hash"
	entitySet       = "redis:set"
	entityList      = "redis:list"
	entityScoredSet = "redis:zset"
	entityBitmap    = "redis:bitmap"
	entityHLL       = "redis:hll"
	entityGeo       = "redis:geo"
)

var redisEntities = []redisType{
	{name: "string", entity: entityString},
	{name: "hash", entity: entityHash},
	{name: "set", entity: entitySet},
	{name: "list", entity: entityList},
	{name: "zset", entity: entityScoredSet},
	{name: "bitmap", entity: entityBitmap},
	{name: "hll", entity: entityHLL},
	{name: "geo", entity: entityGeo},
}

type redisType struct {
	name   string
	entity string
}

type setOptions struct {
	ttl time.Duration
	nx  bool
	xx  bool
}

type commandHandler func(context.Context, *session, []respArg) respValue

func (s *Server) handleCommand(ctx context.Context, state *session, args []respArg) respValue {
	command := strings.ToUpper(string(args[0]))
	if !state.authenticated && !commandAllowedBeforeAuth(command) {
		return noAuth()
	}

	if state.inTransaction {
		return s.handleQueuedCommand(ctx, state, command, args)
	}

	handler, ok := s.commandHandlers()[command]
	if !ok {
		return errorString("ERR unknown command '" + strings.ToLower(command) + "'")
	}
	return handler(ctx, state, args)
}

func (s *Server) commandHandlers() map[string]commandHandler {
	return map[string]commandHandler{
		"AUTH":    func(_ context.Context, state *session, args []respArg) respValue { return s.handleAuth(state, args) },
		"HELLO":   func(_ context.Context, state *session, args []respArg) respValue { return s.handleHello(state, args) },
		"PING":    func(_ context.Context, _ *session, args []respArg) respValue { return handlePing(args) },
		"QUIT":    handleQuit,
		"SELECT":  func(_ context.Context, state *session, args []respArg) respValue { return handleSelect(state, args) },
		"CLIENT":  func(_ context.Context, _ *session, args []respArg) respValue { return handleClient(args) },
		"COMMAND": func(context.Context, *session, []respArg) respValue { return arrayValue() },
		"MULTI":   func(_ context.Context, state *session, args []respArg) respValue { return handleMulti(state, args) },
		"EXEC":    s.handleExec,
		"DISCARD": func(_ context.Context, state *session, args []respArg) respValue { return handleDiscard(state, args) },
		"EVAL":    s.handleEval,
		"EVALSHA": s.handleEvalSHA,
		"SCRIPT":  s.handleScript,
		"GET":     s.handleGet,
		"SET":     s.handleSet,
		"DEL":     s.handleDel,
		"EXISTS":  s.handleExists,
		"EXPIRE":  s.handleExpire,
		"TTL":     s.handleTTL,
		"INCR": func(ctx context.Context, state *session, args []respArg) respValue {
			return s.handleIncrement(ctx, state, args, 1)
		},
		"DECR": func(ctx context.Context, state *session, args []respArg) respValue {
			return s.handleIncrement(ctx, state, args, -1)
		},
		"INCRBY":    s.handleIncrementBy,
		"DECRBY":    s.handleDecrementBy,
		"MGET":      s.handleMGet,
		"MSET":      s.handleMSet,
		"TYPE":      s.handleType,
		"HSET":      s.handleHSet,
		"HGET":      s.handleHGet,
		"HDEL":      s.handleHDel,
		"HGETALL":   s.handleHGetAll,
		"HEXISTS":   s.handleHExists,
		"HLEN":      s.handleHLen,
		"SADD":      s.handleSAdd,
		"SREM":      s.handleSRem,
		"SISMEMBER": s.handleSIsMember,
		"SMEMBERS":  s.handleSMembers,
		"SCARD":     s.handleSCard,
		"LPUSH":     s.handleLPush,
		"RPUSH":     s.handleRPush,
		"LPOP":      s.handleLPop,
		"RPOP":      s.handleRPop,
		"LRANGE":    s.handleLRange,
		"LLEN":      s.handleLLen,
		"ZADD":      s.handleZAdd,
		"ZREM":      s.handleZRem,
		"ZRANGE":    s.handleZRange,
		"ZCARD":     s.handleZCard,
		"SETBIT":    s.handleSetBit,
		"GETBIT":    s.handleGetBit,
		"BITCOUNT":  s.handleBitCount,
		"PFADD":     s.handlePFAdd,
		"PFCOUNT":   s.handlePFCount,
		"PFMERGE":   s.handlePFMerge,
		"GEOADD":    s.handleGeoAdd,
		"GEODIST":   s.handleGeoDist,
		"GEORADIUS": s.handleGeoRadius,
	}
}

func commandAllowedBeforeAuth(command string) bool {
	switch command {
	case "AUTH", "HELLO", "PING", "QUIT":
		return true
	default:
		return false
	}
}

func (s *Server) handleAuth(state *session, args []respArg) respValue {
	if len(args) != 3 {
		return errorString("ERR AUTH requires username and password")
	}
	user := string(args[1])
	password := string(args[2])
	if !validUsername(user) || !s.authenticate(user, password) {
		return wrongPass()
	}
	state.authenticated = true
	state.namespace = user
	return simpleString("OK")
}

func (s *Server) handleHello(state *session, args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	protocolVersion, err := strconv.Atoi(string(args[1]))
	if err != nil || protocolVersion < 2 || protocolVersion > 3 {
		return errorString("NOPROTO unsupported protocol version")
	}

	if errResp := s.applyHelloOptions(state, args[2:]); errResp != nil {
		return *errResp
	}
	if !state.authenticated {
		return noAuth()
	}

	state.protocol = protocolVersion
	if protocolVersion == 3 {
		return helloMap()
	}
	return helloArray()
}

func (s *Server) applyHelloOptions(state *session, args []respArg) *respValue {
	for index := 0; index < len(args); {
		next, errResp := s.applyHelloOption(state, args, index)
		if errResp != nil {
			return errResp
		}
		index = next
	}
	return nil
}

func (s *Server) applyHelloOption(state *session, args []respArg, index int) (int, *respValue) {
	option := strings.ToUpper(string(args[index]))
	switch option {
	case "AUTH":
		return s.applyHelloAuth(state, args, index)
	case "SETNAME":
		if index+1 >= len(args) {
			err := syntaxError()
			return index, &err
		}
		return index + 2, nil
	default:
		err := syntaxError()
		return index, &err
	}
}

func (s *Server) applyHelloAuth(state *session, args []respArg, index int) (int, *respValue) {
	if index+2 >= len(args) {
		err := syntaxError()
		return index, &err
	}
	user := string(args[index+1])
	password := string(args[index+2])
	if !validUsername(user) || !s.authenticate(user, password) {
		err := wrongPass()
		return index, &err
	}
	state.authenticated = true
	state.namespace = user
	return index + 3, nil
}

func handlePing(args []respArg) respValue {
	switch len(args) {
	case 1:
		return simpleString("PONG")
	case 2:
		return bulkString(args[1])
	default:
		return syntaxError()
	}
}

func handleQuit(_ context.Context, state *session, _ []respArg) respValue {
	state.close = true
	return simpleString("OK")
}

func handleSelect(state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	db, err := strconv.Atoi(string(args[1]))
	if err != nil || db < 0 {
		return errorString("ERR invalid DB index")
	}
	state.db = db
	return simpleString("OK")
}

func handleClient(args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	switch strings.ToUpper(string(args[1])) {
	case "SETINFO", "SETNAME":
		return simpleString("OK")
	case "GETNAME":
		return nullBulkString()
	case "ID":
		return integerValue(0)
	default:
		return simpleString("OK")
	}
}
