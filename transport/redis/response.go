package redis

import (
	"errors"
	"math"
	"strings"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func helloArray() respValue {
	return arrayValue(
		bulkText("server"), bulkText("nespa"),
		bulkText("version"), bulkText("dev"),
		bulkText("proto"), integerValue(2),
		bulkText("mode"), bulkText("standalone"),
		bulkText("role"), bulkText("master"),
		bulkText("modules"), arrayValue(),
	)
}

func helloMap() respValue {
	return mapValue(
		mapField("server", "nespa"),
		mapField("version", "dev"),
		respMapField{key: bulkText("proto"), value: integerValue(3)},
		mapField("mode", "standalone"),
		mapField("role", "master"),
		respMapField{key: bulkText("modules"), value: arrayValue()},
	)
}

func noAuth() respValue {
	return errorString("NOAUTH Authentication required.")
}

func wrongPass() respValue {
	return errorString("WRONGPASS invalid username-password pair or user is disabled.")
}

func wrongType() respValue {
	return errorString("WRONGTYPE Operation against a key holding the wrong kind of value")
}

func syntaxError() respValue {
	return errorString("ERR syntax error")
}

func integerError() respValue {
	return errorString("ERR value is not an integer or out of range")
}

func invalidExpireTime() respValue {
	return errorString("ERR invalid expire time in set")
}

func integerCount(count uint64) respValue {
	if count > math.MaxInt64 {
		return errorString("ERR integer overflow")
	}
	return integerValue(int64(count))
}

func serviceError(err error) respValue {
	if errors.Is(err, engine.ErrInvalidKey) {
		return errorString("ERR invalid key")
	}
	return errorString("ERR " + strings.TrimSpace(err.Error()))
}
