package redis

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

func (s *Server) handleGet(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityString); wrong != nil {
		return *wrong
	}
	record, found, err := state.activeService(s).Get(ctx, s.key(state, entityString, key), cache.GetOptions{})
	if err != nil {
		return serviceError(err)
	}
	if !found {
		return nullBulkString()
	}
	return bulkString(record.Value)
}

func (s *Server) handleSet(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	key := string(args[1])
	options, errResp := parseSetOptions(args[3:])
	if errResp != nil {
		return *errResp
	}

	existing, found, err := state.activeService(s).Get(ctx, s.key(state, entityString, key), cache.GetOptions{})
	if err != nil {
		return serviceError(err)
	}
	if options.nx && s.existsAny(ctx, state, key) {
		return nullBulkString()
	}
	if options.xx && !found {
		return nullBulkString()
	}

	s.deleteNonStringTypes(ctx, state, key)
	setOpts := cache.SetOptions{TTL: options.ttl}
	if options.xx {
		setOpts.ExpectedVersion = existing.Version
	}
	_, err = state.activeService(s).Set(ctx, s.key(state, entityString, key), args[2], setOpts)
	if err != nil {
		return serviceError(err)
	}
	return simpleString("OK")
}

func parseSetOptions(args []respArg) (setOptions, *respValue) {
	var out setOptions
	for index := 0; index < len(args); {
		next, errResp := applySetOption(args, index, &out)
		if errResp != nil {
			return out, errResp
		}
		index = next
	}
	if out.nx && out.xx {
		err := syntaxError()
		return out, &err
	}
	return out, nil
}

func applySetOption(args []respArg, index int, out *setOptions) (int, *respValue) {
	option := strings.ToUpper(string(args[index]))
	switch option {
	case "EX":
		return applySetTTL(args, index, time.Second, out)
	case "PX":
		return applySetTTL(args, index, time.Millisecond, out)
	case "NX":
		out.nx = true
		return index + 1, nil
	case "XX":
		out.xx = true
		return index + 1, nil
	default:
		err := syntaxError()
		return index, &err
	}
}

func applySetTTL(args []respArg, index int, unit time.Duration, out *setOptions) (int, *respValue) {
	if index+1 >= len(args) {
		err := syntaxError()
		return index, &err
	}
	value, ok := parsePositiveInt64(args[index+1])
	if !ok {
		err := invalidExpireTime()
		return index, &err
	}
	out.ttl = time.Duration(value) * unit
	return index + 2, nil
}

func (s *Server) handleDel(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	var deleted int64
	for index := 1; index < len(args); index++ {
		if s.deleteAny(ctx, state, string(args[index])) {
			deleted++
		}
	}
	return integerValue(deleted)
}

func (s *Server) handleExists(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	var exists int64
	for index := 1; index < len(args); index++ {
		if s.existsAny(ctx, state, string(args[index])) {
			exists++
		}
	}
	return integerValue(exists)
}

func (s *Server) handleExpire(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	seconds, ok := parsePositiveInt64(args[2])
	if !ok {
		return invalidExpireTime()
	}
	typ, found := s.typeOf(ctx, state, string(args[1]))
	if !found {
		return integerValue(0)
	}
	touched, err := state.activeService(s).Touch(ctx, s.key(state, typ.entity, string(args[1])), cache.TouchOptions{
		TTL: time.Duration(seconds) * time.Second,
	})
	if err != nil {
		return serviceError(err)
	}
	if touched {
		return integerValue(1)
	}
	return integerValue(0)
}

func (s *Server) handleTTL(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	typ, found := s.typeOf(ctx, state, string(args[1]))
	if !found {
		return integerValue(-2)
	}
	record, found, err := state.activeService(s).Get(ctx, s.key(state, typ.entity, string(args[1])), cache.GetOptions{})
	if err != nil {
		return serviceError(err)
	}
	if !found {
		return integerValue(-2)
	}
	return ttlResponse(record)
}

func ttlResponse(record cache.Record) respValue {
	if record.ExpireAt.IsZero() {
		return integerValue(-1)
	}
	ttl := time.Until(record.ExpireAt)
	if ttl <= 0 {
		return integerValue(-2)
	}
	return integerValue(int64(math.Ceil(ttl.Seconds())))
}

func (s *Server) handleIncrement(ctx context.Context, state *session, args []respArg, delta int64) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	return s.increment(ctx, state, string(args[1]), delta)
}

func (s *Server) handleIncrementBy(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	delta, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return integerError()
	}
	return s.increment(ctx, state, string(args[1]), delta)
}

func (s *Server) handleDecrementBy(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	delta, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || delta == math.MinInt64 {
		return integerError()
	}
	return s.increment(ctx, state, string(args[1]), -delta)
}

func (s *Server) increment(ctx context.Context, state *session, key string, delta int64) respValue {
	if wrong := s.wrongTypeFor(ctx, state, key, entityString); wrong != nil {
		return *wrong
	}
	result, err := state.activeService(s).Adjust(ctx, s.key(state, entityString, key), cache.AdjustOptions{
		Delta:        delta,
		InitialValue: 0,
	})
	if err != nil {
		if errors.Is(err, engine.ErrInvalidCounter) {
			return integerError()
		}
		return serviceError(err)
	}
	value, err := strconv.ParseInt(string(result.Record.Value), 10, 64)
	if err != nil {
		return integerError()
	}
	return integerValue(value)
}

func (s *Server) handleMGet(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	items := make([]respValue, 0, len(args)-1)
	for index := 1; index < len(args); index++ {
		items = append(items, s.mgetItem(ctx, state, string(args[index])))
	}
	return arrayValue(items...)
}

func (s *Server) mgetItem(ctx context.Context, state *session, key string) respValue {
	if wrong := s.wrongTypeFor(ctx, state, key, entityString); wrong != nil {
		return nullBulkString()
	}
	record, found, err := state.activeService(s).Get(ctx, s.key(state, entityString, key), cache.GetOptions{})
	if err != nil || !found {
		return nullBulkString()
	}
	return bulkString(record.Value)
}

func (s *Server) handleMSet(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 || len(args)%2 == 0 {
		return syntaxError()
	}
	for index := 1; index < len(args); index += 2 {
		key := string(args[index])
		s.deleteNonStringTypes(ctx, state, key)
		if _, err := state.activeService(s).Set(ctx, s.key(state, entityString, key), args[index+1], cache.SetOptions{}); err != nil {
			return serviceError(err)
		}
	}
	return simpleString("OK")
}

func (s *Server) handleType(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	typ, found := s.typeOf(ctx, state, string(args[1]))
	if !found {
		return simpleString("none")
	}
	return simpleString(typ.name)
}
