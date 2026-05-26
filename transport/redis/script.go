package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	lua "github.com/yuin/gopher-lua"
)

const (
	defaultScriptTimeout  = 5 * time.Second
	defaultScriptMaxCalls = 1000
)

var errScriptAbort = errors.New("redis script aborted")

type scriptRuntime struct {
	server    *Server
	state     *session
	ctx       context.Context
	callCount int
	maxCalls  int
}

func (s *Server) handleEval(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	return s.evalScript(ctx, state, string(args[1]), args[2:])
}

func (s *Server) handleEvalSHA(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	sha := string(args[1])
	script, ok := s.scriptForSHA(sha)
	if !ok {
		return errorString("NOSCRIPT No matching script. Please use EVAL.")
	}
	return s.evalScript(ctx, state, script, args[2:])
}

func (s *Server) handleScript(_ context.Context, _ *session, args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	switch strings.ToUpper(string(args[1])) {
	case "LOAD":
		return s.handleScriptLoad(args)
	case "EXISTS":
		return s.handleScriptExists(args)
	case "FLUSH":
		return s.handleScriptFlush(args)
	default:
		return syntaxError()
	}
}

func (s *Server) handleScriptLoad(args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	sha := scriptSHA(string(args[2]))
	s.storeScript(sha, string(args[2]))
	return bulkText(sha)
}

func (s *Server) handleScriptExists(args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	items := make([]respValue, 0, len(args)-2)
	for index := 2; index < len(args); index++ {
		items = append(items, s.scriptExistsResponse(string(args[index])))
	}
	return arrayValue(items...)
}

func (s *Server) scriptExistsResponse(sha string) respValue {
	if _, ok := s.scriptForSHA(sha); ok {
		return integerValue(1)
	}
	return integerValue(0)
}

func (s *Server) handleScriptFlush(args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	s.flushScripts()
	return simpleString("OK")
}

func (s *Server) evalScript(ctx context.Context, state *session, script string, keySpec []respArg) respValue {
	numKeys, rest, errResp := parseScriptKeyCount(keySpec)
	if errResp != nil {
		return *errResp
	}
	keys := rest[:numKeys]
	argv := rest[numKeys:]
	s.storeScript(scriptSHA(script), script)

	var response respValue
	err := s.runScriptAtomically(ctx, state, func(txCtx context.Context) error {
		response = s.runLuaScript(txCtx, state, script, keys, argv)
		if response.kind == respError {
			return fmt.Errorf("%w: %s", errScriptAbort, response.text)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errScriptAbort) {
			return response
		}
		return serviceError(err)
	}
	return response
}

func parseScriptKeyCount(args []respArg) (int, []respArg, *respValue) {
	if len(args) == 0 {
		err := syntaxError()
		return 0, nil, &err
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys < 0 || len(args)-1 < numKeys {
		out := errorString("ERR Number of keys can't be greater than number of args")
		return 0, nil, &out
	}
	return numKeys, args[1:], nil
}

func (s *Server) runScriptAtomically(
	ctx context.Context,
	state *session,
	fn func(context.Context) error,
) error {
	if state.txService != nil {
		return fn(ctx)
	}
	if err := state.activeService(s).Transaction(ctx, func(txCtx context.Context, txService cache.Service) error {
		state.txService = txService
		defer func() {
			state.txService = nil
		}()
		return fn(txCtx)
	}); err != nil {
		return fmt.Errorf("run redis script transaction: %w", err)
	}
	return nil
}

func (s *Server) runLuaScript(
	ctx context.Context,
	state *session,
	script string,
	keys []respArg,
	argv []respArg,
) respValue {
	scriptCtx, cancel := context.WithTimeout(ctx, defaultScriptTimeout)
	defer cancel()

	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	L.SetContext(scriptCtx)
	openScriptLibs(L)

	runtime := &scriptRuntime{
		server:   s,
		state:    state,
		ctx:      scriptCtx,
		maxCalls: defaultScriptMaxCalls,
	}
	L.SetGlobal("KEYS", luaStringTable(L, keys))
	L.SetGlobal("ARGV", luaStringTable(L, argv))
	L.SetGlobal("redis", runtime.redisTable(L))

	fn, err := L.LoadString(script)
	if err != nil {
		return errorString("ERR Error compiling script: " + err.Error())
	}
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		if errors.Is(scriptCtx.Err(), context.DeadlineExceeded) {
			return errorString("BUSY Redis Lua script timed out")
		}
		return errorString("ERR Error running script: " + err.Error())
	}
	return luaValueToRESP(L.Get(-1))
}

func (r *scriptRuntime) call(state *lua.LState) int {
	response, err := r.callRedis(state)
	if err != nil {
		state.RaiseError("%s", err.Error())
		return 0
	}
	if response.kind == respError {
		state.RaiseError("%s", response.text)
		return 0
	}
	state.Push(respToLuaValue(state, response))
	return 1
}

func (r *scriptRuntime) pcall(state *lua.LState) int {
	response, err := r.callRedis(state)
	if err != nil {
		state.Push(luaErrorTable(state, err.Error()))
		return 1
	}
	if response.kind == respError {
		state.Push(luaErrorTable(state, response.text))
		return 1
	}
	state.Push(respToLuaValue(state, response))
	return 1
}

func (r *scriptRuntime) callRedis(state *lua.LState) (respValue, error) {
	if err := r.ctx.Err(); err != nil {
		return respValue{}, fmt.Errorf("check Lua script context: %w", err)
	}
	r.callCount++
	if r.callCount > r.maxCalls {
		return respValue{}, errors.New("ERR Lua redis.call limit exceeded")
	}
	args := make([]respArg, 0, state.GetTop())
	for index := 1; index <= state.GetTop(); index++ {
		arg, err := luaValueToArg(state.Get(index))
		if err != nil {
			return respValue{}, err
		}
		args = append(args, arg)
	}
	if len(args) == 0 {
		return respValue{}, errors.New("ERR redis.call requires a command")
	}
	if !scriptCommandAllowed(strings.ToUpper(string(args[0]))) {
		return respValue{}, errors.New("ERR command is not allowed from Lua script")
	}
	return r.server.handleCommand(r.ctx, r.state, args), nil
}

func scriptCommandAllowed(command string) bool {
	switch command {
	case "AUTH", "HELLO", "QUIT", "SELECT", "CLIENT", "COMMAND":
		return false
	case "MULTI", "EXEC", "DISCARD", "EVAL", "EVALSHA", "SCRIPT":
		return false
	default:
		return true
	}
}

func luaValueToArg(value lua.LValue) (respArg, error) {
	switch typed := value.(type) {
	case lua.LString:
		return respArg([]byte(string(typed))), nil
	case lua.LNumber:
		return respArg([]byte(strconv.FormatFloat(float64(typed), 'f', -1, 64))), nil
	case lua.LBool:
		if bool(typed) {
			return respArg("1"), nil
		}
		return respArg("0"), nil
	default:
		return nil, errors.New("ERR Lua redis command arguments must be strings or integers")
	}
}
