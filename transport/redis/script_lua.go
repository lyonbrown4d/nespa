package redis

import lua "github.com/yuin/gopher-lua"

func openScriptLibs(state *lua.LState) {
	lua.OpenBase(state)
	lua.OpenTable(state)
	lua.OpenString(state)
	lua.OpenMath(state)
}

func luaStringTable(state *lua.LState, args []respArg) *lua.LTable {
	table := state.NewTable()
	for index := range args {
		table.RawSetInt(index+1, lua.LString(args[index]))
	}
	return table
}

func (r *scriptRuntime) redisTable(state *lua.LState) *lua.LTable {
	table := state.NewTable()
	state.SetField(table, "call", state.NewFunction(r.call))
	state.SetField(table, "pcall", state.NewFunction(r.pcall))
	return table
}

func respToLuaValue(state *lua.LState, value respValue) lua.LValue {
	switch value.kind {
	case respSimpleString:
		return luaStatusTable(state, value.text)
	case respError:
		return luaErrorTable(state, value.text)
	case respInteger:
		return lua.LNumber(value.number)
	case respBulkString:
		return lua.LString(value.bytes)
	case respNullBulkString:
		return lua.LFalse
	case respArray:
		return luaArrayTable(state, value.items)
	case respMap:
		return lua.LNil
	default:
		return lua.LNil
	}
}

func luaStatusTable(state *lua.LState, message string) *lua.LTable {
	table := state.NewTable()
	state.SetField(table, "ok", lua.LString(message))
	return table
}

func luaErrorTable(state *lua.LState, message string) *lua.LTable {
	table := state.NewTable()
	state.SetField(table, "err", lua.LString(message))
	return table
}

func luaArrayTable(state *lua.LState, items []respValue) *lua.LTable {
	table := state.NewTable()
	for index := range items {
		table.RawSetInt(index+1, respToLuaValue(state, items[index]))
	}
	return table
}
