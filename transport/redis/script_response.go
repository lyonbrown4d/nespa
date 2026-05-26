package redis

import lua "github.com/yuin/gopher-lua"

type luaRESPConverter func(lua.LValue) respValue

var luaRESPConverters map[string]luaRESPConverter

func init() {
	luaRESPConverters = map[string]luaRESPConverter{
		lua.LTBool.String():   luaBoolToRESP,
		lua.LTNumber.String(): luaNumberToRESP,
		lua.LTString.String(): luaStringToRESP,
		lua.LTTable.String():  luaTableToRESP,
	}
}

func luaValueToRESP(value lua.LValue) respValue {
	if value == lua.LNil {
		return nullBulkString()
	}
	converter, ok := luaRESPConverters[value.Type().String()]
	if !ok {
		return nullBulkString()
	}
	return converter(value)
}

func luaBoolToRESP(value lua.LValue) respValue {
	if lua.LVAsBool(value) {
		return integerValue(1)
	}
	return nullBulkString()
}

func luaNumberToRESP(value lua.LValue) respValue {
	number, ok := value.(lua.LNumber)
	if !ok {
		return nullBulkString()
	}
	return integerValue(int64(number))
}

func luaStringToRESP(value lua.LValue) respValue {
	text, ok := value.(lua.LString)
	if !ok {
		return nullBulkString()
	}
	return bulkText(string(text))
}

func luaTableToRESP(value lua.LValue) respValue {
	table, ok := value.(*lua.LTable)
	if !ok {
		return nullBulkString()
	}
	if status := table.RawGetString("ok"); status != lua.LNil {
		return simpleString(status.String())
	}
	if errValue := table.RawGetString("err"); errValue != lua.LNil {
		return errorString(errValue.String())
	}
	return luaTableArrayToRESP(table)
}

func luaTableArrayToRESP(table *lua.LTable) respValue {
	items := make([]respValue, 0, table.Len())
	for index := 1; index <= table.Len(); index++ {
		items = append(items, luaValueToRESP(table.RawGetInt(index)))
	}
	return arrayValue(items...)
}
