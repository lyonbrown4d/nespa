package redis

func sliceRedisRange[T any](values []T, start, stop int64) []T {
	length := int64(len(values))
	if length == 0 {
		return nil
	}

	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return nil
	}
	return values[start : stop+1]
}
