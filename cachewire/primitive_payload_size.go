package cachewire

func primitiveResultPayloadSize(result PrimitiveResult) int {
	total := len(result.Value)
	for index := range result.Fields {
		total += len(result.Fields[index].Value)
	}
	for index := range result.Values {
		total += len(result.Values[index].Value)
	}
	return total
}

func primitiveResultsPayloadSize(items []PrimitiveResult) int {
	var total int
	for index := range items {
		total += primitiveResultPayloadSize(items[index])
	}
	return total
}
