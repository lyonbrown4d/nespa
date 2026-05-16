package cachewire

func appendMembers(raw []byte, members []string) []byte {
	raw = appendCount(raw, stringCount(members))
	for index := range members {
		raw = appendString(raw, members[index])
	}
	return raw
}

func (c *metadataCursor) readMembers() ([]string, error) {
	count, err := c.readCount()
	if err != nil {
		return nil, err
	}
	members := make([]string, 0, count)
	for range count {
		member, readErr := c.readString()
		if readErr != nil {
			return nil, readErr
		}
		members = append(members, member)
	}
	return members, nil
}

func appendScoredMembers(raw []byte, members []ScoredMember) []byte {
	raw = appendCount(raw, scoredMemberCount(members))
	for index := range members {
		raw = appendString(raw, members[index].Member)
		raw = appendFloat64(raw, members[index].Score)
	}
	return raw
}

func (c *metadataCursor) readScoredMembers() ([]ScoredMember, error) {
	count, err := c.readCount()
	if err != nil {
		return nil, err
	}
	members := make([]ScoredMember, 0, count)
	for range count {
		member, readErr := c.readScoredMember()
		if readErr != nil {
			return nil, readErr
		}
		members = append(members, member)
	}
	return members, nil
}

func (c *metadataCursor) readScoredMember() (ScoredMember, error) {
	member, err := c.readString()
	if err != nil {
		return ScoredMember{}, err
	}
	score, err := c.readFloat64()
	if err != nil {
		return ScoredMember{}, err
	}
	return ScoredMember{Member: member, Score: score}, nil
}

func primitiveRequestCount(items []PrimitiveRequest) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}

func primitiveResultCount(items []PrimitiveResult) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}

func mapFieldCount(items []MapField) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}

func stringCount(items []string) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}

func scoredMemberCount(items []ScoredMember) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}
