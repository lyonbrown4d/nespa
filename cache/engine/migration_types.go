package engine

import "time"

type RangeOptions struct {
	Namespace  string
	Space      string
	VSlotStart uint32
	VSlotEnd   uint32
	Now        time.Time
}

type ImportResult struct {
	Imported uint64
}

type DeleteRangeResult struct {
	Deleted uint64
}
