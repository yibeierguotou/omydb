package bitekv

type Bitmask uint16

func (f Bitmask) HasFlag(flag Bitmask) bool { return f&flag != 0 }
func (f *Bitmask) AddFlag(flag Bitmask)     { *f |= flag }
func (f *Bitmask) ClearFlag(flag Bitmask)   { *f &= ^flag }
func (f *Bitmask) ToggleFlag(flag Bitmask)  { *f ^= flag }

const (
	ENTRY_DELETED Bitmask = 1 << iota
)
