package bitekv

import (
	"fmt"
	"testing"
)

func TestSetDeletedFlag(t *testing.T) {
	var mask Bitmask

	mask.AddFlag(ENTRY_DELETED)
	if mask.HasFlag(ENTRY_DELETED) {
		fmt.Println("YES")
	}
}
