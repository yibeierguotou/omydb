package bitekv

import (
	"fmt"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	key := []byte("hello")
	val := []byte("babe")

	e := NewEntry(key, val)

	byts, _ := Encode(e)
	fmt.Println(byts)
	fmt.Println(len(byts))

	ne, err := Decode(byts)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(ne)
}
