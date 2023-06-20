package main

import (
	"fmt"

	"github.com/iPush/bitekv"
)

type pair struct {
	key string
	val string
}

func main() {

	db, err := bitekv.Open("./demo", 0600)
	if err != nil {
		panic(err)
	}
	pairs := []*pair{&pair{"hello", "world"}, &pair{"Dire", "Straits"}, &pair{"Ella", "Fitz"}}
	for _, kv := range pairs {
		db.Put([]byte(kv.key), []byte(kv.val))
	}

	for _, kv := range pairs {
		v, err := db.Get([]byte(kv.key))
		if err != nil {
			fmt.Printf("failed to get key: %s. err: %s\n", kv.key, err.Error())
			continue
		}
		fmt.Printf("Key: %s. Value: %s\n", kv.key, string(v))
	}
	v, err := db.Get([]byte("NOTHINGATALL"))
	if err != nil {
		fmt.Println(err.Error())
	} else {

		fmt.Println(v)
	}

	b1 := []byte("hello")
	b2 := []byte("babe")

	b1 = append(b1, b2...)
	fmt.Println(b1)
}
