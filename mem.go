package bitekv

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

var (
	ErrKeyNotFound = func(key []byte) error { return fmt.Errorf("key %s not found", string(key)) }
)

type KeyDIR struct {
	index  map[string]*ValueIndex
	rwLock sync.RWMutex
}

type ValueIndex struct {
	FD        *os.File
	Size      uint32
	Offset    int64
	Timestamp int64
}

func NewValueIndex(fd *os.File, size uint32, offset, ts int64) *ValueIndex {
	return &ValueIndex{fd, size, offset, ts}
}

func NewKeyDir() *KeyDIR {
	return &KeyDIR{
		index: make(map[string]*ValueIndex),
	}
}

// func (k *KeyDIR) Put(key, value[]byte)
// FIXME: should be a db layer api
func (k *KeyDIR) Put(key []byte, fd *os.File, vSize uint32, vOffset, ts int64) error {
	vIdx := NewValueIndex(fd, vSize, vOffset, ts)
	k.rwLock.Lock()
	k.index[string(key)] = vIdx
	k.rwLock.Unlock()
	return nil
}

func (k *KeyDIR) Get(key []byte) (idx *ValueIndex, err error) {
	k.rwLock.RLock()
	defer k.rwLock.RUnlock()

	if idx, ok := k.index[string(key)]; ok {
		return idx, nil
	}
	return nil, ErrKeyNotFound(key)
}

func (k *KeyDIR) String() {
	byts, _ := json.Marshal(k)
	fmt.Println(byts)
}
