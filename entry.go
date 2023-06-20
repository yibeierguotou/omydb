package bitekv

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/howeyc/crc16"
)

var (
	ErrCRCFailed = func(want, got uint16) error {
		return fmt.Errorf("CRC16 check failed. Expected: %d, Got: %d", want, got)
	}

	ErrHeaderBufSize = func(byts []byte) error {
		return fmt.Errorf("EntryHeader expected []byte size of %d, but %d found. Check your header underlying []byte", EntryHeaderSize, len(byts))
	}
)

/*
|         Header                             |          Body       |
CRC16:timestamp: key size:value size:bitmask | key bytes:value bytes
2 + 8  + 4 + 4 + 2
*/
const (
	//
	EntryHeaderSize = 20
)

type Header struct {
	KeySize   uint32
	ValueSize uint32
	Timestamp int64
	CRC16     uint16
	Bitmask   Bitmask
}

type Entry struct {
	Header *Header
	Key    []byte
	Value  []byte
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Header: &Header{
			Timestamp: time.Now().Unix(),
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
		},
		Key:   key,
		Value: value,
	}
}

func (e *Entry) String() string {
	unixTimeUTC := time.Unix(e.Header.Timestamp, 0) //gives unix time stamp in utc

	tsStr := unixTimeUTC.Format(time.RFC3339)
	return fmt.Sprintf("@%s, Key: |%s|, Value: |%v|", tsStr, string(e.Key), string(e.Value))
}

func (e *Entry) Size() int64 {
	return EntryHeaderSize + int64(e.Header.KeySize) + int64(e.Header.ValueSize)
}

func Encode(e *Entry) ([]byte, error) {
	byts := make([]byte, e.Size())

	binary.LittleEndian.PutUint64(byts[2:10], uint64(e.Header.Timestamp))
	binary.LittleEndian.PutUint32(byts[10:14], e.Header.KeySize)
	binary.LittleEndian.PutUint32(byts[14:18], e.Header.ValueSize)
	binary.LittleEndian.PutUint16(byts[18:20], uint16(e.Header.Bitmask))
	copy(byts[EntryHeaderSize:EntryHeaderSize+e.Header.KeySize], e.Key)
	copy(byts[EntryHeaderSize+e.Header.KeySize:], e.Value)

	// crc16 filling
	crc := crc16.Checksum(byts[2:], crc16.IBMTable)
	binary.LittleEndian.PutUint16(byts[0:2], crc)

	return byts, nil
}

func Decode(byts []byte) (*Entry, error) {
	h, err := ReadHeader(byts)
	if err != nil {
		return nil, err
	}

	return &Entry{
		Header: h,
		Key:    byts[EntryHeaderSize : EntryHeaderSize+h.KeySize],
		Value:  byts[EntryHeaderSize+h.KeySize:],
	}, nil

}

func ReadHeader(byts []byte) (*Header, error) {
	if len(byts) < EntryHeaderSize {
		return nil, ErrHeaderBufSize(byts)
	}

	return &Header{
		//CRC16:     binary.LittleEndian.Uint16(byts[0:2]),
		Timestamp: int64(binary.LittleEndian.Uint64(byts[2:10])),
		KeySize:   binary.LittleEndian.Uint32(byts[10:14]),
		ValueSize: binary.LittleEndian.Uint32(byts[14:18]),
		Bitmask:   Bitmask(binary.LittleEndian.Uint16(byts[18:20])),
	}, nil
}

func DecodeHeader(byts []byte) (*Header, error) {
	want := crc16.Checksum(byts[2:], crc16.IBMTable)
	got := binary.LittleEndian.Uint16(byts[0:2])
	fmt.Printf("DecodeHeader for %x(len: %d). \n\tcrc want: %d, got: %d\n", byts, len(byts), want, got)
	if want != got {
		return nil, ErrCRCFailed(want, got)
	}
	return &Header{
		//CRC16:     binary.LittleEndian.Uint16(byts[0:2]),
		Timestamp: int64(binary.LittleEndian.Uint64(byts[2:10])),
		KeySize:   binary.LittleEndian.Uint32(byts[10:14]),
		ValueSize: binary.LittleEndian.Uint32(byts[14:18]),
		Bitmask:   Bitmask(binary.LittleEndian.Uint16(byts[18:20])),
	}, nil
}
