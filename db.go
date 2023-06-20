package bitekv

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type DB struct {
	path string
	file *DBFile

	// in-mem index according to bitcask paper
	keyDir *KeyDIR

	mmapEnabled bool
}

type DBFile struct {
	fd         *os.File
	offset     int64
	tailOffset int64
}

func (df *DBFile) Read(offset int64) (*Entry, error) {

	// read header first
	headerBuf := make([]byte, EntryHeaderSize)
	_, err := df.fd.ReadAt(headerBuf, offset)
	if err != nil {
		return nil, err
	}
	header, err := ReadHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	keyBuf := make([]byte, header.KeySize)
	if _, err := df.fd.ReadAt(keyBuf, offset+EntryHeaderSize); err != nil {
		return nil, err
	}

	valBuf := make([]byte, header.ValueSize)
	if _, err = df.fd.ReadAt(valBuf, offset+EntryHeaderSize+int64(header.KeySize)); err != nil {
		return nil, err
	}

	// check CRC
	{

		//crcWant := crc16.Checksum(/**/, tab *Table)
	}

	return &Entry{
		Header: header,
		Key:    keyBuf,
		Value:  valBuf,
	}, nil

}

func (df *DBFile) Delete(offset int64) error {
	headerBuf := make([]byte, EntryHeaderSize)
	_, err := df.fd.ReadAt(headerBuf, offset)
	if err != nil {
		return err
	}
	header, err := ReadHeader(headerBuf)
	if err != nil {
		return err
	}

	// TODO: recaculate CRC-16
	header.Bitmask.AddFlag(ENTRY_DELETED)
	binary.BigEndian.PutUint16(headerBuf[18:20], uint16(header.Bitmask))

	_, err = df.fd.WriteAt(headerBuf, offset)
	return err

}

func (df *DBFile) Write(e *Entry) error {
	byts, err := Encode(e)
	if err != nil {
		return err
	}
	n, err := df.fd.WriteAt(byts, df.tailOffset)
	if err != nil {
		return err
	}

	df.tailOffset = df.tailOffset + int64(n)

	return nil
}

func NewDBFile(fd *os.File) *DBFile {
	return &DBFile{fd: fd}
}

func Open(path string, mode os.FileMode) (*DB, error) {
	db := &DB{path: path}
	db.keyDir = NewKeyDir()

	var err error
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {

		return nil, err
	}
	db.file = NewDBFile(fd)

	// load file , refill the keyDir
	err = db.LoadFile()
	if err != nil {
		return nil, err
	}

	fmt.Println(db.keyDir)

	return db, nil
}

func (db *DB) LoadFile() error {
	// read file from begining
	var offset int64 = 0
	for {
		fmt.Printf("load from offset: %d\n", offset)
		e, err := db.file.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		err = db.keyDir.Put(e.Key, db.file.fd, e.Header.ValueSize, offset, e.Header.Timestamp)
		if err != nil {
			return err
		}

		offset = offset + e.Size()
	}
	return nil
}

func (db *DB) Close() error {
	return db.file.fd.Close()
}

// TODO: delte older one if key exists
func (db *DB) Put(key, value []byte) error {
	// delete old entry if key exists already
	idx, err := db.keyDir.Get(key)
	if err == nil {
		db.file.Delete(idx.Offset)
	}

	entry := NewEntry(key, value)

	err = db.file.Write(entry)
	if err != nil {
		return err
	}

	return db.keyDir.Put(key, db.file.fd, entry.Header.ValueSize, db.file.tailOffset, entry.Header.Timestamp)

}

func (db *DB) Get(key []byte) ([]byte, error) {
	//
	idx, err := db.keyDir.Get(key)
	if err != nil {
		return nil, err
	}

	entry, err := db.file.Read(int64(idx.Offset))
	if err != nil {
		return nil, err
	}
	return entry.Value, nil

}

func (db *DB) Delete(key []byte) error {
	db.keyDir.rwLock.Lock()
	db.keyDir.rwLock.Unlock()

	idx, err := db.keyDir.Get(key)
	if err != nil {
		return err
	}

	db.file.Delete(idx.Offset)

	delete(db.keyDir.index, string(key))
	return nil

}
