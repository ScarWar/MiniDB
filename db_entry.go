package minidb

import (
	"hash/crc32"
)

const HEADER_SIZE = 8 + 8 + crc32.Size
const MARK_SIZE = 1

type DBEntryHeader struct {
	CRC       uint32
	KeySize   uint64
	ValueSize uint64
}

type DBEntry struct {
	Header DBEntryHeader
	Value  []byte
	Key    []byte
	Mark   DBEntryAction
}

type DBEntryAction int8

func (e *DBEntry) GetSize() int64 {
	return int64(e.Header.KeySize + e.Header.ValueSize + HEADER_SIZE + MARK_SIZE)
}

const (
	PUT DBEntryAction = iota
	DEL
)

func NewEntry(key []byte, value []byte, action DBEntryAction) (entry *DBEntry) {
	crc := crc32.ChecksumIEEE(append(key, value...))
	entry = &DBEntry{
		Header: DBEntryHeader{
			CRC:       crc,
			KeySize:   uint64(len(key)),
			ValueSize: uint64(len(value)),
		},
		Key:   key,
		Value: value,
		Mark:  action,
	}
	return entry
}
