package minidb

import (
	"bytes"
	"encoding/binary"
	"os"
)

type DBFile struct {
	Offset int64
	File   *os.File
}

const FILE_NAME = "mini.db"
const MERGE_FILE_NAME = "~merge.mini.db"

func NewDBFile(dirPath string) (*DBFile, error) {
	return CreateNewDBFile(dirPath, FILE_NAME)
}

func NewMergeDBFile(dirPath string) (*DBFile, error) {
	return CreateNewDBFile(dirPath, MERGE_FILE_NAME)
}

func CreateNewDBFile(dirPath string, fileName string) (*DBFile, error) {
	dbFilePath := dirPath + string(os.PathSeparator) + fileName
	file, err := os.OpenFile(dbFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}

	return &DBFile{
		Offset: stat.Size(),
		File:   file,
	}, nil
}

func (dbFile *DBFile) Write(entry *DBEntry) error {
	encode := Encode(entry)
	n, err := dbFile.File.Write(encode)
	if err != nil {
		return err
	}
	dbFile.Offset += int64(n)
	return nil
}

func Encode(entry *DBEntry) []byte {
	buf := make([]byte, entry.GetSize())
	binary.BigEndian.PutUint32(buf[:4], entry.Header.CRC)
	binary.BigEndian.PutUint64(buf[4:12], entry.Header.KeySize)
	binary.BigEndian.PutUint64(buf[12:20], entry.Header.ValueSize)
	copy(buf[HEADER_SIZE:HEADER_SIZE+entry.Header.KeySize], entry.Key)
	copy(buf[HEADER_SIZE+entry.Header.KeySize:HEADER_SIZE+entry.Header.KeySize+entry.Header.ValueSize],
		entry.Value)
	copy(buf[HEADER_SIZE+entry.Header.KeySize:HEADER_SIZE+entry.Header.KeySize+entry.Header.ValueSize], entry.Value)
	copy(buf[len(buf)-1:], []byte{byte(entry.Mark)})

	return buf
}

func Decode(b []byte) (e *DBEntry) {
	e = &DBEntry{}

	reader := bytes.NewReader(b)
	binary.Read(reader, binary.BigEndian, &e.Header)

	offset := uint64(HEADER_SIZE)
	e.Key = b[offset : offset+e.Header.KeySize]

	offset += e.Header.KeySize
	e.Value = b[offset : offset+e.Header.ValueSize]

	offset += e.Header.ValueSize
	e.Mark = DBEntryAction(b[offset])

	return e
}

func (dbFile *DBFile) Read(offset int64) (entry *DBEntry, err error) {
	entry = &DBEntry{}

	// create buffer with size if the header
	var headerBuffer = make([]byte, HEADER_SIZE)
	_, err = dbFile.File.ReadAt(headerBuffer, offset)
	if err != nil {
		return nil, err
	}

	offset += int64(len(headerBuffer))

	// Read header
	reader := bytes.NewReader(headerBuffer)
	err = binary.Read(reader, binary.BigEndian, &entry.Header)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, entry.Header.KeySize+entry.Header.ValueSize+1)
	_, err = dbFile.File.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}

	entry = Decode(append(headerBuffer, buffer...))
	return entry, nil
}
