package minidb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeAndDecode(t *testing.T) {
	encode := Encode(&DBEntry{
		Header: DBEntryHeader{
			CRC:       0,
			KeySize:   8,
			ValueSize: 8,
		},
		Key:   []byte("test_key"),
		Value: []byte("test_val"),
		Mark:  PUT,
	})
	entry := Decode(encode)

	assert.Equal(t, uint32(0), entry.Header.CRC, "CRC value is incorrect")
	assert.Equal(t, uint64(8), entry.Header.KeySize, "KeySize value is incorrect")
	assert.Equal(t, uint64(8), entry.Header.ValueSize, "ValueSize value is incorrect")
	assert.Equal(t, []byte("test_key"), entry.Key, "Key is incorrect")
	assert.Equal(t, []byte("test_val"), entry.Value, "Value is incorrect")
	assert.Equal(t, PUT, entry.Mark, "Mark value is incorrect")
}

func TestEncode(t *testing.T) {
	e := Encode(&DBEntry{
		Header: DBEntryHeader{
			CRC:       0,
			KeySize:   0,
			ValueSize: 0,
		},
		Key:   []byte(""),
		Value: []byte(""),
		Mark:  PUT,
	})

	assert.Equal(t, make([]byte, 21), e)
}

func TestDecode(t *testing.T) {
	// Arrange
	b := make([]byte, 21)
	b[20] = byte(DEL)

	// Act
	entry := Decode(b)

	// Assert
	assert.Equal(t, uint32(0), entry.Header.CRC, "CRC value is incorrect")
	assert.Equal(t, uint64(0), entry.Header.KeySize, "KeySize value is incorrect")
	assert.Equal(t, uint64(0), entry.Header.ValueSize, "ValueSize value is incorrect")
	assert.Equal(t, []byte(""), entry.Key, "Key is incorrect")
	assert.Equal(t, []byte(""), entry.Value, "Value is incorrect")
	assert.Equal(t, DEL, entry.Mark, "Mark value is incorrect")
}
