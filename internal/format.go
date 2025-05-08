package internal

import (
	"encoding/binary"
)

/*
The format for each key-value on disk is as follows:
-------------------------------------------------
| timestamp | key_size | value_size | key | value |
-------------------------------------------------
timestamp, key_size, value_size form the header of the entry and each of these must be 4 bytes at most
*/
const headerSize = 12

// Metadata about the KV pair, which is what we insert into the keydir
type KeyEntry struct {
	timestamp uint32
	totalSize uint32
	position  uint32
}

func NewKeyEntry(timestamp, position, size uint32) KeyEntry {
	return KeyEntry{
		timestamp: timestamp,
		totalSize: size,
		position:  position,
	}
}

func encodeHeader(timestamp, keySize, valueSize uint32) []byte {
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], timestamp)
	binary.LittleEndian.PutUint32(header[4:8], keySize)
	binary.LittleEndian.PutUint32(header[8:12], valueSize)
	return header
}

func decodeHeader(header []byte) (uint32, uint32, uint32) {
	timestamp := binary.LittleEndian.Uint32(header[0:4])
	keySize := binary.LittleEndian.Uint32(header[4:8])
	valueSize := binary.LittleEndian.Uint32(header[8:12])
	return timestamp, keySize, valueSize
}

func encodeKV(timestamp uint32, key string, value string) (int, []byte) {
	header := encodeHeader(timestamp, uint32(len(key)), uint32(len(value)))
	data := append([]byte(key), []byte(value)...)
	return headerSize + len(data), append(header, data...)
}

func decodeKV(data []byte) (uint32, string, string) {
	timestamp, keySize, valueSize := decodeHeader(data[0:headerSize])
	key := string(data[headerSize : headerSize+keySize])
	value := string(data[headerSize+keySize : headerSize+keySize+valueSize])
	return timestamp, key, value
}
