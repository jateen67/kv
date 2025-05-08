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
const headerSize = 13

// Metadata about the KV pair, which is what we insert into the keydir
type KeyEntry struct {
	tombstone uint8
	timestamp uint32
	totalSize uint32
	position  uint32
}

func NewKeyEntry(tombstone uint8, timestamp, position, size uint32) KeyEntry {
	return KeyEntry{
		tombstone: tombstone,
		timestamp: timestamp,
		totalSize: size,
		position:  position,
	}
}

func encodeHeader(tombstone uint8, timestamp, keySize, valueSize uint32) []byte {
	header := make([]byte, headerSize)
	header[0] = tombstone
	binary.LittleEndian.PutUint32(header[1:5], timestamp)
	binary.LittleEndian.PutUint32(header[5:9], keySize)
	binary.LittleEndian.PutUint32(header[9:13], valueSize)
	return header
}

func decodeHeader(header []byte) (uint8, uint32, uint32, uint32) {
	tombstone := uint8(header[0])
	timestamp := binary.LittleEndian.Uint32(header[1:5])
	keySize := binary.LittleEndian.Uint32(header[5:9])
	valueSize := binary.LittleEndian.Uint32(header[9:13])
	return tombstone, timestamp, keySize, valueSize
}

func encodeKV(tombstone uint8, timestamp uint32, key string, value string) (int, []byte) {
	header := encodeHeader(tombstone, timestamp, uint32(len(key)), uint32(len(value)))
	data := append([]byte(key), []byte(value)...)
	return headerSize + len(data), append(header, data...)
}

func decodeKV(data []byte) (uint8, uint32, string, string) {
	tombstone, timestamp, keySize, valueSize := decodeHeader(data[0:headerSize])
	key := string(data[headerSize : headerSize+keySize])
	value := string(data[headerSize+keySize : headerSize+keySize+valueSize])
	return tombstone, timestamp, key, value
}
