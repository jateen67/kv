package internal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

/*
The format for each key-value on disk is as follows:
-------------------------------------------------
| timestamp | key_size | value_size | key | value |
-------------------------------------------------
timestamp, key_size, value_size form the header of the entry and each of these must be 4 bytes at most
*/
const headerSize = 17

// Metadata about the KV pair, which is what we insert into the keydir
type KeyEntry struct {
	TimeStamp uint32
	Position  uint32
	TotalSize uint32
}

type Header struct {
	CheckSum  uint32
	Tombstone uint8
	TimeStamp uint32
	KeySize   uint32
	ValueSize uint32
}

type Record struct {
	Header    Header
	Key       string
	Value     string
	TotalSize uint32
}

func NewKeyEntry(timestamp, position, totalSize uint32) KeyEntry {
	return KeyEntry{
		TimeStamp: timestamp,
		Position:  position,
		TotalSize: totalSize,
	}
}

func (h *Header) encodeHeader() []byte {
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], h.CheckSum)
	header[4] = h.Tombstone
	binary.LittleEndian.PutUint32(header[5:9], h.TimeStamp)
	binary.LittleEndian.PutUint32(header[9:13], h.KeySize)
	binary.LittleEndian.PutUint32(header[13:17], h.ValueSize)
	return header
}

func (h *Header) decodeHeader(header []byte) error {
	h.CheckSum = binary.LittleEndian.Uint32(header[0:4])
	h.Tombstone = uint8(header[4])
	h.TimeStamp = binary.LittleEndian.Uint32(header[5:9])
	h.KeySize = binary.LittleEndian.Uint32(header[9:13])
	h.ValueSize = binary.LittleEndian.Uint32(header[13:17])
	return nil
}

func (r *Record) EncodeKV() (int, []byte) {
	header := r.Header.encodeHeader()
	data := append([]byte(r.Key), []byte(r.Value)...)
	return headerSize + len(data), append(header, data...)
}

func (r *Record) DecodeKV(data []byte) error {
	err := r.Header.decodeHeader(data[0:headerSize])
	if err != nil {
		return err
	}
	r.Key = string(data[headerSize : headerSize+r.Header.KeySize])
	r.Value = string(data[headerSize+r.Header.KeySize : headerSize+r.Header.KeySize+r.Header.ValueSize])
	r.TotalSize = headerSize + r.Header.KeySize + r.Header.ValueSize
	return nil
}

func (r *Record) CalculateChecksum() uint32 {
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.Tombstone)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.TimeStamp)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.KeySize)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.ValueSize)
	data := append([]byte(r.Key), []byte(r.Value)...)
	buf := append(headerBuf.Bytes(), data...)
	return crc32.ChecksumIEEE(buf)
}
