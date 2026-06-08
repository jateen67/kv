package internal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/jateen67/kv/utils"
)

/*
-------------------------------------------------
| checksum | tombstone | timestamp | key_size | value_size | key | value |
-------------------------------------------------
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

func (h *Header) encodeHeader(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, &h.CheckSum)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	
	err = binary.Write(buf, binary.LittleEndian, &h.Tombstone)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	
	err = binary.Write(buf, binary.LittleEndian, &h.TimeStamp)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	
	err = binary.Write(buf, binary.LittleEndian, &h.KeySize)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	
	err = binary.Write(buf, binary.LittleEndian, &h.ValueSize)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}

	return nil
}

func (h *Header) decodeHeader(buf []byte) error {
	// must pass in reference b/c go is call by value and won't modify original otherwise
	_, err := binary.Decode(buf[:4], binary.LittleEndian, &h.CheckSum)
	if err != nil {
		return utils.ErrDecodingHeaderFailed
	}

	_, err = binary.Decode(buf[4:5], binary.LittleEndian, &h.Tombstone)
	if err != nil {
		return utils.ErrDecodingHeaderFailed
	}

	_, err = binary.Decode(buf[5:9], binary.LittleEndian, &h.TimeStamp)
	if err != nil {
		return utils.ErrDecodingHeaderFailed
	}

	_, err = binary.Decode(buf[9:13], binary.LittleEndian, &h.KeySize)
	if err != nil {
		return utils.ErrDecodingHeaderFailed
	}

	_, err = binary.Decode(buf[13:17], binary.LittleEndian, &h.ValueSize)
	if err != nil {
		return utils.ErrDecodingHeaderFailed
	}

	return nil
}

func (r *Record) EncodeKV(buf *bytes.Buffer) error {
	err := r.Header.encodeHeader(buf)
	if err != nil {
		return err
	}

	_, err = buf.WriteString(r.Key)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(r.Value)
	return err
}

func (r *Record) DecodeKV(buf []byte) error {
	err := r.Header.decodeHeader(buf[:headerSize])
	if err != nil {
		return err
	}
	r.Key = string(buf[headerSize : headerSize+r.Header.KeySize])
	r.Value = string(buf[headerSize+r.Header.KeySize : headerSize+r.Header.KeySize+r.Header.ValueSize])
	r.TotalSize = headerSize + r.Header.KeySize + r.Header.ValueSize
	return nil
}

func (r *Record) CalculateChecksum() (uint32, error) {
	headerBuf := new(bytes.Buffer)
	
	err := binary.Write(headerBuf, binary.LittleEndian, &r.Header.Tombstone)
	if err != nil {
		return 0, err
	}

	err = binary.Write(headerBuf, binary.LittleEndian, &r.Header.TimeStamp)
	if err != nil {
		return 0, err
	}

	err = binary.Write(headerBuf, binary.LittleEndian, &r.Header.KeySize)
	if err != nil {
		return 0, err
	}

	err = binary.Write(headerBuf, binary.LittleEndian, &r.Header.ValueSize)
	if err != nil {
		return 0, err
	}
	
	data := append([]byte(r.Key), []byte(r.Value)...)
	buf := append(headerBuf.Bytes(), data...)
	return crc32.ChecksumIEEE(buf), nil
}
