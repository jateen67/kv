package internal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"time"
)

/*
notes:
ok so a bitcask on disk is just a directory (our databse server),
with multiple files inside it
	-> 1 active file, 0 or more inactive files

ok so how do we actually create the bitcask?
	-> single file on disk called the "main database server"
	-> this file will contain 1 or more data files (active/inactive)

within each data file:
	-> data format is: tstamp | ksz | value_sz | key | val
	-> a data file is nothing more than a linear sequence of the above entries

*note: the active data file will automatically close once it reaches a certain size threshold

this is DISK storage, so this will all be stored in SSD/HDD, therefore being persistent
*/

type DiskStore struct {
	serverFile    *os.File
	writePosition int
	keyDir        map[string]KeyEntry
}

func fileExists(fileName string) bool {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	ds := &DiskStore{keyDir: make(map[string]KeyEntry)}
	if fileExists(fileName) {
		err := ds.initKeyDir(fileName)
		if err != nil {
			return nil, errors.New("newdiskstore() error: could not init keydir")
		}
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.serverFile = file

	return ds, err
}

func (ds *DiskStore) Set(key string, value string) error {
	_, ok := ds.keyDir[key]
	if ok {
		return errors.New("set() error: key already exists")
	}
	if len(key) == 0 {
		return errors.New("set() error: key empty")
	}
	if len(value) == 0 {
		return errors.New("set() error: value empty")
	}

	header := Header{
		CheckSum:  0,
		Tombstone: 0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	record := Record{
		Header:    header,
		Key:       key,
		Value:     value,
		TotalSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum = record.CalculateChecksum()

	buf := new(bytes.Buffer)
	if err := record.EncodeKV(buf); err != nil {
		return errors.New("set() error: could not encode record")
	}
	err := ds.writeToFile(buf.Bytes())
	if err != nil {
		return errors.New("set() error: could not write to file")
	}

	ds.keyDir[key] = NewKeyEntry(header.TimeStamp, uint32(ds.writePosition), record.TotalSize)
	ds.writePosition += int(record.TotalSize)
	return nil
}

func (ds *DiskStore) Get(key string) (string, error) {
	keyEntry, ok := ds.keyDir[key]
	if !ok {
		return "", errors.New("get() error: key not found")
	}

	entireEntry := make([]byte, keyEntry.TotalSize)
	ds.serverFile.ReadAt(entireEntry, int64(keyEntry.Position))

	record := Record{}
	if err := record.DecodeKV(entireEntry); err != nil {
		return "", errors.New("get() error: decoding failed")
	}

	return record.Value, nil
}

func (ds *DiskStore) Delete(key string) error {
	// key note: this is an APPEND-ONLY db, so it wouldn't make sense to
	// overwrite existing data and place a tombstone value there
	// thus we have to write a semi-copy of the record w/ the tombstone val activated
	_, ok := ds.keyDir[key]
	if !ok {
		return errors.New("delete() error: key not found")
	}

	tempVal := ""
	header := Header{
		CheckSum:  0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(tempVal)),
	}
	header.Tombstone = 1

	record := Record{
		Header:    header,
		Key:       key,
		Value:     tempVal,
		TotalSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum = record.CalculateChecksum()

	buf := new(bytes.Buffer)
	if err := record.EncodeKV(buf); err != nil {
		return errors.New("delete() error: could not encode record")
	}
	ds.writeToFile(buf.Bytes())

	delete(ds.keyDir, key)
	return nil
}

func (ds *DiskStore) Close() bool {
	ds.serverFile.Sync()
	if err := ds.serverFile.Close(); err != nil {
		return false
	}
	return true
}

func (ds *DiskStore) initKeyDir(existingFile string) error {
	file, _ := os.Open(existingFile)
	defer file.Close()

	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		h := &Header{}
		err = h.decodeHeader(header)
		if err != nil {
			return err
		}

		key := make([]byte, h.KeySize)
		value := make([]byte, h.ValueSize)

		_, err = io.ReadFull(file, key)
		if err != nil {
			return err
		}

		_, err = io.ReadFull(file, value)
		if err != nil {
			return err
		}

		totalSize := headerSize + h.KeySize + h.ValueSize
		ds.keyDir[string(key)] = NewKeyEntry(h.TimeStamp, uint32(ds.writePosition), totalSize)
		if h.Tombstone == 1 {
			delete(ds.keyDir, string(key))
		}
		ds.writePosition += int(totalSize)
	}
	return nil
}

func (ds *DiskStore) writeToFile(data []byte) error {
	if _, err := ds.serverFile.Write(data); err != nil {
		return err
	}
	// file consistency very complex (comp310)
	if err := ds.serverFile.Sync(); err != nil {
		return err
	}
	return nil
}
