package internal

import (
	"bytes"
	"os"

	"github.com/jateen67/kv/utils"
)

const WALBatchThreshold = 1024 * 1024 * 3

// writeAheadLog maintains the log and batches operations to minimize disk writes
type writeAheadLog struct {
	file     *os.File
	opsBatch []byte
	size     int
}

func (w *writeAheadLog) clearBatch() {
	w.opsBatch = []byte{}
	w.size = 0
}

func (w *writeAheadLog) appendWALOperation(op Operation, record *Record) error {
	buf := new(bytes.Buffer)
	// Store operation as only 1 byte (only WAL entries will have this extra byte)
	buf.WriteByte(byte(op))

	// encode the entire key, value entry
	err := record.EncodeKV(buf)
	if err != nil {
		return utils.ErrEncodingKVFailed
	}

	// store in the batch
	w.opsBatch = append(w.opsBatch, buf.Bytes()...)
	w.size += len(buf.Bytes())

	if w.size >= WALBatchThreshold {
		return w.flushToDisk()
	}

	return nil
}

// Flushes the current batch of operations to disk, only called if size reaches WALBatchThreshold
func (w *writeAheadLog) flushToDisk() error {
	err := writeToFile(w.opsBatch, w.file)
	if err != nil {
		return err
	}

	w.clearBatch()
	return nil
}
