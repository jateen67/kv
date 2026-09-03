package internal

import (
	"bufio"
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
)

type Bucket struct {
	minTableSize  uint32
	avgBucketSize uint32
	bucketLow     float32
	bucketHigh    float32
	tables        []*SSTable
}

const DefaultTableSizeInBytes uint32 = 3_000

func InitBucket(table *SSTable) *Bucket {
	bucket := &Bucket{
		minTableSize: DefaultTableSizeInBytes,
		bucketLow:    0.5,
		bucketHigh:   1.5,
		tables:       []*SSTable{table},
	}
	bucket.calculateAvgBucketSize()
	return bucket
}

func InitEmptyBucket() *Bucket {
	bucket := &Bucket{
		minTableSize:  DefaultTableSizeInBytes,
		avgBucketSize: DefaultTableSizeInBytes,
		bucketLow:     0.5,
		bucketHigh:    1.5,
		tables:        []*SSTable{},
	}
	return bucket
}

// bucketLow/bucketHigh determine how close to the avg bucket size an SSTable can be (50% lower/higher by default).
func (b *Bucket) AdjustSizeThresholdParams(bucketLow, bucketHigh float32) {
	b.bucketLow = bucketLow
	b.bucketHigh = bucketHigh
}

func (b *Bucket) AppendTableToBucket(table *SSTable) {
	if table.totalSize < b.minTableSize {
		return
	}

	if len(b.tables) == 0 {
		b.tables = append(b.tables, table)
		b.calculateAvgBucketSize()
		return
	}

	lowerSizeThreshold := uint32(b.bucketLow * float32(b.avgBucketSize))   // 50% lower than avg size
	higherSizeThreshold := uint32(b.bucketHigh * float32(b.avgBucketSize)) // 50% higher than avg size

	// calculate low and high thresholds-- this avoids a skewed distribution of SSTable sizes within a given bucket
	if lowerSizeThreshold <= table.totalSize && table.totalSize <= higherSizeThreshold {
		b.tables = append(b.tables, table)
	} else {
		fmt.Println("Could not append table. Out of range")
	}

	//update avg size on each append
	b.calculateAvgBucketSize()
}

func (b *Bucket) calculateAvgBucketSize() {
	if len(b.tables) == 0 {
		b.avgBucketSize = DefaultTableSizeInBytes
		return
	}

	var sum uint32 = 0
	for i := range b.tables {
		sum += b.tables[i].totalSize
	}
	b.avgBucketSize = sum / uint32(len(b.tables))
}

func (b *Bucket) NeedsCompaction(minNumTables, maxNumTables int) bool {
	return len(b.tables) >= minNumTables && len(b.tables) <= maxNumTables
}

func (b *Bucket) TriggerCompaction() (*SSTable, error) {
	var allSortedRuns [][]*Record

	for i := range b.tables {
		// Set seek to 0 for every table otherwise the seek position will be at the end of each file by default
		_, err := b.tables[i].dataFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("error seeking to start of sstable %d: %w", i, err)
		}

		reader := bufio.NewReader(b.tables[i].dataFile)

		var currSortedRun []*Record
		headerBuf := make([]byte, headerSize)

		for {
			_, err := io.ReadFull(reader, headerBuf)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return nil, fmt.Errorf("error reading header from sstable %d: %w", i, err)
			}

			h := &Header{}
			err = h.decodeHeader(headerBuf)
			if err != nil {
				return nil, fmt.Errorf("error decoding header from sstable %d: %w", i, err)
			}

			// move the cursor so we can read the rest of the record
			// Read in the key-value after the header (cursor naturally moves)
			kvBuf := make([]byte, h.KeySize+h.ValueSize)
			_, err = io.ReadFull(reader, kvBuf)
			if err != nil {
				return nil, fmt.Errorf("error reading key-value from sstable %d: %w", i, err)
			}

			// Append the header and kv together in order to decode as a whole
			r := &Record{}
			err = r.DecodeKV(append(headerBuf, kvBuf...))
			if err != nil {
				return nil, fmt.Errorf("error decoding record from sstable %d: %w", i, err)
			}

			currSortedRun = append(currSortedRun, r) // store pointer, no dereference
		}
		allSortedRuns = append(allSortedRuns, currSortedRun)
	}

	// * Push all records into the min-heap for merging
	h := MinRecordHeap{}
	for i := range allSortedRuns {
		for j := range allSortedRuns[i] {
			heap.Push(&h, allSortedRuns[i][j])
		}
	}

	// now that they're all in a heap, we need to throw it into 1 big sstable
	finalSortedRun := make([]*Record, 0, h.Len())
	for h.Len() > 0 {
		ele := heap.Pop(&h)
		finalSortedRun = append(finalSortedRun, ele.(*Record))
	}

	finalSortedRun = filterAndDeleteTombstones(finalSortedRun)
	finalSortedRun = removeOutdatedEntries(finalSortedRun)

	// once the new merged table gets created, add it to a new bucket
	mergedSSTable, err := InitSSTableOnDisk(b.tables[0].nodeId, "storage", finalSortedRun)
	if err != nil {
		return nil, fmt.Errorf("error creating merged sstable: %w", err)
	}

	// ! now we need to delete the old sstables from disk to free up space
	err = deleteOldSSTables(b.tables)
	if err != nil {
		return nil, fmt.Errorf("error deleting old sstables: %w", err)
	}
	b.tables = b.tables[:0]

	return mergedSSTable, nil
}

// removes all records whose key appears as a tombstone and returns the filtered slice
func filterAndDeleteTombstones(sortedRun []*Record) []*Record {
	tombstones := make(map[string]struct{})
	for _, r := range sortedRun {
		if r.Header.Tombstone == 1 {
			tombstones[r.Key] = struct{}{}
		}
	}

	result := sortedRun[:0]
	for _, r := range sortedRun {
		if _, isTombstone := tombstones[r.Key]; !isTombstone {
			result = append(result, r)
		}
	}

	return result
}

func removeOutdatedEntries(sortedRun []*Record) []*Record {
	// take every entry -> append to a map, if value for a given map key is > 1,
	// then sort the value (which will be a slice) & delete all values except the last 1 in the overall slice

	var tempMap = make(map[string][]*Record)

	for i := range sortedRun {
		tempMap[sortedRun[i].Key] = append(tempMap[sortedRun[i].Key], sortedRun[i])
	}

	for _, v := range tempMap {
		if len(v) > 1 {
			slices.SortFunc(v, func(a, b *Record) int {
				return cmp.Compare(a.Header.TimeStamp, b.Header.TimeStamp)
			})

			// remove all but the most recent entry from the sorted run
			for i := 0; i < len(v)-1; i++ {
				idx := slices.Index(sortedRun, v[i])
				if idx != -1 {
					sortedRun = slices.Delete(sortedRun, idx, idx+1)
				}
			}
		}
	}

	return sortedRun
}

func deleteOldSSTables(tables []*SSTable) error {
	for i := range tables {
		files := []string{tables[i].dataFile.Name(), tables[i].indexFile.Name(), tables[i].bloomFilter.file.Name()}

		err := tables[i].Close()
		if err != nil {
			return fmt.Errorf("error losing sstable before deletion: %w", err)
		}

		for _, file := range files {
			err := os.Remove(file)
			if err != nil {
				return fmt.Errorf("error deleting sstable file %s: %w", file, err)
			}
		}
	}

	return nil
}
