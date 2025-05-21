package internal

import (
	"testing"
)

func BenchmarkMemtable_Put(b *testing.B) {
	memtable := NewMemtable()

	for i := 0; i < b.N; i++ {
		key := generateRandomKey()
		record := &Record{
			Header:    Header{},
			Key:       key,
			Value:     "testVal",
			TotalSize: 0,
		}
		memtable.Set(&key, record)
	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkMemtable_Get(b *testing.B) {
	memtable := NewMemtable()

	for i := 0; i < 1_000_000; i++ {
		key := generateRandomKey()
		record := &Record{
			Header:    Header{},
			Key:       key,
			Value:     "testVal",
			TotalSize: 0,
		}
		memtable.Set(&key, record)
	}
	testKey := "Fuzzy"
	memtable.Set(&testKey, &Record{})
	b.ResetTimer()

	for i := 0; i < 1_000_000; i++ {
		memtable.Get(&testKey)

	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}
