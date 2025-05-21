package internal

import (
	"math/rand"
	"testing"
	"time"
)

var epoch = 1_000

func BenchmarkDiskStore_Put(b *testing.B) {
	store, _ := NewDiskStore()
	val := "val"
	for i := 0; i < b.N; i++ {
		key := generateRandomKey()
		store.Set(&key, &val)
	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkDiskStore_Get(b *testing.B) {
	store, _ := NewDiskStore()
	testK := "Fuzzy"
	val := "val"
	for i := 0; i < 1_000_000; i++ {
		if i == 4313 {
			store.Set(&testK, &val)
		} else {
			key := generateRandomKey()
			store.Set(&key, &val)
		}
	}
	store.FlushMemtable()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Get("Fuzzy")
	}
	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func generateRandomKey() string {
	return generateRandomString(10)
}

// generateRandomString generates a random string of a given length
func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]

	}
	return string(b)
}
