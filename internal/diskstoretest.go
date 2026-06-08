package internal

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkDiskStore_Put(b *testing.B) {
	store, _ := newStore(1)
	val := "val"
	for i := 0; i < b.N; i++ {
		key := generateRandomKey()
		err := store.Set(&key, &val)
		if err != nil {
			return
		}
	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkDiskStore_Get(b *testing.B) {
	store, _ := newStore(1)
	testK := "Fuzzy"
	val := "val"
	for i := 0; i < 1_000_000; i++ {
		if i == 4313 {
			err := store.Set(&testK, &val)
			if err != nil {
				return
			}
		} else {
			key := generateRandomKey()
			err := store.Set(&key, &val)
			if err != nil {
				return
			}
		}
	}
	store.FlushMemtable()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Get("Fuzzy")
		if err != nil {
			return
		}
	}
	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func generateRandomKey() string {
	return generateRandomString(10)
}

// generateRandomString generates a random string of a given length
func generateRandomString(length int) string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, length)
	for i := range b {
		b[i] = chars[rng.Intn(len(chars))]
	}
	
	return string(b)
}

func generateRandomEntry(store map[string]string) {
	// Generate a random string for the key
	key := generateRandomString(10)

	// Generate a random color from a predefined list
	colors := []string{"red", "green", "blue", "yellow", "orange", "purple", "pink", "brown", "black", "white"}
	color := colors[rand.Intn(len(colors))]

	// Store the key-value pair in the map
	store[key] = color
}
