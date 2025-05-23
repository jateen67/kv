package main

import (
	"github.com/jateen67/kv/internal"
)

func main() {
	c := internal.NewDiskStoreDistributed(5)
	c.Open()
}
