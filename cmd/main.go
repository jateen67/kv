package main

import (
	"github.com/jateen67/kv/internal"
)

func main() {
	c := internal.NewCluster(5)
	c.Open()
}
