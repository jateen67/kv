package main

import (
	"fmt"

	"github.com/jateen67/kv/internal"
)

func main() {
	store, _ := internal.NewDiskStore("books.db")
	err := store.Set("othello", "shakespeare")
	if err != nil {
		fmt.Println(err)
	}
	author, err := store.Get("othello")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(author)
	// store.Delete("othello")
	// author, err = store.Get("othello")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(author)
}
