package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jateen67/kv/internal"
)

func main() {
	commands := "Commands:\n" +
		"\t- set     <key> <value>   : insert a key-value pair\n" +
		"\t- get     <key>           : get a key value\n" +
		"\t- del     <key>           : delete a key\n" +
		"\t- ctrl+c                  : exit\n" +
		"\t- help                    : show this message"

	store, _ := internal.NewDiskStore()

	fmt.Println(commands)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nEnter command: ")
		scanner.Scan()
		args := strings.Split(scanner.Text(), " ")

		switch args[0] {
		case "set":
			if len(args) != 3 {
				log.Fatal("Insufficient num of args")
			} else {
				key := args[1]
				val := args[2]
				store.Set(&key, &val)
			}
		case "get":
			if len(args) != 2 {
				log.Fatal("Insufficient num of args")
			} else {
				key := args[1]
				res, _ := store.Get(key)
				fmt.Println(res)
			}
		case "del":
			if len(args) != 2 {
				log.Fatal("Insufficient num of args")
			} else {
				key := args[1]
				err := store.Delete(key)
				if err != nil {
					fmt.Println("err: could not del key")
				} else {
					fmt.Println("deletion: success")
				}
			}
		case "help":
			fmt.Println("\n" + commands)
		}
	}
}
