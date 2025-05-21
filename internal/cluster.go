package internal

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jateen67/kv/http"
	"github.com/serialx/hashring"
)

type Node struct {
	ID    string
	addr  string
	Store *DiskStore
}

type Cluster struct {
	hashRing *hashring.HashRing
	Nodes    map[string]*Node
}

var startingNodePort = 11000

func (c *Cluster) initNodes(numOfNodes int) {
	c.Nodes = make(map[string]*Node)
	var nodeAddrs []string

	for i := 0; i < numOfNodes; i++ {
		store, _ := NewDiskStore()
		node := Node{
			ID:    fmt.Sprintf("node-%d", i+1),
			addr:  fmt.Sprintf(":%d", startingNodePort),
			Store: store,
		}
		c.Nodes[node.addr] = &node
		startingNodePort++
		nodeAddrs = append(nodeAddrs, node.addr)
	}

	c.hashRing = hashring.New(nodeAddrs)
}

var defaultPort = ":8080"

func (c *Cluster) Open() {
	clusterService := http.NewClusterService(defaultPort, c)
	clusterService.Start()

	fmt.Println("HTTP server started successfully")
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	// Block until one of the signals above is received
	select {
	case <-signalCh:
		log.Println("signal received, shutting down...")
		err := clusterService.Close()
		if err != nil {
			fmt.Println(err)
		}
		c.PrintDiagnostics()
	}
}

func (c *Cluster) Get(key string) (string, error) {
	fmt.Printf("key = %s\t", key)
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.Nodes[nodeAddr]

	if ok {
		fmt.Printf("found @ node addr = %s\n", nodeAddr)
		return node.Store.Get(key)
	}

	return "", nil
}

func (c *Cluster) Set(key, value string) error {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	fmt.Printf("key = %s\t", key)
	fmt.Printf("added @ node addr = %s\n", nodeAddr)
	node, ok := c.Nodes[nodeAddr]

	if ok {
		return node.Store.Set(&key, &value)
	}
	return nil
}

func (c *Cluster) Delete(key string) error {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.Nodes[nodeAddr]

	if ok {
		fmt.Printf("deleted @ node addr = %s\n", nodeAddr)
		return node.Store.Delete(key)
	}

	return nil
}

func (c *Cluster) PrintDiagnostics() {
	fmt.Println("DIAGNOSTICS:")
	for _, v := range c.Nodes {
		fmt.Printf(v.ID + " @ address " + v.addr + " , num keys: ")
		v.Store.LengthOfMemtable()
	}
}
