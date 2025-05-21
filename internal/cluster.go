package internal

import (
	"fmt"

	"github.com/serialx/hashring"
)

type Node struct {
	ID    string
	Store *DiskStore
}

type Cluster struct {
	hashRing *hashring.HashRing
	Nodes    map[string]*Node
}

var nodeCount = 1

func (c *Cluster) initNodes(numOfNodes int) {
	c.Nodes = make(map[string]*Node)
	var nodeAddrs []string

	for i := 0; i < numOfNodes; i++ {
		store, _ := NewDiskStore()
		node := Node{
			ID:    fmt.Sprintf("node-%d", nodeCount),
			Store: store,
		}
		c.Nodes[node.ID] = &node
		nodeCount++
		nodeAddrs = append(nodeAddrs, node.ID)
	}

	c.hashRing = hashring.New(nodeAddrs)
}

func (c *Cluster) Get(key string) (string, error) {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.Nodes[nodeAddr]

	if ok {
		fmt.Println("key found at " + nodeAddr)
		return node.Store.Get(key)
	}

	return "", nil
}

func (c *Cluster) Set(key, value string) {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.Nodes[nodeAddr]

	if ok {
		node.Store.Set(&key, &value)
	}
}

func (c *Cluster) Delete(key string) error {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.Nodes[nodeAddr]

	if ok {
		fmt.Println("key deleted at " + nodeAddr)
		return node.Store.Delete(key)
	}

	return nil
}

func (c *Cluster) PrintDiagnostics() {
	for k, v := range c.Nodes {
		fmt.Printf("%s", k+" num keys: ")
		v.Store.LengthOfMemtable()
	}
}
