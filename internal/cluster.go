package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jateen67/kv/http"
	"github.com/jateen67/kv/proto"
	"github.com/serialx/hashring"
	"google.golang.org/grpc"
)

type Node struct {
	server *grpc.Server
	ID     string
	Addr   string
	Store  *DiskStore
}

type Cluster struct {
	hashRing    *hashring.HashRing
	Nodes       map[string]*Node
	accumulator *dataMigrationAccumulator
}

var nodeCounter uint32 = 1
var currentNodePort uint32 = 11000

func (c *Cluster) initNodes(numOfNodes int) {
	c.Nodes = make(map[string]*Node)
	var nodeAddrs []string
	c.accumulator = &dataMigrationAccumulator{}

	for i := 0; i < numOfNodes; i++ {
		store, _ := NewDiskStore()
		node := Node{
			ID:    fmt.Sprintf("node-%d", nodeCounter),
			Addr:  fmt.Sprintf(":%d", currentNodePort),
			Store: store,
		}

		c.Nodes[node.Addr] = &node
		node.server = StartGRPCServer(node.Addr, &node)
		atomic.AddUint32(&currentNodePort, 1)
		atomic.AddUint32(&nodeCounter, 1)
		nodeAddrs = append(nodeAddrs, node.Addr)
	}

	c.hashRing = hashring.New(nodeAddrs)
	c.accumulator = &dataMigrationAccumulator{}
}

func (c *Cluster) AddNode() {
	store, _ := NewDiskStore()
	node := Node{
		ID:    fmt.Sprintf("node-%d", nodeCounter),
		Addr:  fmt.Sprintf(":%d", currentNodePort),
		Store: store,
	}
	c.Nodes[node.Addr] = &node
	node.server = StartGRPCServer(node.Addr, &node)
	atomic.AddUint32(&nodeCounter, 1)
	atomic.AddUint32(&currentNodePort, 1)

	// refresh the hash ring w/ new node
	c.hashRing = c.hashRing.AddNode(node.Addr)
	c.rebalance()
}

func (c *Cluster) RemoveNode(addr string) {
	_, ok := c.Nodes[addr]
	if ok {
		c.hashRing = c.hashRing.RemoveNode(addr)
		c.rebalance()
		c.Nodes[addr].server.GracefulStop()
		delete(c.Nodes, addr)
	} else {
		fmt.Printf("node @ addr %s not found", addr)
	}
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
		c.PrintDiagnostics()
		log.Println("signal received, shutting down...")
		err := clusterService.Close()
		if err != nil {
			fmt.Println(err)
		}
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
		fmt.Printf(v.ID + " @ address " + v.Addr + " , num keys: ")
		v.Store.LengthOfMemtable()
	}
}

type dataMigrationAccumulator struct {
	data map[string]map[string][]Record
}

func (d *dataMigrationAccumulator) Init(nodeAddresses []string) {
	d.data = make(map[string]map[string][]Record)
	for _, addr := range nodeAddresses {
		d.data[addr] = make(map[string][]Record)
	}
}

func (d *dataMigrationAccumulator) Append(srcNode string, destNode string, data *Record) {
	_, ok := d.data[srcNode][destNode]
	if !ok {
		d.data[srcNode][destNode] = make([]Record, 0)
	}
	d.data[srcNode][destNode] = append(d.data[srcNode][destNode], *data)
}

func (d *dataMigrationAccumulator) ClearAccumulator() {
	d.data = nil
}

func (c *Cluster) getAllNodeAddrs() []string {
	var addrs []string
	for addr, _ := range c.Nodes {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (c *Cluster) rebalance() {
	// go through every key in the system and see if the key's GetNode pos doesn't match up
	c.accumulator.Init(c.getAllNodeAddrs())

	for _, node := range c.Nodes {
		pairsMap := node.Store.memtable.GetAllKVPairs()

		for key, record := range pairsMap {
			newAddr, _ := c.hashRing.GetNode(key)

			if newAddr != node.Addr {
				c.accumulator.Append(node.Addr, newAddr, &record)
				node.Store.memtable.data.Remove(key)
			}
		}
	}

	for srcNode, v := range c.accumulator.data {
		for destNode, pairs := range v {
			if len(pairs) > 0 {
				c.transferDataBetweenNodes(srcNode, destNode, &pairs)
			}
		}
	}
	c.accumulator.ClearAccumulator()
}

func (c *Cluster) transferDataBetweenNodes(srcNodeAddr string, destNodeServerAddr string, data *[]Record) {
	client, conn := StartGRPCClient(destNodeServerAddr)
	defer conn.Close()

	kvPairs := convertRecordsToProtoKVPairs(data)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.MigrateKeyValuePairs(ctx, &proto.KeyValueMigrationRequest{
		SourceNodeAddr: srcNodeAddr,
		DestNodeAddr:   destNodeServerAddr,
		KvPairs:        kvPairs,
	})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(res)
}

func convertProtoRecordToStoreRecord(record *proto.Record) *Record {
	return &Record{
		Header: Header{
			CheckSum:  record.Header.Checksum,
			Tombstone: uint8(record.Header.Tombstone),
			TimeStamp: record.Header.Timestamp,
			KeySize:   record.Header.KeySize,
			ValueSize: record.Header.ValueSize,
		},
		Key:       record.Key,
		Value:     record.Value,
		TotalSize: record.TotalSize,
	}
}

func convertRecordsToProtoKVPairs(records *[]Record) []*proto.KVPair {
	var KVPairs []*proto.KVPair
	for _, rec := range *records {
		convRec := &proto.KVPair{
			Record: &proto.Record{
				Header: &proto.Header{
					Checksum:  rec.Header.CheckSum,
					Tombstone: uint32(rec.Header.Tombstone),
					Timestamp: rec.Header.TimeStamp,
					KeySize:   rec.Header.KeySize,
					ValueSize: rec.Header.ValueSize,
				},
				Key:       rec.Key,
				Value:     rec.Value,
				TotalSize: rec.TotalSize,
			},
		}
		KVPairs = append(KVPairs, convRec)
	}
	return KVPairs
}
