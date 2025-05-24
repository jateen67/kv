# Distributed Key-Value Store

This is a distributed LSM tree key-value store built with Go and gRPC, based on popular LSM tree databases like Apache Cassandra and ScyllaDB.

# How to Use

Make sure you have [https://grpc.io/docs/languages/go/quickstart/](gRPC) set up beforehand <br/>
Then, from the root directory, run `go run /cmd/main.go` <br/></br>
This will run a cluster with 5 nodes. The number of nodes can be easily changed in `cmd/main.go`
