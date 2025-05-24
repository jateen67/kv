# Distributed Key-Value Store

This is a distributed LSM tree key-value store built with Go and gRPC, based on popular LSM tree databases like Apache Cassandra and ScyllaDB.

# How to Use

Make sure you have [https://grpc.io/docs/languages/go/quickstart/](gRPC) set up beforehand. <br/>
Then, from the root directory, run `go run /cmd/main.go`. <br/></br>
This will run a cluster with 5 nodes. The number of nodes can be easily changed in `cmd/main.go`.<br/></br>
When running the cluster, an HTTP server will open on port `8080`. It can be used to get, set, or delete keys. The nodes are hosted on ports `11000`, `11001`, etc.

### Get, Set, Delete key-value pairs

In another tab, run:

```
curl -XPOST localhost:8080/key -d '{"song1": "ohms", "song2": "song for the deaf", "song3": "around the fur"}'

curl -XGET localhost:8080/key/song2
-> song for the deaf

curl -XDELETE localhost:8080/key/song3
```

This will result in the following print statements:

```
key = song1 added @ node addr = :11004
key = song2 added @ node addr = :11001
key = song3 added @ node addr = :11003
deleted song3 @ node addr = :11003
```

### Add additional nodes

To add additional nodes and actually see the data redistribution in action, we can first add 50 key-value pairs:

```
curl -XPOST localhost:8080/key -d '{
    "song1": "ohms", "song2": "song for the deaf", "song3": "around the fur",
    "song4": "world in my eyes", "song5": "no one knows", "song6": "be quiet and drive",
    "song7": "up in arms", "song8": "straight jacket fitting", "song9": "better living through chemistry",
    "song10": "serve the servants", "song11": "everlong", "song12": "risk",
    "song13": "ceremony", "song14": "blue dress", "song15": "in my head",
    "song16": "selfless", "song17": "automatic stop", "song18": "7 words",
    "song19": "tempest", "song20": "pneuma", "song21": "minerva",
    "song22": "juicebox", "song23": "give it all", "song24": "bored",
    "song25": "slow animals", "song26": "carnavoyeur", "song27": "paper machete",
    "song28": "festival song", "song29": "anything", "song30": "lsf",
    "song31": "first it giveth", "song32": "song for the dead", "song33": "six shooter",
    "song34": "do it again", "song35": "gonna leave you", "song36": "go with the flow",
    "song37": "tomorrow", "song38": "another love song", "song39": "mosquito song",
    "song40": "976-evil", "song41": "headless", "song42": "radiant city",
    "song43": "hole in the earth", "song44": "urantia", "song45": "genesis",
    "song46": "headup", "song47": "this link is dead", "song48": "pompeji",
    "song49": "rosemary", "song50": "dai the flu"
}'
```

And then add an additional node, which automatically triggers data redistribution over the wire using gRPC:

```
curl -XPOST localhost:8080/add-node
```

This will result in all the operations being printed, including some lines that detail the data migration request.

### Removing nodes

```
curl -XPOST localhost:8080/remove-node/<node_port_number>
```
