package internal

import (
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jateen67/kv/proto"
)

func StartGRPCClient(destNodeAddr string) (proto.DataMigrationServiceClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(destNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("grpc client started on port ", destNodeAddr)
	client := proto.NewDataMigrationServiceClient(conn)
	return client, conn
}
