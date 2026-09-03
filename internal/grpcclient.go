package internal

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jateen67/kv/proto"
)

func StartGRPCClient(destNodeAddr string) (proto.DataMigrationServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(destNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("error creating gRPC client on port %s: %w", destNodeAddr, err)
	}
	fmt.Println("grpc client started on port ", destNodeAddr)
	client := proto.NewDataMigrationServiceClient(conn)
	return client, conn, nil
}
