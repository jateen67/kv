package internal

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/jateen67/kv/proto"
	"google.golang.org/grpc"
)

type dataMigrationServer struct {
	proto.UnimplementedDataMigrationServiceServer

	underlyingNode *Node
}

func (d *dataMigrationServer) MigrateKeyValuePairs(ctx context.Context, req *proto.KeyValueMigrationRequest) (*proto.KeyValueMigrationResponse, error) {
	fmt.Println(req)
	migrationResults := make([]*proto.MigrationResult, 0, len(req.KvPairs))
	overallSuccess := true

	for _, kv := range req.KvPairs {
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("error: migration cancelled: %w", err)
		}

		result := &proto.MigrationResult{Key: kv.Record.Key}

		err = d.underlyingNode.Store.PutRecordFromGRPC(kv.Record)
		if err != nil {
			// record the failure but continue -- caller needs to know which pairs succeeded
			result.Success = false
			result.ErrorMsg = err.Error()
			overallSuccess = false
		} else {
			result.Success = true
		}
		migrationResults = append(migrationResults, result)
	}

	return &proto.KeyValueMigrationResponse{
		Success:          overallSuccess,
		MigrationResults: migrationResults,
	}, nil
}

func StartGRPCServer(addr string, node *Node) (*grpc.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("error listen on port %s: %w", addr, err)
	}

	server := grpc.NewServer()
	service := &dataMigrationServer{underlyingNode: node}
	proto.RegisterDataMigrationServiceServer(server, service)

	go func() {
		log.Printf("gRPC server listening on port %s", addr)
		err = server.Serve(ln)
		if err != nil {
			log.Printf("failed to start gRPC server: %v", err)
		}
	}()
	return server, nil
}
