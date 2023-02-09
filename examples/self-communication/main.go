package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/bartke/datastream/examples/shared"
	"github.com/bartke/datastream/generated/datastream"
	"github.com/bartke/datastream/storage"
	"github.com/bartke/datastream/storage/service"

	"google.golang.org/grpc"
)

func main() {
	ps, err := storage.NewGitRepository(storage.GitRepositoryConfig{
		RepoPath:     ".", // Use the current directory as a git repository
		SyncInterval: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("error creating service: %v", err)
	}

	startGrpcServer(service.NewDataServiceServer(ps))

	client := createGrpcClient(connectToGrpcServer())
	capabilities := getCapabilities(client)

	// Log the capabilities returned by the server
	log.Printf("Capabilities: %v", capabilities)
}

func startGrpcServer(srv datastream.DataServiceServer) *grpc.Server {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating listener: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(shared.LogMiddleware))
	datastream.RegisterDataServiceServer(grpcServer, srv)

	log.Println("Starting gRPC server on :8080")
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("error starting gRPC server: %v", err)
		}
	}()

	return grpcServer
}

func connectToGrpcServer() *grpc.ClientConn {
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error connecting to server: %v", err)
	}

	return conn
}

func createGrpcClient(conn *grpc.ClientConn) datastream.DataServiceClient {
	return datastream.NewDataServiceClient(conn)
}

func getCapabilities(client datastream.DataServiceClient) []*datastream.Capability {
	resp, err := client.ListCapabilities(context.Background(), &datastream.ListCapabilitiesRequest{})
	if err != nil {
		log.Fatalf("error sending ListCapabilities request: %v", err)
	}

	return resp.Capabilities
}
