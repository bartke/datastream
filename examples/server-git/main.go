package main

import (
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
	errorChan := make(chan error)
	ps, err := storage.NewGitRepository(storage.GitRepositoryConfig{
		RepoPath:     ".", // Use the current directory as a git repository
		SyncInterval: 5 * time.Second,
		ErrorChannel: errorChan,
	})
	if err != nil {
		log.Fatalf("error creating service: %v", err)
	}

	// subscribe to error channel from subscribe handler errors
	go func() {
		for err := range errorChan {
			log.Printf("error: %v", err)
		}
	}()

	srv := service.NewDataServiceServer(ps)

	// Start gRPC server
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating listener: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(shared.LogMiddleware))
	datastream.RegisterDataServiceServer(grpcServer, srv)

	log.Println("Starting gRPC server on :8080")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("error starting gRPC server: %v", err)
	}
}
