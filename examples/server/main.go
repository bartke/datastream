package main

import (
	"log"
	"net"

	"github.com/bartke/datastream/examples/server/settings"
	"github.com/bartke/datastream/examples/shared"
	"github.com/bartke/datastream/generated/datastream"

	"google.golang.org/grpc"
)

func main() {
	// Open a SQLite database
	s, err := settings.New("./example.db")
	if err != nil {
		log.Fatalf("error creating service: %v", err)
	}
	defer s.Close()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating listener: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(shared.LogMiddleware))
	datastream.RegisterDataServiceServer(grpcServer, s)

	log.Println("Starting gRPC server on :8080")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("error starting gRPC server: %v", err)
	}
}
