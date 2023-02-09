package main

import (
	"log"
	"net"
	"os"
	"time"

	"github.com/bartke/datastream/examples/shared"
	"github.com/bartke/datastream/generated/datastream"
	"github.com/bartke/datastream/storage"
	"github.com/bartke/datastream/storage/service"

	"google.golang.org/grpc"
)

func main() {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	region := os.Getenv("MINIO_REGION")
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	bucket := os.Getenv("MINIO_BUCKET")

	ps, err := storage.NewS3Storage(storage.S3StorageConfig{
		Endpoint:     endpoint,
		Region:       region,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		Bucket:       bucket,
		SyncInterval: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("error creating service: %v", err)
	}

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
