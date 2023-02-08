package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/bartke/datastream/generated/datastream"
	"github.com/bartke/datastream/storage"
	"github.com/bartke/datastream/storage/service"

	"google.golang.org/grpc"
)

func main() {
	ps, err := storage.NewGitRepository(storage.Params{
		RepoPath:     ".", // Use the current directory as a git repository
		SyncInterval: 5 * time.Second,
	})

	srv := service.NewDataServiceServer(ps)

	// Start gRPC server
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating listener: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(logMiddleware))
	datastream.RegisterDataServiceServer(grpcServer, srv)

	log.Println("Starting gRPC server on :8080")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("error starting gRPC server: %v", err)
	}
}

func logMiddleware(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	log.Printf("method: %s, duration: %s, error: %v", info.FullMethod, time.Since(start), err)
	return resp, err
}

type loggingMiddleware struct {
	next datastream.DataServiceServer
	datastream.UnimplementedDataServiceServer
}

func (mw *loggingMiddleware) ListCapabilities(ctx context.Context, req *datastream.ListCapabilitiesRequest) (*datastream.ListCapabilitiesResponse, error) {
	log.Printf("Received ListCapabilities request")
	return mw.next.ListCapabilities(ctx, req)
}

func (mw *loggingMiddleware) Sync(ctx context.Context, req *datastream.DataRequest) (*datastream.DataResponse, error) {
	log.Printf("Received Sync request with keys: %v", req.Keys)
	return mw.next.Sync(ctx, req)
}

func (mw *loggingMiddleware) Subscribe(req *datastream.DataRequest, stream datastream.DataService_SubscribeServer) error {
	log.Printf("Received Subscribe request with keys: %v", req.Keys)
	return mw.next.Subscribe(req, stream)
}
