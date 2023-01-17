package main

import (
	"context"
	"log"

	"github.com/bartke/datastream/generated/datastream"
	"google.golang.org/grpc"
)

func main() {
	// Connect to the server
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error connecting to server: %v", err)
	}
	defer conn.Close()

	// Create a client
	client := datastream.NewDataServiceClient(conn)

	// Send a PushUpdate request to the server
	_, err = client.PushUpdate(context.Background(), &datastream.Data{
		Key:       "max_connections",
		Value:     []byte("20"),
		ValueType: "int",
	})
	if err != nil {
		log.Fatalf("error sending PushUpdate request: %v", err)
	}
}
