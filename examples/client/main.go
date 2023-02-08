package main

import (
	"context"
	"io"
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

	// Send a ListCapabilities request to the server
	resp, err := client.ListCapabilities(context.Background(), &datastream.ListCapabilitiesRequest{})
	if err != nil {
		log.Fatalf("error sending ListCapabilities request: %v", err)
	}

	// Log the capabilities returned by the server
	log.Printf("Capabilities: %v", resp.Capabilities)

	subkey := resp.Capabilities[0].Key

	// sync for subkey
	content, err := client.Sync(context.Background(), &datastream.DataRequest{Keys: []string{subkey}})
	if err != nil {
		log.Fatalf("error sending Sync request: %v", err)
	}

	for key, data := range content.Data {
		log.Printf("Received sync update for key %s value %v, stringified data %s", key, data.Value, string(data.Value))
	}

	log.Printf("Subscribing to key: %s", subkey)

	// Create a stream to receive updates
	stream, err := client.Subscribe(context.Background(), &datastream.DataRequest{Keys: []string{subkey}})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	for {
		// Receive updates from the stream
		update, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("stream closed")
				break
			}
			log.Fatalf("Failed to receive update: %v", err)
		}

		for key, data := range update.Data {
			log.Printf("Received subscription update for key %s value %v, stringified data %s", key, data.Value, string(data.Value))
		}
	}
}
