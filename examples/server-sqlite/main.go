package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/bartke/datastream/examples/shared"
	"github.com/bartke/datastream/generated/datastream"
	"github.com/bartke/datastream/storage"
	"github.com/bartke/datastream/storage/service"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	db, err := initDB("./example.db")
	if err != nil {
		log.Fatalf("error creating service: %v", err)
	}

	ps, err := storage.NewSQLiteStorage(storage.SQLConfig{
		DB:           db,
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

func initDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	// Create the sample data table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS data (
			key TEXT PRIMARY KEY,
			value BLOB,
			value_type TEXT,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TRIGGER IF NOT EXISTS set_updated_at
		BEFORE UPDATE ON data
		BEGIN
			UPDATE data SET updated_at = datetime('now') WHERE rowid = new.rowid;
		END;
	`)
	if err != nil {
		return nil, fmt.Errorf("error creating data table: %w", err)
	}

	// seed sample settings
	stmt, err := db.Prepare("INSERT OR REPLACE INTO data (key, value, value_type) VALUES (?, ?, ?)")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error preparing statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec("max_connections", []byte("10"), "int")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error inserting data: %v", err)
	}

	_, err = stmt.Exec("rate_limit", []byte("1000"), "int")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error inserting data: %v", err)
	}

	_, err = stmt.Exec("debug_enabled", []byte("false"), "bool")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error inserting data: %v", err)
	}

	return db, nil
}
