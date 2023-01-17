package settings

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/bartke/datastream/generated/datastream"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	_ "github.com/mattn/go-sqlite3"
)

type Service struct {
	db *sql.DB
	datastream.UnimplementedDataServiceServer
}

func (s *Service) Close() error {
	return s.db.Close()
}

func New(path string) (*Service, error) {
	rand.Seed(time.Now().UnixNano())

	srv := Service{}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	srv.db = db

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

	return &srv, nil
}

func (s *Service) ListCapabilities(ctx context.Context, req *datastream.ListCapabilitiesRequest) (*datastream.ListCapabilitiesResponse, error) {
	rows, err := s.db.Query("SELECT key, value_type FROM data")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error querying data: %v", err)
	}
	defer rows.Close()

	var capabilities []*datastream.Capability
	for rows.Next() {
		var key, valueType string
		if err := rows.Scan(&key, &valueType); err != nil {
			return nil, status.Errorf(codes.Internal, "error scanning row: %v", err)
		}

		capabilities = append(capabilities, &datastream.Capability{
			Key:       key,
			ValueType: valueType,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return &datastream.ListCapabilitiesResponse{Capabilities: capabilities}, nil
}

func (s *Service) Sync(ctx context.Context, req *datastream.DataRequest) (*datastream.DataResponse, error) {
	stmt, err := s.db.Prepare("SELECT value, value_type, updated_at FROM data WHERE key = ?")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error preparing statement: %v", err)
	}
	defer stmt.Close()

	data := make(map[string]*datastream.Data)

	for _, key := range req.Keys {
		var d datastream.Data
		if err := stmt.QueryRow(key).Scan(&d.Value, &d.ValueType, &d.UpdatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "error scanning rows: %v", err)
		}

		data[key] = &d
	}

	return &datastream.DataResponse{Data: data}, nil
}

func (s *Service) Subscribe(req *datastream.DataRequest, stream datastream.DataService_SubscribeServer) error {
	// Start a goroutine to periodically check for updates
	go func() {
		// Keep track of the last update time
		var lastUpdate time.Time

		for {
			// Get all rows where updated_at is greater than lastUpdate
			rows, err := s.db.Query(`SELECT key, value, value_type, updated_at FROM data WHERE updated_at > ? and key in (?)`,
				lastUpdate, strings.Join(req.Keys, ","))
			if err != nil {
				log.Printf("Error querying data: %v", err)
				continue
			}
			defer rows.Close()

			// Iterate through the rows and send updates to the client
			data := make(map[string]*datastream.Data)
			for rows.Next() {
				var key, valueType string
				var value []byte
				var updatedAt time.Time
				if err := rows.Scan(&key, &value, &valueType, &updatedAt); err != nil {
					log.Printf("Error scanning row: %v", err)
					continue
				}

				// Add the update to the data map
				data[key] = &datastream.Data{
					Key:       key,
					Value:     value,
					ValueType: valueType,
					UpdatedAt: timestamppb.New(updatedAt),
				}

				// Update the last update time
				lastUpdate = updatedAt
			}

			if len(data) == 0 {
				continue
			}

			// Send the updates to the client
			if err := stream.Send(&datastream.DataResponse{Data: data}); err != nil {
				log.Printf("Error sending updates: %v", err)
				return
			}

			time.After(1 * time.Second)
		}
	}()

	// Wait for the client to cancel the request
	<-stream.Context().Done()
	return nil
}

func (s *Service) PushUpdate(ctx context.Context, req *datastream.Data) (*emptypb.Empty, error) {
	stmt, err := s.db.Prepare("INSERT OR REPLACE INTO data (key, value, value_type) VALUES (?, ?, ?)")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error preparing statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(req.Key, req.Value, req.ValueType)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error inserting data: %v", err)
	}

	return &emptypb.Empty{}, nil
}
