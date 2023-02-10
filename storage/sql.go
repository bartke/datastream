package storage

import (
	"database/sql"
	"strings"
	"time"
)

type SQLTable struct {
	db    *sql.DB
	table string

	syncInterval time.Duration
	errorChannel chan<- error
}

type SQLConfig struct {
	// DB is the database connection to use
	DB *sql.DB

	// Table is the name of the table to use for storing data
	Table string

	// SyncInterval is the interval at which the storage will sync to disk
	SyncInterval time.Duration

	// Optional
	ErrorChan chan<- error
}

func (s *SQLTable) forwardError(err error) {
	if s.errorChannel != nil {
		s.errorChannel <- err
	}
}

func (s *SQLTable) ListCapabilities() ([]Capability, error) {
	rows, err := s.db.Query("SELECT key, value_type FROM data")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var capabilities []Capability
	for rows.Next() {
		var capability Capability
		if err := rows.Scan(&capability.Key, &capability.ValueType); err != nil {
			return nil, err
		}
		capabilities = append(capabilities, capability)
	}
	return capabilities, nil
}

func (s *SQLTable) Sync(keys []string) (map[string]Data, error) {
	query := "SELECT key, value, value_type, updated_at FROM data WHERE key IN ("
	params := make([]interface{}, len(keys))
	for i, key := range keys {
		if i > 0 {
			query += ","
		}
		query += "?"
		params[i] = key
	}
	query += ")"
	rows, err := s.db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]Data)
	for rows.Next() {
		var data Data
		var updatedAtString string
		if err := rows.Scan(&data.Key, &data.Value, &data.ValueType, &updatedAtString); err != nil {
			return nil, err
		}
		data.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtString)
		if err != nil {
			return nil, err
		}
		result[data.Key] = data
	}
	return result, nil
}

func (s *SQLTable) Subscribe(keys []string) (<-chan Data, error) {
	dataChannel := make(chan Data)

	var last time.Time
	go func() {
		// Continuously poll the database for changes in the specified keys
		for {
			timeFormat := "2006-01-02 15:04:05"
			since := last.Format(timeFormat)

			rows, err := s.db.Query("SELECT key, value, value_type, updated_at FROM "+s.table+" WHERE key IN (?) and updated_at > ?", strings.Join(keys, ","), since)
			if err != nil {
				s.forwardError(err)
				break
			}
			defer rows.Close()

			for rows.Next() {
				var data Data
				var updatedAtString string
				if err := rows.Scan(&data.Key, &data.Value, &data.ValueType, &updatedAtString); err != nil {
					s.forwardError(err)
					continue
				}
				data.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtString)
				if err != nil {
					s.forwardError(err)
					continue
				}
				// if last is zero or the updated_at is after last, update last
				if last.IsZero() || data.UpdatedAt.After(last) {
					last = data.UpdatedAt
				}
				dataChannel <- data
			}

			<-time.After(s.syncInterval)
		}
		close(dataChannel)
	}()

	return dataChannel, nil
}

func (s *SQLTable) PushUpdate(data *Data) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec("INSERT OR REPLACE INTO data (key, value, value_type, updated_at) VALUES (?, ?, ?, ?)", data.Key, data.Value, data.ValueType, data.UpdatedAt.Format(time.RFC3339))
	if err != nil {
		return err
	}
	return tx.Commit()
}
