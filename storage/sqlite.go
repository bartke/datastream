package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteTable struct {
	db    *sql.DB
	table string
}

// NewSQLiteStorage creates a new instance of a SQLite-based storage implementation
func NewSQLiteStorage(db *sql.DB, table string) (Storage, error) {
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// ensure that table exists and has the columns: key, value, value_type, updated_at
	// if not, error out
	stmt, err := db.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var name string
	if err := stmt.QueryRow(table).Scan(&name); err != nil {
		return nil, err
	}
	if name != table {
		return nil, fmt.Errorf("table %s does not exist", table)
	}

	// check that the table has the correct columns
	stmt, err = db.Prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var sql string

	if err := stmt.QueryRow(table).Scan(&sql); err != nil {
		return nil, err
	}
	if !strings.Contains(sql, "key") {
		return nil, fmt.Errorf("table %s does not have a column named 'key'", table)
	}
	if !strings.Contains(sql, "value") {
		return nil, fmt.Errorf("table %s does not have a column named 'value'", table)
	}
	if !strings.Contains(sql, "value_type") {
		return nil, fmt.Errorf("table %s does not have a column named 'value_type'", table)
	}
	if !strings.Contains(sql, "updated_at") {
		return nil, fmt.Errorf("table %s does not have a column named 'updated_at'", table)
	}

	return &SQLiteTable{db: db, table: table}, nil
}

func (s *SQLiteTable) ListCapabilities() ([]Capability, error) {
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

func (s *SQLiteTable) Sync(keys []string) (map[string]Data, error) {
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

func (s *SQLiteTable) Subscribe(keys []string) (<-chan Data, error) {
	dataChannel := make(chan Data)
	var last time.Time
	go func() {
		// Continuously poll the database for changes in the specified keys
		for {
			timeFormat := "2006-01-02 15:04:05"
			since := last.Format(timeFormat)

			rows, err := s.db.Query("SELECT key, value, value_type, updated_at FROM "+s.table+" WHERE key IN (?) and updated_at > ?", strings.Join(keys, ","), since)
			if err != nil {
				fmt.Println("Error querying database: ", err)
				break
			}
			defer rows.Close()

			for rows.Next() {
				var data Data
				var updatedAtString string
				if err := rows.Scan(&data.Key, &data.Value, &data.ValueType, &updatedAtString); err != nil {
					fmt.Println("Error scanning row: ", err)
					break
				}
				data.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtString)
				if err != nil {
					fmt.Println("Error parsing updated_at: ", err)
					break
				}
				// if last is zero or the updated_at is after last, update last
				if last.IsZero() || data.UpdatedAt.After(last) {
					last = data.UpdatedAt
				}
				dataChannel <- data
			}

			<-time.After(5 * time.Second)
		}
		close(dataChannel)
	}()
	return dataChannel, nil
}

func (s *SQLiteTable) PushUpdate(data *Data) error {
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
