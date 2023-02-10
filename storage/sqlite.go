package storage

import (
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// NewSQLiteStorage creates a new instance of a SQLite-based storage implementation
func NewSQLiteStorage(config SQLConfig) (Storage, error) {
	if err := config.DB.Ping(); err != nil {
		return nil, err
	}

	// ensure that table exists and has the columns: key, value, value_type, updated_at
	// if not, error out
	stmt, err := config.DB.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var name string
	if err := stmt.QueryRow(config.Table).Scan(&name); err != nil {
		return nil, err
	}
	if name != config.Table {
		return nil, fmt.Errorf("table %s does not exist", config.Table)
	}

	// check that the table has the correct columns
	stmt, err = config.DB.Prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var sql string

	if err := stmt.QueryRow(config.Table).Scan(&sql); err != nil {
		return nil, err
	}
	if !strings.Contains(sql, "key") {
		return nil, fmt.Errorf("table %s does not have a column named 'key'", config.Table)
	}
	if !strings.Contains(sql, "value") {
		return nil, fmt.Errorf("table %s does not have a column named 'value'", config.Table)
	}
	if !strings.Contains(sql, "value_type") {
		return nil, fmt.Errorf("table %s does not have a column named 'value_type'", config.Table)
	}
	if !strings.Contains(sql, "updated_at") {
		return nil, fmt.Errorf("table %s does not have a column named 'updated_at'", config.Table)
	}

	if config.SyncInterval == 0 {
		config.SyncInterval = DefaultSyncInterval
	}

	return &SQLTable{
		db:           config.DB,
		table:        config.Table,
		syncInterval: config.SyncInterval,
		errorChannel: config.ErrorChan,
	}, nil
}
