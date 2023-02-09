package storage

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

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

	return &SQLTable{db: db, table: table}, nil
}
