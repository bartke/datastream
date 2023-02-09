package storage

import (
	"fmt"
)

// PostgresStorage implements the Storage interface for a Postgres database
func NewPostgresStorage(config SQLConfig) (Storage, error) {
	// ensure that table exists and has the columns: key, value, value_type, updated_at
	// if not, error out
	stmt, err := config.DB.Prepare("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=?")
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
	stmt, err = config.DB.Prepare("SELECT column_name FROM information_schema.columns WHERE table_name=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(config.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !contains(columns, "key") {
		return nil, fmt.Errorf("table %s does not have a column named 'key'", config.Table)
	}
	if !contains(columns, "value") {
		return nil, fmt.Errorf("table %s does not have a column named 'value'", config.Table)
	}
	if !contains(columns, "value_type") {
		return nil, fmt.Errorf("table %s does not have a column named 'value_type'", config.Table)
	}
	if !contains(columns, "updated_at") {
		return nil, fmt.Errorf("table %s does not have a column named 'updated_at'", config.Table)
	}

	return &SQLTable{
		db:           config.DB,
		table:        config.Table,
		syncInterval: config.SyncInterval,
		errorChannel: config.ErrorChan,
	}, nil
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
