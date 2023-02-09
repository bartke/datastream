package storage

import (
	"database/sql"
	"fmt"
)

// SQLTable implements the Storage interface for a SQL database
func NewPostgresStorage(db *sql.DB, table string) (Storage, error) {
	// ensure that table exists and has the columns: key, value, value_type, updated_at
	// if not, error out
	stmt, err := db.Prepare("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=?")
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
	stmt, err = db.Prepare("SELECT column_name FROM information_schema.columns WHERE table_name=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(table)
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
		return nil, fmt.Errorf("table %s does not have a column named 'key'", table)
	}
	if !contains(columns, "value") {
		return nil, fmt.Errorf("table %s does not have a column named 'value'", table)
	}
	if !contains(columns, "value_type") {
		return nil, fmt.Errorf("table %s does not have a column named 'value_type'", table)
	}
	if !contains(columns, "updated_at") {
		return nil, fmt.Errorf("table %s does not have a column named 'updated_at'", table)
	}

	return &SQLTable{
		db:    db,
		table: table,
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
