package migraches

import (
	"database/sql"
)

func (r *RollupService) rollupTable(dbname, createTableQuery string, tx *sql.Tx) error {

	_, err := tx.Exec(createTableQuery)
	if err != nil {
		return err
	}

	return nil
}
