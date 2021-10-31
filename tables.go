package migraches

import (
	"database/sql"

	"github.com/pkg/errors"
)

func (r *RollupService) rollupTable(dbname, createTableQuery string, tx *sql.Tx) error {

	_, err := tx.Exec(createTableQuery)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
