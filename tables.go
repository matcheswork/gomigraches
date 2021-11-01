package migraches

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

func (r *RollupService) rollupTableTx(dbname, createTableQuery string, tx *sql.Tx) error {

	_, err := tx.Exec(createTableQuery)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *RollupService) rollupTablePrivilegesTx(ownerName, tableName string, tx *sql.Tx) error {
	query := fmt.Sprintf("GRANT ALL PRIVILEGES ON %s TO %s", tableName, ownerName)

	_, err := tx.Exec(query)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *RollupService) rollupIDSeqPrivilegesTx(ownerName, tableName string, tx *sql.Tx) error {
	query := fmt.Sprintf("GRANT USAGE, SELECT ON SEQUENCE %s_id_seq TO %s", tableName, ownerName)

	_, err := tx.Exec(query)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
