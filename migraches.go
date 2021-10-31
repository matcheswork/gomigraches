package migraches

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Rollupper interface {
	Rollup(dbname string) error
}

// RollupService is a 'postgres' implementation
// or Rollupper interface.
// Please use NewRollupService constructor only
type RollupService struct {
	db            *sqlx.DB
	createTableQs []string
}

var ErrNilDB error = fmt.Errorf("ErrNilDB")
var ErrZeroCreateTableQs error = fmt.Errorf("ErrZeroCreateTableQs")
var ErrNilCreateTableQs error = fmt.Errorf("ErrNilCreateTableQs")

/*
	NewRollupService creates RullupService with
	provided meta database and keep create table
	queries in memory. The create table query may include multiple queries:

	CREATE TABLE mock (
		id serial primary key,
		created_at timestamp not null default CURRENT_TIMESTAMP
	);

	GRANT ALL PRIVILEGES ON mock TO mock_user;

	GRANT USAGE, SELECT ON SEQUENCE mock_id_seq TO mock_user;
*/
func NewRollupService(metaDB *sqlx.DB, createTableQs []string) *RollupService {
	return &RollupService{
		db:            metaDB,
		createTableQs: createTableQs,
	}
}

// Rollup created database with provided name
// and rolles up tables that was provided
// in NewRollupService constructor
func (r *RollupService) Rollup(name string) (err error) {
	// prevent nil db
	if r.db == nil {
		return ErrNilDB
	}

	// prevent nil qs arr
	if r.createTableQs == nil {
		return ErrNilCreateTableQs
	}

	// prevent zero qs arr
	if len(r.createTableQs) == 0 {
		return ErrZeroCreateTableQs
	}

	err = r.rollupDB(name)
	if err != nil {
		return err
	}
	dbCreated := true

	err = r.rollupUser(name)
	if err != nil {
		return err
	}
	userCreated := true

	createdDB, err := connect(name)
	if err != nil {
		return err
	}

	tx, err := createdDB.BeginTx(context.Background(), nil)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() {
		if err != nil {
			// ignore error, but should be handled
			if tx != nil {
				tx.Rollback()
			}

			// close created db if opened
			createdDB.Close()

			if dbCreated {
				r.rollbackDB(name)
			}

			if userCreated {
				r.rollbackUser(name)
			}

			return
		}

		tx.Commit()
	}()

	for _, tq := range r.createTableQs {
		err = r.rollupTable(name, tq, tx)
		if err != nil {
			return err
		}
	}

	return
}
