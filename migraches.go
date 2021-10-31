package migraches

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
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

func NewRollupService(db *sqlx.DB, createTableQs []string) *RollupService {
	return &RollupService{
		db,
		createTableQs,
	}
}

// Rollup created database with provided name
// and rolles up tables that was provided
// in NewRollupService constructor
func (r *RollupService) Rollup(name string) error {
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

	err := r.rollupDB(name)
	if err != nil {
		return err
	}

	err = r.rollupUser(name)
	if err != nil {
		return err
	}

	createdDB, err := connect(name)
	if err != nil {
		return err
	}

	tx, err := createdDB.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}

	var txErr error

	defer func() {
		defer createdDB.Close()

		if txErr != nil {
			// ignore error, but should be handled
			tx.Rollback()

			r.rollbackDB(name)
			r.rollbackUser(name)

			return
		}

		tx.Commit()
	}()

	for _, tq := range r.createTableQs {
		txErr = r.rollupTable(name, tq, tx)
		if txErr != nil {
			return txErr
		}
	}

	return nil
}
