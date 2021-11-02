package migraches

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Rollupper interface {
	Rollup(dbname string) error
}

type Table struct {
	Name string

	/*
		Fields must contain fields of
		table:

		[]string{
			"id serial primary key",
			"created_at timestamp not null default CURRENT_TIMESTAMP",
			"created_by int not null",
		}
	*/
	Fields []string

	/*
		WithSeq means that table has
		primary key with autoincrement
		id sequence.
		If true it runs grant access query as:

		GRANT USAGE, SELECT ON SEQUENCE {.Name}_id_seq TO {.Owner};
	*/
	WithSeq bool

	/*
		compiled the optimisation that contains the result
		of table.Srting() method.
	*/
	compiled string
}

// String implements Stringer interface
// and returns the ready for execute
// create table query string.
//
// First run caches result at Table.compiled
// and retuns it in each subsequent call
func (t *Table) String() string {
	if t.compiled != "" {
		return t.compiled
	}

	space := " "
	comma := ","

	var columns string

	for idx, field := range t.Fields {
		if idx == len(t.Fields)-1 {
			columns += field
			break
		}

		columns += field + comma
	}

	// prepare create table query
	q := strings.Join(
		[]string{
			fmt.Sprintf("CREATE TABLE %s", t.Name),
			"(",
			columns,
			");",
		},
		space,
	)

	// cache
	t.compiled = q

	return q
}

// RollupService is a 'postgres' implementation
// or Rollupper interface.
// Please use NewRollupService constructor only
type RollupService struct {
	db     *sqlx.DB
	tables []Table
}

var ErrNilDB error = fmt.Errorf("ErrNilDB")
var ErrZeroCreateTableQs error = fmt.Errorf("ErrZeroCreateTableQs")
var ErrNilCreateTableQs error = fmt.Errorf("ErrNilCreateTableQs")

/*
	NewRollupService creates RollupService with
	provided meta database and keeps tables
	schemas in Table structs in memory.
*/
func NewRollupService(metaDB *sqlx.DB, tables []Table) *RollupService {
	return &RollupService{
		db:     metaDB,
		tables: tables,
	}
}

// Rollup created database with provided name
// and rolles up tables that was provided
// with NewRollupService constructor
func (r *RollupService) Rollup(name string) (err error) {
	// prevent nil db
	if r.db == nil {
		return ErrNilDB
	}

	// prevent nil qs arr
	if r.tables == nil {
		return ErrNilCreateTableQs
	}

	// prevent zero qs arr
	if len(r.tables) == 0 {
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

	for _, table := range r.tables {
		err = r.rollupTableTx(name, table.String(), tx)
		if err != nil {
			return err
		}

		err = r.rollupTablePrivilegesTx(name, table.Name, tx)
		if err != nil {
			return err
		}

		if table.WithSeq {
			err = r.rollupIDSeqPrivilegesTx(name, table.Name, tx)
			if err != nil {
				return err
			}
		}
	}

	return
}
