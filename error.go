package migraches

import "fmt"

var (
	ErrNoTables          error = fmt.Errorf("ErrNoTables")
	ErrNilDB             error = fmt.Errorf("ErrNilDB")
	ErrZeroCreateTableQs error = fmt.Errorf("ErrZeroCreateTableQs")
	ErrNilCreateTableQs  error = fmt.Errorf("ErrNilCreateTableQs")
)
