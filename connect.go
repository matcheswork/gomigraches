package migraches

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

func connect(dbname string) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"localhost", 5432, dbname, "", dbname)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return db, nil
}
