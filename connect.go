package migraches

import (
	"database/sql"
	"fmt"
)

func connect(dbname string) (*sql.DB, error) {
	pqConnURI := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"localhost", 5432, dbname, "", dbname)

	db, err := sql.Open("postgres", pqConnURI)
	if err != nil {
		return nil, err
	}

	return db, nil
}
