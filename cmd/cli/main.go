package main

import (
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	migraches "github.com/matcheswork/gomigraches"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func main() {
	args := os.Args[1:]

	if len(args) < 3 {
		fmt.Println("please provide database name to create: ./gomigraches db_to_create_name meta_db_name meta_db_user")
		return
	}

	dbName := args[0]
	metaDBName := args[1]
	metaDBUser := args[2]

	fmt.Printf("Enter meta db password: ")

	metaDBPassword := ""

	fmt.Scanf("%s", &metaDBPassword)

	pqConnURI := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"localhost", 5432, metaDBUser, metaDBPassword, metaDBName)

	psqlDB, err := sqlx.Connect("postgres", pqConnURI)
	if err != nil {
		fmt.Printf("%+v", errors.WithStack(err))
		return
	}

	rup := migraches.NewRollupService(psqlDB, []string{createMockTableQ})

	err = rup.Rollup(dbName)
	if err != nil {
		fmt.Printf("%+v", errors.WithStack(err))
		return
	}
}

var createMockTableQ string = `CREATE TABLE mock (
	id serial primary key,
	created_at timestamp not null default CURRENT_TIMESTAMP
)`
