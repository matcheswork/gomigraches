package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/jmoiron/sqlx"
	migraches "github.com/matcheswork/gomigraches"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func main() {
	args := os.Args[1:]

	if len(args) < 3 {
		fmt.Println("please provide required data:")
		fmt.Printf("\n%s\n%s\n%s\n\n",
			"db_to_create_name - database name that you want to rollup",
			"meta_db_name - database that has access to create new databases and roles",
			"meta_db_user - the role that has access to create new databases and roles",
		)
		fmt.Println("./gomigraches db_to_create_name meta_db_name meta_db_user")
		return
	}

	dbName := args[0]
	metaDBName := args[1]
	metaDBUser := args[2]

	fmt.Printf("Enter meta db password: ")

	metaDBPassword := ""

	fmt.Scanf("%s", &metaDBPassword)

	manageDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"localhost", 5432, metaDBUser, metaDBPassword, metaDBName)

	psqlDB, err := sqlx.Connect("postgres", manageDSN)
	if err != nil {
		fmt.Printf("%+v\n", errors.WithStack(err))
		return
	}

	file, err := ioutil.ReadFile("./tables.json")
	if err != nil {
		fmt.Printf("%+v\n", errors.WithStack(err))
		return
	}

	tables := []migraches.Table{}

	err = json.Unmarshal(file, &tables)
	if err != nil {
		fmt.Printf("%+v\n", errors.WithStack(err))
		return
	}
	if len(tables) == 0 {
		fmt.Printf("%+v\n", errors.WithStack(migraches.ErrNoTables))
		return
	}

	rup := migraches.NewRollupService(psqlDB, tables)

	err = rup.Rollup(dbName)
	if err != nil {
		fmt.Printf("%+v\n", err)
		return
	}

	fmt.Printf("database '%s' rolled up\n", dbName)
}
