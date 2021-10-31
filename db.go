package migraches

import "github.com/pkg/errors"

func (r *RollupService) rollupDB(dbName string) error {
	// create database
	_, err := r.db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
