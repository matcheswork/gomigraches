package migraches

func (r *RollupService) rollupDB(dbName string) error {
	// create database
	_, err := r.db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		return err
	}

	return nil
}
