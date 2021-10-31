package migraches

import "github.com/pkg/errors"

func (r *RollupService) rollbackDB(name string) error {
	_, err := r.db.Exec("DROP DATABASE " + name)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
