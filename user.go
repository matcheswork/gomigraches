package migraches

import (
	"fmt"

	"github.com/pkg/errors"
)

func (r *RollupService) rollupUser(name string) error {
	// create user

	_, err := r.db.Exec("CREATE USER " + name)
	if err != nil {
		return errors.WithStack(err)
	}

	// grant privileges
	_, err = r.db.Exec(
		fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s",
			name, name))
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *RollupService) rollbackUser(name string) error {
	_, err := r.db.Exec("DROP ROLE " + name)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
