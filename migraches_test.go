package migraches

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableString(t *testing.T) {
	tbl := &Table{
		Name: "fix_name",
		Fields: []string{
			"fix_field_1 serial primary key",
			"fix_field_2 int num null",
		},
	}

	want := `CREATE TABLE fix_name ( fix_field_1 serial primary key,fix_field_2 int num null )`
	got := tbl.String()

	assert.Equal(t, want, got)
}
