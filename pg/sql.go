package pg

import (
	"fmt"
	"github.com/meowmeowcode/hohin/sqldb"
)

type Dialect struct{}

func (d Dialect) ProcessParam(p any, number int) (string, any) {
	return fmt.Sprintf("$%d", number), p
}

var dialect Dialect

func NewSql(strs ...string) *sqldb.Sql {
	return sqldb.NewSql(dialect, strs...)
}
