package pg

import (
	"fmt"
	"github.com/meowmeowcode/hohin/sqldb"
)

type Dialect struct{}

func (d Dialect) ProcessParam(p any, number int) (string, any) {
	return fmt.Sprintf("$%d", number), p
}

func (d Dialect) LimitAndOffset(l, o int) string {
	result := ""
	if l > 0 {
		result += fmt.Sprintf(" LIMIT %d", l)
	}
	if o > 0 {
		result += fmt.Sprintf(" OFFSET %d", o)
	}
	return result
}

func (d Dialect) ForUpdate() string {
	return "FOR UPDATE"
}

var dialect Dialect

func NewSql(strs ...string) *sqldb.Sql {
	return sqldb.NewSql(dialect, strs...)
}

func NewRepo[T any](conf sqldb.Conf[T]) *sqldb.Repo[T] {
	return sqldb.NewRepo(dialect, conf)
}