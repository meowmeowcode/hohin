package mysql

import "github.com/meowmeowcode/hohin/sqldb"

type Dialect struct{}

var dialect Dialect

func (d Dialect) ProcessParam(p any, number int) (string, any) {
	return "?", p
}

func NewSql(strs ...string) *sqldb.Sql {
	return sqldb.NewSql(dialect, strs...)
}
