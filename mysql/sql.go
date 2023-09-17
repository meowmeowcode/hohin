package mysql

import "github.com/meowmeowcode/hohin/sqldb"

type mySQLDialect struct{}

var dialect mySQLDialect

func (d mySQLDialect) ProcessParam(p any, number int) (string, any) {
	return "?", p
}

// NewSQL creates a new SQL builder for MySQL.
func NewSQL(strs ...string) *sqldb.SQL {
	return sqldb.NewSQL(dialect, strs...)
}
