package clickhouse

import (
	"github.com/meowmeowcode/hohin/sqldb"
)

type clickHouseDialect struct{}

func (d clickHouseDialect) ProcessParam(p any, number int) (string, any) {
	return "?", p
}

var dialect clickHouseDialect

// NewSQL creates a new SQL builder for ClickHouse.
func NewSQL(strs ...string) *sqldb.SQL {
	return sqldb.NewSQL(dialect, strs...)
}
