package sqlite3

import (
	"github.com/meowmeowcode/hohin/sqldb"
	"time"
)

type sqlite3Dialect struct{}

func (d sqlite3Dialect) ProcessParam(p any, _ int) (string, any) {
	if param, ok := p.(time.Time); ok {
		text, err := param.MarshalText()
		if err != nil {
			panic(err)
		}
		return "?", string(text)
	}
	return "?", p
}

var dialect sqlite3Dialect

// NewSQL creates a new SQL builder for SQLite3.
func NewSQL(strs ...string) *sqldb.SQL {
	return sqldb.NewSQL(dialect, strs...)
}
