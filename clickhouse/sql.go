package clickhouse

import (
	"github.com/meowmeowcode/hohin/sqldb"
	"time"
)

type Dialect struct{}

func (d Dialect) ProcessParam(p any, number int) (string, any) {
	if param, ok := p.(time.Time); ok {
		text, err := param.MarshalText()
		if err != nil {
			panic(err)
		}
		return "?", string(text[:len(text)-1])
	}
	return "?", p
}

var dialect Dialect

func NewSql(strs ...string) *sqldb.Sql {
	return sqldb.NewSql(dialect, strs...)
}
