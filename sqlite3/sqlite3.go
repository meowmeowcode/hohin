package sqlite3

import (
	"fmt"
	"github.com/meowmeowcode/hohin/sqldb"
	"math"
	"time"
)

type Dialect struct{}

func (d Dialect) ProcessParam(p any, _ int) (string, any) {
	if param, ok := p.(time.Time); ok {
		text, err := param.MarshalText()
		if err != nil {
			panic(err)
		}
		return "?", string(text)
	}
	return "?", p
}

func (d Dialect) LimitAndOffset(l, o int) string {
	result := ""
	if l == 0 && o > 0 {
		l = math.MaxInt64
	}
	if l > 0 {
		result += fmt.Sprintf(" LIMIT %d", l)
	}
	if o > 0 {
		result += fmt.Sprintf(" OFFSET %d", o)
	}
	return result
}

func (d Dialect) ForUpdate() string {
	return ""
}

var dialect Dialect

func NewSql(strs ...string) *sqldb.Sql {
	return sqldb.NewSql(dialect, strs...)
}

func NewRepo[T any](conf sqldb.Conf[T]) *sqldb.Repo[T] {
	return sqldb.NewRepo(dialect, conf)
}
