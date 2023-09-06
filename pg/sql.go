package pg

import (
	"fmt"
	"github.com/meowmeowcode/hohin/sqldb"
	"net/netip"
)

type Dialect struct{}

func (d Dialect) ProcessParam(p any, number int) (string, any) {
	if val, ok := p.(netip.Addr); ok {
		return fmt.Sprintf("$%d", number), val.String()
	}
	return fmt.Sprintf("$%d", number), p
}

var dialect Dialect

func NewSql(strs ...string) *sqldb.Sql {
	return sqldb.NewSql(dialect, strs...)
}
