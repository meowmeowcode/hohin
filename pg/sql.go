package pg

import (
	"fmt"
	"github.com/meowmeowcode/hohin/sqldb"
	"net/netip"
)

type pgDialect struct{}

func (d pgDialect) ProcessParam(p any, number int) (string, any) {
	if val, ok := p.(netip.Addr); ok {
		return fmt.Sprintf("$%d", number), val.String()
	}
	return fmt.Sprintf("$%d", number), p
}

var dialect pgDialect

// NewSQL creates a new SQL builder for PostgreSQL.
func NewSQL(strs ...string) *sqldb.SQL {
	return sqldb.NewSQL(dialect, strs...)
}
