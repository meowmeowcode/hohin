package sqldb

import (
	"strings"
)

type Dialect interface {
	ProcessParam(p any, number int) (string, any)
}

type Sql struct {
	dialect Dialect
	strs    []string
	params  []any
}

func NewSql(d Dialect, strs ...string) *Sql {
	s := &Sql{dialect: d}
	return s.Add(strs...)
}

func (s *Sql) Add(strs ...string) *Sql {
	s.strs = append(s.strs, strs...)
	return s
}

func (s *Sql) Param(p any) *Sql {
	str, param := s.dialect.ProcessParam(p, len(s.params)+1)
	s.params = append(s.params, param)
	s.strs = append(s.strs, str)
	return s
}

func (s *Sql) Join(sep string, strs ...string) *Sql {
	count := len(strs)
	for i, str := range strs {
		s.Add(str)
		if i < count-1 {
			s.Add(sep)
		}
	}
	return s
}

func (s *Sql) JoinParams(sep string, ps ...any) *Sql {
	count := len(ps)
	for i, p := range ps {
		s.Param(p)
		if i < count-1 {
			s.Add(sep)
		}
	}
	return s
}

func (s *Sql) RemoveLast() *Sql {
	s.strs = s.strs[:len(s.strs)-1]
	return s
}

func (s *Sql) String() string {
	return strings.Join(s.strs, "")
}

func (s *Sql) Params() []any {
	return s.params
}

func (s *Sql) Build() (string, []any) {
	return s.String(), s.Params()
}
