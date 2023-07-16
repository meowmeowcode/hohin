package pg

import (
	"fmt"
	"strings"
)

type Sql struct {
	strs   []string
	params []any
}

func NewSql(strs ...string) *Sql {
	s := &Sql{}
	return s.Add(strs...)
}

func (s *Sql) Add(strs ...string) *Sql {
	s.strs = append(s.strs, strs...)
	return s
}

func (s *Sql) AddParam(p any) *Sql {
	s.params = append(s.params, p)
	s.strs = append(s.strs, fmt.Sprintf("$%d", len(s.params)))
	return s
}

func (s *Sql) AddSep(sep string, strs ...string) *Sql {
	count := len(strs)
	for i, str := range strs {
		s.Add(str)
		if i < count-1 {
			s.Add(sep)
		}
	}
	return s
}

func (s *Sql) AddParamsSep(sep string, ps ...any) *Sql {
	count := len(ps)
	for i, p := range ps {
		s.AddParam(p)
		if i < count-1 {
			s.Add(sep)
		}
	}
	return s
}

func (s *Sql) Pop() *Sql {
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
