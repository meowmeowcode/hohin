// Package sqldb contains a very primitive SQL builder.
package sqldb

import (
	"strings"
)

// Dialect is used by an [SQL] to handle cases specific to a concrete database system.
type Dialect interface {
	// ProcessParam takes a parameter and its index in the list of all parameters
	// and returns a parameter placeholder for an SQL string
	// and the parameter itself in an appropriate form.
	ProcessParam(p any, number int) (string, any)
}

// SQL is a builder of SQL queries.
type SQL struct {
	dialect Dialect
	strs    []string
	params  []any
}

// NewSQL creates an [SQL].
func NewSQL(d Dialect, strs ...string) *SQL {
	s := &SQL{dialect: d}
	return s.Add(strs...)
}

// Add appends strings to an SQL query.
func (s *SQL) Add(strs ...string) *SQL {
	s.strs = append(s.strs, strs...)
	return s
}

// Param appends an escaped parameter to an SQL query.
func (s *SQL) Param(p any) *SQL {
	str, param := s.dialect.ProcessParam(p, len(s.params)+1)
	s.params = append(s.params, param)
	s.strs = append(s.strs, str)
	return s
}

// Join appends strings joined by a separator to an SQL query.
func (s *SQL) Join(sep string, strs ...string) *SQL {
	count := len(strs)
	for i, str := range strs {
		s.Add(str)
		if i < count-1 {
			s.Add(sep)
		}
	}
	return s
}

// JoinParams appends escaped parameters joined by a separator to an SQL query.
func (s *SQL) JoinParams(sep string, ps ...any) *SQL {
	count := len(ps)
	for i, p := range ps {
		s.Param(p)
		if i < count-1 {
			s.Add(sep)
		}
	}
	return s
}

// RemoveLast removes the last string appended to an SQL query.
func (s *SQL) RemoveLast() *SQL {
	s.strs = s.strs[:len(s.strs)-1]
	return s
}

// String returns a string with an SQL query.
func (s *SQL) String() string {
	return strings.Join(s.strs, "")
}

// Params returns parameters for an SQL query.
func (s *SQL) Params() []any {
	return s.params
}

// Build returns a string with an SQL query and parameters for it.
func (s *SQL) Build() (string, []any) {
	return s.String(), s.Params()
}
