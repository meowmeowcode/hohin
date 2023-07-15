package hohin

import "errors"

import "github.com/meowmeowcode/hohin/filter"
import "github.com/meowmeowcode/hohin/query"

var NotFound error = errors.New("object not found")

type Repo[T any] interface {
	Get(Db, filter.Filter) (T, error)
	GetForUpdate(Db, filter.Filter) (T, error)
	GetMany(Db, query.Query) ([]T, error)
	Add(Db, T) error
	Update(Db, filter.Filter, T) error
	Delete(Db, filter.Filter) error
	Exists(Db, filter.Filter) (bool, error)
	Count(Db, filter.Filter) (int, error)
	CountAll(Db) (int, error)
	Clear(Db) error
}

type Db interface {
	Transaction(func(Db) error) error
}
