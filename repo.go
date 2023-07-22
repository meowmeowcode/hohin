package hohin

import "errors"

var NotFound error = errors.New("object not found")

type Repo[T any] interface {
	Get(Db, Filter) (T, error)
	GetForUpdate(Db, Filter) (T, error)
	GetMany(Db, Query) ([]T, error)
	Add(Db, T) error
	Update(Db, Filter, T) error
	Delete(Db, Filter) error
	Exists(Db, Filter) (bool, error)
	Count(Db, Filter) (int, error)
	CountAll(Db) (int, error)
	Clear(Db) error
}

type Db interface {
	Transaction(func(Db) error) error
}
