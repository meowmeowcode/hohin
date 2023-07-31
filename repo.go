package hohin

import (
	"context"
	"errors"
)

var NotFound error = errors.New("object not found")

type Repo[T any] interface {
	Get(context.Context, Db, Filter) (T, error)
	GetForUpdate(context.Context, Db, Filter) (T, error)
	GetMany(context.Context, Db, Query) ([]T, error)
	GetFirst(context.Context, Db, Query) (T, error)
	Add(context.Context, Db, T) error
	Update(context.Context, Db, Filter, T) error
	Delete(context.Context, Db, Filter) error
	Exists(context.Context, Db, Filter) (bool, error)
	Count(context.Context, Db, Filter) (int, error)
	CountAll(context.Context, Db) (int, error)
	Clear(context.Context, Db) error
	Simple() SimpleRepo[T]
}

type Db interface {
	Transaction(context.Context, func(context.Context, Db) error) error
	Tx(context.Context, IsolationLevel, func(context.Context, Db) error) error
	Simple() SimpleDb
}
