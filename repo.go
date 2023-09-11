package hohin

import (
	"context"
	"errors"
)

var NotFound error = errors.New("object not found")

type Repo[T any] interface {
	Get(context.Context, DB, Filter) (T, error)
	GetForUpdate(context.Context, DB, Filter) (T, error)
	GetMany(context.Context, DB, Query) ([]T, error)
	GetFirst(context.Context, DB, Query) (T, error)
	Add(context.Context, DB, T) error
	AddMany(context.Context, DB, []T) error
	Update(context.Context, DB, Filter, T) error
	Delete(context.Context, DB, Filter) error
	Exists(context.Context, DB, Filter) (bool, error)
	Count(context.Context, DB, Filter) (uint64, error)
	CountAll(context.Context, DB) (uint64, error)
	Clear(context.Context, DB) error
	Simple() SimpleRepo[T]
}

type DB interface {
	Transaction(context.Context, func(context.Context, DB) error) error
	Tx(context.Context, IsolationLevel, func(context.Context, DB) error) error
	Simple() SimpleDB
}
