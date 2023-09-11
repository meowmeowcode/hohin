package hohin

import "context"

type SimpleRepo[T any] struct {
	repo Repo[T]
}

func NewSimpleRepo[T any](r Repo[T]) SimpleRepo[T] {
	return SimpleRepo[T]{repo: r}
}

func (r *SimpleRepo[T]) Get(db SimpleDB, f Filter) (T, error) {
	return r.repo.Get(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) GetForUpdate(db SimpleDB, f Filter) (T, error) {
	return r.repo.GetForUpdate(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) GetMany(db SimpleDB, q Query) ([]T, error) {
	return r.repo.GetMany(context.Background(), db.db, q)
}

func (r *SimpleRepo[T]) GetFirst(db SimpleDB, q Query) (T, error) {
	return r.repo.GetFirst(context.Background(), db.db, q)
}

func (r *SimpleRepo[T]) Add(db SimpleDB, entity T) error {
	return r.repo.Add(context.Background(), db.db, entity)
}

func (r *SimpleRepo[T]) AddMany(db SimpleDB, entities []T) error {
	return r.repo.AddMany(context.Background(), db.db, entities)
}

func (r *SimpleRepo[T]) Update(db SimpleDB, f Filter, entity T) error {
	return r.repo.Update(context.Background(), db.db, f, entity)
}

func (r *SimpleRepo[T]) Delete(db SimpleDB, f Filter) error {
	return r.repo.Delete(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) Exists(db SimpleDB, f Filter) (bool, error) {
	return r.repo.Exists(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) Count(db SimpleDB, f Filter) (uint64, error) {
	return r.repo.Count(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) CountAll(db SimpleDB) (uint64, error) {
	return r.repo.CountAll(context.Background(), db.db)
}

func (r *SimpleRepo[T]) Clear(db SimpleDB) error {
	return r.repo.Clear(context.Background(), db.db)
}

type SimpleDB struct {
	db DB
}

func (d *SimpleDB) Transaction(f func(db SimpleDB) error) error {
	return d.db.Transaction(context.Background(), func(_ context.Context, db DB) error {
		return f(db.Simple())
	})
}

func (d *SimpleDB) Tx(l IsolationLevel, f func(db SimpleDB) error) error {
	return d.db.Tx(context.Background(), l, func(_ context.Context, db DB) error {
		return f(db.Simple())
	})
}

func NewSimpleDB(db DB) SimpleDB {
	return SimpleDB{db: db}
}
