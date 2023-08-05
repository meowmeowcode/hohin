package hohin

import "context"

type SimpleRepo[T any] struct {
	repo Repo[T]
}

func NewSimpleRepo[T any](r Repo[T]) SimpleRepo[T] {
	return SimpleRepo[T]{repo: r}
}

func (r *SimpleRepo[T]) Get(db SimpleDb, f Filter) (T, error) {
	return r.repo.Get(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) GetForUpdate(db SimpleDb, f Filter) (T, error) {
	return r.repo.GetForUpdate(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) GetMany(db SimpleDb, q Query) ([]T, error) {
	return r.repo.GetMany(context.Background(), db.db, q)
}

func (r *SimpleRepo[T]) GetFirst(db SimpleDb, q Query) (T, error) {
	return r.repo.GetFirst(context.Background(), db.db, q)
}

func (r *SimpleRepo[T]) Add(db SimpleDb, entity T) error {
	return r.repo.Add(context.Background(), db.db, entity)
}

func (r *SimpleRepo[T]) AddMany(db SimpleDb, entities []T) error {
	return r.repo.AddMany(context.Background(), db.db, entities)
}

func (r *SimpleRepo[T]) Update(db SimpleDb, f Filter, entity T) error {
	return r.repo.Update(context.Background(), db.db, f, entity)
}

func (r *SimpleRepo[T]) Delete(db SimpleDb, f Filter) error {
	return r.repo.Delete(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) Exists(db SimpleDb, f Filter) (bool, error) {
	return r.repo.Exists(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) Count(db SimpleDb, f Filter) (int, error) {
	return r.repo.Count(context.Background(), db.db, f)
}

func (r *SimpleRepo[T]) CountAll(db SimpleDb) (int, error) {
	return r.repo.CountAll(context.Background(), db.db)
}

func (r *SimpleRepo[T]) Clear(db SimpleDb) error {
	return r.repo.Clear(context.Background(), db.db)
}

type SimpleDb struct {
	db Db
}

func (d *SimpleDb) Transaction(f func(db SimpleDb) error) error {
	return d.db.Transaction(context.Background(), func(_ context.Context, db Db) error {
		return f(db.Simple())
	})
}

func (d *SimpleDb) Tx(l IsolationLevel, f func(db SimpleDb) error) error {
	return d.db.Tx(context.Background(), l, func(_ context.Context, db Db) error {
		return f(db.Simple())
	})
}

func NewSimpleDb(db Db) SimpleDb {
	return SimpleDb{db: db}
}
