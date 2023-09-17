package hohin

import "context"

// SimpleRepo is a wrapper around a [Repo] for a case
// when there is no need of passing a Context to Repo's methods.
type SimpleRepo[T any] struct {
	repo Repo[T]
}

// NewSimpleRepo creates a new SimpleRepo.
func NewSimpleRepo[T any](r Repo[T]) SimpleRepo[T] {
	return SimpleRepo[T]{repo: r}
}

// Get finds an entity and returns it.
func (r *SimpleRepo[T]) Get(db SimpleDB, f Filter) (T, error) {
	return r.repo.Get(context.Background(), db.db, f)
}

// GetForUpdate finds an entity and locks it for update.
func (r *SimpleRepo[T]) GetForUpdate(db SimpleDB, f Filter) (T, error) {
	return r.repo.GetForUpdate(context.Background(), db.db, f)
}

// GetMany finds and returns several entities.
func (r *SimpleRepo[T]) GetMany(db SimpleDB, q Query) ([]T, error) {
	return r.repo.GetMany(context.Background(), db.db, q)
}

// GetFirst finds and returns the first entity matching given criteria.
func (r *SimpleRepo[T]) GetFirst(db SimpleDB, q Query) (T, error) {
	return r.repo.GetFirst(context.Background(), db.db, q)
}

// Add saves a new entity to the repository.
func (r *SimpleRepo[T]) Add(db SimpleDB, entity T) error {
	return r.repo.Add(context.Background(), db.db, entity)
}

// AddMany saves several entities to the repository.
func (r *SimpleRepo[T]) AddMany(db SimpleDB, entities []T) error {
	return r.repo.AddMany(context.Background(), db.db, entities)
}

// Update saves an updated entity.
func (r *SimpleRepo[T]) Update(db SimpleDB, f Filter, entity T) error {
	return r.repo.Update(context.Background(), db.db, f, entity)
}

// Delete removes entities matching a given filter.
func (r *SimpleRepo[T]) Delete(db SimpleDB, f Filter) error {
	return r.repo.Delete(context.Background(), db.db, f)
}

// Exists checks if there is an entity matching a given filter.
func (r *SimpleRepo[T]) Exists(db SimpleDB, f Filter) (bool, error) {
	return r.repo.Exists(context.Background(), db.db, f)
}

// Count returns a number of entities matching a given filter.
func (r *SimpleRepo[T]) Count(db SimpleDB, f Filter) (uint64, error) {
	return r.repo.Count(context.Background(), db.db, f)
}

// CountAll returns a number of all entities in the repository.
func (r *SimpleRepo[T]) CountAll(db SimpleDB) (uint64, error) {
	return r.repo.CountAll(context.Background(), db.db)
}

// Clear removes all entities from the repository.
func (r *SimpleRepo[T]) Clear(db SimpleDB) error {
	return r.repo.Clear(context.Background(), db.db)
}

// SimpleDB is a wrapper around a [DB] for a case
// when there is no need of passing a Context to DB's methods.
type SimpleDB struct {
	db DB
}

// Transaction executes a given function within a transaction.
// If the function returns an error then the transaction rolls back.
func (d *SimpleDB) Transaction(f func(db SimpleDB) error) error {
	return d.db.Transaction(context.Background(), func(_ context.Context, db DB) error {
		return f(db.Simple())
	})
}

// Tx is similar to Transaction but requires to choose an isolation level.
func (d *SimpleDB) Tx(l IsolationLevel, f func(db SimpleDB) error) error {
	return d.db.Tx(context.Background(), l, func(_ context.Context, db DB) error {
		return f(db.Simple())
	})
}

// Creates [SimpleDB].
func NewSimpleDB(db DB) SimpleDB {
	return SimpleDB{db: db}
}
