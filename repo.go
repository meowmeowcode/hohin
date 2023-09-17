// Package hohin contains interfaces that must be implemented by other packages.
package hohin

import (
	"context"
	"errors"
)

// NotFound is returned when an entity cannot be found in the repository.
var NotFound error = errors.New("object not found")

// Repo is a repository of entities.
// It saves entities to the database and loads or deletes them from it.
type Repo[T any] interface {
	// Get finds an entity and returns it.
	Get(context.Context, DB, Filter) (T, error)
	// GetForUpdate finds an entity and locks it for update.
	GetForUpdate(context.Context, DB, Filter) (T, error)
	// GetMany finds and returns several entities.
	GetMany(context.Context, DB, Query) ([]T, error)
	// GetFirst finds and returns the first entity matching given criteria.
	GetFirst(context.Context, DB, Query) (T, error)
	// Add saves a new entity to the repository.
	Add(context.Context, DB, T) error
	// AddMany saves several entities to the repository.
	AddMany(context.Context, DB, []T) error
	// Update saves an updated entity.
	Update(context.Context, DB, Filter, T) error
	// Delete removes entities matching a given filter.
	Delete(context.Context, DB, Filter) error
	// Exists checks if there is an entity matching a given filter.
	Exists(context.Context, DB, Filter) (bool, error)
	// Count returns a number of entities matching a given filter.
	Count(context.Context, DB, Filter) (uint64, error)
	// CountAll returns a number of all entities in the repository.
	CountAll(context.Context, DB) (uint64, error)
	// Clear removes all entities from the repository.
	Clear(context.Context, DB) error
	// Simple returns the repository wrapped into an object with a simplified interface.
	Simple() SimpleRepo[T]
}

// DB is a wrapper around a database connection.
type DB interface {
	// Transaction executes a given function within a transaction.
	// If the function returns an error then the transaction rolls back.
	Transaction(context.Context, func(context.Context, DB) error) error
	// Tx is similar to Transaction but requires to choose an isolation level.
	Tx(context.Context, IsolationLevel, func(context.Context, DB) error) error
	// Simple returns the DB wrapped into an object with a simplified interface.
	Simple() SimpleDB
}
