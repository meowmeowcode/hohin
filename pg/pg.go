// Package pg contains implementations of hohin interfaces for PostgreSQL.
// It uses the pgx driver.
package pg

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/meowmeowcode/hohin"
	"github.com/meowmeowcode/hohin/maps"
	"github.com/meowmeowcode/hohin/operations"
	"github.com/meowmeowcode/hohin/sqldb"
	"reflect"
	"strings"
)

type executor interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// DB implements hohin.DB for PostgreSQL.
type DB struct {
	executor executor
}

func (db *DB) Transaction(ctx context.Context, f func(context.Context, hohin.DB) error) error {
	return db.Tx(ctx, hohin.DefaultIsolation, f)
}

func (db *DB) Tx(ctx context.Context, level hohin.IsolationLevel, f func(context.Context, hohin.DB) error) error {
	executor, ok := db.executor.(*pgxpool.Pool)
	if !ok {
		panic("nested transactions are not supported")
	}
	txOptions := pgx.TxOptions{}
	switch level {
	case hohin.ReadUncommitted:
		txOptions.IsoLevel = pgx.ReadUncommitted
	case hohin.ReadCommitted:
		txOptions.IsoLevel = pgx.ReadCommitted
	case hohin.RepeatableRead:
		txOptions.IsoLevel = pgx.RepeatableRead
	case hohin.Serializable:
		txOptions.IsoLevel = pgx.Serializable
	}
	tx, err := executor.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	err = f(ctx, &DB{executor: tx})
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func (db *DB) Simple() hohin.SimpleDB {
	return hohin.NewSimpleDB(db)
}

// NewDB creates a [DB].
func NewDB(pool *pgxpool.Pool) *DB {
	return &DB{executor: pool}
}

// Scanner allows to fetch data from a result of an SQL query.
type Scanner interface {
	Scan(dest ...any) error
}

// Implementation of hohin.Repo for PostgreSQL.
type Repo[T any] struct {
	table           string
	mapping         map[string]string
	fields          []string
	columns         []string
	query           string
	queryCustomized bool
	dump            func(T) (map[string]any, error)
	load            func(Scanner) (T, error)
	afterAdd        func(T) []*sqldb.SQL
	afterUpdate     func(T) []*sqldb.SQL
}

// Conf contains configuration of a [Repo].
type Conf[T any] struct {
	Table   string            // name of a database table
	Mapping map[string]string // mapping of entity fields to table columns
	Query   string            // SQL query to select records from the database
	// function that transforms an entity to a map where keys are
	// column names of a database table and values are data for a row in that table
	Dump func(T) (map[string]any, error)
	// function that loads a result of an SQL query to an entity
	Load func(Scanner) (T, error)
	// function that builds and returns a sequence of SQL queries to execute after a call of [Repo.Add]
	AfterAdd func(T) []*sqldb.SQL
	// function that builds and returns a sequence of SQL queries to execute after a call of [Repo.Update]
	AfterUpdate func(T) []*sqldb.SQL
}

// NewRepo creates a [Repo].
func NewRepo[T any](conf Conf[T]) *Repo[T] {
	if conf.Table == "" {
		panic("table name is required to create a repository")
	}

	r := &Repo[T]{table: conf.Table}

	if conf.Mapping != nil {
		r.mapping = conf.Mapping
	} else {
		var entity T
		v := reflect.ValueOf(entity)
		t := v.Type()
		r.mapping = make(map[string]string)
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i).Name
			r.mapping[field] = field
		}
	}

	r.fields, r.columns = maps.Split(r.mapping)

	if conf.Query != "" {
		r.query = conf.Query
	} else {
		r.query = NewSQL("SELECT ").Join(", ", r.columns...).Add(" FROM ", r.table).String()
	}

	if conf.Dump != nil {
		r.dump = conf.Dump
	} else {
		r.dump = func(entity T) (map[string]any, error) {
			v := reflect.ValueOf(entity)
			data := make(map[string]any)
			for i, field := range r.fields {
				col := r.columns[i]
				data[col] = v.FieldByName(field).Interface()
			}
			return data, nil
		}
	}

	if conf.Load != nil {
		r.load = conf.Load
	} else {
		r.load = func(row Scanner) (T, error) {
			var entity T
			a := reflect.ValueOf(&entity)
			fields := make([]any, 0, len(r.fields))
			for _, field := range r.fields {
				addr := a.Elem().FieldByName(field).Addr().Interface()
				fields = append(fields, addr)
			}
			err := row.Scan(fields...)
			return entity, err
		}
	}

	r.afterAdd = conf.AfterAdd
	r.afterUpdate = conf.AfterUpdate
	return r
}

func (r *Repo[T]) Simple() hohin.SimpleRepo[T] {
	return hohin.NewSimpleRepo[T](r)
}

func (r *Repo[T]) Get(ctx context.Context, d hohin.DB, f hohin.Filter) (T, error) {
	var zero T
	if r.load == nil {
		return zero, errors.New("repository isn't configured to load entities")
	}
	db := d.(*DB)
	sqlBuilder := NewSQL(r.query, " WHERE ")
	if err := r.applyFilter(sqlBuilder, f); err != nil {
		return zero, err
	}
	query, params := sqlBuilder.Build()
	row := db.executor.QueryRow(ctx, query, params...)
	entity, err := r.load(row)
	if err == pgx.ErrNoRows {
		return zero, hohin.NotFound
	}
	if err != nil {
		return zero, fmt.Errorf("%w while executing query `%s`", err, query)
	}
	return entity, nil
}

func (r *Repo[T]) applyFilter(s *sqldb.SQL, f hohin.Filter) error {
	col, ok := r.mapping[f.Field]
	if len(f.Field) > 0 && !ok {
		return fmt.Errorf("unknown field `%s` in a filter", f.Field)
	}
	switch f.Operation {
	case operations.Not:
		s.Add("NOT (")
		err := r.applyFilter(s, f.Value.(hohin.Filter))
		if err != nil {
			return err
		}
		s.Add(")")
	case operations.And:
		for _, filter := range f.Value.([]hohin.Filter) {
			err := r.applyFilter(s, filter)
			if err != nil {
				return err
			}
			s.Add(" AND ")
		}
		s.RemoveLast()
	case operations.Or:
		for _, filter := range f.Value.([]hohin.Filter) {
			err := r.applyFilter(s, filter)
			if err != nil {
				return err
			}
			s.Add(" OR ")
		}
		s.RemoveLast()
	case operations.IsNull:
		s.Add(col, " IS NULL")
	case operations.Eq:
		s.Add(col, " = ").Param(f.Value)
	case operations.IEq:
		s.Add(col, " ILIKE ").Param(f.Value)
	case operations.Ne:
		s.Add(col, " != ").Param(f.Value)
	case operations.INe:
		s.Add(col, " NOT ILIKE ").Param(f.Value)
	case operations.Lt:
		s.Add(col, " < ").Param(f.Value)
	case operations.Gt:
		s.Add(col, " > ").Param(f.Value)
	case operations.Lte:
		s.Add(col, " <= ").Param(f.Value)
	case operations.Gte:
		s.Add(col, " >= ").Param(f.Value)
	case operations.In:
		switch val := f.Value.(type) {
		case []any:
			s.Add(col, " IN (").JoinParams(", ", val...).Add(")")
		default:
			return fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Contains:
		s.Add(col, " LIKE '%' || ").Param(f.Value).Add(" || '%' ")
	case operations.IContains:
		s.Add(col, " ILIKE '%' || ").Param(f.Value).Add(" || '%' ")
	case operations.HasPrefix:
		s.Add(col, " LIKE ").Param(f.Value).Add(" || '%' ")
	case operations.IHasPrefix:
		s.Add(col, " ILIKE ").Param(f.Value).Add(" || '%' ")
	case operations.HasSuffix:
		s.Add(col, " LIKE '%' || ").Param(f.Value)
	case operations.IHasSuffix:
		s.Add(col, " ILIKE '%' || ").Param(f.Value)
	case operations.IPWithin:
		s.Add(col, "::inet << ").Param(f.Value).Add("::inet")
	default:
		return fmt.Errorf("operation %s is not supported", f.Operation)
	}
	return nil
}

func (r *Repo[T]) GetForUpdate(ctx context.Context, d hohin.DB, f hohin.Filter) (T, error) {
	var zero T
	if r.load == nil {
		return zero, errors.New("repository isn't configured to load entities")
	}
	db := d.(*DB)
	sqlBuilder := NewSQL(r.query, " WHERE ")
	if err := r.applyFilter(sqlBuilder, f); err != nil {
		return zero, err
	}
	sqlBuilder.Add(" FOR UPDATE")
	query, params := sqlBuilder.Build()
	row := db.executor.QueryRow(ctx, query, params...)
	entity, err := r.load(row)
	if err == pgx.ErrNoRows {
		return zero, hohin.NotFound
	}
	if err != nil {
		return zero, fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return entity, nil
}

func (r *Repo[T]) Exists(ctx context.Context, d hohin.DB, f hohin.Filter) (bool, error) {
	var result bool
	db := d.(*DB)
	sql := NewSQL("SELECT EXISTS (", r.query, " WHERE ")
	err := r.applyFilter(sql, f)
	if err != nil {
		return result, err
	}
	sql.Add(")")
	query, params := sql.Build()
	row := db.executor.QueryRow(ctx, query, params...)
	err = row.Scan(&result)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return result, err
}

func (r *Repo[T]) Delete(ctx context.Context, d hohin.DB, f hohin.Filter) error {
	db := d.(*DB)
	sql := NewSQL("DELETE FROM ", r.table, " WHERE ")
	err := r.applyFilter(sql, f)
	query, params := sql.Build()
	_, err = db.executor.Exec(ctx, query, params...)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return err
}

func (r *Repo[T]) Add(ctx context.Context, d hohin.DB, entity T) error {
	db := d.(*DB)
	data, err := r.dump(entity)
	if err != nil {
		return err
	}
	columns, values := maps.Split(data)
	query, params := r.buildInsertQuery(columns, values)
	_, err = db.executor.Exec(ctx, query, params...)
	if err != nil {
		return fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	if r.afterAdd != nil {
		for _, sql := range r.afterAdd(entity) {
			query, params := sql.Build()
			if _, err := db.executor.Exec(ctx, query, params...); err != nil {
				return fmt.Errorf("cannot execute query `%s`: %w", query, err)
			}
		}
	}
	return nil
}

func (r *Repo[T]) buildInsertQuery(columns []string, values []any) (string, []any) {
	return NewSQL("INSERT INTO ", r.table, " (").
		Join(", ", columns...).
		Add(") VALUES (").
		JoinParams(", ", values...).
		Add(")").
		Build()
}

func (r *Repo[T]) AddMany(ctx context.Context, d hohin.DB, entities []T) error {
	if len(entities) == 0 {
		return nil
	}
	db := d.(*DB)
	var rows [][]any
	var columns []string
	for _, e := range entities {
		data, err := r.dump(e)
		if err != nil {
			return err
		}
		if columns == nil {
			columns, _ = maps.Split(data)
		}
		var row []any
		for _, c := range columns {
			row = append(row, data[c])
		}
		rows = append(rows, row)
	}
	for i, c := range columns {
		columns[i] = strings.ToLower(c)
	}
	_, err := db.executor.CopyFrom(
		ctx,
		pgx.Identifier{r.table},
		columns,
		pgx.CopyFromRows(rows),
	)
	return err
}

func (r *Repo[T]) Update(ctx context.Context, d hohin.DB, f hohin.Filter, entity T) error {
	db := d.(*DB)
	data, err := r.dump(entity)
	if err != nil {
		return err
	}
	sql := NewSQL("UPDATE ", r.table, " SET ")
	for k, v := range data {
		sql.Add(k, " = ").Param(v).Add(", ")
	}
	sql.RemoveLast().Add(" WHERE ")
	err = r.applyFilter(sql, f)
	if err != nil {
		return err
	}
	query, params := sql.Build()
	if _, err := db.executor.Exec(ctx, query, params...); err != nil {
		return fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	if r.afterUpdate != nil {
		for _, sql := range r.afterUpdate(entity) {
			query, params := sql.Build()
			if _, err := db.executor.Exec(ctx, query, params...); err != nil {
				return fmt.Errorf("cannot execute query `%s`: %w", query, err)
			}
		}
	}
	return nil
}

func (r Repo[T]) Count(ctx context.Context, d hohin.DB, f hohin.Filter) (uint64, error) {
	var result uint64
	db := d.(*DB)
	sql := NewSQL("SELECT COUNT(1) FROM (", r.query, " WHERE ")
	err := r.applyFilter(sql, f)
	if err != nil {
		return result, err
	}
	sql.Add(") AS q")
	query, params := sql.Build()
	row := db.executor.QueryRow(ctx, query, params...)
	err = row.Scan(&result)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return result, err
}

func (r *Repo[T]) GetMany(ctx context.Context, d hohin.DB, q hohin.Query) ([]T, error) {
	db := d.(*DB)
	result := make([]T, 0)
	sql := NewSQL(r.query)
	if q.Filter.Operation != "" {
		sql.Add(" WHERE ")
		if err := r.applyFilter(sql, q.Filter); err != nil {
			return nil, err
		}
	}
	if len(q.Order) > 0 {
		sql.Add(" ORDER BY ")
		for _, o := range q.Order {
			sql.Add(o.Field)
			if o.Desc {
				sql.Add(" DESC")
			}
			sql.Add(", ")
		}
		sql.RemoveLast()
	}
	if q.Limit > 0 {
		sql.Add(" LIMIT ").Param(q.Limit)
	}
	if q.Offset > 0 {
		sql.Add(" OFFSET ").Param(q.Offset)
	}
	query, params := sql.Build()
	rows, err := db.executor.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		entity, err := r.load(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, entity)
	}

	return result, nil
}

func (r *Repo[T]) GetFirst(ctx context.Context, d hohin.DB, q hohin.Query) (T, error) {
	q.Limit = 1
	var zero T
	result, err := r.GetMany(ctx, d, q)
	if err != nil {
		return zero, err
	}
	if len(result) == 0 {
		return zero, hohin.NotFound
	}
	return result[0], nil
}

func (r *Repo[T]) CountAll(ctx context.Context, d hohin.DB) (uint64, error) {
	var result uint64
	db := d.(*DB)
	query := NewSQL("SELECT COUNT(1) FROM (", r.query, ") AS q").String()
	row := db.executor.QueryRow(ctx, query)
	err := row.Scan(&result)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return result, err
}

func (r *Repo[T]) Clear(ctx context.Context, d hohin.DB) error {
	db := d.(*DB)
	query := NewSQL("DELETE FROM ", r.table).String()
	_, err := db.executor.Exec(ctx, query)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return err
}
