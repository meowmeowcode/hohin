package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/meowmeowcode/hohin"
	"github.com/meowmeowcode/hohin/maps"
	"github.com/meowmeowcode/hohin/operations"
	"github.com/meowmeowcode/hohin/sqldb"
	"math"
	"reflect"
)

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type Db struct {
	executor Executor
}

func (db *Db) Transaction(ctx context.Context, f func(context.Context, hohin.Db) error) error {
	return db.Tx(ctx, hohin.DefaultIsolation, f)
}

func (db *Db) Tx(ctx context.Context, level hohin.IsolationLevel, f func(context.Context, hohin.Db) error) error {
	executor, ok := db.executor.(*sql.DB)
	if !ok {
		panic("nested transactions are not supported")
	}
	txOptions := sql.TxOptions{}
	switch level {
	case hohin.ReadUncommitted:
		txOptions.Isolation = sql.LevelReadUncommitted
	case hohin.ReadCommitted:
		txOptions.Isolation = sql.LevelReadCommitted
	case hohin.RepeatableRead:
		txOptions.Isolation = sql.LevelRepeatableRead
	case hohin.Serializable:
		txOptions.Isolation = sql.LevelSerializable
	}
	tx, err := executor.BeginTx(ctx, &txOptions)
	if err != nil {
		return err
	}
	err = f(ctx, &Db{executor: tx})
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *Db) Simple() hohin.SimpleDb {
	return hohin.NewSimpleDb(db)
}

func NewDb(pool *sql.DB) *Db {
	return &Db{executor: pool}
}

type Scanner interface {
	Scan(dest ...any) error
}

type Repo[T any] struct {
	table           string
	mapping         map[string]string
	fields          []string
	columns         []string
	query           string
	queryCustomized bool
	dump            func(T) (map[string]any, error)
	load            func(Scanner) (T, error)
	afterAdd        func(T) []*sqldb.Sql
	afterUpdate     func(T) []*sqldb.Sql
}

type Conf[T any] struct {
	Table       string
	Mapping     map[string]string
	Query       string
	Dump        func(T) (map[string]any, error)
	Load        func(Scanner) (T, error)
	AfterAdd    func(T) []*sqldb.Sql
	AfterUpdate func(T) []*sqldb.Sql
}

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
		r.query = NewSql("SELECT ").Join(", ", r.columns...).Add(" FROM ", r.table).String()
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

func (r *Repo[T]) Get(ctx context.Context, d hohin.Db, f hohin.Filter) (T, error) {
	var zero T
	if r.load == nil {
		return zero, errors.New("repository isn't configured to load entities")
	}
	db := d.(*Db)
	sqlBuilder := NewSql(r.query, " WHERE ")
	if err := r.applyFilter(sqlBuilder, f); err != nil {
		return zero, err
	}
	query, params := sqlBuilder.Build()
	row := db.executor.QueryRowContext(ctx, query, params...)
	entity, err := r.load(row)
	if err == sql.ErrNoRows {
		return zero, hohin.NotFound
	}
	if err != nil {
		return zero, fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return entity, nil
}

func (r *Repo[T]) applyFilter(s *sqldb.Sql, f hohin.Filter) error {
	col, ok := r.mapping[f.Field]
	if len(f.Field) > 0 && !ok {
		return fmt.Errorf("unknown field `%s` in a filter", f.Field)
	}
	switch f.Operation {
	case operations.Not:
		s.Add("NOT (")
		r.applyFilter(s, f.Value.(hohin.Filter))
		s.Add(")")
	case operations.And:
		for _, filter := range f.Value.([]hohin.Filter) {
			r.applyFilter(s, filter)
			s.Add(" AND ")
		}
		s.RemoveLast()
	case operations.Or:
		for _, filter := range f.Value.([]hohin.Filter) {
			r.applyFilter(s, filter)
			s.Add(" OR ")
		}
		s.RemoveLast()
	case operations.Eq:
		if val, ok := f.Value.(float64); ok {
			s.Add(col, " LIKE ").Param(val)
		} else {
			s.Add(col, " = ").Param(f.Value)
		}
	case operations.IEq:
		s.Add("UPPER(", col, ") = UPPER(").Param(f.Value).Add(")")
	case operations.Ne:
		if val, ok := f.Value.(float64); ok {
			s.Add(col, " NOT LIKE ").Param(val)
		} else {
			s.Add(col, " != ").Param(f.Value)
		}
	case operations.INe:
		s.Add("UPPER(", col, ") != UPPER(").Param(f.Value).Add(")")
	case operations.Lt:
		if val, ok := f.Value.(float64); ok {
			s.Add(col, " - ").Param(val).Add(" < -0.0001")
		} else {
			s.Add(col, " < ").Param(f.Value)
		}
	case operations.Gt:
		if val, ok := f.Value.(float64); ok {
			s.Add(col, " - ").Param(val).Add(" > 0.0001")
		} else {
			s.Add(col, " > ").Param(f.Value)
		}
	case operations.Lte:
		if val, ok := f.Value.(float64); ok {
			s.Add("(", col, " LIKE ").
				Param(val).
				Add(" OR ").
				Add(col, " - ").
				Param(val).
				Add(" < -0.0001)")
		} else {
			s.Add(col, " <= ").Param(f.Value)
		}
	case operations.Gte:
		if val, ok := f.Value.(float64); ok {
			s.Add("(", col, " LIKE ").
				Param(val).
				Add(" OR ").
				Add(col, " - ").
				Param(val).
				Add(" > 0.0001)")
		} else {
			s.Add(col, " >= ").Param(f.Value)
		}
	case operations.In:
		switch val := f.Value.(type) {
		case []any:
			s.Add(col, " IN (").JoinParams(", ", val...).Add(")")
		default:
			return fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Contains:
		s.Add(col, " LIKE CONCAT('%' ,").Param(f.Value).Add(", '%')")
	case operations.IContains:
		s.Add("UPPER(", col, ") LIKE CONCAT('%' , UPPER(").Param(f.Value).Add("), '%')")
	case operations.HasPrefix:
		s.Add(col, " LIKE CONCAT(").Param(f.Value).Add(", '%')")
	case operations.IHasPrefix:
		s.Add("UPPER(", col, ") LIKE CONCAT(UPPER(").Param(f.Value).Add("), '%')")
	case operations.HasSuffix:
		s.Add(col, " LIKE CONCAT('%', ").Param(f.Value).Add(")")
	case operations.IHasSuffix:
		s.Add("UPPER(", col, ") LIKE CONCAT('%', UPPER(").Param(f.Value).Add("))")
	default:
		return fmt.Errorf("operation %s is not supported", f.Operation)
	}
	return nil
}

func (r *Repo[T]) GetForUpdate(ctx context.Context, d hohin.Db, f hohin.Filter) (T, error) {
	var zero T
	if r.load == nil {
		return zero, errors.New("repository isn't configured to load entities")
	}
	db := d.(*Db)
	sqlBuilder := NewSql(r.query, " WHERE ")
	if err := r.applyFilter(sqlBuilder, f); err != nil {
		return zero, err
	}
	sqlBuilder.Add(" FOR UPDATE")
	query, params := sqlBuilder.Build()
	row := db.executor.QueryRowContext(ctx, query, params...)
	entity, err := r.load(row)
	if err == sql.ErrNoRows {
		return zero, hohin.NotFound
	}
	if err != nil {
		return zero, fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return entity, nil
}

func (r *Repo[T]) Exists(ctx context.Context, d hohin.Db, f hohin.Filter) (bool, error) {
	var result bool
	db := d.(*Db)
	sql := NewSql("SELECT EXISTS (", r.query, " WHERE ")
	r.applyFilter(sql, f)
	sql.Add(")")
	query, params := sql.Build()
	row := db.executor.QueryRowContext(ctx, query, params...)
	err := row.Scan(&result)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return result, err
}

func (r *Repo[T]) Delete(ctx context.Context, d hohin.Db, f hohin.Filter) error {
	db := d.(*Db)
	sql := NewSql("DELETE FROM ", r.table, " WHERE ")
	r.applyFilter(sql, f)
	query, params := sql.Build()
	_, err := db.executor.ExecContext(ctx, query, params...)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return err
}

func (r *Repo[T]) Add(ctx context.Context, d hohin.Db, entity T) error {
	db := d.(*Db)
	data, err := r.dump(entity)
	if err != nil {
		return err
	}
	columns, values := maps.Split(data)
	query, params := r.buildInsertQuery(columns, values)
	_, err = db.executor.ExecContext(ctx, query, params...)
	if err != nil {
		return fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	if r.afterAdd != nil {
		for _, sql := range r.afterAdd(entity) {
			query, params := sql.Build()
			if _, err := db.executor.ExecContext(ctx, query, params...); err != nil {
				return fmt.Errorf("cannot execute query `%s`: %w", query, err)
			}
		}
	}
	return nil
}

func (r *Repo[T]) buildInsertQuery(columns []string, values []any) (string, []any) {
	return NewSql("INSERT INTO ", r.table, " (").
		Join(", ", columns...).
		Add(") VALUES (").
		JoinParams(", ", values...).
		Add(")").
		Build()
}

func (r *Repo[T]) AddMany(ctx context.Context, d hohin.Db, entities []T) error {
	if len(entities) == 0 {
		return nil
	}
	db := d.(*Db)
	var data []map[string]any
	for _, e := range entities {
		d, err := r.dump(e)
		if err != nil {
			return err
		}
		data = append(data, d)
	}
	columns, values := maps.Split(data[0])
	query, _ := r.buildInsertQuery(columns, values)
	stmt, err := db.executor.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, d := range data {
		values := make([]any, 0, len(d))
		for _, c := range columns {
			_, p := dialect.ProcessParam(d[c], 0)
			values = append(values, p)
		}
		_, err = stmt.ExecContext(ctx, values...)
		if err != nil {
			panic(err)
			return err
		}
	}
	return nil
}

func (r *Repo[T]) Update(ctx context.Context, d hohin.Db, f hohin.Filter, entity T) error {
	db := d.(*Db)
	data, err := r.dump(entity)
	if err != nil {
		return err
	}
	sql := NewSql("UPDATE ", r.table, " SET ")
	for k, v := range data {
		sql.Add(k, " = ").Param(v).Add(", ")
	}
	sql.RemoveLast().Add(" WHERE ")
	r.applyFilter(sql, f)
	query, params := sql.Build()
	if _, err := db.executor.ExecContext(ctx, query, params...); err != nil {
		return fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	if r.afterUpdate != nil {
		for _, sql := range r.afterUpdate(entity) {
			query, params := sql.Build()
			if _, err := db.executor.ExecContext(ctx, query, params...); err != nil {
				return fmt.Errorf("cannot execute query `%s`: %w", query, err)
			}
		}
	}
	return nil
}

func (r Repo[T]) Count(ctx context.Context, d hohin.Db, f hohin.Filter) (uint64, error) {
	var result uint64
	db := d.(*Db)
	sql := NewSql("SELECT COUNT(1) FROM (", r.query, " WHERE ")
	r.applyFilter(sql, f)
	sql.Add(") AS q")
	query, params := sql.Build()
	row := db.executor.QueryRowContext(ctx, query, params...)
	err := row.Scan(&result)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return result, err
}

func (r *Repo[T]) GetMany(ctx context.Context, d hohin.Db, q hohin.Query) ([]T, error) {
	db := d.(*Db)
	result := make([]T, 0)
	sql := NewSql(r.query)
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
	if q.Limit > 0 && q.Offset > 0 {
		sql.Add(" LIMIT ").JoinParams(", ", q.Offset, q.Limit)
	} else if q.Offset > 0 {
		sql.Add(" LIMIT ").JoinParams(", ", q.Offset, math.MaxInt64)
	} else if q.Limit > 0 {
		sql.Add(" LIMIT ").Param(q.Limit)
	}
	query, params := sql.Build()
	rows, err := db.executor.QueryContext(ctx, query, params...)
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

func (r *Repo[T]) GetFirst(ctx context.Context, d hohin.Db, q hohin.Query) (T, error) {
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

func (r *Repo[T]) CountAll(ctx context.Context, d hohin.Db) (uint64, error) {
	var result uint64
	db := d.(*Db)
	query := NewSql("SELECT COUNT(1) FROM (", r.query, ") AS q").String()
	row := db.executor.QueryRowContext(ctx, query)
	err := row.Scan(&result)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return result, err
}

func (r *Repo[T]) Clear(ctx context.Context, d hohin.Db) error {
	db := d.(*Db)
	query := NewSql("DELETE FROM ", r.table).String()
	_, err := db.executor.ExecContext(ctx, query)
	if err != nil {
		err = fmt.Errorf("cannot execute query `%s`: %w", query, err)
	}
	return err
}
