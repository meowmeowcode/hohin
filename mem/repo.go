package mem

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/meowmeowcode/hohin"
	"github.com/meowmeowcode/hohin/operations"
	"github.com/shopspring/decimal"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

type DB struct {
	data  map[string][][]byte
	mutex sync.RWMutex
}

func (db *DB) Transaction(ctx context.Context, f func(context.Context, hohin.DB) error) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	t := db.copy()
	err := f(ctx, t)
	if err != nil {
		return err
	}
	db.data = t.data
	return nil
}

func (db *DB) Tx(ctx context.Context, _ hohin.IsolationLevel, f func(context.Context, hohin.DB) error) error {
	return db.Transaction(ctx, f)
}

func (db *DB) Simple() hohin.SimpleDB {
	return hohin.NewSimpleDB(db)
}

func (db *DB) copy() *DB {
	c := NewDB()
	for k, v := range db.data {
		c.data[k] = make([][]byte, 0)
		for _, record := range v {
			c.data[k] = append(c.data[k], record)
		}
	}
	return c
}

func NewDB() *DB {
	return &DB{data: make(map[string][][]byte)}
}

type Repo[T any] struct {
	collection string
}

func NewRepo[T any](collection string) *Repo[T] {
	return &Repo[T]{collection: collection}
}

func (r *Repo[T]) Simple() hohin.SimpleRepo[T] {
	return hohin.NewSimpleRepo[T](r)
}

func (r *Repo[T]) Get(ctx context.Context, d hohin.DB, f hohin.Filter) (T, error) {
	var zero T
	db := d.(*DB)
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	for _, record := range db.data[r.collection] {
		entity, err := r.load(record)
		if err != nil {
			return zero, err
		}
		found, err := r.matchesFilter(entity, f)
		if err != nil {
			return zero, err
		}
		if found {
			return entity, nil
		}
	}
	return zero, hohin.NotFound
}

func (r *Repo[T]) GetForUpdate(ctx context.Context, d hohin.DB, f hohin.Filter) (T, error) {
	return r.Get(ctx, d, f)
}

func (r *Repo[T]) Exists(ctx context.Context, d hohin.DB, f hohin.Filter) (bool, error) {
	db := d.(*DB)
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	for _, record := range db.data[r.collection] {
		entity, err := r.load(record)
		if err != nil {
			return false, err
		}
		found, err := r.matchesFilter(entity, f)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

func (r *Repo[T]) Delete(ctx context.Context, d hohin.DB, f hohin.Filter) error {
	db := d.(*DB)
	db.mutex.Lock()
	defer db.mutex.Unlock()
	indices := make([]int, 0)
	for i, record := range db.data[r.collection] {
		entity, err := r.load(record)
		if err != nil {
			return err
		}
		found, err := r.matchesFilter(entity, f)
		if err != nil {
			return err
		}
		if found {
			indices = append(indices, i)
		}
	}

	for i := len(indices) - 1; i >= 0; i -= 1 {
		collection := make([][]byte, 0)
		collection = append(collection, db.data[r.collection][:indices[i]]...)
		collection = append(collection, db.data[r.collection][indices[i]+1:]...)
		db.data[r.collection] = collection
	}

	return nil
}

func (r Repo[T]) Count(ctx context.Context, d hohin.DB, f hohin.Filter) (uint64, error) {
	db := d.(*DB)
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	var result uint64
	for _, record := range db.data[r.collection] {
		entity, err := r.load(record)
		if err != nil {
			return 0, err
		}
		found, err := r.matchesFilter(entity, f)
		if err != nil {
			return 0, err
		}
		if found {
			result += 1
		}
	}
	return result, nil
}

func (r *Repo[T]) GetMany(ctx context.Context, d hohin.DB, q hohin.Query) ([]T, error) {
	db := d.(*DB)
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	result := []T{}
	for _, record := range db.data[r.collection] {
		entity, err := r.load(record)
		if err != nil {
			return nil, err
		}
		found, err := r.matchesFilter(entity, q.Filter)
		if err != nil {
			return nil, err
		}
		if found {
			result = append(result, entity)
		}
	}

	if len(q.Order) > 0 {
		sort.SliceStable(result, func(i, j int) bool {
			v1 := reflect.ValueOf(result[i])
			v2 := reflect.ValueOf(result[j])

			for _, o := range q.Order {
				f1 := v1.FieldByName(o.Field)
				f2 := v2.FieldByName(o.Field)
				switch f1.Kind() {
				case reflect.Int:
					a, b := f1.Int(), f2.Int()
					if (!o.Desc && a < b) || (o.Desc && a > b) {
						return true
					}
				case reflect.Float64:
					a, b := f1.Float(), f2.Float()
					if (!o.Desc && a < b) || (o.Desc && a > b) {
						return true
					}
				case reflect.String:
					a, b := f1.String(), f2.String()
					if (!o.Desc && a < b) || (o.Desc && a > b) {
						return true
					}
				case reflect.Bool:
					a, b := f1.Bool(), f2.Bool()
					if (!o.Desc && !a && b) || (o.Desc && a && !b) {
						return true
					}
				}
			}

			return false
		})
	}

	if len(result) > 0 {
		if q.Offset > 0 {
			result = result[q.Offset:]
		}
		if q.Limit > 0 {
			result = result[:q.Limit]
		}
	}

	return result, nil
}

func (r *Repo[T]) matchesFilter(entity T, f hohin.Filter) (bool, error) {
	switch f.Operation {
	case "":
		return true, nil
	case operations.Not:
		result, err := r.matchesFilter(entity, f.Value.(hohin.Filter))
		if err != nil {
			return false, err
		}
		return !result, nil
	case operations.And:
		for _, filter := range f.Value.([]hohin.Filter) {
			result, err := r.matchesFilter(entity, filter)
			if err != nil {
				return false, err
			}
			if !result {
				return false, nil
			}
		}
		return true, nil
	case operations.Or:
		for _, filter := range f.Value.([]hohin.Filter) {
			result, err := r.matchesFilter(entity, filter)
			if err != nil {
				return false, err
			}
			if result {
				return true, nil
			}
		}
		return false, nil
	}

	s := reflect.ValueOf(entity)
	field := s.FieldByName(f.Field)
	if !field.IsValid() {
		return false, fmt.Errorf("unknown field `%s` in a filter", f.Field)
	}

	switch f.Operation {
	case operations.Eq:
		switch val := f.Value.(type) {
		case int:
			return field.Int() == int64(val), nil
		case float64:
			return field.Float() == val, nil
		case string:
			return field.String() == val, nil
		case bool:
			return field.Bool() == val, nil
		case time.Time:
			return field.Interface().(time.Time) == val, nil
		case decimal.Decimal:
			return field.Interface().(decimal.Decimal).Equal(val), nil
		case uuid.UUID:
			return field.Interface().(uuid.UUID) == val, nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.IEq:
		switch val := f.Value.(type) {
		case string:
			return strings.ToUpper(field.String()) == strings.ToUpper(val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Ne:
		switch val := f.Value.(type) {
		case int:
			return field.Int() != int64(val), nil
		case float64:
			return field.Float() != val, nil
		case string:
			return field.String() != val, nil
		case bool:
			return field.Bool() != val, nil
		case time.Time:
			return field.Interface().(time.Time) != val, nil
		case decimal.Decimal:
			return !field.Interface().(decimal.Decimal).Equal(val), nil
		case uuid.UUID:
			return field.Interface().(uuid.UUID) != val, nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.INe:
		switch val := f.Value.(type) {
		case string:
			return strings.ToUpper(field.String()) != strings.ToUpper(val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Lt:
		switch val := f.Value.(type) {
		case int:
			return field.Int() < int64(val), nil
		case float64:
			return field.Float() < val, nil
		case time.Time:
			return field.Interface().(time.Time).Before(val), nil
		case decimal.Decimal:
			return field.Interface().(decimal.Decimal).LessThan(val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Gt:
		switch val := f.Value.(type) {
		case int:
			return field.Int() > int64(val), nil
		case float64:
			return field.Float() > val, nil
		case time.Time:
			return field.Interface().(time.Time).After(val), nil
		case decimal.Decimal:
			return field.Interface().(decimal.Decimal).GreaterThan(val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Lte:
		switch val := f.Value.(type) {
		case int:
			return field.Int() <= int64(val), nil
		case float64:
			return field.Float() <= val, nil
		case time.Time:
			return field.Interface().(time.Time).Compare(val) <= 0, nil
		case decimal.Decimal:
			return field.Interface().(decimal.Decimal).LessThanOrEqual(val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Gte:
		switch val := f.Value.(type) {
		case int:
			return field.Int() >= int64(val), nil
		case float64:
			return field.Float() >= val, nil
		case time.Time:
			return field.Interface().(time.Time).Compare(val) >= 0, nil
		case decimal.Decimal:
			return field.Interface().(decimal.Decimal).GreaterThanOrEqual(val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.HasPrefix:
		switch val := f.Value.(type) {
		case string:
			return strings.HasPrefix(field.String(), val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.IHasPrefix:
		switch val := f.Value.(type) {
		case string:
			return strings.HasPrefix(strings.ToUpper(field.String()), strings.ToUpper(val)), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.HasSuffix:
		switch val := f.Value.(type) {
		case string:
			return strings.HasSuffix(field.String(), val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.IHasSuffix:
		switch val := f.Value.(type) {
		case string:
			return strings.HasSuffix(strings.ToUpper(field.String()), strings.ToUpper(val)), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.Contains:
		switch val := f.Value.(type) {
		case string:
			return strings.Contains(field.String(), val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.IContains:
		switch val := f.Value.(type) {
		case string:
			return strings.Contains(strings.ToUpper(field.String()), strings.ToUpper(val)), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.IPWithin:
		switch val := f.Value.(type) {
		case string:
			prefix, err := netip.ParsePrefix(val)
			if err != nil {
				return false, err
			}
			addr, ok := field.Interface().(netip.Addr)
			if !ok {
				return false, fmt.Errorf("%s is not netip.Addr", f.Field)
			}
			return prefix.Contains(addr), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operations.In:
		switch val := f.Value.(type) {
		case []any:
			for _, item := range val {
				switch x := item.(type) {
				case string:
					if x == field.String() {
						return true, nil
					}
				case int:
					if int64(x) == field.Int() {
						return true, nil
					}
				case int32:
					if int64(x) == field.Int() {
						return true, nil
					}
				case int64:
					if int64(x) == field.Int() {
						return true, nil
					}
				case float32:
					if float64(x) == field.Float() {
						return true, nil
					}
				case float64:
					if x == field.Float() {
						return true, nil
					}
				}
			}
			return false, nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	}

	panic(fmt.Sprintf("unknown operation %s", f.Operation))
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

func (r *Repo[T]) Add(ctx context.Context, d hohin.DB, entity T) error {
	db := d.(*DB)
	db.mutex.Lock()
	defer db.mutex.Unlock()
	records := db.data[r.collection]
	record, err := r.dump(entity)
	if err != nil {
		return err
	}
	db.data[r.collection] = append(records, record)
	return nil
}

func (r *Repo[T]) AddMany(ctx context.Context, d hohin.DB, entities []T) error {
	for _, e := range entities {
		if err := r.Add(ctx, d, e); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repo[T]) Update(ctx context.Context, d hohin.DB, f hohin.Filter, entity T) error {
	db := d.(*DB)
	db.mutex.Lock()
	defer db.mutex.Unlock()
	index := -1
	for i, record := range db.data[r.collection] {
		entity, err := r.load(record)
		if err != nil {
			return err
		}
		found, err := r.matchesFilter(entity, f)
		if err != nil {
			return err
		}
		if found {
			index = i
			break
		}
	}

	if index > -1 {
		record, err := r.dump(entity)
		if err != nil {
			return err
		}
		db.data[r.collection][index] = record
	}

	return nil
}

func (r *Repo[T]) CountAll(ctx context.Context, d hohin.DB) (uint64, error) {
	db := d.(*DB)
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return uint64(len(db.data[r.collection])), nil
}

func (r *Repo[T]) Clear(ctx context.Context, d hohin.DB) error {
	db := d.(*DB)
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.data[r.collection] = nil
	return nil
}

func (r *Repo[T]) dump(entity T) ([]byte, error) {
	return json.Marshal(entity)
}

func (r *Repo[T]) load(record []byte) (T, error) {
	var entity T
	err := json.Unmarshal(record, &entity)
	return entity, err
}
