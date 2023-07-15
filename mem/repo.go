package mem

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/meowmeowcode/hohin"
	"github.com/meowmeowcode/hohin/filter"
	"github.com/meowmeowcode/hohin/filter/operation"
	"github.com/meowmeowcode/hohin/query"
	"github.com/shopspring/decimal"
	"reflect"
	"sort"
	"strings"
	"time"
)

type Db struct {
	data map[string][][]byte
}

func (db *Db) Transaction(f func(hohin.Db) error) error {
	t := db.copy()
	err := f(t)
	if err != nil {
		return err
	}
	db.data = t.data
	return nil
}

func (db *Db) copy() *Db {
	c := NewDb()
	for k, v := range db.data {
		c.data[k] = make([][]byte, 0)
		for _, record := range v {
			c.data[k] = append(c.data[k], record)
		}
	}
	return c
}

func NewDb() *Db {
	return &Db{data: make(map[string][][]byte)}
}

type Repo[T any] struct {
	collection string
}

func NewRepo[T any](collection string) *Repo[T] {
	return &Repo[T]{collection: collection}
}

func (r *Repo[T]) Get(db hohin.Db, f filter.Filter) (T, error) {
	var zero T

	for _, record := range db.(*Db).data[r.collection] {
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

func (r *Repo[T]) GetForUpdate(db hohin.Db, f filter.Filter) (T, error) {
	return r.Get(db, f)
}

func (r *Repo[T]) Exists(db hohin.Db, f filter.Filter) (bool, error) {
	for _, record := range db.(*Db).data[r.collection] {
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

func (r *Repo[T]) Delete(db hohin.Db, f filter.Filter) error {
	dbObj := db.(*Db)
	indices := make([]int, 0)
	for i, record := range dbObj.data[r.collection] {
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
		collection = append(collection, dbObj.data[r.collection][:indices[i]]...)
		collection = append(collection, dbObj.data[r.collection][indices[i]+1:]...)
		dbObj.data[r.collection] = collection
	}

	return nil
}

func (r Repo[T]) Count(db hohin.Db, f filter.Filter) (int, error) {
	result := 0
	for _, record := range db.(*Db).data[r.collection] {
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

func (r *Repo[T]) GetMany(db hohin.Db, q query.Query) ([]T, error) {
	result := []T{}
	for _, record := range db.(*Db).data[r.collection] {
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

	if q.Offset > 0 {
		result = result[q.Offset:]
	}

	if q.Limit > 0 {
		result = result[:q.Limit]
	}

	return result, nil
}

func (r *Repo[T]) matchesFilter(entity T, f filter.Filter) (bool, error) {
	switch f.Operation {
	case "":
		return true, nil
	case operation.Not:
		result, err := r.matchesFilter(entity, f.Value.(filter.Filter))
		if err != nil {
			return false, err
		}
		return !result, nil
	case operation.And:
		for _, filter := range f.Value.([]filter.Filter) {
			result, err := r.matchesFilter(entity, filter)
			if err != nil {
				return false, err
			}
			if !result {
				return false, nil
			}
		}
		return true, nil
	case operation.Or:
		for _, filter := range f.Value.([]filter.Filter) {
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

	switch f.Operation {
	case operation.Eq:
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
	case operation.Ne:
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
	case operation.Lt:
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
	case operation.Gt:
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
	case operation.Lte:
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
	case operation.Gte:
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
	case operation.HasPrefix:
		switch val := f.Value.(type) {
		case string:
			return strings.HasPrefix(field.String(), val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operation.HasSuffix:
		switch val := f.Value.(type) {
		case string:
			return strings.HasSuffix(field.String(), val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operation.Contains:
		switch val := f.Value.(type) {
		case string:
			return strings.Contains(field.String(), val), nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	case operation.In:
		switch val := f.Value.(type) {
		case []string:
			for _, x := range val {
				if x == field.String() {
					return true, nil
				}
			}
			return false, nil
		case []int:
			for _, x := range val {
				if int64(x) == field.Int() {
					return true, nil
				}
			}
			return false, nil
		case []float64:
			for _, x := range val {
				if x == field.Float() {
					return true, nil
				}
			}
			return false, nil
		default:
			return false, fmt.Errorf("operation %s is not supported for %T", f.Operation, val)
		}
	}

	panic(fmt.Sprintf("unknown operation %s", f.Operation))
}

func (r *Repo[T]) Add(db hohin.Db, entity T) error {
	dbObj := db.(*Db)
	records := dbObj.data[r.collection]
	record, err := r.dump(entity)
	if err != nil {
		return err
	}
	dbObj.data[r.collection] = append(records, record)
	return nil
}

func (r *Repo[T]) Update(db hohin.Db, f filter.Filter, entity T) error {
	dbObj := db.(*Db)
	index := -1
	for i, record := range dbObj.data[r.collection] {
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
		dbObj.data[r.collection][index] = record
	}

	return nil
}

func (r *Repo[T]) CountAll(db hohin.Db) (int, error) {
	return len(db.(*Db).data[r.collection]), nil
}

func (r *Repo[T]) Clear(db hohin.Db) error {
	db.(*Db).data[r.collection] = nil
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
