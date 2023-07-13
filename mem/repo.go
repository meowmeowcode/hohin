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
	data map[string][]any
}

func NewDb() Db {
	return Db{data: make(map[string][]any)}
}

type Repo[T any] struct {
	collection string
}

func NewRepo[T any](collection string) Repo[T] {
	return Repo[T]{collection: collection}
}

func (r *Repo[T]) Get(db *Db, f filter.Filter) (T, error) {
	var zero T

	for _, entity := range db.data[r.collection] {
		found, err := matchesFilter(entity, f)
		if err != nil {
			return zero, err
		}
		if found {
			entityCopy, err := copyEntity(entity.(T))
			if err != nil {
				return zero, err
			}
			return entityCopy, nil
		}
	}

	return zero, hohin.NotFound
}

func (r *Repo[T]) GetForUpdate(db *Db, f filter.Filter) (T, error) {
	return r.Get(db, f)
}

func (r *Repo[T]) Exists(db *Db, f filter.Filter) (bool, error) {
	for _, entity := range db.data[r.collection] {
		found, err := matchesFilter(entity, f)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}

	return false, nil
}

func (r *Repo[T]) Delete(db *Db, f filter.Filter) error {
	indices := make([]int, 0)
	for i, entity := range db.data[r.collection] {
		found, err := matchesFilter(entity, f)
		if err != nil {
			return err
		}
		if found {
			indices = append(indices, i)
		}
	}

	for i := len(indices); i >= 0; i -= 1 {
		collection := make([]any, 0)
		collection = append(collection, db.data[r.collection][:i]...)
		collection = append(collection, db.data[r.collection][i+1:]...)
		db.data[r.collection] = collection
	}

	return nil
}

func (r *Repo[T]) Count(db *Db, f filter.Filter) (int, error) {
	result := 0
	for _, entity := range db.data[r.collection] {
		found, err := matchesFilter(entity, f)
		if err != nil {
			return 0, err
		}
		if found {
			result += 1
		}
	}

	return result, nil
}

func (r *Repo[T]) GetMany(db *Db, q query.Query) ([]T, error) {
	result := []T{}
	for _, entity := range db.data[r.collection] {
		found, err := matchesFilter(entity, q.Filter)
		if err != nil {
			return nil, err
		}
		if found {
			entityCopy, err := copyEntity(entity.(T))
			if err != nil {
				return nil, err
			}
			result = append(result, entityCopy)
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

func matchesFilter(entity any, f filter.Filter) (bool, error) {
	switch f.Operation {
	case "":
		return true, nil
	case operation.Not:
		result, err := matchesFilter(entity, f.Value.(filter.Filter))
		if err != nil {
			return false, err
		}
		return !result, nil
	case operation.And:
		for _, filter := range f.Value.([]filter.Filter) {
			result, err := matchesFilter(entity, filter)
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
			result, err := matchesFilter(entity, filter)
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

func (r *Repo[T]) Add(db *Db, entity T) error {
	entities := db.data[r.collection]
	entityCopy, err := copyEntity(entity)
	if err != nil {
		return err
	}
	db.data[r.collection] = append(entities, entityCopy)
	return nil
}

func (r *Repo[T]) Update(db *Db, f filter.Filter, entity T) error {
	index := -1
	for i, entity := range db.data[r.collection] {
		found, err := matchesFilter(entity, f)
		if err != nil {
			return err
		}
		if found {
			index = i
			break
		}
	}

	if index > -1 {
		entityCopy, err := copyEntity(entity)
		if err != nil {
			return err
		}
		db.data[r.collection][index] = entityCopy
	}

	return nil
}

func copyEntity[T any](entity T) (T, error) {
	var entityCopy T
	entity_data, err := json.Marshal(entity)
	if err != nil {
		return entityCopy, err
	}
	err = json.Unmarshal(entity_data, &entityCopy)
	if err != nil {
		return entityCopy, err
	}
	return entityCopy, nil
}

func (r *Repo[T]) CountAll(db *Db) (int, error) {
	return len(db.data[r.collection]), nil
}

func (r *Repo[T]) Clear(db *Db) error {
	db.data[r.collection] = nil
	return nil
}
