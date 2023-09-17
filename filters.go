package hohin

import "github.com/meowmeowcode/hohin/operations"

// Filter is an object used for filtering entities
// before getting them from a repository.
type Filter struct {
	Field     string               // name of an entity field
	Operation operations.Operation // comparison operation
	Value     any                  // value to compare with a field
}

// Eq creates a filter to find entities whose field value is equal to a given one.
func Eq(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Eq, Value: value}
}

// IEq creates a case-insensitive filter
// to find entities whose field value is equal to a given one.
func IEq(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.IEq, Value: value}
}

// Ne creates a filter to find entities whose field value is not equal to a given one.
func Ne(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Ne, Value: value}
}

// INe creates a case-insensitive filter
// to find entities whose field value is not equal to a given one.
func INe(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.INe, Value: value}
}

// IsNull creates a filter to find entities whose field value is null.
func IsNull(field string) Filter {
	return Filter{Field: field, Operation: operations.IsNull}
}

// Lt creates a filter to find entities whose field value is less than a given one.
func Lt(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Lt, Value: value}
}

// Gt creates a filter to find entities whose field value is greater than a given one.
func Gt(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Gt, Value: value}
}

// Lte creates a filter to find entities whose field value is less than or equal to a given one.
func Lte(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Lte, Value: value}
}

// Gte creates a filter to find entities whose field value is greater than or equal to a given one.
func Gte(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Gte, Value: value}
}

// In creates a filter to find entities whose field value is within a given value.
func In(field string, value []any) Filter {
	return Filter{Field: field, Operation: operations.In, Value: value}
}

// Contains creates a filter to find entities whose field value has a given value.
func Contains(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.Contains, Value: value}
}

// IContains creates a case-insensitive filter
// to find entities whose field value has a given value.
func IContains(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IContains, Value: value}
}

// HasPrefix creates a filter to find entities whose field value starts with a given value.
func HasPrefix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.HasPrefix, Value: value}
}

// IHasPrefix creates a case-insensitive filter
// to find entities whose field value starts with a given value.
func IHasPrefix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IHasPrefix, Value: value}
}

// HasSuffix creates a filter to find entities whose field value ends with a given value.
func HasSuffix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.HasSuffix, Value: value}
}

// IHasSuffix creates a case-insensitive filter
// to find entities whose field value ends with a given value.
func IHasSuffix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IHasSuffix, Value: value}
}

// IPWithin creates a filter to find entities
// whose field is an IP address contained within a given subnet.
func IPWithin(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IPWithin, Value: value}
}

// And creates a filter that joins multiple filters with the AND operator.
func And(value ...Filter) Filter {
	return Filter{Operation: operations.And, Value: value}
}

// Or creates a filter that joins multiple filters with the OR operator.
func Or(value ...Filter) Filter {
	return Filter{Operation: operations.Or, Value: value}
}

// Not creates a filter that adds the NOT operator to a wrapped filter.
func Not(value Filter) Filter {
	return Filter{Operation: operations.Not, Value: value}
}
