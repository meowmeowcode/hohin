package filter

import "github.com/meowmeowcode/hohin/filter/operation"

type Filter struct {
	Field     string
	Operation operation.Operation
	Value     any
}

func Eq(field string, value any) Filter {
	return Filter{Field: field, Operation: operation.Eq, Value: value}
}

func Ne(field string, value any) Filter {
	return Filter{Field: field, Operation: operation.Ne, Value: value}
}

func IsNone(field string) Filter {
	return Filter{Field: field, Operation: operation.IsNone}
}

func Lt(field string, value any) Filter {
	return Filter{Field: field, Operation: operation.Lt, Value: value}
}

func Gt(field string, value any) Filter {
	return Filter{Field: field, Operation: operation.Gt, Value: value}
}

func Lte(field string, value any) Filter {
	return Filter{Field: field, Operation: operation.Lte, Value: value}
}

func Gte(field string, value any) Filter {
	return Filter{Field: field, Operation: operation.Gte, Value: value}
}

func In(field string, value any) Filter {
	return Filter{Field: field, Operation: operation.In, Value: value}
}

func Contains(field string, value string) Filter {
	return Filter{Field: field, Operation: operation.Contains, Value: value}
}

func HasPrefix(field string, value string) Filter {
	return Filter{Field: field, Operation: operation.HasPrefix, Value: value}
}

func HasSuffix(field string, value string) Filter {
	return Filter{Field: field, Operation: operation.HasSuffix, Value: value}
}

func And(value ...Filter) Filter {
	return Filter{Operation: operation.And, Value: value}
}

func Or(value ...Filter) Filter {
	return Filter{Operation: operation.Or, Value: value}
}

func Not(value Filter) Filter {
	return Filter{Operation: operation.Not, Value: value}
}
