package hohin

import "github.com/meowmeowcode/hohin/operations"

type Filter struct {
	Field     string
	Operation operations.Operation
	Value     any
}

func Eq(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Eq, Value: value}
}

func IEq(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.IEq, Value: value}
}

func Ne(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Ne, Value: value}
}

func INe(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.INe, Value: value}
}

func IsNone(field string) Filter {
	return Filter{Field: field, Operation: operations.IsNone}
}

func Lt(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Lt, Value: value}
}

func Gt(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Gt, Value: value}
}

func Lte(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Lte, Value: value}
}

func Gte(field string, value any) Filter {
	return Filter{Field: field, Operation: operations.Gte, Value: value}
}

func In(field string, value []any) Filter {
	return Filter{Field: field, Operation: operations.In, Value: value}
}

func Contains(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.Contains, Value: value}
}

func IContains(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IContains, Value: value}
}

func HasPrefix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.HasPrefix, Value: value}
}

func IHasPrefix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IHasPrefix, Value: value}
}

func HasSuffix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.HasSuffix, Value: value}
}

func IHasSuffix(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IHasSuffix, Value: value}
}

func IpWithin(field string, value string) Filter {
	return Filter{Field: field, Operation: operations.IpWithin, Value: value}
}

func And(value ...Filter) Filter {
	return Filter{Operation: operations.And, Value: value}
}

func Or(value ...Filter) Filter {
	return Filter{Operation: operations.Or, Value: value}
}

func Not(value Filter) Filter {
	return Filter{Operation: operations.Not, Value: value}
}
