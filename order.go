package hohin

// Order describes how entities retrieved from a repository must be ordered.
type Order struct {
	Field string // field to order by
	Desc  bool   // defines if ordering must be descending or not
}

// Asc returns an [Order] for ascending ordering by a given field.
func Asc(field string) Order {
	return Order{Field: field}
}

// Desc returns an [Order] for descending ordering by a given field.
func Desc(field string) Order {
	return Order{Desc: true, Field: field}
}
