package hohin

type Order struct {
	Field string
	Desc  bool
}

func Asc(field string) Order {
	return Order{Field: field}
}

func Desc(field string) Order {
	return Order{Desc: true, Field: field}
}
