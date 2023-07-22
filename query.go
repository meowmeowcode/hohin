package hohin

type Query struct {
	Filter Filter
	Limit  int
	Offset int
	Order  []Order
}

func (q Query) OrderBy(o ...Order) Query {
	q.Order = o
	return q
}
