package query

import (
	"github.com/meowmeowcode/hohin/filter"
	"github.com/meowmeowcode/hohin/order"
)

type Query struct {
	Filter filter.Filter
	Limit  int
	Offset int
	Order  []order.Order
}

func New() Query {
	return Query{}
}

func Filter(f filter.Filter) Query {
	return Query{Filter: f}
}

func (q Query) WithFilter(f filter.Filter) Query {
	q.Filter = f
	return q
}

func (q Query) WithLimit(l int) Query {
	q.Limit = l
	return q
}

func (q Query) WithOffset(o int) Query {
	q.Offset = o
	return q
}

func (q Query) WithOrder(o ...order.Order) Query {
	q.Order = o
	return q
}
