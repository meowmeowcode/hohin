package hohin

// Query contains a [Filter] and additional options.
type Query struct {
	Filter Filter  // filter to search entities
	Limit  int     // maximum number of entities to retrieve
	Offset int     // result offset
	Order  []Order // order of entities
}

// OrderBy sets the Order field.
func (q Query) OrderBy(o ...Order) Query {
	q.Order = o
	return q
}
