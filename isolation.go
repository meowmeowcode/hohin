package hohin

type IsolationLevel int

const (
	DefaultIsolation IsolationLevel = iota
	ReadUncommitted
	ReadCommitted
	RepeatableRead
	Serializable
)
