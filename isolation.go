package hohin

// IsolationLevel defines an isolation level of a database transaction.
type IsolationLevel int

const (
	DefaultIsolation IsolationLevel = iota
	ReadUncommitted
	ReadCommitted
	RepeatableRead
	Serializable
)
