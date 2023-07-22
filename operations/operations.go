package operations

type Operation string

const (
	Eq        Operation = "="
	Ne        Operation = "!="
	IsNone    Operation = "IsNone"
	Lt        Operation = "<"
	Gt        Operation = ">"
	Lte       Operation = "<="
	Gte       Operation = ">="
	In        Operation = "In"
	Contains  Operation = "Contains"
	HasPrefix Operation = "HasPrefix"
	HasSuffix Operation = "HasSuffix"
	And       Operation = "And"
	Or        Operation = "Or"
	Not       Operation = "Not"
)
