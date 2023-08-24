package operations

type Operation string

const (
	Eq         Operation = "="
	IEq        Operation = "IEq"
	Ne         Operation = "!="
	INe        Operation = "INe"
	IsNone     Operation = "IsNone"
	Lt         Operation = "<"
	Gt         Operation = ">"
	Lte        Operation = "<="
	Gte        Operation = ">="
	In         Operation = "In"
	Contains   Operation = "Contains"
	IContains  Operation = "IContains"
	HasPrefix  Operation = "HasPrefix"
	IHasPrefix Operation = "IHasPrefix"
	HasSuffix  Operation = "HasSuffix"
	IHasSuffix Operation = "IHasSuffix"
	IpWithin   Operation = "IpWithin"
	And        Operation = "And"
	Or         Operation = "Or"
	Not        Operation = "Not"
)
