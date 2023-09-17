// Package operations contains types of operations
// that can be used for filtering entities.
package operations

// Operation defines how a hohin.Filter must compare values.
type Operation string

const (
	Eq         Operation = "="          // equal
	IEq        Operation = "IEq"        // equal (case-insensitive)
	Ne         Operation = "!="         // not equal
	INe        Operation = "INe"        // not equal (case-insensitive)
	IsNull     Operation = "IsNull"     // is null
	Lt         Operation = "<"          // less than
	Gt         Operation = ">"          // greater than
	Lte        Operation = "<="         // less than or equal
	Gte        Operation = ">="         // greater than or equal
	In         Operation = "In"         // in
	Contains   Operation = "Contains"   // contains
	IContains  Operation = "IContains"  // contains (case-insensitive)
	HasPrefix  Operation = "HasPrefix"  // has prefix
	IHasPrefix Operation = "IHasPrefix" // has prefix (case-insensitive)
	HasSuffix  Operation = "HasSuffix"  // has suffix
	IHasSuffix Operation = "IHasSuffix" // has suffix (case insensitive)
	IPWithin   Operation = "IPWithin"   // an IP address is within a subnet
	And        Operation = "And"        // all conditions are satisfied
	Or         Operation = "Or"         // any condition is satisfied
	Not        Operation = "Not"        // none of conditions is satisfied
)
