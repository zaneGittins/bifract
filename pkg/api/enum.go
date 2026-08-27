package api

// Enumerator is a type that knows the complete set of values it accepts. The
// schema generator emits them, so a caller reading the API description sees what
// a field like an alert's severity will take instead of the word "string".
//
// Implement it on the named type, not on the struct field, so the values live
// with the constants they come from. A test asserts every declared constant of
// the type appears here, which is what keeps the description honest as values
// are added.
type Enumerator interface {
	EnumValues() []string
}
