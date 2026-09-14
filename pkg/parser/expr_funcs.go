package parser

import (
	"fmt"
	"sort"
	"strings"
)

// ExprType is the static type of an expression.
type ExprType int

const (
	// TypeAny is a bare field reference. Stored values are strings, but a field
	// is coercible to a number on demand, so it satisfies both a string and a
	// numeric parameter. This is what keeps `score := bytes * 2` working on a
	// field carrying no type hint.
	TypeAny ExprType = iota
	TypeString
	TypeNumber
	TypeBool
)

func (t ExprType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeNumber:
		return "number"
	case TypeBool:
		return "boolean"
	}
	return "field"
}

// exprParam is one parameter of a scalar function.
type exprParam struct {
	Name     string
	Type     ExprType
	Optional bool
	// Variadic marks a trailing parameter that absorbs the remaining arguments.
	// Only valid on the last parameter.
	Variadic bool
}

// exprFunc is a scalar function usable inside an expression. Render receives the
// arguments already compiled to SQL, in declaration order, with omitted optional
// parameters absent from the slice.
type exprFunc struct {
	Name    string
	Params  []exprParam
	Returns ExprType
	Render  func(args []string) string
	// MinArgs, when non-zero, overrides the required-argument count derived from
	// Params. Used by variadic functions that need at least N.
	MinArgs int
}

var exprFuncs = map[string]*exprFunc{}

func registerExprFunc(f *exprFunc, aliases ...string) {
	exprFuncs[f.Name] = f
	for _, a := range aliases {
		exprFuncs[a] = f
	}
}

// ExprFunctionNames returns every callable name including aliases, so the
// built-in query reference can be checked against the real registry.
func ExprFunctionNames() []string {
	names := make([]string, 0, len(exprFuncs))
	for name := range exprFuncs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// required counts the parameters that must be supplied.
func (f *exprFunc) required() int {
	if f.MinArgs > 0 {
		return f.MinArgs
	}
	n := 0
	for _, p := range f.Params {
		if !p.Optional && !p.Variadic {
			n++
		}
	}
	return n
}

// bindArgs matches a call's positional and named arguments to the signature,
// returning them in declaration order. A variadic tail collects the remainder.
func (f *exprFunc) bindArgs(call *ExprNode) ([]*ExprNode, error) {
	bound := make([]*ExprNode, 0, len(call.Args)+len(call.Named))
	positional := call.Args

	for i, p := range f.Params {
		if p.Variadic {
			if _, named := call.Named[p.Name]; named {
				return nil, fmt.Errorf("%s(): %s takes a list, so it cannot be given by name", f.Name, p.Name)
			}
			bound = append(bound, positional...)
			positional = nil
			continue
		}
		named, hasNamed := call.Named[p.Name]
		hasPositional := i < len(positional)
		switch {
		case hasNamed && hasPositional:
			return nil, fmt.Errorf("%s(): %s given both positionally and by name", f.Name, p.Name)
		case hasNamed:
			bound = append(bound, named)
		case hasPositional:
			bound = append(bound, positional[i])
		case p.Optional:
			bound = append(bound, nil)
		default:
			return nil, fmt.Errorf("%s(): missing required argument %s", f.Name, p.Name)
		}
	}

	if n := len(call.Args); n > len(f.Params) && !f.variadic() {
		return nil, fmt.Errorf("%s(): expects at most %d arguments, got %d", f.Name, len(f.Params), n)
	}
	for name := range call.Named {
		if !f.hasParam(name) {
			return nil, fmt.Errorf("%s(): unknown parameter %s (accepts %s)", f.Name, name, f.paramList())
		}
	}
	if countBound(bound) < f.required() {
		return nil, fmt.Errorf("%s(): expects at least %d arguments, got %d", f.Name, f.required(), countBound(bound))
	}
	return bound, nil
}

func countBound(bound []*ExprNode) int {
	n := 0
	for _, b := range bound {
		if b != nil {
			n++
		}
	}
	return n
}

func (f *exprFunc) variadic() bool {
	return len(f.Params) > 0 && f.Params[len(f.Params)-1].Variadic
}

func (f *exprFunc) hasParam(name string) bool {
	for _, p := range f.Params {
		if p.Name == name {
			return true
		}
	}
	return false
}

func (f *exprFunc) paramList() string {
	names := make([]string, len(f.Params))
	for i, p := range f.Params {
		names[i] = p.Name
	}
	return strings.Join(names, ", ")
}

// param returns the signature entry governing argument i, following a variadic
// tail past the end of the declared list.
func (f *exprFunc) param(i int) exprParam {
	if i < len(f.Params) {
		return f.Params[i]
	}
	return f.Params[len(f.Params)-1]
}

func str(name string) exprParam    { return exprParam{Name: name, Type: TypeString} }
func num(name string) exprParam    { return exprParam{Name: name, Type: TypeNumber} }
func anyp(name string) exprParam   { return exprParam{Name: name, Type: TypeAny} }
func optNum(name string) exprParam { return exprParam{Name: name, Type: TypeNumber, Optional: true} }

func init() {
	// String functions. `field` is the conventional first parameter name, matching
	// the existing commands' field= argument.
	registerExprFunc(&exprFunc{
		Name: "lower", Params: []exprParam{str("field")}, Returns: TypeString,
		Render: func(a []string) string { return "lower(" + a[0] + ")" },
	}, "lowercase")
	registerExprFunc(&exprFunc{
		Name: "upper", Params: []exprParam{str("field")}, Returns: TypeString,
		Render: func(a []string) string { return "upper(" + a[0] + ")" },
	}, "uppercase")
	registerExprFunc(&exprFunc{
		Name: "len", Params: []exprParam{str("field")}, Returns: TypeNumber,
		Render: func(a []string) string { return "length(" + a[0] + ")" },
	}, "length")
	registerExprFunc(&exprFunc{
		Name:    "substr",
		Params:  []exprParam{str("field"), num("start"), optNum("length")},
		Returns: TypeString,
		Render: func(a []string) string {
			if a[2] == "" {
				return fmt.Sprintf("substring(%s, %s)", a[0], a[1])
			}
			return fmt.Sprintf("substring(%s, %s, %s)", a[0], a[1], a[2])
		},
	}, "substring")
	registerExprFunc(&exprFunc{
		Name:    "concat",
		Params:  []exprParam{{Name: "parts", Type: TypeString, Variadic: true}},
		Returns: TypeString, MinArgs: 2,
		Render: func(a []string) string { return "concat(" + strings.Join(a, ", ") + ")" },
	})
	registerExprFunc(&exprFunc{
		Name:    "coalesce",
		Params:  []exprParam{{Name: "fields", Type: TypeString, Variadic: true}},
		Returns: TypeString, MinArgs: 2,
		Render: func(a []string) string {
			conds := make([]string, 0, len(a)*2+1)
			for _, v := range a {
				conds = append(conds, fmt.Sprintf("%s != ''", v), v)
			}
			return "multiIf(" + strings.Join(conds, ", ") + ", '')"
		},
	})
	registerExprFunc(&exprFunc{
		Name:    "splitat",
		Params:  []exprParam{str("field"), str("delimiter"), num("index")},
		Returns: TypeString,
		Render: func(a []string) string {
			return fmt.Sprintf("arrayElement(splitByString(%s, %s), toInt32(%s))", a[1], a[0], a[2])
		},
	})
	registerExprFunc(&exprFunc{
		Name: "base64decode", Params: []exprParam{str("field")}, Returns: TypeString,
		Render: func(a []string) string { return "tryBase64Decode(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name: "urldecode", Params: []exprParam{str("field")}, Returns: TypeString,
		Render: func(a []string) string { return "decodeURLComponent(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name:    "replaceregex",
		Params:  []exprParam{str("field"), str("pattern"), str("replacement")},
		Returns: TypeString,
		Render: func(a []string) string {
			return fmt.Sprintf("replaceRegexpAll(%s, %s, %s)", a[0], a[1], a[2])
		},
	})
	registerExprFunc(&exprFunc{
		Name: "trim", Params: []exprParam{str("field")}, Returns: TypeString,
		Render: func(a []string) string { return "trimBoth(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name:    "hash",
		Params:  []exprParam{{Name: "parts", Type: TypeString, Variadic: true}},
		Returns: TypeString, MinArgs: 1,
		Render: func(a []string) string { return "hex(cityHash64(" + strings.Join(a, ", ") + "))" },
	})
	registerExprFunc(&exprFunc{
		Name:    "editdistance",
		Params:  []exprParam{str("a"), str("b")},
		Returns: TypeNumber,
		Render: func(a []string) string {
			return fmt.Sprintf("damerauLevenshteinDistance(%s, %s)", a[0], a[1])
		},
	}, "levenshtein")

	// Predicates.
	registerExprFunc(&exprFunc{
		Name: "isempty", Params: []exprParam{anyp("field")}, Returns: TypeBool,
		Render: func(a []string) string { return "(" + a[0] + " = '')" },
	})
	registerExprFunc(&exprFunc{
		Name:    "startswith",
		Params:  []exprParam{str("field"), str("prefix")},
		Returns: TypeBool,
		Render:  func(a []string) string { return fmt.Sprintf("startsWith(%s, %s)", a[0], a[1]) },
	})
	registerExprFunc(&exprFunc{
		Name:    "endswith",
		Params:  []exprParam{str("field"), str("suffix")},
		Returns: TypeBool,
		Render:  func(a []string) string { return fmt.Sprintf("endsWith(%s, %s)", a[0], a[1]) },
	})
	registerExprFunc(&exprFunc{
		Name:    "contains",
		Params:  []exprParam{str("field"), str("substring")},
		Returns: TypeBool,
		Render: func(a []string) string {
			// Bracketed: the render is an infix comparison behind a call node, and
			// ClickHouse puts =, >, < at one left-associative level, so
			// contains(a,"x") = contains(b,"y") would regroup.
			return fmt.Sprintf("(positionCaseInsensitive(%s, %s) > 0)", a[0], a[1])
		},
	})

	// cidr() is a boolean predicate, so it composes inside an expression exactly
	// as it does as a pipeline filter. Both spellings render the same SQL.
	registerExprFunc(&exprFunc{
		Name:    "cidr",
		Params:  []exprParam{str("field"), str("range")},
		Returns: TypeBool,
		Render:  func(a []string) string { return cidrPredicateSQL(a[0], a[1]) },
	})

	// Network. Every one guards its input: ClickHouse throws CANNOT_PARSE_IPV4 on a
	// value that is not an address, and a single bad row would abort the query, so
	// the conversion is only ever handed a real address (a sentinel otherwise).
	registerExprFunc(&exprFunc{
		Name: "isipv4", Params: []exprParam{str("field")}, Returns: TypeBool,
		Render: func(a []string) string { return "isIPv4String(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name: "isipv6", Params: []exprParam{str("field")}, Returns: TypeBool,
		Render: func(a []string) string { return "isIPv6String(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name: "isprivateip", Params: []exprParam{str("field")}, Returns: TypeBool,
		Render: func(a []string) string { return privateIPPredicateSQL(a[0]) },
	})
	registerExprFunc(&exprFunc{
		Name:    "ipprefix",
		Params:  []exprParam{str("field"), num("bits")},
		Returns: TypeString,
		Render:  func(a []string) string { return ipPrefixSQL(a[0], a[1]) },
	})

	// Time.
	registerExprFunc(&exprFunc{
		Name:    "datediff",
		Params:  []exprParam{str("unit"), str("start"), str("end")},
		Returns: TypeNumber,
		Render: func(a []string) string {
			return fmt.Sprintf("dateDiff(%s, %s, %s)", a[0], lenientDateTime(a[1]), lenientDateTime(a[2]))
		},
	})

	// Numeric.
	registerExprFunc(&exprFunc{
		Name: "abs", Params: []exprParam{num("value")}, Returns: TypeNumber,
		Render: func(a []string) string { return "abs(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name: "floor", Params: []exprParam{num("value")}, Returns: TypeNumber,
		Render: func(a []string) string { return "floor(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name: "ceil", Params: []exprParam{num("value")}, Returns: TypeNumber,
		Render: func(a []string) string { return "ceil(" + a[0] + ")" },
	})
	registerExprFunc(&exprFunc{
		Name: "round", Params: []exprParam{num("value"), optNum("digits")}, Returns: TypeNumber,
		Render: func(a []string) string {
			if a[1] == "" {
				return "round(" + a[0] + ")"
			}
			return fmt.Sprintf("round(%s, %s)", a[0], a[1])
		},
	})
	registerExprFunc(&exprFunc{
		Name: "tonumber", Params: []exprParam{anyp("value")}, Returns: TypeNumber,
		Render: func(a []string) string { return "toFloat64OrNull(toString(" + a[0] + "))" },
	})
	registerExprFunc(&exprFunc{
		Name: "tostring", Params: []exprParam{anyp("value")}, Returns: TypeString,
		Render: func(a []string) string { return "toString(" + a[0] + ")" },
	})

	// Conditional. The branches are type-checked against each other in compileCall.
	registerExprFunc(&exprFunc{
		Name:    "if",
		Params:  []exprParam{{Name: "condition", Type: TypeBool}, anyp("then"), anyp("else")},
		Returns: TypeAny,
		Render: func(a []string) string {
			return fmt.Sprintf("if(%s, %s, %s)", a[0], a[1], a[2])
		},
	})
}

// privateIPRanges are the non-routable and carrier-internal ranges isPrivateIP
// treats as private: RFC1918, loopback, link-local, CGNAT, and their IPv6
// equivalents (unique-local, loopback, link-local).
var privateIPRanges = []string{
	"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
	"127.0.0.0/8", "169.254.0.0/16", "100.64.0.0/10",
	"fc00::/7", "::1/128", "fe80::/10",
}

// privateIPPredicateSQL tests membership of any private range. The address is
// validity-checked and a sentinel substituted, so isIPAddressInRange is never
// handed a value it would throw on.
func privateIPPredicateSQL(fieldRef string) string {
	valid := fmt.Sprintf("(isIPv4String(%[1]s) OR isIPv6String(%[1]s))", fieldRef)
	safeAddr := fmt.Sprintf("if(%s, %s, '0.0.0.0')", valid, fieldRef)
	tests := make([]string, len(privateIPRanges))
	for i, r := range privateIPRanges {
		tests[i] = fmt.Sprintf("isIPAddressInRange(%s, '%s')", safeAddr, r)
	}
	return fmt.Sprintf("((%s) AND %s)", strings.Join(tests, " OR "), valid)
}

// ipPrefixSQL returns the network address of the field's enclosing block as
// "network/bits", or empty for a value that is not an address. Grouping by it
// answers questions no membership test can, such as which source subnets are
// sweeping ports.
func ipPrefixSQL(fieldRef, bits string) string {
	v4 := fmt.Sprintf("toIPv4(if(isIPv4String(%s), %s, '0.0.0.0'))", fieldRef, fieldRef)
	v6 := fmt.Sprintf("toIPv6(if(isIPv6String(%s), %s, '::'))", fieldRef, fieldRef)
	width := fmt.Sprintf("toUInt8(%s)", bits)
	v4Prefix := fmt.Sprintf("concat(toString(IPv4CIDRToRange(%s, %s).1), '/', toString(%s))", v4, width, bits)
	v6Prefix := fmt.Sprintf("concat(toString(IPv6CIDRToRange(%s, %s).1), '/', toString(%s))", v6, width, bits)
	return fmt.Sprintf("multiIf(isIPv4String(%s), %s, isIPv6String(%s), %s, '')",
		fieldRef, v4Prefix, fieldRef, v6Prefix)
}
