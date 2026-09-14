package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Command arguments are parsed once into typed nodes, guided by the command's
// declared schema, and never reconstituted from text.
//
// The previous representation was []string: the parser had a typed view of every
// argument and discarded it, so each handler rebuilt a fragment of a parser from
// the text. Every round trip lost something, and each loss was its own defect:
// quoting (a regex pattern read as a call), whitespace (`a - b` becoming the
// field `a-b`), structure (`bytes * 8` becoming the field `bytes*8`), and shape
// (`bucket(function=...)` emitting an empty identifier).

// ArgKind discriminates a parsed argument.
type ArgKind int

const (
	// ArgExpr is a field reference or an expression over one.
	ArgExpr ArgKind = iota
	// ArgLiteral is a quoted string, a number, or a bare keyword.
	ArgLiteral
	// ArgList is a bracket list or a comma-separated list.
	ArgList
	// ArgAggSpec is an aggregate specification: count(), sum(x), multi(...).
	ArgAggSpec
	// ArgRegex is a /pattern/flags literal.
	ArgRegex
)

// Argument is one parsed command argument.
type Argument struct {
	// Name is the parameter it was bound to by name, or "" when positional.
	Name string
	Kind ArgKind
	Expr *ExprNode // ArgExpr
	// Text is the literal's contents, or for an expression the source it was
	// written as, which is what a derived alias is built from.
	Text string
	Agg  *AggSpec   // ArgAggSpec
	List []Argument // ArgList
	// Quoted records that the text was written in quotes. Structural, so a value
	// that happens to look like code is never read as code.
	Quoted bool
	Pos    int
}

// AggSpec is an aggregate specification. These are not scalar functions: they
// are dispatched by the stats evaluator, and multi() nests further specs.
type AggSpec struct {
	Name string
	Args []Argument
	Pos  int
}

// String renders an argument back to BQL, for error messages only. Nothing
// parses this back.
func (a Argument) String() string {
	var body string
	switch a.Kind {
	case ArgExpr:
		if a.Text != "" {
			body = a.Text
		} else if a.Expr != nil {
			body = a.Expr.String()
		}
	case ArgRegex:
		body = "/" + a.Text + "/"
	case ArgLiteral:
		if a.Quoted {
			body = strconv.Quote(a.Text)
		} else {
			body = a.Text
		}
	case ArgList:
		parts := make([]string, len(a.List))
		for i, e := range a.List {
			parts[i] = e.String()
		}
		body = "[" + strings.Join(parts, ", ") + "]"
	case ArgAggSpec:
		if a.Agg != nil {
			body = a.Agg.String()
		}
	}
	if a.Name != "" {
		return a.Name + "=" + body
	}
	return body
}

func (s *AggSpec) String() string {
	parts := make([]string, len(s.Args))
	for i, a := range s.Args {
		parts[i] = a.String()
	}
	return s.Name + "(" + strings.Join(parts, ", ") + ")"
}

// Value returns the argument's text form for a handler that wants a plain
// string: a literal's contents, an expression's field name, or a rendered
// expression.
func (a Argument) Value() string {
	switch a.Kind {
	case ArgLiteral, ArgRegex:
		return a.Text
	case ArgExpr:
		if a.Expr != nil && a.Expr.Kind == ExprField {
			return a.Expr.Value
		}
		if a.Text != "" {
			return a.Text
		}
		if a.Expr != nil {
			return a.Expr.String()
		}
	case ArgAggSpec:
		if a.Agg != nil {
			return a.Agg.String()
		}
	}
	return ""
}

// IsFieldName reports whether the argument is a bare field reference rather
// than a computed expression.
func (a Argument) IsFieldName() bool {
	return a.Kind == ArgExpr && a.Expr != nil && a.Expr.Kind == ExprField
}

// FieldName is the log field the argument names, or "" when it computes one. A
// quoted value counts: field="user" has always meant the field, not the string.
func (a Argument) FieldName() string {
	switch a.Kind {
	case ArgExpr:
		if a.Expr != nil && a.Expr.Kind == ExprField {
			return a.Expr.Value
		}
	case ArgLiteral:
		return a.Text
	}
	return ""
}

// Bound holds a command's arguments after they have been matched to its schema.
type Bound struct {
	bound      []boundArg // every matched argument, in written order
	positional []Argument
	// extra holds bare arguments the schema has no parameter left for, which the
	// validator reports. Counting positions instead would miscount once a
	// parameter has been filled by name.
	extra []Argument
}

// boundArg pairs an argument with the parameter it filled.
type boundArg struct {
	param string
	arg   Argument
}

// Bind matches parsed arguments to a command's declared parameters. Positional
// arguments fill the positional parameters in order, with a variadic tail
// absorbing the rest.
func Bind(spec *CommandSpec, args []Argument) (*Bound, error) {
	b := &Bound{}
	cursor := newPositionalCursor(spec)
	for _, a := range args {
		if a.Name != "" {
			p, ok := spec.lookup(a.Name)
			if !ok {
				if spec.FreeForm {
					b.positional = append(b.positional, a)
					continue
				}
				return nil, fmt.Errorf("%s(): unknown parameter %s (accepts %s)", spec.Name, a.Name, spec.paramList())
			}
			cursor.fill(p.Name)
			b.bound = append(b.bound, boundArg{strings.ToLower(p.Name), a})
			continue
		}
		b.positional = append(b.positional, a)
		p, ok := cursor.take()
		if !ok {
			b.extra = append(b.extra, a)
			continue
		}
		b.bound = append(b.bound, boundArg{strings.ToLower(p.Name), a})
	}
	return b, nil
}

// All returns every argument bound to a parameter, positionally or by name.
func (b *Bound) All(name string) []Argument {
	return b.Ordered(name)
}

// Ordered returns the arguments bound to any of the given parameters, in the
// order they were written. Order is part of the meaning wherever a command
// combines several fields (hash, concat), so it is never regrouped by name.
func (b *Bound) Ordered(names ...string) []Argument {
	if b == nil {
		return nil
	}
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[strings.ToLower(n)] = true
	}
	var out []Argument
	for _, e := range b.bound {
		if want[e.param] {
			out = append(out, e.arg)
		}
	}
	return out
}

// FlatOrdered is Ordered with one level of list flattened.
func (b *Bound) FlatOrdered(names ...string) []Argument {
	var out []Argument
	for _, a := range b.Ordered(names...) {
		if a.Kind == ArgList {
			out = append(out, a.List...)
			continue
		}
		out = append(out, a)
	}
	return out
}

// Flat returns a parameter's arguments with one level of list flattened, so the
// bracket spelling of a variadic parameter (dedup([a,b])) yields the same
// arguments as the bare one (dedup(a,b)).
func (b *Bound) Flat(name string) []Argument {
	return b.FlatOrdered(name)
}

// First returns the first argument bound to a parameter.
func (b *Bound) First(name string) (Argument, bool) {
	all := b.All(name)
	if len(all) == 0 {
		return Argument{}, false
	}
	return all[0], true
}

// FirstOf returns the first argument bound to any of the given parameters, for a
// command that accepts a value under several names (tags=/tag=/field=).
func (b *Bound) FirstOf(names ...string) (Argument, bool) {
	for _, n := range names {
		if a, ok := b.First(n); ok {
			return a, true
		}
	}
	return Argument{}, false
}

// StrOf returns the text of the first of the given parameters that was given.
func (b *Bound) StrOf(names ...string) string {
	if a, ok := b.FirstOf(names...); ok {
		return a.Value()
	}
	return ""
}

// Str returns a parameter's text, or def when it was not given.
func (b *Bound) Str(name, def string) string {
	if a, ok := b.First(name); ok {
		if v := a.Value(); v != "" {
			return v
		}
	}
	return def
}

// Int returns a parameter's integer value, or def when absent or unparseable.
func (b *Bound) Int(name string, def int) int {
	if n, err := strconv.Atoi(strings.TrimSpace(b.Str(name, ""))); err == nil {
		return n
	}
	return def
}

// Float returns a parameter's float value, or def when absent or unparseable.
func (b *Bound) Float(name string, def float64) float64 {
	if f, err := strconv.ParseFloat(strings.TrimSpace(b.Str(name, "")), 64); err == nil {
		return f
	}
	return def
}

// Flag returns a boolean parameter. Anything that is not recognisably a yes or
// a no leaves the default, so a typo cannot silently flip a switch.
func (b *Bound) Flag(name string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(b.Str(name, ""))) {
	case "false", "0", "no":
		return false
	case "true", "1", "yes":
		return true
	}
	return def
}

// Strings returns a parameter's values as text, flattening a list. This is what
// a list of field names or literal values binds to.
func (b *Bound) Strings(name string) []string {
	var out []string
	for _, a := range b.All(name) {
		if a.Kind == ArgList {
			for _, e := range a.List {
				if v := e.Value(); v != "" {
					out = append(out, v)
				}
			}
			continue
		}
		if v := a.Value(); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// CSV returns a parameter's values with each one further split on commas, for a
// list parameter whose legacy spelling packs the values into a single quoted
// string (hash="a,b").
func (b *Bound) CSV(name string) []string {
	var out []string
	for _, v := range b.Strings(name) {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

// Agg returns the aggregate specification bound to a parameter.
func (b *Bound) Agg(name string) *AggSpec {
	if a, ok := b.First(name); ok {
		return a.Agg
	}
	return nil
}

// Has reports whether a parameter was given.
func (b *Bound) Has(name string) bool {
	return len(b.All(name)) > 0
}

// Positional returns the arguments given without a name, in order.
func (b *Bound) Positional() []Argument {
	if b == nil {
		return nil
	}
	return b.positional
}

// BindCommand matches a command's typed arguments to its schema. It fails when
// the command has no schema or the typed parser could not read its arguments;
// a command written with no arguments binds to an empty set.
func BindCommand(cmd CommandNode) (*Bound, error) {
	spec, ok := CommandSpecFor(cmd.Name)
	if !ok {
		return nil, fmt.Errorf("%s(): no argument schema", cmd.Name)
	}
	if cmd.ArgsErr != nil {
		return nil, cmd.ArgsErr
	}
	return Bind(spec, cmd.Args)
}

// FieldNames returns every log field a command's arguments reference, including
// inside expressions, lists and aggregate specifications. Callers that want to
// know which fields a query touches read this rather than guessing from text.
func (c CommandNode) FieldNames() []string {
	var out []string
	var walk func([]Argument)
	walk = func(args []Argument) {
		for _, a := range args {
			switch a.Kind {
			case ArgExpr:
				out = append(out, exprFieldNames(a.Expr)...)
			case ArgList:
				walk(a.List)
			case ArgAggSpec:
				if a.Agg != nil {
					walk(a.Agg.Args)
				}
			}
		}
	}
	walk(c.Args)
	return out
}

// ResolveArg turns an argument in a field position into SQL: an expression is
// compiled, a bare name resolves through the registry. This replaces the
// resolveFieldRef/exprArgSQL pair, which had to guess from text.
func ResolveArg(a Argument, registry *FieldRegistry) (string, error) {
	switch a.Kind {
	case ArgExpr:
		if a.Expr == nil {
			return "", fmt.Errorf("empty expression")
		}
		if a.Expr.Kind == ExprField {
			return resolveFieldRef(a.Expr.Value, registry), nil
		}
		sql, _, err := compileExpr(a.Expr, registry, "")
		return sql, err
	case ArgLiteral, ArgRegex:
		return resolveFieldRef(a.Text, registry), nil
	}
	return "", fmt.Errorf("%s is not a field", a.String())
}

// ArgAlias is the output column an argument projects under: its own name when it
// is a bare field, a derived name when it is an expression.
func ArgAlias(a Argument) (string, error) {
	if a.IsFieldName() {
		return sanitizeIdentifier(a.Expr.Value)
	}
	if a.Kind == ArgLiteral || a.Kind == ArgRegex {
		return sanitizeIdentifier(a.Text)
	}
	return sanitizeIdentifier(exprArgAlias(a.Value()))
}

// argList renders arguments back to BQL for an error message.
func argList(args []Argument) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = a.String()
	}
	return strings.Join(parts, ", ")
}
