package parser

import (
	"fmt"
	"sort"
	"strings"
)

// A command's arguments are described once, declaratively, instead of being
// re-parsed by hand in each handler. One binder then validates every command the
// same way, so arity, unknown-parameter and missing-argument errors are uniform
// and cannot drift from what the handler actually accepts.
//
// This mirrors exprFunc/bindArgs, which does the same for scalar functions and
// is the half of the parser that two reviews found no defects in.

// ParamKind is what an argument position accepts.
type ParamKind int

const (
	// ParamField is a field name or an expression yielding a value.
	ParamField ParamKind = iota
	// ParamLiteral is a quoted string, a number, or a bare keyword (desc, true).
	ParamLiteral
	// ParamList is a bracket list or a comma-separated list.
	ParamList
	// ParamAggSpec is an aggregate specification: count(), sum(x), multi(...).
	ParamAggSpec
	// ParamBlock is a brace-delimited sub-pipeline.
	ParamBlock
	// ParamAny accepts whatever the handler chooses to make of it.
	ParamAny
)

func (k ParamKind) String() string {
	switch k {
	case ParamField:
		return "field"
	case ParamLiteral:
		return "literal"
	case ParamList:
		return "list"
	case ParamAggSpec:
		return "aggregate"
	case ParamBlock:
		return "block"
	}
	return "any"
}

// ParamSpec is one parameter of a command.
type ParamSpec struct {
	Name     string
	Kind     ParamKind
	Required bool
	// Variadic absorbs the remaining positional arguments. Only valid last.
	Variadic bool
	// Positional marks a parameter that may be given without its name. A
	// parameter that is not positional must always be written name=value.
	Positional bool
}

// CommandSpec declares a command's argument surface.
type CommandSpec struct {
	Name   string
	Params []ParamSpec
	// FreeForm marks a command whose arguments cannot be described positionally,
	// because the handler interprets raw text (a case block, a chain pattern).
	// Validation is limited to the named parameters the spec does declare.
	FreeForm bool
}

var commandSpecs = map[string]*CommandSpec{}

// registerSpec records the argument schema for one or more command names.
func registerSpec(spec *CommandSpec, names ...string) {
	if len(names) == 0 {
		names = []string{spec.Name}
	}
	for _, n := range names {
		commandSpecs[strings.ToLower(n)] = spec
	}
}

// CommandSpecFor returns the schema for a command name, if one is registered.
func CommandSpecFor(name string) (*CommandSpec, bool) {
	s, ok := commandSpecs[strings.ToLower(name)]
	return s, ok
}

// CommandSpecNames returns every name with a registered schema.
func CommandSpecNames() []string {
	names := make([]string, 0, len(commandSpecs))
	for n := range commandSpecs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (s *CommandSpec) positionalParams() []ParamSpec {
	var out []ParamSpec
	for _, p := range s.Params {
		if p.Positional {
			out = append(out, p)
		}
	}
	return out
}

func (s *CommandSpec) lookup(name string) (ParamSpec, bool) {
	for _, p := range s.Params {
		if strings.EqualFold(p.Name, name) {
			return p, true
		}
	}
	return ParamSpec{}, false
}

// paramList renders the accepted parameters with their kinds, so an error tells
// the author what to write, not just that what they wrote was wrong.
func (s *CommandSpec) paramList() string {
	if len(s.Params) == 0 {
		return "no parameters"
	}
	out := make([]string, len(s.Params))
	for i, p := range s.Params {
		out[i] = p.Name + " (" + p.Kind.String() + ")"
	}
	return strings.Join(out, ", ")
}

// HasVariadicParam reports whether the command takes an open-ended positional
// list, which the reference names loosely (fields, field1, ...).
func (s *CommandSpec) HasVariadicParam() bool {
	for _, p := range s.Params {
		if p.Variadic {
			return true
		}
	}
	return false
}

// ValidateCommandArgs checks a command's arguments against its schema: every
// named argument must be a declared parameter, every required parameter must be
// present, and a command with no variadic tail must not be given more positional
// arguments than it declares.
//
// The first of those is enforced while parsing, which is where the position is
// known; this reports the arity the parser cannot judge on its own. An argument
// dropped without comment is how `include=a,b` lost its second column.
func ValidateCommandArgs(cmd CommandNode) error {
	spec, ok := CommandSpecFor(cmd.Name)
	if !ok {
		return nil
	}
	if cmd.ArgsErr != nil {
		return cmd.ArgsErr
	}
	// A free-form command's block is text the handler parses, so the spec names
	// its parameters for the reference without modelling their arity.
	if spec.FreeForm {
		return nil
	}

	b, err := Bind(spec, cmd.Args)
	if err != nil {
		return err
	}
	for _, p := range spec.Params {
		if !p.Variadic && len(b.All(p.Name)) > 1 {
			return fmt.Errorf("%s(): %s given more than once", cmd.Name, strings.ToLower(p.Name))
		}
	}
	if len(b.extra) > 0 {
		return fmt.Errorf("%s(): %s fills no remaining parameter (accepts %s)",
			cmd.Name, b.extra[0], spec.paramList())
	}
	for _, p := range spec.Params {
		if !p.Required || b.Has(p.Name) {
			continue
		}
		return fmt.Errorf("%s(): missing required argument %s", cmd.Name, p.Name)
	}
	return nil
}

// validateCommandSchemas checks every command in a pipeline against its schema.
func validateCommandSchemas(pipeline *PipelineNode) error {
	var err error
	ForEachCommand(pipeline, func(cmd CommandNode) {
		if err == nil {
			err = ValidateCommandArgs(cmd)
		}
	})
	return err
}

// field, lit, list and friends keep the spec declarations readable.
func field(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamField, Positional: true}
}

func reqField(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamField, Positional: true, Required: true}
}

func fields(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamField, Positional: true, Variadic: true}
}

func lit(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamLiteral, Positional: true}
}

func reqLit(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamLiteral, Positional: true, Required: true}
}

func namedLit(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamLiteral}
}

func namedField(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamField}
}

func namedList(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamList}
}

func namedAgg(name string) ParamSpec {
	return ParamSpec{Name: name, Kind: ParamAggSpec}
}

// as is the output-column parameter almost every projecting command accepts.
func as() ParamSpec {
	return ParamSpec{Name: "as", Kind: ParamLiteral}
}

// positionalCursor hands out positional parameters in order, skipping any that a
// named argument already filled. A parameter may be written either way, so
// sort(field=bytes, desc) has to mean the same as sort(bytes, desc): the bare
// value fills the next parameter still open, not the one already given.
type positionalCursor struct {
	params []ParamSpec
	filled map[string]bool
	next   int
}

func newPositionalCursor(spec *CommandSpec) *positionalCursor {
	return &positionalCursor{params: spec.positionalParams(), filled: map[string]bool{}}
}

// fill marks a parameter as given by name.
func (c *positionalCursor) fill(name string) {
	c.filled[strings.ToLower(name)] = true
}

// take returns the parameter the next bare argument fills. A variadic tail stays
// open and absorbs the rest.
func (c *positionalCursor) take() (ParamSpec, bool) {
	for c.next < len(c.params) {
		p := c.params[c.next]
		if c.filled[strings.ToLower(p.Name)] {
			c.next++
			continue
		}
		if !p.Variadic {
			c.next++
			c.filled[strings.ToLower(p.Name)] = true
		}
		return p, true
	}
	return ParamSpec{}, false
}
