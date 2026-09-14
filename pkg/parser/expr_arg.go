package parser

import (
	"fmt"
	"strings"
)

// Expressions in command arguments: groupby(lower(user)), sort(len(commandline)),
// table(user, round(bytes, 2)), sum(bytes * 8).
//
// The schema says which positions hold an expression, so each one is parsed as
// an expression up front and reaches the handler as an ExprNode. What is left
// here is the derived output name and the pass that compiles every expression
// before Execute, where an error can still be reported.

// exprArgAlias derives the output column name for an expression argument, so
// `groupby(lower(user))` produces a column called lower_user. Callers that accept
// as= should prefer what the author wrote.
func exprArgAlias(arg string) string {
	var b strings.Builder
	prevUnderscore := false
	for _, r := range arg {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevUnderscore = false
		default:
			if !prevUnderscore && b.Len() > 0 {
				b.WriteRune('_')
				prevUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

// validateExprArgs rejects a command argument that is written as an expression
// but cannot be compiled. It runs before Execute so a handler that resolves an
// argument without returning an error cannot turn a broken expression into a
// field reference that matches nothing.
func validateExprArgs(pipeline *PipelineNode, registry *FieldRegistry) error {
	var err error
	ForEachCommand(pipeline, func(cmd CommandNode) {
		if err != nil {
			return
		}
		if e := validateArgExprs(cmd.Args, registry); e != nil {
			err = fmt.Errorf("%s(): %w", cmd.Name, e)
		}
	})
	return err
}

// validateArgExprs compiles every expression a command's arguments hold. An
// aggregate specification is the owning handler's to name-check, since it
// reports an unknown one with aggregate-specific advice; its arguments are field
// positions and are checked here.
func validateArgExprs(args []Argument, registry *FieldRegistry) error {
	for _, a := range args {
		switch a.Kind {
		case ArgExpr:
			if a.Expr == nil || a.Expr.Kind == ExprField {
				continue
			}
			if a.Expr.Kind == ExprCall && aggSpecNames[strings.ToLower(a.Expr.Value)] {
				if spec, ok := asAggSpec(a); ok {
					if err := validateArgExprs(spec.Args, registry); err != nil {
						return err
					}
				}
				continue
			}
			if _, _, err := compileExpr(a.Expr, registry, ""); err != nil {
				return err
			}
		case ArgList:
			if err := validateArgExprs(a.List, registry); err != nil {
				return err
			}
		case ArgAggSpec:
			if a.Agg != nil {
				if err := validateArgExprs(a.Agg.Args, registry); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// firstFieldArg binds a command and returns the argument given for param. ok is
// false when the parameter was not given.
func firstFieldArg(cmd CommandNode, param string) (*Bound, Argument, bool, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return nil, Argument{}, false, err
	}
	a, ok := b.First(param)
	return b, a, ok, nil
}

// aggOutputAlias is the column an aggregate projects when the author did not
// name one: a fixed prefix plus the operand's name. The whole identifier is
// validated here, since it reaches SQL unquoted and a field name is user input:
// stddev("x, 1 AS y") would otherwise add a column of its own.
func aggOutputAlias(prefix string, arg Argument) (string, error) {
	part, err := aggAliasPart(arg)
	if err != nil {
		return "", err
	}
	return sanitizeIdentifier(prefix + part)
}

// aggAliasPart is the text an aggregate embeds in its output alias: the field's
// own name, or a derived name when the argument computes its value.
func aggAliasPart(arg Argument) (string, error) {
	if f := arg.FieldName(); f != "" {
		return f, nil
	}
	alias, err := ArgAlias(arg)
	return strings.Trim(alias, "`"), err
}
