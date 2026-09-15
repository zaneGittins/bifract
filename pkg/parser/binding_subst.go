package parser

import (
	"fmt"
	"strings"
)

// Bindings are substituted into the AST as they are parsed, so everything
// downstream sees the query the author would have written by hand. A binding may
// reference one declared before it; referencing itself or a later one cannot
// resolve, which is what stops a cycle.

// substitutePipeline resolves every binding reference left in the tree after the
// pipeline is parsed. Filter leaves and expression stage heads substitute as they
// are read; this covers the rest, chiefly command arguments and := assignments.
// Anything it misses is caught at compile time, where an unresolved & is an
// error rather than a JSON field that matches nothing.
func (p *Parser) substitutePipeline(pipeline *PipelineNode) error {
	if len(p.bindings) == 0 || pipeline == nil {
		return nil
	}
	if pipeline.Filter != nil {
		for i := range pipeline.Filter.Conditions {
			if err := p.substituteFilterCondition(&pipeline.Filter.Conditions[i]); err != nil {
				return err
			}
		}
	}
	for i := range pipeline.Assignments {
		if err := p.substituteExpr(pipeline.Assignments[i].Expr); err != nil {
			return err
		}
	}
	for i := range pipeline.HavingConditions {
		if err := p.substituteHavingCondition(&pipeline.HavingConditions[i]); err != nil {
			return err
		}
	}
	for i := range pipeline.Commands {
		if err := p.substituteCommand(&pipeline.Commands[i]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) substituteCommand(cmd *CommandNode) error {
	for i := range cmd.Args {
		if err := p.substituteArg(&cmd.Args[i], cmd.Name, true); err != nil {
			return err
		}
	}
	return nil
}

// substituteArg resolves the bindings one argument names. whole says the argument
// is the command's own, not a member of a list: a result set is a whole argument
// or nothing, since a list of them has no meaning.
func (p *Parser) substituteArg(a *Argument, cmdName string, whole bool) error {
	if name, ok := bareBindingRef(a); ok {
		b, err := p.lookupBinding(Token{Value: name, Pos: a.Pos})
		if err != nil {
			return err
		}
		if b.Kind == BindingPipeline {
			switch {
			case !whole:
				return fmt.Errorf("%s(): %s is a result set and cannot be a member of a list", cmdName, b.Name)
			case !strings.EqualFold(cmdName, "in"):
				return fmt.Errorf("%s(): %s is a result set; only in() and a join() block take one", cmdName, b.Name)
			}
			a.Kind, a.Binding, a.Expr = ArgBinding, b, nil
			b.SetRefs++
			return nil
		}
		// A literal becomes a literal argument, text and all. Substituting only the
		// expression left Text reading "&name", and a handler that takes its value
		// from the text (in(), and every list parameter) compiled the sigil into
		// the query as if it were the value.
		if b.Kind == BindingValue && b.Expr != nil {
			switch b.Expr.Kind {
			case ExprString, ExprNumber, ExprBoolean:
				a.Kind, a.Expr, a.List, a.Agg = ArgLiteral, nil, nil, nil
				a.Text, a.Quoted = b.Expr.Value, b.Expr.Kind == ExprString
				return nil
			}
		}
	}
	if err := p.substituteExpr(a.Expr); err != nil {
		return err
	}
	for i := range a.List {
		if err := p.substituteArg(&a.List[i], cmdName, false); err != nil {
			return err
		}
	}
	if a.Agg != nil {
		for i := range a.Agg.Args {
			if err := p.substituteArg(&a.Agg.Args[i], cmdName, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// bareBindingRef reports the binding an argument names when the argument is
// nothing but that reference, as in in(user, &admins).
func bareBindingRef(a *Argument) (string, bool) {
	if a.Kind != ArgExpr || a.Expr == nil || a.Expr.Kind != ExprField {
		return "", false
	}
	if !strings.HasPrefix(a.Expr.Value, "&") {
		return "", false
	}
	return a.Expr.Value, true
}

// substituteExpr replaces every binding reference in an expression with a copy of
// what it names.
func (p *Parser) substituteExpr(e *ExprNode) error {
	if e == nil {
		return nil
	}
	switch e.Kind {
	case ExprField:
		if !strings.HasPrefix(e.Value, "&") {
			return nil
		}
		b, err := p.lookupBinding(Token{Value: e.Value, Pos: e.Pos})
		if err != nil {
			return err
		}
		if b.Kind != BindingValue {
			return fmt.Errorf("%s is %s, not a value: it cannot be used inside an expression", b.Name, b.Kind.describe())
		}
		clone, err := cloneExpr(b.Expr, p.budget())
		if err != nil {
			return err
		}
		*e = *clone
		return nil
	case ExprUnary:
		return p.substituteExpr(e.Arg)
	case ExprBinary:
		if err := p.substituteExpr(e.Left); err != nil {
			return err
		}
		return p.substituteExpr(e.Right)
	case ExprCall:
		for _, a := range e.Args {
			if err := p.substituteExpr(a); err != nil {
				return err
			}
		}
		for _, a := range e.Named {
			if err := p.substituteExpr(a); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Parser) substituteHavingCondition(c *HavingCondition) error {
	if err := p.substituteExpr(c.Expr); err != nil {
		return err
	}
	for i := range c.Children {
		if err := p.substituteHavingCondition(&c.Children[i]); err != nil {
			return err
		}
	}
	if c.Command != nil {
		return p.substituteCommand(c.Command)
	}
	return nil
}

func (p *Parser) substituteFilterCondition(c *ConditionNode) error {
	if err := p.substituteExpr(c.Expr); err != nil {
		return err
	}
	for i := range c.Children {
		if err := p.substituteFilterCondition(&c.Children[i]); err != nil {
			return err
		}
	}
	if c.Command != nil {
		return p.substituteCommand(c.Command)
	}
	return nil
}

// bindingNodeBudget bounds how much expression a query's bindings may expand to.
// Each reference is a copy, so bindings that reference each other multiply:
// twenty-four lines of `let &b(n) = &b(n-1) + &b(n-1);` is a 500-byte query that
// expands to sixteen million nodes. The budget makes that an error rather than a
// server that stops answering. Generous next to any hand-written expression.
const bindingNodeBudget = 50000

var errBindingTooLarge = fmt.Errorf("the bindings expand to too large an expression; simplify one that references another more than once")

// cloneExpr deep-copies an expression so one binding used twice never shares a
// node with itself, charging every node to the query's budget.
func cloneExpr(e *ExprNode, budget *int) (*ExprNode, error) {
	if e == nil {
		return nil, nil
	}
	if *budget <= 0 {
		return nil, errBindingTooLarge
	}
	*budget--
	out := *e
	var err error
	if out.Left, err = cloneExpr(e.Left, budget); err != nil {
		return nil, err
	}
	if out.Right, err = cloneExpr(e.Right, budget); err != nil {
		return nil, err
	}
	if out.Arg, err = cloneExpr(e.Arg, budget); err != nil {
		return nil, err
	}
	if e.Args != nil {
		out.Args = make([]*ExprNode, len(e.Args))
		for i, a := range e.Args {
			if out.Args[i], err = cloneExpr(a, budget); err != nil {
				return nil, err
			}
		}
	}
	if e.Named != nil {
		out.Named = make(map[string]*ExprNode, len(e.Named))
		for k, v := range e.Named {
			if out.Named[k], err = cloneExpr(v, budget); err != nil {
				return nil, err
			}
		}
	}
	return &out, nil
}

// budget returns the query's remaining expansion allowance, starting it on first
// use so a query with no bindings pays nothing.
func (p *Parser) budget() *int {
	if p.bindingBudget == nil {
		n := bindingNodeBudget
		p.bindingBudget = &n
	}
	return p.bindingBudget
}

func cloneCondition(c *HavingCondition, budget *int) (*HavingCondition, error) {
	if c == nil {
		return nil, nil
	}
	out := *c
	var err error
	if out.Expr, err = cloneExpr(c.Expr, budget); err != nil {
		return nil, err
	}
	if c.Values != nil {
		out.Values = append([]string(nil), c.Values...)
	}
	if c.Children != nil {
		out.Children = make([]HavingCondition, len(c.Children))
		for i := range c.Children {
			child, err := cloneCondition(&c.Children[i], budget)
			if err != nil {
				return nil, err
			}
			out.Children[i] = *child
		}
	}
	return &out, nil
}
