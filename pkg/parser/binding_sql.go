package parser

import (
	"errors"
	"fmt"
	"strings"
)

// A binding that names a set of rows becomes a subquery: an IN test, or the body
// of a join(). The query it holds is translated in the same scope as the query
// around it, and materialised once when more than one place reads it.

// bindingSetMaxRows caps the rows a result-set binding contributes to an IN
// list. IN materialises the whole set in memory, so it is bounded like join().
const bindingSetMaxRows = 50000

// bindingSubquerySQL renders a result-set binding as a single-column subquery for
// an IN test. The column is the one named after the field being tested, matching
// how a join() block names its key; a binding that returns exactly one column
// needs no name at all.
func bindingSubquerySQL(b *BindingNode, ctx *CommandContext, field string) (string, error) {
	if b == nil || b.Pipe == nil {
		return "", fmt.Errorf("%s is not a result set", bindingName(b))
	}
	if err := chargeBindingWork(&ctx.Opts); err != nil {
		return "", err
	}
	result, err := TranslateToSQLWithOrder(b.Pipe, subqueryOptions(ctx, bindingSetMaxRows))
	if err != nil {
		// The budget error names the whole query's problem, not this binding's, so
		// it travels unwrapped rather than accreting a prefix per level.
		if errors.Is(err, errBindingWorkExhausted) {
			return "", err
		}
		return "", fmt.Errorf("%s: %w", b.Name, err)
	}
	ctx.Plan.usesBindingSet = true
	column, err := bindingSetColumn(b, result.FieldOrder, field)
	if err != nil {
		return "", err
	}
	if b.SetRefs > 1 {
		name, err := bindingCTEName(b.Name)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("SELECT %s FROM %s", column, ctx.Plan.addBindingCTE(name, result.SQL)), nil
	}
	return fmt.Sprintf("SELECT %s FROM (%s)", column, result.SQL), nil
}

func bindingSetColumn(b *BindingNode, outputs []string, field string) (string, error) {
	if field != "" && contains(outputs, field) {
		return field, nil
	}
	if len(outputs) == 1 {
		return outputs[0], nil
	}
	if len(outputs) == 0 {
		return "", fmt.Errorf("%s returns no columns", b.Name)
	}
	return "", fmt.Errorf("%s returns [%s]; name the column to test by making the binding return only it, or test a field one of them is named after",
		b.Name, strings.Join(outputs, ", "))
}

// bindingCTEName is the SQL identifier a materialised binding is read by. The
// _b_ prefix keeps it clear of the _dfr_, _join_ and _mlk_ columns the translator
// generates, and of any log field, which cannot start with an underscore here.
func bindingCTEName(name string) (string, error) {
	return sanitizeIdentifier("_b_" + strings.TrimPrefix(name, "&"))
}

func bindingName(b *BindingNode) string {
	if b == nil {
		return "the binding"
	}
	return b.Name
}

// bindingWorkBudget bounds how many result-set bindings one query may translate.
// Far beyond any query a person writes, and low enough that the worst case stays
// in milliseconds.
const bindingWorkBudget = 200

var errBindingWorkExhausted = errors.New("too many result-set bindings to resolve; a binding that another reads more than once multiplies the work at every level")

// chargeBindingWork debits one translation from the query's allowance, starting
// it on first use so a query without bindings pays nothing.
func chargeBindingWork(opts *QueryOptions) error {
	if opts.bindingWork == nil {
		n := bindingWorkBudget
		opts.bindingWork = &n
	}
	if *opts.bindingWork <= 0 {
		return errBindingWorkExhausted
	}
	*opts.bindingWork--
	return nil
}
