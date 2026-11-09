package parser

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// inHandler handles in(field, values=[v1,v2,...])
type inHandler struct{ rowFilter }

func (h *inHandler) predicate(cmd CommandNode, ctx *CommandContext) (predicate, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return predicate{}, err
	}
	fieldArg, ok := b.First("field")
	if !ok {
		return predicate{}, fmt.Errorf("in() needs a field and values, e.g. in(user, values=[\"a\"])")
	}

	field := fieldArg.FieldName()
	var fieldRef string
	switch {
	case field == "":
		if fieldRef, err = ResolveArg(fieldArg, ctx.Registry); err != nil {
			return predicate{}, fmt.Errorf("in(): %w", err)
		}
	case ctx.Registry.IsComputed(field):
		// The expression, not the alias: an alias names the field's final value.
		fieldRef = ctx.Registry.Resolve(field)
	case field == "timestamp" || field == normLogColumn || field == "log_id" || field == "normalizer":
		fieldRef = field
	default:
		fieldRef = jsonFieldRef(field)
	}

	if set, ok := b.First("values"); ok && set.Kind == ArgBinding {
		sub, err := bindingSubquerySQL(set.Binding, ctx, field)
		if err != nil {
			if errors.Is(err, errBindingWorkExhausted) {
				return predicate{}, err
			}
			return predicate{}, fmt.Errorf("in(): %w", err)
		}
		return atom(fmt.Sprintf("%s %s (%s)", fieldRef, ctx.Opts.subqueryIn(cmd.Negate), sub)), nil
	}

	for _, a := range b.All("values") {
		for _, e := range a.List {
			if e.Kind == ArgList {
				return predicate{}, fmt.Errorf("in(): values cannot be nested lists")
			}
		}
	}
	var values []string
	for _, v := range b.CSV("values") {
		values = append(values, fmt.Sprintf("'%s'", escapeString(v)))
	}
	// No values would add no condition, so the filter would match every event.
	if len(values) == 0 {
		return predicate{}, fmt.Errorf("in() needs at least one value")
	}
	op := "IN"
	if cmd.Negate {
		op = "NOT IN"
	}
	return atom(fmt.Sprintf("%s %s (%s)", fieldRef, op, strings.Join(values, ", "))), nil
}

func (h *inHandler) appliesNegate() {}

// cidrHandler handles cidr(field, "range") - filter by CIDR range
type cidrHandler struct{ rowFilter }

func (h *cidrHandler) appliesNegate() {}

func (h *cidrHandler) predicate(cmd CommandNode, ctx *CommandContext) (predicate, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return predicate{}, err
	}
	fieldArg, hasField := b.First("field")
	rangeArg, hasRange := b.First("range")
	if !hasField || !hasRange {
		return predicate{}, fmt.Errorf("cidr() needs a field and a range, e.g. cidr(src_ip, \"10.0.0.0/8\")")
	}
	cidrRange := rangeArg.Value()
	if err := validateCIDR(cidrRange); err != nil {
		return predicate{}, fmt.Errorf("cidr(): %w", err)
	}
	fieldRef, err := ResolveArg(fieldArg, ctx.Registry)
	if err != nil {
		return predicate{}, fmt.Errorf("cidr(): %w", err)
	}
	cidrExpr := cidrPredicateSQL(fieldRef, "'"+escapeString(cidrRange)+"'")
	if cmd.Negate {
		cidrExpr = "NOT " + cidrExpr
	}
	return atom(cidrExpr), nil
}

// validateCIDR rejects a range ClickHouse cannot parse. isIPAddressInRange
// throws on anything without a prefix length, so an empty or malformed range
// failed at the server with "The text does not contain '/'".
func validateCIDR(s string) error {
	if s == "" {
		return fmt.Errorf("needs a CIDR range, e.g. \"10.0.0.0/8\"")
	}
	if _, _, err := net.ParseCIDR(s); err != nil {
		return fmt.Errorf("%q is not a CIDR range; it needs an address and a prefix length, e.g. \"10.0.0.0/8\"", s)
	}
	return nil
}

// cidrPredicateSQL builds the range test shared by the cidr() command and the
// cidr() expression function. quotedRange is already SQL-quoted.
//
// isIPAddressInRange throws CANNOT_PARSE_TEXT on any value that is not a valid IP
// literal. Type-hinted JSON fields default missing values to ”, and some sources
// store non-IP values, so a single bad row would abort the whole query. Guard both
// ends without relying on short-circuit evaluation: feed the function a real
// address only (sentinel otherwise), and AND the validity check so non-IP rows are
// always "not in range", correct for every CIDR including 0.0.0.0/0.
// isIPv4String/isIPv6String accept any string without throwing.
func cidrPredicateSQL(fieldRef, quotedRange string) string {
	valid := fmt.Sprintf("(isIPv4String(%[1]s) OR isIPv6String(%[1]s))", fieldRef)
	safeAddr := fmt.Sprintf("if(%s, %s, '0.0.0.0')", valid, fieldRef)
	return fmt.Sprintf("(isIPAddressInRange(%s, %s) AND %s)", safeAddr, quotedRange, valid)
}

// commentHandler handles comment(): the logs the server found matching comments
// for, resolved before translation.
type commentHandler struct{ rowFilter }

func (h *commentHandler) readsLogRows() {}

func (h *commentHandler) predicate(cmd CommandNode, ctx *CommandContext) (predicate, error) {
	if !ctx.Opts.HasCommentFilter {
		return predicate{}, fmt.Errorf("comment() requires server-side pre-processing")
	}
	if len(ctx.Opts.CommentLogIDs) == 0 {
		return atom("1 = 0"), nil
	}
	quoted := make([]string, len(ctx.Opts.CommentLogIDs))
	for i, id := range ctx.Opts.CommentLogIDs {
		quoted[i] = fmt.Sprintf("'%s'", escapeString(id))
	}
	return atom(fmt.Sprintf("log_id IN (%s)", strings.Join(quoted, ", "))), nil
}

func init() {
	registerCommand(&inHandler{}, "in")
	registerCommand(&cidrHandler{}, "cidr")
	registerCommand(&commentHandler{}, "comment")
	registerCommand(&commentHandler{}, "comments")
}

func init() {
	registerSpec(&CommandSpec{Name: "in", Params: []ParamSpec{
		reqField("field"),
		ParamSpec{Name: "values", Kind: ParamList, Positional: true, Required: true},
	}})
	registerSpec(&CommandSpec{Name: "cidr", Params: []ParamSpec{reqField("field"), reqLit("range")}})
}
