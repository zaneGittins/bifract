package parser

import "fmt"

// condOperand is one side of a comparison: the SQL reference plus the traits
// that decide how it has to be coerced before the two sides can be compared.
type condOperand struct {
	ref  string
	form refForm
	// isJSON marks a raw JSON sub-column, which reads as NULL when the field is
	// absent from the event.
	isJSON bool
	// computed marks a reference whose ClickHouse type is not String (a pipeline
	// output), so text comparison must go through toString().
	computed bool
	// dateTime marks a DateTime column. Neither toString() nor textToNumber()
	// alone compares two of them meaningfully, so each gets its own coercion.
	dateTime bool
}

// text renders the operand for a string comparison.
func (o condOperand) text() string {
	if o.dateTime || o.computed {
		return "toString(" + o.ref + ")"
	}
	return o.ref
}

// num renders the operand for an ordering comparison. A DateTime becomes epoch
// milliseconds, which orders correctly against another DateTime.
func (o condOperand) num() string {
	if o.dateTime {
		return "toUnixTimestamp64Milli(" + o.ref + ")"
	}
	if o.computed {
		return textToNumber("toString(" + o.ref + ")")
	}
	return textToNumber(o.ref)
}

// textToNumber reads text for an ordering comparison. A non-number becomes NaN,
// which every comparison rejects. OrZero would match it as 0; OrNull makes the
// condition Nullable, and sequenceMatch then skips the row for every chain step.
func textToNumber(ref string) string {
	return "toFloat64OrDefault(" + ref + ", nan)"
}

// dateTimeColumns are base columns typed DateTime rather than String. ClickHouse
// refuses to compare one with a String (code 386) and refuses textToNumber on
// it (code 43), so both sides of a comparison have to know.
func isDateTimeColumn(field string) bool {
	return field == "timestamp" || field == "ingest_timestamp"
}

// lhsOperand describes an already-resolved left-hand side. A DateTime name that
// still resolved to a JSON sub-column is a String, not the base column, so the
// trait follows the reference rather than the name.
func lhsOperand(field, ref string, isJSON, computed bool) condOperand {
	return condOperand{ref: ref, isJSON: isJSON, computed: computed, dateTime: !isJSON && !computed && isDateTimeColumn(field)}
}

// resolveValueField resolves the right-hand side of `lhs op field(name)` by the
// same rules the left-hand side uses: a pipeline-produced column is referenced
// by its alias or inlined expression, anything else is a log field.
func resolveValueField(name string, registry *FieldRegistry) condOperand {
	at := func(r sqlRef) condOperand { return condOperand{ref: r.sql, form: r.form} }
	// A grouping key is addressable after the aggregation only by its alias; the
	// sub-column it resolves from is not in the GROUP BY (ClickHouse code 215).
	if alias, ok := registry.GroupKeyAlias(name); ok {
		return at(registry.Column(alias))
	}
	if entry := registry.Get(name); entry != nil {
		switch entry.Kind {
		case FieldKindPerRow, FieldKindAssignment:
			o := at(registry.Ref(name))
			o.computed = true
			return o
		case FieldKindBase:
			// NewFieldRegistry pre-registers timestamp, log_id and friends as
			// Base, so this branch is what a base column actually takes.
			o := at(registry.Column(name))
			o.dateTime = isDateTimeColumn(name)
			return o
		case FieldKindJSON:
			// Falls through to the sub-column reference below.
		default:
			o := at(registry.Column(entry.Name))
			o.computed = true
			return o
		}
	}
	switch name {
	case normLogColumn, "timestamp", "ingest_timestamp", "log_id":
		o := at(registry.Column(name))
		o.dateTime = isDateTimeColumn(name)
		return o
	case "normalizer", "_normalizer":
		return at(registry.Column("normalizer"))
	}
	// Bare aggregate names resolve to their numeric aliases, matching the
	// left-hand side; without this they would read as a JSON field named "count".
	if registry.bareAggregate(name) {
		o := at(registry.Column("_" + name))
		o.computed = true
		return o
	}
	o := at(registry.Ref(name))
	o.isJSON = true
	return o
}

// buildFieldComparisonSQL renders `lhs op field(rhs)`. Equality compares the two
// as text, matching what equality against a literal does; the ordering operators
// coerce both sides to Float64, as they do for a numeric literal. A side that is
// absent from the event reads as NULL, so equality never matches it and
// inequality always does, which is how != against a literal already behaves.
func buildFieldComparisonSQL(lhs, rhs condOperand, operator string) string {
	switch operator {
	case "=":
		return fmt.Sprintf("%s = %s", lhs.text(), rhs.text())
	case "!=":
		var guards string
		if lhs.isJSON {
			guards += lhs.ref + " IS NULL OR "
		}
		if rhs.isJSON {
			guards += rhs.ref + " IS NULL OR "
		}
		if guards != "" {
			return fmt.Sprintf("(%s%s != %s)", guards, lhs.text(), rhs.text())
		}
		return fmt.Sprintf("%s != %s", lhs.text(), rhs.text())
	case ">", "<", ">=", "<=":
		return fmt.Sprintf("%s %s %s", lhs.num(), operator, rhs.num())
	}
	return ""
}
