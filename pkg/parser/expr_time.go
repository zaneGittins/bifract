package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Times and durations in expressions. A time is a DateTime or DateTime64: the
// timestamp columns, now(), bin() or a time computed from one. A duration is
// written 30s, 5m, 1h, 7d or 2w and is a number of seconds in SQL, so once stored
// it reads anywhere a number does.
//
// The operators follow what the values mean: time - time is a duration, time and
// duration add to a time, durations scale by numbers, and a comparison needs two
// of a kind. A bare number next to a time or a duration is an error, since 30
// could be seconds or minutes. A stored field next to a time is read as a time,
// the way dateDiff() reads one.

const durationHelp = "durations are a whole number and a unit: s, m, h, d or w, such as 90s, 5m, 1h, 7d or 2w"

// durationUnits are the seconds each unit holds, matching timechart's span=.
var durationUnits = map[byte]int64{'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}

func isDurationUnit(b byte) bool {
	_, ok := durationUnits[b]
	return ok
}

// isDurationLiteral reports whether s is written as a duration.
func isDurationLiteral(s string) bool {
	_, err := durationSeconds(s)
	return err == nil
}

// durationSeconds is the seconds a duration literal holds.
func durationSeconds(s string) (int64, error) {
	if len(s) < 2 || !isDurationUnit(s[len(s)-1]) {
		return 0, fmt.Errorf("%q: %s", s, durationHelp)
	}
	digits := s[:len(s)-1]
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%q: %s", s, durationHelp)
		}
	}
	// 32 bits of weeks still fits in int64 milliseconds.
	n, err := strconv.ParseInt(digits, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%q: %s", s, durationHelp)
	}
	return n * durationUnits[s[len(s)-1]], nil
}

// durationSpan splits a duration literal into the count and unit bucket SQL takes.
func durationSpan(s string) (int, string) {
	n, _ := strconv.Atoi(s[:len(s)-1])
	unit := map[byte]string{'s': "SECOND", 'm': "MINUTE", 'h': "HOUR", 'd': "DAY", 'w': "WEEK"}[s[len(s)-1]]
	return n, unit
}

func isTimeType(t ExprType) bool { return t == TypeTime || t == TypeDuration }

// operand is one compiled side of a binary expression.
type operand struct {
	sql  string
	typ  ExprType
	node *ExprNode
}

// readsAsTime reports whether a side next to a time is read as one: text, a stored
// field or a field of unknown type.
func readsAsTime(t ExprType) bool {
	return t == TypeText || t == TypeAny || t == TypeString
}

// unitless reports whether the node is a bare number, which has no unit.
func unitless(e *ExprNode) bool {
	if e.Kind == ExprUnary && e.Value == "-" {
		e = e.Arg
	}
	return e.Kind == ExprNumber
}

func unitError(e *ExprNode) error {
	return fmt.Errorf("write %s with a unit, such as %ss or %sm: a bare number next to a time or a duration has no unit", e, e, e)
}

// compileTimeBinary compiles an operator with a time or a duration on either side.
// ok is false for an operator it leaves to the caller (AND, OR).
func compileTimeBinary(e *ExprNode, l, r operand, join func(l, r, op string) string) (string, ExprType, bool, error) {
	var sql string
	var typ ExprType
	var err error
	switch e.Value {
	case "+", "-":
		sql, typ, err = timeSum(e, l, r, join)
	case "*", "/":
		sql, typ, err = durationScale(e, l, r, join)
	case "=", "!=", ">", "<", ">=", "<=":
		sql, typ, err = timeCompare(e, l, r, join)
	default:
		return "", TypeAny, false, nil
	}
	return sql, typ, true, err
}

func timeSum(e *ExprNode, l, r operand, join func(l, r, op string) string) (string, ExprType, error) {
	switch {
	case l.typ == TypeTime && (r.typ == TypeTime || readsAsTime(r.typ)),
		r.typ == TypeTime && readsAsTime(l.typ):
		if e.Value == "+" {
			return "", TypeAny, fmt.Errorf("%s: cannot add two times; subtract them for the duration between", e)
		}
		a, err := asTime(l.sql, l.typ, l.node)
		if err != nil {
			return "", TypeAny, err
		}
		b, err := asTime(r.sql, r.typ, r.node)
		if err != nil {
			return "", TypeAny, err
		}
		return fmt.Sprintf("dateDiff('millisecond', %s, %s) / 1000", b, a), TypeDuration, nil
	case l.typ == TypeTime:
		t, err := asTime(l.sql, l.typ, l.node)
		if err != nil {
			return "", TypeAny, err
		}
		return shiftTime(t, e.Value, r)
	case r.typ == TypeTime:
		if e.Value == "-" {
			return "", TypeAny, fmt.Errorf("%s: cannot subtract a time from a duration", e)
		}
		t, err := asTime(r.sql, r.typ, r.node)
		if err != nil {
			return "", TypeAny, err
		}
		return shiftTime(t, "+", l)
	}
	a, err := asSeconds(l)
	if err != nil {
		return "", TypeAny, err
	}
	b, err := asSeconds(r)
	if err != nil {
		return "", TypeAny, err
	}
	return join(a, b, e.Value), TypeDuration, nil
}

// shiftTime moves a time by a duration. A literal is folded to milliseconds.
func shiftTime(t, op string, d operand) (string, ExprType, error) {
	if d.node.Kind == ExprDuration {
		seconds, err := durationSeconds(d.node.Value)
		if err != nil {
			return "", TypeAny, err
		}
		ms := seconds * 1000
		if op == "-" {
			ms = -ms
		}
		return fmt.Sprintf("addMilliseconds(%s, %d)", t, ms), TypeTime, nil
	}
	seconds, err := asSeconds(d)
	if err != nil {
		return "", TypeAny, err
	}
	sign := ""
	if op == "-" {
		sign = "-"
	}
	return fmt.Sprintf("addMilliseconds(%s, %stoInt64(round(%s * 1000)))", t, sign, operandSQL(d.node, seconds)), TypeTime, nil
}

// asSeconds reads a side as the seconds of a duration: a duration, or any number
// but a bare one.
func asSeconds(o operand) (string, error) {
	switch {
	case o.typ == TypeDuration:
		return o.sql, nil
	case o.typ == TypeTime:
		return "", fmt.Errorf("expected a duration, got the time %s", o.node)
	case unitless(o.node):
		return "", unitError(o.node)
	}
	return asNumber(o.sql, o.typ, o.node)
}

func durationScale(e *ExprNode, l, r operand, join func(l, r, op string) string) (string, ExprType, error) {
	for _, o := range []operand{l, r} {
		if o.typ == TypeTime {
			return "", TypeAny, fmt.Errorf("%s: expected a number, got the time %s", e.Value, o.node)
		}
	}
	switch {
	case l.typ == TypeDuration && r.typ == TypeDuration:
		if e.Value == "*" {
			return "", TypeAny, fmt.Errorf("%s: a duration times a duration has no meaning; multiply a duration by a number", e)
		}
		return join(l.sql, r.sql, "/"), TypeNumber, nil
	case l.typ == TypeDuration:
		n, err := asNumber(r.sql, r.typ, r.node)
		if err != nil {
			return "", TypeAny, err
		}
		return join(l.sql, n, e.Value), TypeDuration, nil
	}
	if e.Value == "/" {
		return "", TypeAny, fmt.Errorf("%s: divide a duration by a number or by another duration", e)
	}
	n, err := asNumber(l.sql, l.typ, l.node)
	if err != nil {
		return "", TypeAny, err
	}
	return join(n, r.sql, "*"), TypeDuration, nil
}

func timeCompare(e *ExprNode, l, r operand, join func(l, r, op string) string) (string, ExprType, error) {
	if l.typ != TypeTime && r.typ != TypeTime {
		a, err := asSeconds(l)
		if err != nil {
			return "", TypeAny, err
		}
		b, err := asSeconds(r)
		if err != nil {
			return "", TypeAny, err
		}
		return join(a, b, e.Value), TypeBool, nil
	}
	other := r
	if r.typ == TypeTime {
		other = l
	}
	switch {
	case other.typ == TypeTime || readsAsTime(other.typ):
	case other.typ == TypeBool:
		return "", TypeAny, fmt.Errorf("%s compares a time with a condition", e)
	case other.typ == TypeDuration:
		return "", TypeAny, fmt.Errorf("%s compares a time with a duration; compare it with a time, such as now() - %s", e, other.node)
	default:
		return "", TypeAny, fmt.Errorf("%s compares a time with a number; compare it with a time", e)
	}
	a, err := asTime(l.sql, l.typ, l.node)
	if err != nil {
		return "", TypeAny, err
	}
	b, err := asTime(r.sql, r.typ, r.node)
	if err != nil {
		return "", TypeAny, err
	}
	return join(a, b, e.Value), TypeBool, nil
}

// asTime reads a value as a time. Text and numbers (epoch seconds through
// nanoseconds) are parsed the way dateDiff() parses them, leniently, to NULL when
// they are no time; a quoted literal is checked here, since a typo would otherwise
// match nothing.
func asTime(sql string, typ ExprType, node *ExprNode) (string, error) {
	switch typ {
	case TypeTime:
		if node.Kind == ExprField && isDateTimeColumn(node.Value) {
			// The final SELECT renders toString(timestamp) AS timestamp, and an
			// expression computed beside it reads that String alias, not the column.
			// This reads either as the same instant.
			return "toDateTime64(" + sql + ", 3, 'UTC')", nil
		}
		return sql, nil
	case TypeBool, TypeDuration:
		return "", fmt.Errorf("expected a time, got %s", typ)
	}
	if node.Kind == ExprString && !isTimeLiteral(node.Value) {
		return "", fmt.Errorf("%q is not a time: write it as 2026-01-31 or 2026-01-31 14:00:00", node.Value)
	}
	return lenientDateTime(asString(sql, typ), typ == TypeText || node.Kind == ExprString), nil
}

// timeLiteralLayouts are the written forms a quoted time may take, all of which
// ClickHouse's best-effort parse reads the same way.
var timeLiteralLayouts = []string{
	"2006-01-02", "2006-01-02 15:04", "2006-01-02 15:04:05", "2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04", "2006-01-02T15:04:05", "2006-01-02T15:04:05.999999999", time.RFC3339, time.RFC3339Nano,
}

func isTimeLiteral(s string) bool {
	s = strings.TrimSpace(s)
	if s != "" && strings.Trim(s, "0123456789") == "" {
		switch len(s) {
		case 10, 13, 16, 19: // epoch seconds, milliseconds, microseconds, nanoseconds
			return true
		}
	}
	for _, layout := range timeLiteralLayouts {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}

// zoneArg is the trailing time zone argument for a calendar function, empty for
// UTC so the SQL matches what the columns already are.
func zoneArg(tz string) string {
	if tz == "" || tz == "UTC" {
		return ""
	}
	return ", " + sqlString(tz)
}

// displayZone is the zone calendar boundaries snap to for a viewer: theirs when it
// is a real zone, else UTC, so a bad preference never fails a query.
func displayZone(tz string) string {
	if tz == "" || tz == "UTC" {
		return "UTC"
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return "UTC"
	}
	return tz
}

// callZone is the zone a calendar function uses: its timezone= argument, which
// must be a real zone, or the viewer's.
func callZone(c funcCall, i int) (string, error) {
	node := c.nodes[i]
	if node == nil {
		return c.ctx.registry.zone(), nil
	}
	if node.Kind != ExprString {
		return "", fmt.Errorf("timezone must be a quoted zone name, such as \"America/New_York\"")
	}
	if node.Value == "" || node.Value == "Local" {
		return "", fmt.Errorf("unknown time zone %q", node.Value)
	}
	if _, err := time.LoadLocation(node.Value); err != nil {
		return "", fmt.Errorf("unknown time zone %q", node.Value)
	}
	return node.Value, nil
}

// checkTimeFilters rejects an ordering filter that compares a time with a number
// or a duration, neither of which is a time.
func checkTimeFilters(cond Condition, registry *FieldRegistry) error {
	if cond.IsCompound() {
		for _, child := range cond.Children {
			if err := checkTimeFilters(child, registry); err != nil {
				return err
			}
		}
		return nil
	}
	if cond.Expr != nil || cond.ValueField != "" || !isOrdering(cond.Operator) || !isTimeField(cond.Field, registry) {
		return nil
	}
	switch {
	case isDurationLiteral(cond.Value):
		return fmt.Errorf("%s %s %s compares a time with a duration; for the last %s write (now() - %s) < %s",
			cond.Field, cond.Operator, cond.Value, cond.Value, cond.Field, cond.Value)
	case validateNumeric(cond.Value) == nil:
		return fmt.Errorf("%s %s %s compares a time with a number; compare it with a time, such as \"2026-01-31 14:00\"",
			cond.Field, cond.Operator, cond.Value)
	}
	return nil
}

// isTimeField reports whether a filter's field resolves to a time.
func isTimeField(name string, registry *FieldRegistry) bool {
	entry := registry.Get(name)
	if entry == nil {
		return isDateTimeColumn(name)
	}
	return entry.typ == typeTime || entry.Kind == FieldKindBase && isDateTimeColumn(name)
}

func optStr(name string) exprParam { return exprParam{Name: name, Type: TypeString, Optional: true} }

func init() {
	registerExprFunc(&exprFunc{
		Name: "now", Returns: TypeTime,
		Render: func([]string) string { return "now64(3)" },
	})
	registerExprFunc(&exprFunc{
		Name:    "bin",
		Params:  []exprParam{timep("field"), anyp("span"), optStr("timezone")},
		Returns: TypeTime,
		Compile: func(c funcCall) (string, error) {
			span := c.nodes[1]
			if span.Kind != ExprDuration {
				return "", fmt.Errorf("span must be a duration such as 5m, 1h or 1d, got %s", span)
			}
			n, unit := durationSpan(span.Value)
			if n <= 0 {
				return "", fmt.Errorf("span must be longer than zero")
			}
			tz, err := callZone(c, 2)
			if err != nil {
				return "", err
			}
			return bucketExpr(c.args[0], n, unit, tz), nil
		},
	})
	registerExprFunc(&exprFunc{
		Name:    "hourofday",
		Params:  []exprParam{timep("field"), optStr("timezone")},
		Returns: TypeNumber,
		Compile: func(c funcCall) (string, error) {
			tz, err := callZone(c, 1)
			if err != nil {
				return "", err
			}
			return "toHour(" + c.args[0] + zoneArg(tz) + ")", nil
		},
	})
	registerExprFunc(&exprFunc{
		Name:    "dayofweek",
		Params:  []exprParam{timep("field"), optStr("timezone")},
		Returns: TypeNumber,
		Compile: func(c funcCall) (string, error) {
			tz, err := callZone(c, 1)
			if err != nil {
				return "", err
			}
			// Mode 0 counts Monday as 1 through Sunday as 7 (ISO 8601).
			return "toDayOfWeek(" + c.args[0] + ", 0" + zoneArg(tz) + ")", nil
		},
	})
}
