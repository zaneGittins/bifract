package parser

import (
	"fmt"
	"slices"
	"strings"
)

// checkRecordedReads holds every predicate and ordering of a planned query to the
// SQL it was built as: what it recorded may claim no less than the SQL reads. A
// reference built without the registry records nothing, which would let a filter
// on a stored field pass for a model-only or scan-only one.
func checkRecordedReads(q *plannedQuery) {
	plan, reg := q.ctx.Plan, q.ctx.Registry
	joined := func(c string) bool {
		e := reg.Get(c)
		return e != nil && e.Kind == FieldKindJoined
	}
	check := func(what, sql string, d deps) {
		if d.foreign {
			return // reads another table, so it claims neither scan nor model
		}
		for _, c := range sqlColumns(sql) {
			switch {
			case d.scan && !scanColumns[c] && !strings.HasPrefix(c, icePromotedPrefix):
				panic(fmt.Sprintf("%s recorded as reading only the scan, but reads %s: %s", what, c, sql))
			case d.model && !slices.Contains(plan.lookup.Fields, c):
				panic(fmt.Sprintf("%s recorded as reading only the model, but reads %s: %s", what, c, sql))
			case !d.join && joined(c):
				panic(fmt.Sprintf("%s recorded as reading no join, but reads %s: %s", what, c, sql))
			}
		}
	}
	preds := func(what string, ps []predicate) {
		for _, p := range ps {
			check(what, p.sql, p.deps)
		}
	}
	terms := func(ts []OrderTerm) {
		for _, o := range ts {
			check("ORDER BY", o.Expr, o.deps)
			if o.form == refColumn && strings.TrimSpace(o.Expr) != o.Expr {
				panic("ORDER BY term recorded as a column: " + o.Expr)
			}
		}
	}
	limitBy := func(lb string, d deps) {
		if lb != "" {
			check("LIMIT BY", strings.TrimPrefix(lb, "LIMIT 1 BY "), d)
		}
	}
	for _, st := range plan.Stages {
		l := st.Layer
		preds("WHERE", l.Where)
		preds("HAVING", l.Having)
		terms(l.OrderBy)
		limitBy(l.LimitBy, l.limitByDeps)
		limitBy(l.ScanLimitBy, l.scanLimitByDeps)
		for _, c := range l.Caps {
			preds("WHERE after a cap", c.After)
			terms(c.OrderBy)
			limitBy(c.ScanLimitBy, c.scanDeps)
		}
	}
	for _, f := range plan.lookup.PostJoinWhere {
		check("post-join WHERE", f.Pred.sql, f.Pred.deps)
	}
	preds("model WHERE", plan.lookup.Where)
	preds("deferred WHERE", plan.deferred.where)
}

// sqlColumns lists the column names generated SQL reads, by its text: identifiers
// that are not functions, keywords, sub-columns, types, interval units or lambda
// parameters. A qualifier (the fields of fields.`x`) counts as the column it
// qualifies. The translator never asks this; the test holds recorded reads to it.
func sqlColumns(sql string) []string {
	type token struct {
		name             string
		call, qualified  bool
		lambda, interval bool
	}
	var toks []token
	params := map[string]bool{}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'':
			i = skipQuoted(sql, i)
			continue
		case c == '`' || c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			start := i
			var name string
			if c == '`' {
				end := skipQuoted(sql, i)
				name = strings.ReplaceAll(sql[i+1:max(i+1, end-1)], "``", "`")
				i = end
			} else {
				for i < len(sql) && isWord(sql[i]) {
					i++
				}
				name = sql[start:i]
				if strings.EqualFold(name, "nan") || strings.EqualFold(name, "inf") {
					continue // Float64 literals unquoted
				}
			}
			rest := strings.TrimLeft(sql[i:], " ")
			t := token{
				name:      name,
				call:      strings.HasPrefix(rest, "("),
				qualified: start > 0 && sql[start-1] == '.' || start > 1 && sql[start-2:start] == "::",
				lambda:    strings.HasPrefix(rest, "->"),
				interval:  len(toks) > 0 && strings.EqualFold(toks[len(toks)-1].name, "INTERVAL"),
			}
			if t.lambda {
				params[name] = true
			}
			toks = append(toks, t)
			continue
		case c >= '0' && c <= '9':
			for i < len(sql) && (isWord(sql[i]) || sql[i] == '.') {
				i++
			}
			continue
		}
		i++
	}
	var cols []string
	for _, t := range toks {
		if t.call || t.qualified || t.interval || params[t.name] || sqlKeywords[strings.ToUpper(t.name)] {
			continue
		}
		cols = append(cols, t.name)
	}
	return cols
}

var sqlKeywords = map[string]bool{
	"AND": true, "OR": true, "NOT": true, "IN": true, "GLOBAL": true, "IS": true, "NULL": true,
	"LIKE": true, "ILIKE": true, "BETWEEN": true, "CASE": true, "WHEN": true, "THEN": true,
	"ELSE": true, "END": true, "AS": true, "TRUE": true, "FALSE": true, "INTERVAL": true,
}

func isWord(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_'
}
