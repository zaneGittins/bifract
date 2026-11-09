package parser

import "strings"

// precedence is how loosely a predicate's own top-level operators bind, which
// decides whether combining it with others needs brackets. It is known where the
// predicate is built, so nothing reads it back out of the SQL.
type precedence int

const (
	// precAtom is a single operand: a comparison, a call, or anything bracketed.
	precAtom precedence = iota
	// precAnd is a chain of AND, safe beside other conjuncts.
	precAnd
	// precOr has a top-level OR, so it is bracketed wherever it is ANDed.
	precOr
)

// predicate is a boolean SQL expression and its precedence.
type predicate struct {
	sql  string
	prec precedence
	// candidates is set on a provenance filter that reads its plan's candidate
	// set, so narrowing that set can rebuild it (see narrowPgrCandidates).
	candidates *candidateFilter
}

// atomHook, when set, sees every predicate claimed as one operand. Tests use it
// to hold each claim to the SQL it was made for.
var atomHook func(sql string)

// atom is a predicate built as a single operand.
func atom(sql string) predicate {
	if atomHook != nil {
		atomHook(sql)
	}
	return predicate{sql: sql}
}

// atoms is each SQL text as an atom.
func atoms(sqls ...string) []predicate {
	out := make([]predicate, len(sqls))
	for i, s := range sqls {
		out[i] = atom(s)
	}
	return out
}

func (p predicate) empty() bool { return p.sql == "" }

// conjunct renders the predicate to be joined to others with AND. AND binds
// tighter than OR, so only a disjunction needs brackets.
func (p predicate) conjunct() string {
	if p.prec == precOr {
		return "(" + p.sql + ")"
	}
	return p.sql
}

// andOf joins predicates with AND. Empty ones are skipped; a single one is
// returned as it is.
func andOf(ps ...predicate) predicate {
	return joinPredicates(ps, " AND ", precAnd, predicate.conjunct)
}

// orOf joins predicates with OR. AND binds tighter, so no operand needs brackets.
func orOf(ps ...predicate) predicate {
	return joinPredicates(ps, " OR ", precOr, func(p predicate) string { return p.sql })
}

func joinPredicates(ps []predicate, sep string, prec precedence, operand func(predicate) string) predicate {
	var parts []string
	var only predicate
	for _, p := range ps {
		if !p.empty() {
			parts, only = append(parts, operand(p)), p
		}
	}
	switch len(parts) {
	case 0:
		return predicate{}
	case 1:
		return only
	}
	return predicate{sql: strings.Join(parts, sep), prec: prec}
}

// notOf negates a predicate.
func notOf(p predicate) predicate {
	if p.empty() {
		return p
	}
	return predicate{sql: "NOT (" + p.sql + ")"}
}

// group is the predicate as one operand, bracketed unless it already is one.
func group(p predicate) predicate {
	if p.prec == precAtom {
		return p
	}
	return predicate{sql: "(" + p.sql + ")"}
}

// whereClause renders filters as a WHERE clause, or "" when there are none.
func whereClause(where []predicate) string {
	if p := andOf(where...); !p.empty() {
		return " WHERE " + p.sql
	}
	return ""
}

// conjuncts renders each predicate to be joined with AND, for a caller that joins
// them itself.
func conjuncts(ps []predicate) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.conjunct()
	}
	return out
}
