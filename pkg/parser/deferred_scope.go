package parser

import (
	"fmt"
	"strings"
)

// deferredScope exports source-scope expressions to a query layer above the source
// scan.
//
// Anything relocated past that scan -- a post-join filter, a post-window sort -- can
// only reference columns the scan actually projected. A raw JSON sub-column
// expression does not resolve there (ClickHouse code 47), which is why an OR that
// mixes a joined column with a log field, or a sort() on a log field alongside a
// deferred filter, used to fail. Each such expression is instead projected ONCE in the
// source scan under a hidden _dfr_<n> alias, which the relocated expression reads.
//
// This hooks in where an expression is relocated -- deferred WHERE and deferred ORDER
// BY -- rather than in the commands that trigger relocation. model_lookup(), join()
// and the window functions all route through it today, and any future command that
// defers an expression is covered without touching this file.
type deferredScope struct {
	alias  map[string]string // source expression -> hidden alias
	exprs  []string          // source expressions, in allocation order
	names  []string          // matching hidden aliases
	labels []string          // the BQL field name behind each, for error messages

	// exported is set once the hidden columns are actually projected into the source
	// scan; only then may the deferred wrap EXCEPT them.
	exported bool
}

func newDeferredScope() *deferredScope {
	return &deferredScope{alias: make(map[string]string)}
}

// ref returns the reference to use for a source expression at the deferred layer,
// allocating a hidden projection for it if needed. A bare identifier (timestamp,
// _count, _modified_z, _join__count) is already a named column at every layer above
// the scan and is returned unchanged; only real expressions need exporting. label is
// the field name the expression came from, used only to phrase errors.
func (s *deferredScope) ref(expr, label string) string {
	if s == nil {
		return expr
	}
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" || isBareIdentifier(trimmed) {
		return expr
	}
	if name, ok := s.alias[trimmed]; ok {
		return name
	}
	name := fmt.Sprintf("_dfr_%d", len(s.names))
	if label == "" {
		label = trimmed
	}
	s.alias[trimmed] = name
	s.exprs = append(s.exprs, trimmed)
	s.names = append(s.names, name)
	s.labels = append(s.labels, label)
	return name
}

// used reports whether anything was exported.
func (s *deferredScope) used() bool { return s != nil && len(s.names) > 0 }

// markExported records that the projections were added to the source scan.
func (s *deferredScope) markExported() {
	if s != nil {
		s.exported = true
	}
}

// projections returns the hidden SELECT entries the source scan must add.
func (s *deferredScope) projections() []SelectExpr {
	if s == nil {
		return nil
	}
	out := make([]SelectExpr, len(s.names))
	for i, name := range s.names {
		out[i] = SelectExpr{Expr: s.exprs[i] + " AS " + name}
	}
	return out
}

// isBareIdentifier reports whether s is a plain column name, i.e. already resolvable
// by name in any layer that selects from the scan.
func isBareIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
