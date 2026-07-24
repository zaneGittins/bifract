package parser

import "strings"

// FieldKind categorizes the type of a field in the query pipeline.
type FieldKind int

const (
	FieldKindBase      FieldKind = iota // timestamp, raw_log, log_id
	FieldKindJSON                       // fields.`name`.:String
	FieldKindPerRow                     // strftime, lowercase, eval, etc.
	FieldKindAggregate                  // COUNT(*), sum(), etc.
	FieldKindWindow                     // _modified_z, _is_outlier (from window wrappers)
	// FieldKindAssignment: per-row scalar already numeric (len, levenshtein, := assignments).
	// Always routes to WHERE — it is computed before aggregation, never after.
	// Differs from FieldKindPerRow only in that no toFloat64OrZero() coercion is needed.
	FieldKindAssignment
	// FieldKindJoined: a column produced by a JOIN wrapper -- model_lookup()
	// (beacon_score, confidence, z_score, ...) or join() (_join_<col>). It only
	// exists AFTER the join wrap, so conditions on it must defer (post-join) and
	// references resolve to the bare output column name (never a JSON sub-column).
	FieldKindJoined
)

// FieldEntry tracks a single field's metadata.
type FieldEntry struct {
	Name       string
	Kind       FieldKind
	Expr       string // SQL expression that produces this field
	ProducedBy int    // command index (-1 for base fields)
	ResolveAs  string // Override for Resolve(); when set, returned instead of Expr
	Inline     bool   // when true, references are folded in as the expression, never an alias
	// OriginKind preserves the field's identity BEFORE any stage-scoping downgrade.
	// ScopeToOutputs re-registers an aggregate/window output as FieldKindAssignment
	// (a plain column of the new stage) for expression resolution, which loses the
	// information that a filter on it must route to HAVING/deferred rather than the
	// source WHERE. OriginKind keeps that original kind so condition classification
	// stays correct across carry-forward stages.
	OriginKind FieldKind
}

// ClassifyKind returns the FieldKind that condition routing (WHERE vs HAVING vs
// deferred) should use. Expression resolution uses the scoped Kind (a carried
// aggregate resolves to a bare column reference), but routing must honor the
// pre-scope identity: a carried aggregate still needs HAVING, a carried window
// value still defers. A genuine per-row assignment (OriginKind == Assignment)
// keeps routing to WHERE.
func (e *FieldEntry) ClassifyKind() FieldKind {
	if e.Kind == FieldKindAssignment &&
		e.OriginKind != FieldKindAssignment && e.OriginKind != FieldKindBase {
		return e.OriginKind
	}
	return e.Kind
}

// FieldRegistry is a single source of truth for all field metadata in a query pipeline.
// It replaces the old computedFields, computedFieldExprs, aggregationOutputs, and perRowExprs maps.
type FieldRegistry struct {
	fields     map[string]*FieldEntry
	order      []string
	sourceMode SourceMode // Hot vs Iceberg; controls field-ref/content-column codegen
	// icePromoted is the set of field names whose `_ice_` column exists on the
	// specific Iceberg table this query targets. Nil in hot mode, and nil-safe in
	// iceberg mode (no `_ice_` pruning). See icebergEqualityPredicate.
	icePromoted map[string]bool
}

// NewFieldRegistry creates a registry pre-populated with base fields for the
// given source mode. The norm_log base column resolves to the norm_log column in
// both modes (materialized + indexed in the hot store; a plain JSON String in
// the archive).
func NewFieldRegistry(mode SourceMode, icePromoted map[string]bool) *FieldRegistry {
	r := &FieldRegistry{
		fields:      make(map[string]*FieldEntry),
		sourceMode:  mode,
		icePromoted: icePromoted,
	}
	// Register base columns
	for _, name := range []string{"timestamp", normLogColumn, "log_id", "fractal_id", "ingest_timestamp", "normalizer"} {
		expr := name
		if name == normLogColumn && mode == SourceIceberg {
			expr = contentColMode(mode)
		}
		r.fields[name] = &FieldEntry{
			Name:       name,
			Kind:       FieldKindBase,
			Expr:       expr,
			ProducedBy: -1,
		}
	}
	return r
}

// fieldRef returns the source-mode-appropriate reference for a JSON/MAP field.
func (r *FieldRegistry) fieldRef(field string) string {
	return fieldRefMode(field, r.sourceMode)
}

// Register adds or updates a field entry in the registry.
func (r *FieldRegistry) Register(name string, kind FieldKind, expr string, producedBy int) {
	if _, exists := r.fields[name]; !exists {
		r.order = append(r.order, name)
	}
	r.fields[name] = &FieldEntry{
		Name:       name,
		Kind:       kind,
		Expr:       expr,
		ProducedBy: producedBy,
		OriginKind: kind,
	}
}

// Get returns the field entry for a name, or nil if not found.
func (r *FieldRegistry) Get(name string) *FieldEntry {
	return r.fields[name]
}

// Has returns true if the field is registered.
func (r *FieldRegistry) Has(name string) bool {
	_, ok := r.fields[name]
	return ok
}

// Resolve returns the SQL expression to use when referencing a field.
// If ResolveAs is set (from Execute-phase updates), returns that.
// Otherwise returns Expr (from Declare-phase registration).
// Any trailing " AS alias" suffix is stripped so the result is a bare expression.
// For unknown fields, returns jsonFieldRef.
func (r *FieldRegistry) Resolve(name string) string {
	if entry, ok := r.fields[name]; ok {
		expr := entry.ResolveAs
		if expr == "" {
			// No Execute-phase expression set. If the Declare-phase Expr is just
			// a placeholder (same as the field name) and the field is not a base
			// column, fall through to jsonFieldRef so we get the proper
			// fields.`name`.:String reference. Joined columns are excluded: they
			// resolve to their bare join-output column name, not a JSON field.
			if entry.Expr == name && entry.Kind != FieldKindBase && entry.Kind != FieldKindJoined {
				return r.fieldRef(name)
			}
			expr = entry.Expr
		}
		if idx := strings.LastIndex(expr, " AS "); idx != -1 {
			return expr[:idx]
		}
		return expr
	}
	return r.fieldRef(name)
}

// SetResolveExpr updates the resolve expression for a field during the Execute phase.
// If the field is not yet registered, it is auto-registered as FieldKindJSON.
func (r *FieldRegistry) SetResolveExpr(name, expr string) {
	if entry, ok := r.fields[name]; ok {
		entry.ResolveAs = expr
	} else {
		r.Register(name, FieldKindJSON, expr, -1)
		r.fields[name].ResolveAs = expr
	}
}

// RegisterInlineExpr registers a field whose value is always folded in at each
// reference because it is never materialized as a SELECT column, e.g. a
// pre-aggregation assignment that is inlined into the aggregate consuming it.
// Chained references (b := a * 2) then resolve to the full underlying
// expression rather than a non-existent alias.
func (r *FieldRegistry) RegisterInlineExpr(name, expr string, producedBy int) {
	r.Register(name, FieldKindAssignment, expr, producedBy)
	entry := r.fields[name]
	entry.ResolveAs = expr
	entry.Inline = true
}

// IsInline reports whether references to the field should be folded in as its
// expression instead of an alias.
func (r *FieldRegistry) IsInline(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.Inline
	}
	return false
}

// IsAggregate returns true if the field is an aggregate kind.
func (r *FieldRegistry) IsAggregate(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.Kind == FieldKindAggregate
	}
	return false
}

// IsPerRow returns true if the field is a per-row computation.
func (r *FieldRegistry) IsPerRow(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.Kind == FieldKindPerRow
	}
	return false
}

// IsWindow returns true if the field is a window/post-aggregation kind.
func (r *FieldRegistry) IsWindow(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.Kind == FieldKindWindow
	}
	return false
}

// IsComputed returns true if the field is any non-base, non-JSON kind.
func (r *FieldRegistry) IsComputed(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.Kind != FieldKindBase && entry.Kind != FieldKindJSON
	}
	return false
}

// IsPerRowOrAssignment returns true if the field is a per-row computed field
// (strftime, lowercase, etc.) or an assignment field (len, levenshtein, etc.).
func (r *FieldRegistry) IsPerRowOrAssignment(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.Kind == FieldKindPerRow || entry.Kind == FieldKindAssignment
	}
	return false
}

// IsNumericComputed returns true if the field is an assignment-kind computed
// field that already produces a numeric value (e.g. length(), levenshtein()).
// These must not be wrapped with toFloat64OrNull (which requires String input).
func (r *FieldRegistry) IsNumericComputed(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.Kind == FieldKindAssignment
	}
	return false
}

// FieldsOfKind returns all field names of a given kind, in registration order.
func (r *FieldRegistry) FieldsOfKind(kind FieldKind) []string {
	var result []string
	for _, name := range r.order {
		if r.fields[name].Kind == kind {
			result = append(result, name)
		}
	}
	return result
}

// ScopeToOutputs resets the registry so only the given output fields remain.
// Each output field becomes a plain column reference to the previous stage's
// subquery alias (not a JSON path). This is used when pushing a new groupby
// stage: the new stage should only see the previous stage's output columns.
//
// Crucially, the transition is TYPE-PRESERVING. A column that was numeric in the
// prior stage (an aggregate output such as _count/_sum, a numeric assignment, or a
// window value) is re-registered as FieldKindAssignment so that downstream numeric
// comparisons reference it bare. Wrapping such a column in toFloat64OrZero() (which
// only accepts String) would raise a ClickHouse type error. String-typed columns
// (carried group keys) stay FieldKindBase, which still coerces on numeric compare.
// In both cases ResolveAs is the bare alias so references resolve to the column,
// never to a fields.`name` JSON path.
func (r *FieldRegistry) ScopeToOutputs(outputs map[string]bool) {
	prev := r.fields
	r.fields = make(map[string]*FieldEntry)
	r.order = nil
	for name := range outputs {
		kind := FieldKindBase
		// Preserve the pre-scope identity for condition routing (see OriginKind).
		// Defaults to the scoped kind for plain carried columns (group keys).
		origin := kind
		if e, ok := prev[name]; ok {
			switch e.Kind {
			case FieldKindAggregate, FieldKindAssignment, FieldKindWindow:
				// Already numeric in the prior stage: no coercion downstream.
				kind = FieldKindAssignment
			}
			if e.OriginKind != FieldKindBase {
				origin = e.OriginKind
			} else {
				origin = e.Kind
			}
		}
		r.fields[name] = &FieldEntry{
			Name:       name,
			Kind:       kind,
			Expr:       name,
			ResolveAs:  name,
			ProducedBy: -1,
			OriginKind: origin,
		}
		r.order = append(r.order, name)
	}
	// Preserve inline-resolved fields (pre-aggregation assignments folded into
	// base log fields) that are not themselves carried outputs. They are never
	// selected as columns, but pre-aggregation conditions bound to the source
	// stage still resolve through them, and scoping must not strand that.
	for name, e := range prev {
		if e.Inline && !outputs[name] {
			r.fields[name] = e
			r.order = append(r.order, name)
		}
	}
}

// AllComputed returns a map[string]bool of all non-base/non-JSON field names.
// This provides backward compatibility with the old computedFields map.
func (r *FieldRegistry) AllComputed() map[string]bool {
	result := make(map[string]bool)
	for name, entry := range r.fields {
		if entry.Kind != FieldKindBase && entry.Kind != FieldKindJSON {
			result[name] = true
		}
	}
	return result
}
