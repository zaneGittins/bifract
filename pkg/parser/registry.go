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
	// Differs from FieldKindPerRow only in that no textToNumber() coercion is needed.
	FieldKindAssignment
	// FieldKindJoined: a column a join adds, modelLookup() (beacon_score,
	// coverage, z_score, ...) or join() (_join_<col>). It exists only after the
	// join, so conditions on it bind there, and references resolve to the bare
	// output column name (never a JSON sub-column).
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
	// operand marks an expression that is one SQL operand, so a reference folds
	// it in without brackets. Changing the expression clears it.
	operand bool
	// typ is the SQL type the expression is known to have, recorded where it is
	// computed: a String, as a stored field is, or a number, which the String
	// coercions a filter applies (textToNumber) reject. Changing the expression
	// clears it.
	typ valueType
	// reads is what a computed value reads, recorded where it was computed, when
	// readsKnown says it was.
	reads      deps
	readsKnown bool
	// column marks a ResolveAs that is another column by name, as x := log_id is.
	column bool
	// cols are the columns the value reads by name, which an expression folding it
	// in reads too, NUL-separated so entries stay comparable.
	cols string
}

// readCols lists the columns the value reads by name.
func (e *FieldEntry) readCols() []string {
	if e.cols == "" {
		return nil
	}
	return strings.Split(e.cols, "\x00")
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
	// groupKeys maps a grouping key's field name to the alias it is addressable by
	// after the aggregation. See SetGroupKeyAlias.
	groupKeys map[string]string
	// groupKeyCmd is the command index of the groupby that recorded groupKeys. A
	// later groupby groups by its own keys, so the earlier stage's are superseded
	// rather than accumulated.
	groupKeyCmd int
	// rowLevel marks a registry conditions resolve against row by row, with no
	// aggregation before them (the first stage, a case branch, a chain step): count,
	// sum and avg name log fields there, not aggregates.
	rowLevel bool
	// normLogIndexHints allows norm_log index hints on scan filters (see normLogHint).
	normLogIndexHints bool
	// rec records the references the registry hands out (see track).
	rec *refRecorder
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
		rec:         &refRecorder{},
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
		r.order = append(r.order, name)
	}
	return r
}

// Clone returns an independent copy. A chain step compiles its own projections
// (len, match, lookupIP) into the registry to resolve its own predicates; those
// must not outlive the step, since nothing downstream of the aggregate can
// reference them.
func (r *FieldRegistry) Clone() *FieldRegistry {
	c := &FieldRegistry{
		fields:      make(map[string]*FieldEntry, len(r.fields)),
		order:       append([]string(nil), r.order...),
		sourceMode:  r.sourceMode,
		icePromoted: r.icePromoted,
		groupKeys:   make(map[string]string, len(r.groupKeys)),
		groupKeyCmd: r.groupKeyCmd,
		rowLevel:    r.rowLevel,
		rec:         r.rec,
	}
	c.normLogIndexHints = r.normLogIndexHints
	for k, v := range r.groupKeys {
		c.groupKeys[k] = v
	}
	for k, v := range r.fields {
		entry := *v
		c.fields[k] = &entry
	}
	return c
}

// changedSince lists the names whose entries differ from before.
func (r *FieldRegistry) changedSince(before *FieldRegistry) []string {
	var names []string
	for _, name := range r.order {
		if b, ok := before.fields[name]; !ok || *b != *r.fields[name] {
			names = append(names, name)
		}
	}
	return names
}

// reinstate puts back a command's Declare entries before it executes, except
// where an earlier command or assignment has since redefined the name.
func (r *FieldRegistry) reinstate(entries []FieldEntry, undeclared *FieldRegistry) {
	for _, e := range entries {
		if cur, ok := r.fields[e.Name]; ok {
			if u, ok := undeclared.fields[e.Name]; !ok || *u != *cur {
				continue
			}
		} else {
			r.order = append(r.order, e.Name)
		}
		entry := e
		r.fields[e.Name] = &entry
	}
}

// rowLevelClone is a copy for conditions evaluated row by row.
func (r *FieldRegistry) rowLevelClone() *FieldRegistry {
	c := r.Clone()
	c.rowLevel = true
	return c
}

// bareAggregate reports whether a name with no registry entry stands for an
// aggregate here: count, sum and avg do once an aggregation can precede them.
func (r *FieldRegistry) bareAggregate(field string) bool {
	return (r == nil || !r.rowLevel) && isBareAggregate(field)
}

// fieldRef returns the source-mode-appropriate reference for a JSON/MAP field.
func (r *FieldRegistry) fieldRef(field string) string {
	return fieldRefMode(field, r.sourceMode)
}

// SetGroupKeyAlias records that a field is one of the plan's grouping keys, and the
// alias it is addressable by after the aggregation. Recorded at classify time because
// only that stage has the plan; read when resolving an operand that must survive the
// GROUP BY.
func (r *FieldRegistry) SetGroupKeyAlias(field, alias string, cmdIndex int) {
	if r.groupKeys == nil || cmdIndex > r.groupKeyCmd {
		// A later groupby replaces the previous stage's keys: those are not in the
		// new GROUP BY, and treating them as still addressable would emit a column
		// ClickHouse rejects (code 215) after the aggregation check waved it through.
		r.groupKeys = map[string]string{}
		r.groupKeyCmd = cmdIndex
	}
	if cmdIndex < r.groupKeyCmd {
		return // an earlier stage's key, already superseded
	}
	r.groupKeys[field] = alias
}

// AddGroupKeyAlias records a key that joins the keys already in effect, as chain()
// does by extending the GROUP BY of the stage it runs in.
func (r *FieldRegistry) AddGroupKeyAlias(field, alias string) {
	if r.groupKeys == nil {
		r.groupKeys = map[string]string{}
	}
	r.groupKeys[field] = alias
}

// GroupKeyAlias returns the alias a grouping key is addressable by, and whether the
// field is a grouping key at all.
func (r *FieldRegistry) GroupKeyAlias(field string) (string, bool) {
	alias, ok := r.groupKeys[field]
	return alias, ok
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
	return r.Ref(name).sql
}

// setColumn records that a field's expression is the column it copies.
func (r *FieldRegistry) setColumn(name string) {
	if entry, ok := r.fields[name]; ok {
		entry.column = true
	}
}

// setRead records what a computed field read, as recorded while it was computed.
func (r *FieldRegistry) setRead(name string, got reads) {
	if entry, ok := r.fields[name]; ok {
		entry.reads, entry.readsKnown, entry.cols = got.deps(), true, strings.Join(got.columns(), "\x00")
	}
}

// setReads records what a computed field reads.
func (r *FieldRegistry) setReads(name string, d deps) {
	if entry, ok := r.fields[name]; ok {
		entry.reads, entry.readsKnown = d, true
	}
}

// isText reports whether the name resolves to a String.
func (r *FieldRegistry) isText(name string) bool {
	return r.typeOf(name) == typeText
}

// setType records the SQL type a field's expression has, when it is known.
func (r *FieldRegistry) setType(name string, t valueType) {
	if entry, ok := r.fields[name]; ok && t != typeUnknown {
		entry.typ = t
	}
}

// setText records that a field's expression is a String.
func (r *FieldRegistry) setText(name string) {
	if entry, ok := r.fields[name]; ok {
		entry.typ = typeText
	}
}

// resolvesToStored reports whether the name resolves to the stored log field: it
// is unregistered, or registered with only a placeholder expression (its own
// name) and is not a base column or a join, aggregate or window output, which
// resolve to their own column names.
func (r *FieldRegistry) resolvesToStored(name string) bool {
	entry, ok := r.fields[name]
	if !ok {
		return true
	}
	switch entry.Kind {
	case FieldKindBase, FieldKindJoined, FieldKindAggregate, FieldKindWindow:
		return false
	}
	return entry.ResolveAs == "" && entry.Expr == name
}

// DependsOnJoin reports whether a per-row field reads a join's output, directly or
// through the expression it was computed from, and so exists only where that join
// does.
func (r *FieldRegistry) DependsOnJoin(name string) bool {
	entry, ok := r.fields[name]
	if !ok {
		return false
	}
	switch entry.Kind {
	case FieldKindJoined:
		return true
	case FieldKindPerRow, FieldKindAssignment:
		return r.valueDeps(entry).join
	}
	return false
}

// SetResolveExpr updates the resolve expression for a field during the Execute phase.
// If the field is not yet registered, it is auto-registered as FieldKindJSON.
func (r *FieldRegistry) SetResolveExpr(name, expr string) {
	if entry, ok := r.fields[name]; ok {
		// A new expression reads what it reads: until that is recorded, nothing
		// of the old one's is known.
		entry.ResolveAs, entry.operand, entry.typ = expr, false, typeUnknown
		entry.readsKnown, entry.column, entry.cols = false, false, ""
	} else {
		r.Register(name, FieldKindJSON, expr, -1)
		r.fields[name].ResolveAs = expr
	}
}

// inline folds a field in as expr from here on, keeping what its value reads.
func (r *FieldRegistry) inline(name, expr string, producedBy int) {
	var prior FieldEntry
	if e, ok := r.fields[name]; ok {
		prior = *e
	}
	r.RegisterInlineExpr(name, expr, producedBy)
	if e := r.fields[name]; prior.readsKnown {
		e.reads, e.readsKnown, e.cols = prior.reads, true, prior.cols
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

// setOperand records that a field's expression is one SQL operand.
func (r *FieldRegistry) setOperand(name string) {
	if entry, ok := r.fields[name]; ok {
		entry.operand = true
	}
}

// SetNumeric records that a field's expression is already a number.
func (r *FieldRegistry) SetNumeric(name string) {
	if entry, ok := r.fields[name]; ok {
		entry.typ = typeNumber
	}
}

// IsNumeric reports whether a field's expression needs no numeric coercion.
func (r *FieldRegistry) IsNumeric(name string) bool {
	if entry, ok := r.fields[name]; ok {
		return entry.typ == typeNumber
	}
	return false
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

// IsRowLevel reports whether the field is known to hold a per-row value
// (a base column, or a computed one such as strftime, lowercase, len, logSize)
// rather than a per-group value. ClassifyKind is used so an aggregate or window
// output carried into a later stage (re-registered as Assignment by
// ScopeToOutputs) is not mistaken for a row-level value.
//
// Unregistered fields report false: a handler may have emitted an aggregate
// select without registering it, and treating that as row-level would drop it.
func (r *FieldRegistry) IsRowLevel(name string) bool {
	if entry, ok := r.fields[name]; ok {
		switch entry.ClassifyKind() {
		case FieldKindPerRow, FieldKindAssignment, FieldKindBase:
			return true
		}
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
// comparisons reference it bare. Wrapping such a column in textToNumber() (which
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
		var typ valueType
		if e, ok := prev[name]; ok {
			typ = e.typ
		}
		r.fields[name] = &FieldEntry{
			Name:       name,
			Kind:       kind,
			Expr:       name,
			ResolveAs:  name,
			ProducedBy: -1,
			OriginKind: origin,
			typ:        typ,
			readsKnown: true, // a column of the stage before, which it read itself
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
