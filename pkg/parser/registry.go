package parser

import "strings"

// FieldKind categorizes the type of a field in the query pipeline.
type FieldKind int

const (
	FieldKindBase       FieldKind = iota // timestamp, raw_log, log_id
	FieldKindJSON                        // fields.`name`.:String
	FieldKindPerRow                      // strftime, lowercase, eval, etc.
	FieldKindAggregate                   // COUNT(*), sum(), etc.
	FieldKindWindow                      // _modified_z, _is_outlier (from window wrappers)
	FieldKindAssignment                  // user := assignments
)

// FieldEntry tracks a single field's metadata.
type FieldEntry struct {
	Name       string
	Kind       FieldKind
	Expr       string // SQL expression that produces this field
	ProducedBy int    // command index (-1 for base fields)
	ResolveAs  string // Override for Resolve(); when set, returned instead of Expr
}

// FieldRegistry is a single source of truth for all field metadata in a query pipeline.
// It replaces the old computedFields, computedFieldExprs, aggregationOutputs, and perRowExprs maps.
type FieldRegistry struct {
	fields map[string]*FieldEntry
	order  []string
}

// NewFieldRegistry creates a registry pre-populated with base fields.
func NewFieldRegistry() *FieldRegistry {
	r := &FieldRegistry{
		fields: make(map[string]*FieldEntry),
	}
	// Register base columns
	for _, name := range []string{"timestamp", "raw_log", "log_id", "fractal_id", "ingest_timestamp"} {
		r.fields[name] = &FieldEntry{
			Name:       name,
			Kind:       FieldKindBase,
			Expr:       name,
			ProducedBy: -1,
		}
	}
	return r
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
			// fields.`name`.:String reference.
			if entry.Expr == name && entry.Kind != FieldKindBase {
				return jsonFieldRef(name)
			}
			expr = entry.Expr
		}
		if idx := strings.LastIndex(expr, " AS "); idx != -1 {
			return expr[:idx]
		}
		return expr
	}
	return jsonFieldRef(name)
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
// Each output field is re-registered as a base-like column (it references a
// subquery alias, not a JSON path). This is used when pushing a new groupby
// stage: the new stage should only see the previous stage's output columns.
func (r *FieldRegistry) ScopeToOutputs(outputs map[string]bool) {
	r.fields = make(map[string]*FieldEntry)
	r.order = nil
	for name := range outputs {
		r.fields[name] = &FieldEntry{
			Name:       name,
			Kind:       FieldKindBase, // treat prior output as a plain column reference
			Expr:       name,
			ProducedBy: -1,
		}
		r.order = append(r.order, name)
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

