package parser

import "slices"

// A field reaches SQL only through the registry, as an sqlRef: its SQL and what that
// reads. The registry records every reference it hands out in the scopes open at
// the time (see track), so a filter, a column or a field computed in a scope knows
// what it reads however its SQL was put together, without reading the SQL back.

// refForm is how a reference names its value in SQL.
type refForm int

const (
	// refExpr is the value's own expression: a stored field's path, or the
	// expression a computed field is folded in as.
	refExpr refForm = iota
	// refColumn is a column by name: a base column, an alias, a carried or joined
	// column, which exists only where something selects it under that name.
	refColumn
)

// valueType is the SQL type a value is known to have.
type valueType int

const (
	typeUnknown valueType = iota
	// typeText is a String that is never NULL, as a stored field is.
	typeText
	// typeString is a String that may be NULL, as a function's result may be.
	typeString
	typeNumber
	typeArray
	// typeTime is a DateTime or DateTime64.
	typeTime
)

// deps is what a value reads, as far as placing it is concerned.
type deps struct {
	join    bool // a join output, so it exists only after the join
	wrap    bool // a join() output, which exists only above the join's wrapping SELECT
	foreign bool // another table, through a subquery
	scan    bool // only what the logs table itself has, so its scan can evaluate it
	model   bool // only the joined model's columns
}

// modelColumn is what a column the model lookup joins reads: only the model.
var modelColumn = deps{join: true, model: true}

// noDeps is what a value reading nothing depends on: anything can evaluate it.
var noDeps = deps{scan: true, model: true}

// and is what a value reading both d and o depends on.
func (d deps) and(o deps) deps {
	return deps{join: d.join || o.join, wrap: d.wrap || o.wrap, foreign: d.foreign || o.foreign, scan: d.scan && o.scan, model: d.model && o.model}
}

// sqlRef is a reference as SQL: the field it names, in which form, and what the SQL
// reads.
type sqlRef struct {
	sql  string
	name string
	form refForm
	deps deps
	// cols are the columns an expression reads by name.
	cols []string
	// typ is the SQL type the value is known to have.
	typ valueType
}

// typed is the reference known to have type t.
func (s sqlRef) typed(t valueType) sqlRef {
	s.typ = t
	return s
}

// text is the reference where only a String will do: a number is converted, and
// anything else is read as it is.
func (s sqlRef) text() string {
	if s.typ == typeNumber || s.typ == typeTime {
		return "toString(" + s.sql + ")"
	}
	return s.sql
}

// typeOf is the SQL type a field's value has: what was recorded where it was
// computed, a stored field's String, or what every value of its kind is.
func (r *FieldRegistry) typeOf(name string) valueType {
	entry, ok := r.fields[name]
	switch {
	case !ok || r.resolvesToStored(name):
		return typeText
	case entry.typ != typeUnknown:
		return entry.typ
	}
	switch entry.Kind {
	case FieldKindAggregate, FieldKindAssignment, FieldKindWindow, FieldKindJoined:
		return typeNumber
	}
	return typeUnknown
}

// reads is what a piece of SQL references, in the order it did.
type reads []sqlRef

// deps is what the references together depend on.
func (rs reads) deps() deps {
	d := noDeps
	for _, r := range rs {
		d = d.and(r.deps)
	}
	return d
}

// columns lists the columns read by name, which a SELECT must have: a column
// itself, or those an expression folded in reads.
func (rs reads) columns() []string {
	var out []string
	for _, r := range rs {
		if r.form == refColumn {
			out = append(out, r.sql)
		} else {
			out = append(out, r.cols...)
		}
	}
	return out
}

// refRecorder holds the open scopes of one translation. Every registry of it shares
// the recorder, so a clone resolving a field records into the same scopes.
type refRecorder struct{ scopes []reads }

// record adds a reference to the innermost open scope.
func (r *FieldRegistry) record(ref sqlRef) sqlRef {
	if rec := r.rec; rec != nil && len(rec.scopes) > 0 {
		rec.scopes[len(rec.scopes)-1] = append(rec.scopes[len(rec.scopes)-1], ref)
	}
	return ref
}

// track runs fn in a scope of its own and returns what it referenced. A scope
// inside another adds its references to the outer one too.
func (r *FieldRegistry) track(fn func() error) (reads, error) {
	if r.rec == nil {
		r.rec = &refRecorder{}
	}
	rec := r.rec
	rec.scopes = append(rec.scopes, nil)
	err := fn()
	got := rec.scopes[len(rec.scopes)-1]
	rec.scopes = rec.scopes[:len(rec.scopes)-1]
	if n := len(rec.scopes); n > 0 {
		rec.scopes[n-1] = append(rec.scopes[n-1], got...)
	}
	return got, err
}

// Ref is the reference to a field: its stored path, the expression it was computed
// as, or the column it is selected under.
func (r *FieldRegistry) Ref(name string) sqlRef {
	if r.resolvesToStored(name) {
		return r.Stored(name)
	}
	entry := r.fields[name]
	sql := entry.ResolveAs
	if sql == "" {
		sql = entry.Expr
	}
	if sql == name {
		return r.Column(name)
	}
	form := refExpr
	if entry.column {
		form = refColumn
	}
	return r.record(sqlRef{sql: sql, name: name, form: form, deps: r.valueDeps(entry), cols: entry.readCols(), typ: r.typeOf(name)})
}

// Column is the reference to a field by its column name. Only the logs table's
// own columns exist in its scan, and only the model's own on its side of the join:
// any other column exists only where a SELECT computes it.
func (r *FieldRegistry) Column(name string) sqlRef {
	d := deps{scan: scanColumns[name]}
	if entry, ok := r.fields[name]; ok {
		v := r.valueDeps(entry)
		d.join, d.wrap, d.foreign = v.join, v.wrap, v.foreign
		d.scan = d.scan && entry.Kind == FieldKindBase
		d.model = v.model && entry.Kind == FieldKindJoined
	}
	t := typeUnknown
	if _, ok := r.fields[name]; ok {
		t = r.typeOf(name)
	}
	return r.record(sqlRef{sql: name, name: name, form: refColumn, deps: d, typ: t})
}

// Foreign records a read of another table, through a subquery.
func (r *FieldRegistry) Foreign(table string) {
	r.record(sqlRef{sql: table, name: table, form: refColumn, deps: deps{foreign: true}})
}

// scanColumns are the logs table's own columns, which a subquery reading the table
// with SELECT * exposes.
var scanColumns = map[string]bool{
	"timestamp": true, "log_id": true, "fractal_id": true, "fields": true,
	"ingest_timestamp": true, normLogColumn: true, "normalizer": true,
}

// columnRef is a column the translator itself names, such as timestamp or _count,
// which reads nothing else.
func columnRef(col string) sqlRef {
	return sqlRef{sql: col, name: col, form: refColumn, deps: deps{scan: scanColumns[col]}}
}

// Stored is a read of the stored log field name, whatever the registry holds under
// that name.
func (r *FieldRegistry) Stored(name string) sqlRef {
	return r.record(sqlRef{sql: r.fieldRef(name), name: name, form: refExpr, deps: deps{scan: true}, typ: typeText})
}

// jsonRef is a read of the stored field name as its JSON sub-column, whatever the
// registry holds under that name.
func (r *FieldRegistry) jsonRef(name string) sqlRef {
	return r.record(sqlRef{sql: jsonFieldRef(name), name: name, form: refExpr, deps: deps{scan: true}, typ: typeText})
}

// computed records a read of a computed field through the expression that computes
// it.
func (r *FieldRegistry) computed(name, sql string) sqlRef {
	d := deps{}
	if entry, ok := r.fields[name]; ok {
		d = r.valueDeps(entry)
	}
	return r.record(sqlRef{sql: sql, name: name, form: refExpr, deps: d})
}

// valueDeps is what a registered field's value reads.
func (r *FieldRegistry) valueDeps(entry *FieldEntry) deps {
	switch entry.Kind {
	case FieldKindBase:
		return deps{scan: scanColumns[entry.Name], model: false}
	case FieldKindJoined:
		if entry.readsKnown {
			return entry.reads
		}
		return deps{join: true}
	case FieldKindPerRow, FieldKindAssignment, FieldKindJSON:
		if entry.readsKnown {
			return entry.reads
		}
		// Being computed by the command running now, which records what it read
		// once it finishes; until then it reads at most what the command has.
		if rec := r.rec; rec != nil && len(rec.scopes) > 0 {
			d := noDeps
			for _, scope := range rec.scopes {
				d = d.and(scope.deps())
			}
			return d
		}
		if unrecordedHook != nil {
			unrecordedHook(entry.Name)
		}
	}
	return deps{}
}

// unrecordedHook, when set, sees a computed field read before what it reads was
// recorded. Tests fail on it: its dependencies would be assumed, not known.
var unrecordedHook func(name string)

// predicate builds a predicate in a scope of its own and records on it what it read.
func (r *FieldRegistry) predicate(build func() (predicate, error)) (predicate, error) {
	var p predicate
	got, err := r.track(func() error {
		var err error
		p, err = build()
		return err
	})
	p.deps = got.deps()
	return p, err
}

// isolated runs fn in a scope of its own that adds nothing to the open ones, and
// returns what it referenced.
func (r *FieldRegistry) isolated(fn func()) reads {
	got, _ := r.track(func() error { fn(); return nil })
	if n := len(r.rec.scopes); n > 0 {
		r.rec.scopes[n-1] = r.rec.scopes[n-1][:len(r.rec.scopes[n-1])-len(got)]
	}
	return got
}

// through is a reference as a layer above the scan reads it: a source expression
// from the hidden column the scan exports it as (see deferredScope), which is then
// all its SQL reads. Without a scope it is the reference itself.
func (r *FieldRegistry) through(scope *deferredScope, label string, resolve func() sqlRef) sqlRef {
	if scope == nil {
		return resolve()
	}
	var ref sqlRef
	got := r.isolated(func() { ref = resolve() })
	if _, named := r.fields[ref.name]; named && ref.deps.wrap && ref.form == refExpr {
		// Computed from a join()'s outputs, it is a column above the join's wrap
		// only: the scan cannot export it, and a layer above reads it by name.
		return r.Column(ref.name)
	}
	out, exported := scope.ref(ref, label)
	if exported {
		return r.record(out)
	}
	for _, g := range got {
		r.record(g)
	}
	return out
}

// readMark is a point in the innermost open scope, from which since lists what
// was referenced. A field marks before building its SQL, so what it reads is what
// was referenced since, whatever else its command reads.
type readMark struct{ depth, at int }

// mark is the current point in the innermost open scope.
func (r *FieldRegistry) mark() readMark {
	if r.rec == nil || len(r.rec.scopes) == 0 {
		return readMark{depth: -1}
	}
	n := len(r.rec.scopes) - 1
	return readMark{depth: n, at: len(r.rec.scopes[n])}
}

// since lists what was referenced in m's scope after m.
func (r *FieldRegistry) since(m readMark) reads {
	if m.depth < 0 || m.depth >= len(r.rec.scopes) {
		return nil
	}
	return slices.Clone(r.rec.scopes[m.depth][m.at:])
}

// define sets a field's expression and records what was referenced since m as
// what it reads.
func (r *FieldRegistry) define(name, expr string, m readMark) {
	r.SetResolveExpr(name, expr)
	r.setRead(name, r.since(m))
}
