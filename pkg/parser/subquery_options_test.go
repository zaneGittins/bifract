package parser

import (
	"reflect"
	"testing"
	"time"
)

// A join or result-set binding runs in the outer query's context: same scope,
// tables, dictionaries and models. Only what belongs to the outer query's own
// rows is left out. A new option is inherited unless it is listed here.
func TestSubqueryOptionsInheritTheOuterContext(t *testing.T) {
	notInherited := map[string]bool{
		"MaxRows":              true, // the subquery's own cap
		"HasCommentFilter":     true, // comment() IDs scope the outer rows' base WHERE
		"CommentLogIDs":        true,
		"HasTLSHFilter":        true, // resolved for the outer pipeline's tlsh()
		"TLSHMatches":          true,
		"AlertExtraFields":     true, // outer projection
		"IncludeShardNum":      true,
		"IncludeIngestTime":    true,
		"SourceSubquery":       true, // the outer pipeline's source command
		"SourceColumns":        true,
		"SourceNumericColumns": true,
		"SourceOrderBy":        true,
	}

	var outer QueryOptions
	work := 3
	outer.bindingWork = &work
	v := reflect.ValueOf(&outer).Elem()
	for i := 0; i < v.NumField(); i++ {
		if f := v.Field(i); f.CanSet() {
			f.Set(nonZero(t, v.Type().Field(i)))
		}
	}

	got := subqueryOptions(&CommandContext{Opts: outer}, 42)
	gv := reflect.ValueOf(got)
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		if !v.Field(i).CanInterface() {
			continue
		}
		switch {
		case name == "MaxRows":
			if got.MaxRows != 42 {
				t.Errorf("MaxRows = %d, want the subquery's own 42", got.MaxRows)
			}
		case notInherited[name]:
			if !gv.Field(i).IsZero() {
				t.Errorf("%s inherited, want it left out", name)
			}
		default:
			if !reflect.DeepEqual(gv.Field(i).Interface(), v.Field(i).Interface()) {
				t.Errorf("%s not inherited", name)
			}
		}
	}
	if got.bindingWork != outer.bindingWork {
		t.Error("bindingWork not shared with the outer query")
	}
}

// nonZero returns a non-zero value for an option field.
func nonZero(t *testing.T, f reflect.StructField) reflect.Value {
	t.Helper()
	typ := f.Type
	switch typ {
	case reflect.TypeOf(time.Time{}):
		return reflect.ValueOf(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	}
	switch typ.Kind() {
	case reflect.Bool:
		return reflect.ValueOf(true).Convert(typ)
	case reflect.Int, reflect.Int64:
		return reflect.ValueOf(7).Convert(typ)
	case reflect.String:
		return reflect.ValueOf("x").Convert(typ)
	case reflect.Slice:
		s := reflect.MakeSlice(typ, 1, 1)
		return s
	case reflect.Map:
		m := reflect.MakeMap(typ)
		m.SetMapIndex(reflect.Zero(typ.Key()).Convert(typ.Key()), reflect.Zero(typ.Elem()))
		return m
	}
	t.Fatalf("no non-zero value for %s %s", f.Name, typ)
	return reflect.Value{}
}
