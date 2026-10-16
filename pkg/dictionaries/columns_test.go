package dictionaries

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// A column name becomes a ClickHouse identifier and a match() argument, so the
// editor and a CSV header are held to one rule, and Bifract's own _bf_ columns
// cannot be shadowed. Each refusal is reported as the caller's to fix and
// leaves the definition as it was.
func TestAddColumnRefusals(t *testing.T) {
	m := &Manager{}
	cases := []struct {
		name, col, kind, want string
	}{
		{"duplicate", "ioc", KindValue, "already exists"},
		{"leading digit", "1st", KindValue, "invalid column name"},
		{"space", "user name", KindValue, "invalid column name"},
		{"dot", "winlog.user", KindValue, "invalid column name"},
		{"backtick", "a`b", KindValue, "invalid column name"},
		{"empty", "", KindValue, "invalid column name"},
		{"too long", "a" + strings.Repeat("b", 255), KindValue, "invalid column name"},
		{"row order column", seqColumn, KindValue, "_bf_"},
		{"reserved prefix any case", "_BF_note", KindValue, "_bf_"},
		{"pattern marker", PatternMatchAttr, KindPattern, "reserved"},
	}
	for _, c := range cases {
		dict := &Dictionary{ID: "d", KeyColumn: "ioc", Kind: c.kind, Columns: []DictionaryColumn{{Name: "ioc"}}}
		_, err := m.addColumn(context.Background(), dict, c.col)
		if err == nil || !IsInvalidInput(err) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want an invalid-input error mentioning %q", c.name, err, c.want)
		}
		if len(dict.Columns) != 1 {
			t.Errorf("%s: a refused column was still added to the definition: %+v", c.name, dict.Columns)
		}
	}
}

func TestValidIdentifier(t *testing.T) {
	for _, ok := range []string{"a", "_x", "col-1", "A9_z", "a" + strings.Repeat("b", 254)} {
		if !isValidIdentifier(ok) {
			t.Errorf("%q must be accepted", ok)
		}
	}
	for _, bad := range []string{"", "-a", "9a", "a b", "a.b", "a`b", "a'b", "é", "a" + strings.Repeat("b", 255)} {
		if isValidIdentifier(bad) {
			t.Errorf("%q must be refused", bad)
		}
	}
}

func TestRemoveColumnRefusals(t *testing.T) {
	m := &Manager{}
	cols := []DictionaryColumn{{Name: "ioc"}, {Name: "source"}}
	cases := []struct {
		name, col, want string
	}{
		{"key column", "ioc", "cannot remove the key column"},
		{"unknown column", "nope", `column "nope" not found`},
		{"not an identifier", "a`b", "invalid column name"},
	}
	for _, c := range cases {
		dict := &Dictionary{ID: "d", KeyColumn: "ioc", Columns: slices.Clone(cols)}
		_, err := m.removeColumn(context.Background(), dict, c.col)
		if err == nil || !IsInvalidInput(err) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want an invalid-input error mentioning %q", c.name, err, c.want)
		}
		if len(dict.Columns) != 2 {
			t.Errorf("%s: a refused removal changed the definition: %+v", c.name, dict.Columns)
		}
	}
}

// A blank key can never be found again, so it is refused before anything is read.
func TestDeleteRowNeedsAKey(t *testing.T) {
	err := (&Manager{}).DeleteRow(context.Background(), "d", "")
	if err == nil || !IsInvalidInput(err) {
		t.Errorf("err = %v, want an invalid-input error", err)
	}
}

// keyColumns drives which dictionary objects a reload refreshes and a collision
// check scans: the primary key first, then each promoted column once.
func TestKeyColumns(t *testing.T) {
	dict := &Dictionary{KeyColumn: "ioc", Columns: []DictionaryColumn{
		{Name: "ioc", IsKey: true}, // the primary, even if flagged, is listed once
		{Name: "source"},
		{Name: "hash", IsKey: true},
		{Name: "host", IsKey: true},
	}}
	if got := strings.Join((&Manager{}).keyColumns(dict), ","); got != "ioc,hash,host" {
		t.Errorf("keyColumns = %s, want ioc,hash,host", got)
	}
}

// ClickHouse returns count() as UInt64 and other integer widths elsewhere; an
// unrecognised value counts as zero rather than panicking a request.
func TestNumericConversions(t *testing.T) {
	for _, c := range []struct {
		in   any
		want int64
	}{
		{uint64(7), 7}, {int64(-3), -3}, {uint32(5), 5}, {int32(-2), -2}, {"7", 0}, {nil, 0}, {7.0, 0},
	} {
		if got := toInt64(c.in); got != c.want {
			t.Errorf("toInt64(%#v) = %d, want %d", c.in, got, c.want)
		}
	}
	// An ordinal is never negative: a negative reading is no ordinal at all.
	for _, c := range []struct {
		in   any
		want uint64
	}{
		{uint64(9), 9}, {int64(4), 4}, {int64(-4), 0}, {int64(0), 0}, {uint32(3), 3}, {"9", 0}, {nil, 0},
	} {
		if got := toUint64(c.in); got != c.want {
			t.Errorf("toUint64(%#v) = %d, want %d", c.in, got, c.want)
		}
	}
}

// groupUniqArray comes back as []string from the native driver and as []any
// through some paths; either reads as the strings it holds.
func TestToStrings(t *testing.T) {
	if got := toStrings([]string{"Admin", "admin"}); strings.Join(got, ",") != "Admin,admin" {
		t.Errorf("[]string: %v", got)
	}
	if got := toStrings([]any{"Admin", 7, "admin"}); strings.Join(got, ",") != "Admin,admin" {
		t.Errorf("[]any: %v, want the strings only", got)
	}
	if got := toStrings("Admin"); got != nil {
		t.Errorf("a scalar: %v, want nil", got)
	}
}
