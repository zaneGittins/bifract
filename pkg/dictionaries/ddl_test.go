package dictionaries

import (
	"regexp"
	"strings"
	"testing"

	"bifract/pkg/storage"
)

func ddlManager() *Manager {
	return &Manager{chDB: "logs", ch: &storage.ClickHouseClient{User: "bifract", Password: "secret"}}
}

var declaredAttrRe = regexp.MustCompile("(?m)^    `([^`]+)` String DEFAULT ''")

// declaredAttrs returns the attribute names a CREATE DICTIONARY declares, in order.
func declaredAttrs(ddl string) []string {
	var out []string
	for _, m := range declaredAttrRe.FindAllStringSubmatch(ddl, -1) {
		out = append(out, m[1])
	}
	return out
}

// selectedColumns returns the column names a plain source query selects, in
// order, with lower() unwrapped.
func selectedColumns(q string) []string {
	list := q[len("SELECT "):strings.Index(q, " FROM ")]
	var out []string
	for _, ref := range strings.Split(list, ", ") {
		ref = strings.TrimSuffix(strings.TrimPrefix(ref, "lower("), ")")
		out = append(out, strings.Trim(ref, "`"))
	}
	return out
}

// ClickHouse maps the source query's columns onto the declared structure by
// position. The declaration and the query are built by different functions, so
// this holds them to each other: for the list's own dictionary and for every
// secondary key, whatever column order the list was built with.
func TestDictionaryDeclarationMatchesSourceOrder(t *testing.T) {
	m := ddlManager()
	cols := []DictionaryColumn{{Name: "publisher"}, {Name: "name"}, {Name: "signer", IsKey: true}, {Name: "notes"}}
	dict := &Dictionary{ID: "abc", CHTableName: "dict_abc", CHDictName: "lookup_abc", KeyColumn: "name", Columns: cols}

	for _, ci := range []bool{false, true} {
		dict.CaseInsensitiveKeys = ci

		primary := m.primaryDictionaryDDL(dict, cols)
		want := selectedColumns(m.dictSourceQuery(dict, "name", cols))
		got := append([]string{"name"}, declaredAttrs(primary)...)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("case-insensitive=%v: primary declares %v, source selects %v", ci, got, want)
		}
		if !strings.Contains(primary, "PRIMARY KEY `name`") || !strings.Contains(primary, "LAYOUT(HASHED())") {
			t.Errorf("primary must be keyed by the list's key column on a hashed layout:\n%s", primary)
		}

		secondary := m.keyDictionaryDDL(dict, "signer", cols)
		want = selectedColumns(m.dictSourceQuery(dict, "signer", cols))
		got = append([]string{"signer"}, declaredAttrs(secondary)...)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("case-insensitive=%v: secondary declares %v, source selects %v", ci, got, want)
		}
		if !strings.Contains(secondary, "CREATE OR REPLACE DICTIONARY `lookup_abc_by_signer`") ||
			!strings.Contains(secondary, "PRIMARY KEY `signer`") {
			t.Errorf("secondary must be its own object keyed by its column:\n%s", secondary)
		}
	}
}

// A pattern list declares the match marker first, then the other columns, which
// is the order patternSourceQuery builds its key array in.
func TestPatternDictionaryDeclaresMarkerFirst(t *testing.T) {
	m := ddlManager()
	cols := []DictionaryColumn{{Name: "tool"}, {Name: "pattern"}, {Name: "risk"}}
	dict := &Dictionary{ID: "p", CHTableName: "dict_p", CHDictName: "lookup_p", KeyColumn: "pattern", Kind: KindPattern}

	ddl := m.primaryDictionaryDDL(dict, cols)
	if got := strings.Join(declaredAttrs(ddl), ","); got != PatternMatchAttr+",tool,risk" {
		t.Errorf("declared %s, want the marker then the attributes in order", got)
	}
	src := m.patternSourceQuery(dict, cols)
	keys := src[strings.Index(src, "[")+1 : strings.Index(src, "] AS keys")]
	if got := strings.ReplaceAll(keys, `\'`, ""); got != PatternMatchAttr+", tool, risk" {
		t.Errorf("source key array %s does not line up with the declaration", keys)
	}
	if !strings.Contains(ddl, "LAYOUT(REGEXP_TREE)") {
		t.Errorf("a pattern list must be a regexp tree:\n%s", ddl)
	}

	// A secondary key on a pattern list is an ordinary hashed lookup with no marker.
	sec := m.keyDictionaryDDL(dict, "tool", cols)
	if strings.Contains(sec, PatternMatchAttr) || !strings.Contains(sec, "LAYOUT(HASHED())") {
		t.Errorf("a secondary key must not inherit the pattern layout:\n%s", sec)
	}
}

// singleQuotedEnd walks a ClickHouse string literal opening at s[start] (a
// backslash escapes the next character) and returns the index of its closing
// quote, or -1.
func singleQuotedEnd(s string, start int) int {
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '\'':
			return i
		}
	}
	return -1
}

// The DDL embeds the ClickHouse credentials and a whole source query as string
// literals. Column names reach it from ingested log fields (a dictionary action
// creates a column per field), so no name and no credential may close a literal
// early and append to the statement.
func TestDictionaryDDLLiteralsCannotBeEscaped(t *testing.T) {
	m := &Manager{chDB: "logs", ch: &storage.ClickHouseClient{User: `u'ser`, Password: `p\' QUERY 'SELECT 1`}}
	hostile := `x')) LIFETIME(0) LAYOUT(FLAT()) --\`
	cols := []DictionaryColumn{{Name: "key"}, {Name: hostile}, {Name: "a'b"}}
	dict := &Dictionary{ID: "h", CHTableName: "dict_h", CHDictName: "lookup_h", KeyColumn: "key"}

	for name, ddl := range map[string]string{
		"primary":   m.primaryDictionaryDDL(dict, cols),
		"secondary": m.keyDictionaryDDL(dict, "a'b", cols),
	} {
		// USER, PASSWORD and QUERY are three literals in a row; each must close
		// right where the next clause starts.
		for _, clause := range []struct{ open, next string }{
			{"USER '", " PASSWORD '"},
			{"PASSWORD '", " QUERY '"},
			{"QUERY '", "))\nLIFETIME(MIN 0 MAX 300)"},
		} {
			start := strings.Index(ddl, clause.open) + len(clause.open) - 1
			end := singleQuotedEnd(ddl, start)
			if end < 0 || !strings.HasPrefix(ddl[end+1:], clause.next) {
				t.Errorf("%s: the literal after %q does not close where %q begins:\n%s", name, clause.open, clause.next, ddl)
			}
		}
	}
}

// Every node holds its own copy of a dictionary, and a remote shard runs with
// the default database, so a reload must name the database explicitly.
func TestReloadNamesTheDatabase(t *testing.T) {
	m := &Manager{chDB: "bifract_logs", ch: &storage.ClickHouseClient{}}
	if got, want := m.reloadSQL("lookup_abc"), "SYSTEM RELOAD DICTIONARY `bifract_logs`.`lookup_abc`"; got != want {
		t.Errorf("reloadSQL = %q, want %q", got, want)
	}
}
