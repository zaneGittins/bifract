package dictionaries

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The editor keeps rows where the user put them: a key already in the table
// keeps its ordinal, a renamed row takes the place of the key it replaced, and
// anything new is appended after the highest ordinal, in the order given.
func TestPlanSeqs(t *testing.T) {
	existing := map[string]uint64{"a": 3, "b": 7, "old": 5}
	rows := []DictionaryRow{
		{Key: "a", Fields: map[string]string{"k": "a"}},     // edit in place
		{Fields: map[string]string{"k": "new1"}},            // new
		{Key: "old", Fields: map[string]string{"k": "ren"}}, // rename
		{Fields: map[string]string{"k": "new2"}},            // new
		{Key: "a", Fields: map[string]string{"k": "b"}},     // renamed onto a key that exists
	}
	got := planSeqs("k", rows, existing, 10)
	if fmt.Sprint(got) != "[3 11 5 12 7]" {
		t.Errorf("ordinals %v, want [3 11 5 12 7]", got)
	}
}

// Both spellings of a renamed key are looked up, every key is escaped, and a
// load too large to look up row by row is numbered as new.
func TestSeqLookupQuery(t *testing.T) {
	m := &Manager{}
	dict := &Dictionary{ID: "d", CHTableName: "dict_d", KeyColumn: "k"}
	q := m.seqLookupQuery(dict, []DictionaryRow{
		{Key: "old", Fields: map[string]string{"k": "new"}},
		{Fields: map[string]string{"k": `o'brien\`}},
	})
	want := "SELECT `k`, `_bf_seq` FROM `dict_d` FINAL WHERE `k` IN ('old', 'new', 'o\\'brien\\\\')"
	if q != want {
		t.Errorf("query\n got %s\nwant %s", q, want)
	}

	if q := m.seqLookupQuery(dict, []DictionaryRow{{Fields: map[string]string{"other": "x"}}}); q != "" {
		t.Errorf("no keys to look up, got %s", q)
	}
	big := make([]DictionaryRow, maxSeqLookup+1)
	for i := range big {
		big[i] = DictionaryRow{Fields: map[string]string{"k": fmt.Sprint(i)}}
	}
	if q := m.seqLookupQuery(dict, big); q != "" {
		t.Error("a load past maxSeqLookup was looked up key by key")
	}
}

// The search box is free text inside a SQL string literal, searched across
// every column. It must stay one literal whatever it holds.
func TestRowsQueriesSearch(t *testing.T) {
	m := &Manager{distributed: true}
	dict := &Dictionary{ID: "d", CHTableName: "dict_d", KeyColumn: "ioc",
		Columns: []DictionaryColumn{{Name: "ioc"}, {Name: "note"}}}

	count, page := m.rowsQueries(dict, "", 50, 100)
	if !strings.HasPrefix(count, "SELECT count() FROM `dict_d_distributed` FINAL") || strings.Contains(count, "WHERE") {
		t.Errorf("count without search: %q", count)
	}
	if !strings.HasPrefix(page, "SELECT `ioc`, `note`, `_bf_seq` FROM `dict_d_distributed` FINAL") ||
		!strings.HasSuffix(page, "ORDER BY `_bf_seq` ASC, `ioc` ASC LIMIT 50 OFFSET 100") || strings.Contains(page, "WHERE") {
		t.Errorf("page without search: %q", page)
	}

	hostile := `x') > 0 OR 1=1) --\`
	count, page = m.rowsQueries(dict, hostile, 10, 0)
	for _, q := range []string{count, page} {
		for _, col := range []string{"ioc", "note"} {
			clause := "positionCaseInsensitive(`" + col + "`, '"
			start := strings.Index(q, clause)
			if start < 0 {
				t.Fatalf("search does not cover %s: %s", col, q)
			}
			end := singleQuotedEnd(q, start+len(clause)-1)
			if end < 0 || !strings.HasPrefix(q[end:], "') > 0") {
				t.Errorf("the search literal for %s does not close where it should: %s", col, q)
			}
		}
		if !strings.Contains(q, "WHERE (positionCaseInsensitive(`ioc`") || !strings.Contains(q, ") > 0 OR positionCaseInsensitive(`note`") {
			t.Errorf("search must OR across columns: %s", q)
		}
	}
}

// What the editor shows: every column as text, keyed by the key column, without
// Bifract's own ordinal.
func TestEditorRow(t *testing.T) {
	row := editorRow(map[string]interface{}{"ioc": "1.2.3.4", "hits": uint64(3), seqColumn: uint64(9)}, "ioc")
	if row.Key != "1.2.3.4" || row.Fields["hits"] != "3" || len(row.Fields) != 2 {
		t.Errorf("row = %+v", row)
	}
	if _, leaked := row.Fields[seqColumn]; leaked {
		t.Error("the ordinal reached the editor")
	}
}

// Arguments are positional, so each row carries its declared columns in order,
// then its own ordinal; a column the row lacks is inserted empty rather than
// shifting the rest.
func TestInsertStatement(t *testing.T) {
	m := &Manager{}
	dict := &Dictionary{ID: "d", CHTableName: "dict_d", KeyColumn: "ioc",
		Columns: []DictionaryColumn{{Name: "ioc"}, {Name: "source"}, {Name: "note"}}}
	rows := []DictionaryRow{
		{Fields: map[string]string{"ioc": "a", "source": "feed", "note": "x"}},
		{Fields: map[string]string{"ioc": "b", "note": "y", "unknown": "dropped"}},
	}
	sql, args := m.insertStatement(dict, rows, []uint64{4, 9})
	if sql != "INSERT INTO `dict_d` (`ioc`, `source`, `note`, `_bf_seq`) VALUES (?, ?, ?, ?), (?, ?, ?, ?)" {
		t.Errorf("sql = %s", sql)
	}
	if got := fmt.Sprint(args); got != "[a feed x 4 b  y 9]" {
		t.Errorf("args = %s", got)
	}

	// A cluster insert names the Distributed companion and waits for delivery.
	m.distributed = true
	sql, _ = m.insertStatement(dict, rows[:1], []uint64{1})
	if !strings.HasPrefix(sql, "INSERT INTO `dict_d_distributed` (") ||
		!strings.Contains(sql, ") SETTINGS distributed_foreground_insert = 1 VALUES (") {
		t.Errorf("cluster sql = %s", sql)
	}
}

// match() resolution: the key column maps to the list's own object and each
// promoted column to its own; layout flags are keyed by object, and only the
// list's own object carries one.
func TestScopeAdd(t *testing.T) {
	s := newScope()
	s.add("aa-1", "ips", "ip", []DictionaryColumn{{Name: "ip", IsKey: true}, {Name: "owner", IsKey: true}, {Name: "site"}}, false, KindNetwork)
	s.add("bb-2", "tools", "re", nil, false, KindPattern)
	s.add("cc-3", "users", "name", nil, true, "")

	if got := fmt.Sprint(s.Mappings["ips"]); got != "map[ip:lookup_aa_1 owner:lookup_aa_1_by_owner]" {
		t.Errorf("ips mappings = %s", got)
	}
	if !s.Network["lookup_aa_1"] || s.Network["lookup_aa_1_by_owner"] || len(s.Network) != 1 {
		t.Errorf("network flags = %v, want the primary object only", s.Network)
	}
	if !s.Pattern["lookup_bb_2"] || len(s.Pattern) != 1 {
		t.Errorf("pattern flags = %v", s.Pattern)
	}
	if !s.CaseInsensitive["users"] || s.CaseInsensitive["ips"] {
		t.Errorf("case-insensitive flags = %v", s.CaseInsensitive)
	}

	// Two rows with one name: every field comes from the later row, never a mix.
	s.add("dd-4", "users", "email", nil, false, KindValue)
	if s.Mappings["users"]["email"] != "lookup_dd_4" || s.Mappings["users"]["name"] != "" || s.CaseInsensitive["users"] {
		t.Errorf("a shadowing row left a mix: %v ci=%v", s.Mappings["users"], s.CaseInsensitive["users"])
	}
}

// fakeRow stands in for a Postgres row selected with dictColumns.
type fakeRow struct {
	cols, source, status []byte
	err                  error
}

func (f fakeRow) Scan(dest ...any) error {
	if f.err != nil {
		return f.err
	}
	if len(dest) != 17 {
		return fmt.Errorf("scanned into %d destinations", len(dest))
	}
	*dest[0].(*string) = "8c1168bb-23b0-44e5"
	*dest[1].(*string) = "lolrmm"
	*dest[7].(*string) = "path"
	*dest[8].(*[]byte) = f.cols
	*dest[11].(*string) = KindPattern
	*dest[13].(*time.Time) = time.Unix(0, 0)
	*dest[15].(*[]byte) = f.source
	*dest[16].(*[]byte) = f.status
	return nil
}

func TestScanDictionary(t *testing.T) {
	d, err := scanDictionary(fakeRow{
		cols:   []byte(`[{"name":"path","type":"string"},{"name":"tool","type":"string","is_key":true}]`),
		source: []byte(`{"url":"https://example.com/a.csv","key_field":"path","refresh_minutes":60}`),
		status: []byte(`{"error":"fetch failed","failures":2}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Columns) != 2 || !d.Columns[1].IsKey || d.Kind != KindPattern {
		t.Errorf("columns %+v kind %q", d.Columns, d.Kind)
	}
	if d.Source == nil || d.Source.URL != "https://example.com/a.csv" || d.SourceStatus.Failures != 2 {
		t.Errorf("source %+v status %+v", d.Source, d.SourceStatus)
	}
	// The ClickHouse names are derived, never stored.
	if d.CHTableName != "dict_8c1168bb_23b0_44e5" || d.CHDictName != "lookup_8c1168bb_23b0_44e5" {
		t.Errorf("derived names %q %q", d.CHTableName, d.CHDictName)
	}

	// A plain list has no source and no status: nil, so it reads as editable.
	plain, err := scanDictionary(fakeRow{cols: []byte(`[]`)})
	if err != nil || plain.Source != nil || plain.SourceStatus != nil || requireEditable(plain) != nil {
		t.Errorf("plain list: %+v, %v", plain, err)
	}
	// A sourced list never checked still has a status to fill in.
	fresh, err := scanDictionary(fakeRow{cols: []byte(`[]`), source: []byte(`{"url":"https://x.com/a"}`)})
	if err != nil || fresh.SourceStatus == nil {
		t.Errorf("sourced list without a status: %+v, %v", fresh, err)
	}

	// Columns that cannot be read come back as none rather than failing every
	// read of the list.
	broken, err := scanDictionary(fakeRow{cols: []byte(`not json`)})
	if err != nil || broken.Columns == nil || len(broken.Columns) != 0 {
		t.Errorf("unreadable columns: %+v, %v", broken.Columns, err)
	}
	// A source that cannot be read is an error: treating it as absent would
	// make a read-only list editable and stop it refreshing, with nothing said.
	if _, err := scanDictionary(fakeRow{cols: []byte(`[]`), source: []byte(`{`)}); err == nil {
		t.Error("an unreadable source was dropped silently")
	}
	scanErr := errors.New("no rows in result set")
	if _, err := scanDictionary(fakeRow{err: scanErr}); !errors.Is(err, scanErr) {
		t.Errorf("scan error = %v", err)
	}
}
