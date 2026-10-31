package parser

import (
	"strings"
	"testing"
)

func translateErr(t *testing.T, q string, opts QueryOptions) (*TranslationResult, error) {
	t.Helper()
	pl, err := ParseQuery(q)
	if err != nil {
		t.Fatalf("parse %s: %v", q, err)
	}
	return TranslateToSQLWithOrder(pl, opts)
}

// Only one join() is planned; a second must fail rather than replace the first.
func TestSecondJoinIsRejected(t *testing.T) {
	_, err := translateErr(t, `proto="tcp" | join(dst_ip) { proto="udp" | groupby(dst_ip) } | join(src_ip) { proto="udp" | groupby(src_ip) }`, QueryOptions{MaxRows: 10})
	if err == nil || !strings.Contains(err.Error(), "one join()") {
		t.Errorf("err = %v", err)
	}
}

// ptg() returns its tree as is: after it, only _depth/_path filters and pgraph() apply, so
// anything else fails instead of being dropped.
func TestPtgRejectsWhatItCannotApply(t *testing.T) {
	opts := QueryOptions{MaxRows: 10, ProcessTree: &ProcessTree{}}
	for _, q := range []string{
		`ptg(start="x") | table(image)`,
		`ptg(start="x") | sort(image)`,
		`ptg(start="x") | image="cmd.exe"`,
		`ptg(start="x") | _depth<=2 OR image="cmd.exe"`,
		`ptg(start="x") | limit(5)`,
	} {
		if _, err := translateErr(t, q, opts); err == nil || !strings.Contains(err.Error(), "ptg()") {
			t.Errorf("%s: err = %v", q, err)
		}
	}
	for _, q := range []string{
		`ptg(start="x")`,
		`* | ptg(start="x") | _depth<=2 | pgraph()`,
		`ptg(start="x") | _path=/explorer/ AND _depth>1`,
	} {
		if _, err := translateErr(t, q, opts); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// distinct= counts the distinct values of the last key per group of the others, as it does
// in timechart; grouping by that key as well always counted 1.
func TestGroupByDistinctCountsTheLastKeyPerGroup(t *testing.T) {
	r, err := translateErr(t, `* | groupby(src_ip, dst_ip, distinct=true)`, QueryOptions{MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.SQL, "COUNT(DISTINCT fields.`dst_ip`::String) AS _count") || !strings.Contains(r.SQL, "GROUP BY src_ip ") || strings.Contains(r.SQL, "AS dst_ip") {
		t.Errorf("SQL: %s", r.SQL)
	}
	if strings.Join(r.FieldOrder, ",") != "src_ip,_count" {
		t.Errorf("fields %v", r.FieldOrder)
	}

	r, err = translateErr(t, `* | groupby(user, distinct=true)`, QueryOptions{MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.SQL, "COUNT(DISTINCT fields.`user`::String) AS _count") || strings.Contains(r.SQL, "GROUP BY") || strings.Join(r.FieldOrder, ",") != "_count" {
		t.Errorf("one key counts its distinct values overall: %s %v", r.SQL, r.FieldOrder)
	}
}

// strftime() takes ClickHouse formatDateTime specifiers as is. %R is HH:MM there, so it must
// not be rewritten through %M (the month name); strftime's %A and %B, which ClickHouse lacks,
// mean its %W and %M; %% stays a literal percent.
func TestConvertTimeFormat(t *testing.T) {
	for in, want := range map[string]string{
		"%R":              "%R",
		"%H:%i":           "%H:%i",
		"%M":              "%M",
		"%A %d %B %Y, %R": "%W %d %M %Y, %R",
		"100%% %B":        "100%% %M",
		"%%B is literal":  "%%B is literal",
		"trailing %":      "trailing %",
	} {
		if got := convertTimeFormat(in); got != want {
			t.Errorf("convertTimeFormat(%q) = %q, want %q", in, got, want)
		}
	}
}

// The camelCase spelling places like model_lookup(): the enrichment docs group its output.
func TestModelLookupCamelCaseBeforeGroupBy(t *testing.T) {
	for _, name := range []string{"modelLookup", "model_lookup"} {
		if _, err := translateErr(t, `* | `+name+`(model="fs", key=[user]) | groupby(is_new)`, mlookupOpts()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
