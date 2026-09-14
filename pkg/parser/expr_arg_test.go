package parser

import (
	"strings"
	"testing"
	"time"
)

func listOpts() QueryOptions {
	return QueryOptions{
		StartTime:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:            1000,
		FractalID:          "f1",
		DictionaryDatabase: "bifract",
		Dictionaries:       map[string]map[string]string{"d": {"k": "dict_d_k"}},
		Models: map[string]AnalyticsModelInfo{
			"m": {ID: "i", TableName: "model_m", ModelType: "rarity", MinSample: 1, FractalID: "f1"},
		},
		GeoIPEnabled: true,
	}
}

func listSQL(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, listOpts())
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return result.SQL
}

// A bracket list reaches a handler as one comma-joined argument, so a handler
// that forgets to split it reads a field literally named "a,b". hash() did
// exactly that and gave every row the same digest. Asserting the two spellings
// agree catches the whole class rather than one instance.
func TestListArgumentFormsAgree(t *testing.T) {
	pairs := [][2]string{
		{`* | hash(a, b, as=k)`, `* | hash([a,b], as=k)`},
		{`* | concat(a, b, as=k)`, `* | concat([a,b], as=k)`},
		{`* | table(a, b)`, `* | table([a,b])`},
		{`* | dedup(a, b)`, `* | dedup([a,b])`},
		{`* | groupby(a, b)`, `* | groupby([a,b])`},
		{`* | coalesce(a, b)`, `* | coalesce([a,b])`},
		{`* | analyzeFields(a, b)`, `* | analyzeFields([a,b])`},
	}
	for _, p := range pairs {
		if bare, bracket := listSQL(t, p[0]), listSQL(t, p[1]); bare != bracket {
			t.Errorf("%s and %s disagree\n  bare:    %s\n  bracket: %s", p[0], p[1], bare, bracket)
		}
	}
}

// Commas inside a call are not list separators. Splitting on every comma tore
// substr(image, 1, 10) into three fields, which matters now that a field
// position accepts an expression.
func TestExpressionsSurviveListSplitting(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | table(a, substr(b,1,2))`, "substring(fields.`b`::String, 1, 2)"},
		{`* | concat([a, substr(b,1,2)], as=k)`, "concat(fields.`a`::String, substring(fields.`b`::String, 1, 2))"},
		{`* | hash([a, substr(b,1,2)], as=k)`, "cityHash64(fields.`a`::String, substring(fields.`b`::String, 1, 2))"},
		{`* | groupby([a, lower(b)])`, "lower(fields.`b`::String) AS lower_b"},
		{`* | coalesce(a, lower(b))`, "lower(fields.`b`::String)"},
	}
	for _, c := range cases {
		if sql := listSQL(t, c.query); !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql)
		}
	}
}

// A named list ends at its first comma unless bracketed, so `include=a,b` leaves
// `b` as a stray argument. Silently ignoring it dropped a column the author
// asked for; every named-only command now says so instead.
func TestStrayArgumentIsRejected(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | match(dict="d", field=f, column=k, include=c1,c2)`, "match(): unexpected argument"},
		{`* | lookupIP(field=src_ip, include=country,city)`, "lookupIP(): unexpected argument"},
		{`* | model_lookup(model="m", key=a,b)`, "model_lookup(): unexpected argument"},
	}
	for _, c := range cases {
		pipeline, err := ParseQuery(c.query)
		if err != nil {
			t.Fatalf("parse %q: %v", c.query, err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, listOpts())
		if err == nil {
			t.Errorf("%s: expected a rejection, got none", c.query)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s\n  want %q, got: %v", c.query, c.want, err)
		}
	}
}

func TestListArgHelper(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"a,b", []string{"a", "b"}},
		{"[a,b]", []string{"a", "b"}},
		{" [ a , b ] ", []string{"a", "b"}},
		{"substr(b,1,2)", []string{"substr(b,1,2)"}},
		{"a,substr(b,1,2),c", []string{"a", "substr(b,1,2)", "c"}},
		{`"200","404"`, []string{"200", "404"}},
		{"a,,b", []string{"a", "b"}},
		{"", nil},
	}
	for _, c := range cases {
		got := listArg(c.in)
		if len(got) != len(c.want) {
			t.Errorf("listArg(%q) = %q, want %q", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("listArg(%q) = %q, want %q", c.in, got, c.want)
				break
			}
		}
	}
}
