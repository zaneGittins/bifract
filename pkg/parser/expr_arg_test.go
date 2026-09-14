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
		{`* | match(dict="d", field=f, column=k, include=c1,c2)`, "match(): expects at most 0 positional"},
		{`* | lookupIP(field=src_ip, include=country,city)`, "expects at most 0 positional"},
		{`* | model_lookup(model="m", key=a,b)`, "expects at most 0 positional"},
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

// A command argument is captured as source text and re-lexed when it is resolved,
// so a string literal inside it has to keep its quotes. Without that,
// splitAt(path, "/", 2) came back as splitAt(path,/,2), which no longer parses.
func TestCapturedCallKeepsStringQuoting(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | groupby(splitAt(path, "/", 2))`, "splitByString('/', fields.`path`::String)"},
		{`* | table(splitAt(email, "@", 2))`, "splitByString('@', fields.`email`::String)"},
		{`* | table(replaceRegex(message, "a b", "c"))`, "replaceRegexpAll(fields.`message`::String, 'a b', 'c')"},
		{`* | concat([a, lower("x y")], as=k)`, "lower('x y')"},
	}
	for _, c := range cases {
		if sql := listSQL(t, c.query); !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql)
		}
	}
}

// Resolution returns only a string, so a broken expression there would fall back
// to a field reference and match nothing. validateExprArgs runs first so the
// author is told instead, in every position an expression can appear.
func TestBrokenExpressionArgumentIsRejected(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | groupby(lowr(user))`, "unknown function lowr(); did you mean lower()?"},
		{`* | table(lowr(user))`, "unknown function lowr()"},
		{`* | sort(lowr(user))`, "unknown function lowr()"},
		{`* | dedup(lowr(user))`, "unknown function lowr()"},
		{`* | concat([a, lowr(b)], as=k)`, "unknown function lowr()"},
		{`* | hash([a, lowr(b)], as=k)`, "unknown function lowr()"},
		{`* | groupby(user, function=sum(lowr(x)))`, "unknown function lowr()"},
		{`* | groupby(substr(image))`, "missing required argument start"},
		{`* | table(len(a, b))`, "expects at most 1 arguments"},
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

// The aggregate handlers report an unknown spec with advice of their own, so
// validation must not pre-empt them.
func TestAggregateSpecErrorsStayWithTheHandler(t *testing.T) {
	cases := []struct{ query, want string }{
		{`a=x | groupby(h, function=stats([avg(c,as=ac)]))`, "unknown aggregation function"},
		{`a=x | groupby(h, function=multi([avg(c,as=ac),bogus(c)]))`, "unknown aggregation function in multi()"},
	}
	for _, c := range cases {
		pipeline, err := ParseQuery(c.query)
		if err != nil {
			t.Fatalf("parse %q: %v", c.query, err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, listOpts())
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s\n  want %q, got: %v", c.query, c.want, err)
		}
	}
}
