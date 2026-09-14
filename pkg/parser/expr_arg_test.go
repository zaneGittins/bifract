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

// A named list written without brackets used to end at its first comma, and the
// rest of the values were dropped or rejected as stray arguments. The typed
// parser reads the whole run, so every value the author asked for survives.
func TestUnbracketedNamedListKeepsEveryValue(t *testing.T) {
	cases := []struct {
		query string
		want  []string
	}{
		{`* | match(dict="d", field=f, column=k, include=c1,c2)`, []string{"c1", "c2"}},
		{`* | model_lookup(model="m", key=a,b)`, []string{"_mlk_k0", "_mlk_k1"}},
	}
	for _, c := range cases {
		pipeline, err := ParseQuery(c.query)
		if err != nil {
			t.Fatalf("parse %q: %v", c.query, err)
		}
		sql, err := TranslateToSQLWithOrder(pipeline, listOpts())
		if err != nil {
			t.Errorf("%s: %v", c.query, err)
			continue
		}
		for _, want := range c.want {
			if !strings.Contains(sql.SQL, want) {
				t.Errorf("%s: %q missing from the SQL", c.query, want)
			}
		}
	}
}

// A string literal inside a command argument keeps its quoting all the way to
// the compiler. When arguments round-tripped through text, splitAt(path, "/", 2)
// came back as splitAt(path,/,2), which no longer parses.
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
