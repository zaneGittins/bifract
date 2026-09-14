package parser

import (
	"strings"
	"testing"
)

// A parameter may be written by name or by position, so a bare value after a
// named one has to fill the next parameter still open, not re-fill the named
// one. Every spelling here worked before command arguments became typed.
func TestNamedThenPositionalArguments(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | sort(field=bytes, desc)`, "ORDER BY fields.`bytes`::String DESC"},
		{`* | cidr(field=src_ip, "10.0.0.0/8")`, "isIPAddressInRange"},
		{`* | substr(field=image, 1, 10)`, "substring(fields.`image`::String, 1, 10)"},
		{`* | split(field=path, "/", 2)`, "splitByString('/', fields.`path`::String)[2]"},
		{`* | headTail(field=src_ip, 90)`, "fields.`src_ip`::String AS value"},
		{`* | replace(pattern="p", "r", commandline)`, "replaceRegexpAll(fields.`commandline`::String, 'p', 'r')"},
		{`* | bucket(span=1h, count())`, "COUNT(*) AS bucket_count"},
		{`* | lowercase(field=user, out)`, "lower(fields.`user`::String) AS out"},
	}
	for _, c := range cases {
		pipeline, err := ParseQuery(c.query)
		if err != nil {
			t.Errorf("parse %q: %v", c.query, err)
			continue
		}
		sql, err := TranslateToSQLWithOrder(pipeline, revOpts())
		if err != nil {
			t.Errorf("translate %q: %v", c.query, err)
			continue
		}
		if !strings.Contains(sql.SQL, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql.SQL)
		}
	}
}

// A name= with no value must not read the next argument, or the query text
// before the command, as its value.
func TestEmptyNamedValueTakesNothing(t *testing.T) {
	for _, q := range []string{`user="x" | sort(field=)`, `user="x" | hash(field=)`, `user="x" | sum(field=)`} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			continue
		}
		sql, err := TranslateToSQLWithOrder(pipeline, revOpts())
		if err != nil {
			continue
		}
		// The filter belongs in WHERE only. Finding it inside a projection or an
		// ORDER BY means the empty value swallowed it.
		if strings.Count(sql.SQL, "fields.`user`::String = 'x'") > 1 {
			t.Errorf("%s: the empty value re-parsed the query: %s", q, sql.SQL)
		}
	}
}

// A field position accepts an expression everywhere, not only where a handler
// happens to compile one.
func TestExpressionInEveryFieldPosition(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | len(lower(user))`, "length(lower(fields.`user`::String))"},
		{`* | logSize(lower(user))`, "byteSize(lower(fields.`user`::String))"},
		{`* | levenshtein(lower(user), "admin")`, "damerauLevenshteinDistance(lower(fields.`user`::String), 'admin')"},
		{`* | heatmap(x=lower(user), y=host)`, "lower(fields.`user`::String) AS _heatmap_x"},
		{`* | top(lower(user))`, "topK(10)(lower(fields.`user`::String)) AS top_lower_user"},
		{`* | count(lower(user), unique=true)`, "uniqExact(lower(fields.`user`::String))"},
		{`* | strftime("%Y-%m-%d", field=lower(ts))`, "lower(fields.`ts`::String)"},
		{`* | timechart(span=1h, groupby(lower(user), distinct=true))`, "uniqExact(lower(fields.`user`::String))"},
	}
	for _, c := range cases {
		pipeline, err := ParseQuery(c.query)
		if err != nil {
			t.Errorf("parse %q: %v", c.query, err)
			continue
		}
		sql, err := TranslateToSQLWithOrder(pipeline, revOpts())
		if err != nil {
			t.Errorf("translate %q: %v", c.query, err)
			continue
		}
		if !strings.Contains(sql.SQL, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql.SQL)
		}
	}
}

// An argument a handler cannot honour must be reported. Each of these used to
// run at a default, drop the value, or emit SQL the server rejects.
func TestUnusableArgumentsAreReported(t *testing.T) {
	for _, q := range []string{
		`* | head(-5)`, `* | tail(-5)`, `* | limit(-5)`,
		`* | timechart(span=1h, function=bogus(x))`,
		`* | bucket(span=1h, function=bogus(x))`,
		`* | bucket(span=1h, function=avg(bytes))`,
		`* | heatmap(x=a, y=b, value=bogus(x))`,
		`* | timechart(span=1h, groupby("user, 1 AS y"))`,
		`* | join(user, max=) { event_id="1" | groupby(user) }`,
		`* | groupby(user) | count() | dedup(lower(user))`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			continue
		}
		if _, err := TranslateToSQLWithOrder(pipeline, revOpts()); err == nil {
			t.Errorf("%s: accepted silently", q)
		}
	}
}

// A quoted comma-separated list is still a list, and a filter given an empty
// range stays a filter rather than vanishing.
func TestListAndFilterValuesSurvive(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | hash("a,b")`, "cityHash64(fields.`a`::String, fields.`b`::String)"},
		{`* | concat("a,b")`, "concat(fields.`a`::String, fields.`b`::String)"},
		{`* | cidr(src_ip, "")`, "isIPAddressInRange"},
		{`* | chain([user,host], within=5m) { a="x"; b="y" }`, "[fields.`user`::String, fields.`host`::String]"},
		{`* | chain("user,host", within=5m) { a="x"; b="y" }`, "[fields.`user`::String, fields.`host`::String]"},
	}
	for _, c := range cases {
		pipeline, err := ParseQuery(c.query)
		if err != nil {
			t.Errorf("parse %q: %v", c.query, err)
			continue
		}
		sql, err := TranslateToSQLWithOrder(pipeline, revOpts())
		if err != nil {
			t.Errorf("translate %q: %v", c.query, err)
			continue
		}
		if !strings.Contains(sql.SQL, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql.SQL)
		}
	}
}
