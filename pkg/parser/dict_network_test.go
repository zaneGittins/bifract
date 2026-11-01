package parser

import (
	"strings"
	"testing"
	"time"
)

func networkDictOpts() QueryOptions {
	return QueryOptions{
		StartTime:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:            1000,
		FractalID:          "f1",
		DictionaryDatabase: "bifract",
		Dictionaries: map[string]map[string]string{
			"corp":  {"network": "dict_corp_network", "owner": "dict_corp_by_owner"},
			"names": {"name": "dict_names_name"},
			"tools": {"pattern": "dict_tools_pattern"},
		},
		// Both keyed by the ClickHouse object, not the list: only a list's own
		// dictionary carries its layout.
		NetworkDicts:         map[string]bool{"dict_corp_network": true},
		PatternDicts:         map[string]bool{"dict_tools_pattern": true},
		CaseInsensitiveDicts: map[string]bool{"names": true},
	}
}

func networkSQL(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, networkDictOpts())
	if err != nil {
		t.Fatalf("translate %s: %v", query, err)
	}
	return result.SQL
}

// An IP_TRIE key is a range, so it is probed with an address. ClickHouse rejects
// a string probe outright, so this is the difference between a working lookup and
// a query that will not run.
func TestNetworkDictionaryIsProbedWithAnAddress(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="corp", field=src_ip, column=network, include=[owner])`)
	if !strings.Contains(sql, "toIPv6OrDefault(toString(fields.`src_ip`::String))") {
		t.Errorf("want an address probe, got: %s", sql)
	}
}

// A value list is unchanged: the probe is the string, lowered when the keys were
// hashed lowered.
func TestValueDictionaryStillProbesWithAString(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="names", field=user, column=name, include=[tier])`)
	if !strings.Contains(sql, "lower(toString(fields.`user`::String))") {
		t.Errorf("want a lowered string probe, got: %s", sql)
	}
	if strings.Contains(sql, "toIPv6") {
		t.Errorf("a value list must not be probed with an address: %s", sql)
	}
}

// OrDefault, not a plain cast: the field holds whatever the log carried, and an
// unparseable value has to miss rather than fail the whole query.
func TestNetworkProbeToleratesANonAddress(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="corp", field=user, column=network, include=[owner])`)
	if !strings.Contains(sql, "toIPv6OrDefault(") {
		t.Errorf("want a tolerant cast, got: %s", sql)
	}
}

// Asking for the key column reports membership, which dictHas answers on an
// IP_TRIE as well as on a HASHED dictionary.
func TestNetworkMembershipUsesDictHas(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="corp", field=src_ip, column=network, include=[network])`)
	if !strings.Contains(sql, "dictHas('bifract.dict_corp_network', toIPv6OrDefault(") {
		t.Errorf("want a membership test probed with an address, got: %s", sql)
	}
}

// A REGEXP_TREE has no dictHas, so membership is read from the marker attribute
// every pattern list carries. Reading an ordinary attribute could not tell a row
// that matched but holds an empty value from one that did not match at all.
func TestPatternDictionaryMembershipAvoidsDictHas(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="tools", field=commandline, column=pattern, include=[pattern])`)
	if strings.Contains(sql, "dictHas(") {
		t.Errorf("a pattern list has no dictHas: %s", sql)
	}
	if !strings.Contains(sql, `dictGetOrDefault('bifract.dict_tools_pattern', '_match'`) {
		t.Errorf("want the match marker read, got: %s", sql)
	}
	if !strings.Contains(sql, "= '1'") {
		t.Errorf("want the marker compared to its set value, got: %s", sql)
	}
	if strings.Contains(sql, "tupleElement") {
		t.Errorf("a lone marker lookup needs no tuple: %s", sql)
	}
}

// A pattern is matched against the string as the log carries it, so the probe is
// unchanged. Only membership differs from a value list.
func TestPatternDictionaryProbesWithAString(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="tools", field=commandline, column=pattern, include=[tool])`)
	if !strings.Contains(sql, "dictGetOrDefault('bifract.dict_tools_pattern', ('_match', 'tool'), toString(fields.`commandline`::String)") {
		t.Errorf("want a plain string probe, got: %s", sql)
	}
	if strings.Contains(sql, "toIPv6") {
		t.Errorf("a pattern list must not be probed with an address: %s", sql)
	}
}

// strict= emitted dictHas unconditionally, which a REGEXP_TREE refuses outright
// ("does not support method hasKeys"), so every strict lookup against a pattern
// list failed at the server. Membership has to go through one expression.
func TestPatternStrictDoesNotUseDictHas(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="tools", field=commandline, column=pattern, include=[tool], strict=true)`)
	if strings.Contains(sql, "dictHas(") {
		t.Errorf("a pattern list cannot take dictHas anywhere: %s", sql)
	}
	if !strings.Contains(sql, "tupleElement(identity(dictGetOrDefault('bifract.dict_tools_pattern', ('_match', 'tool'), toString(fields.`commandline`::String), ('', ''))), 1) = '1'") {
		t.Errorf("want strict to test the marker, got: %s", sql)
	}
}

// Every other kind keeps dictHas, which is cheaper and is what an IP_TRIE and a
// HASHED dictionary both support.
func TestNonPatternStrictStillUsesDictHas(t *testing.T) {
	for _, q := range []string{
		`* | match(dict="corp", field=src_ip, column=network, include=[owner], strict=true)`,
		`* | match(dict="names", field=user, column=name, include=[tier], strict=true)`,
	} {
		if sql := networkSQL(t, q); !strings.Contains(sql, "dictHas(") {
			t.Errorf("%s: want dictHas, got: %s", q, sql)
		}
	}
}

// Only a pattern list's own dictionary is a REGEXP_TREE. A secondary key column
// resolves to an ordinary HASHED one, which carries no marker attribute, so
// reading the marker there reported every row as a miss.
func TestSecondaryKeyOnAPatternListIsNotTreatedAsAPattern(t *testing.T) {
	opts := networkDictOpts()
	opts.Dictionaries["tools"]["tool"] = "dict_tools_by_tool"
	pipeline, err := ParseQuery(`* | match(dict="tools", field=commandline, column=tool, include=[tool])`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if strings.Contains(result.SQL, "'_match'") {
		t.Errorf("a secondary key column has no marker attribute: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "dictHas('bifract.dict_tools_by_tool'") {
		t.Errorf("want an ordinary membership test, got: %s", result.SQL)
	}
}

// Expressions are stored as written, so lowering the probe would stop every one
// carrying an upper-case letter from ever matching. A pattern says it for itself
// with (?i).
func TestPatternProbeIsNeverLowered(t *testing.T) {
	opts := networkDictOpts()
	opts.CaseInsensitiveDicts["tools"] = true
	pipeline, err := ParseQuery(`* | match(dict="tools", field=commandline, column=pattern, include=[tool])`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if strings.Contains(result.SQL, "lower(toString(fields.`commandline`") {
		t.Errorf("a pattern probe must not be lowered: %s", result.SQL)
	}
}

// Every field resolves to a String, and ClickHouse's printf rejects a String for
// a numeric conversion, so a format using %d failed at the server for every row.
func TestSprintfCastsArgumentsToTheirConversion(t *testing.T) {
	sql := networkSQL(t, `* | sprintf("https://%s:%d/%.2f", host, port, latency, as=u)`)
	for _, want := range []string{
		"ifNull(fields.`host`::String, '')",
		"toInt64OrZero(ifNull(toString(fields.`port`::String), ''))",
		"toFloat64OrZero(ifNull(toString(fields.`latency`::String), ''))",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("want %s, got: %s", want, sql)
		}
	}
}

func TestPrintfVerbsSkipsEscapedPercent(t *testing.T) {
	cases := map[string]string{
		"%s:%d":        "sd",
		"100%% of %s":  "s",
		"%-10.3f|%05d": "fd",
		"no verbs":     "",
		"trailing %":   "",
	}
	for format, want := range cases {
		if got := string(printfVerbs(format)); got != want {
			t.Errorf("printfVerbs(%q) = %q, want %q", format, got, want)
		}
	}
}

// A secondary key column on a network list is an ordinary HASHED dictionary over
// that column's values, so it is probed with a string. Reading the layout from the
// list rather than from the object cast the probe to an address and the lookup
// could only fail at the server.
func TestSecondaryKeyOnANetworkListProbesWithAString(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="corp", field=user, column=owner, include=[network])`)
	if strings.Contains(sql, "toIPv6") {
		t.Errorf("a HASHED key column must not be probed with an address: %s", sql)
	}
	if !strings.Contains(sql, "dictGetOrDefault('bifract.dict_corp_by_owner', 'network', toString(fields.`user`::String)") {
		t.Errorf("want a string probe against the secondary dictionary, got: %s", sql)
	}
}

// A join's subquery probes a network list the same way the outer query does.
func TestNetworkDictionaryInsideAJoin(t *testing.T) {
	sql := networkSQL(t, `* | join(key=user, include=[owner]) { * | match(dict="corp", field=src_ip, column=network, include=[owner]) | groupby(user, owner) }`)
	if !strings.Contains(sql, "toIPv6OrDefault(toString(fields.`src_ip`::String))") {
		t.Errorf("want an address probe inside the join, got: %s", sql)
	}
}

// A REGEXP_TREE runs every expression it cannot reduce to a literal, which is
// every (?i) one, through RE2 one by one on each row. With a prefilter, a single
// Hyperscan pass over the whole list goes first, and only a row it passes pays
// for the dictionary.
func TestPatternPrefilterGuardsEveryLookup(t *testing.T) {
	opts := networkDictOpts()
	opts.PatternPrefilters = map[string]string{"dict_tools_pattern": "(SELECT pats)"}
	pipeline, err := ParseQuery(`* | match(dict="tools", field=commandline, column=pattern, include=[tool, pattern], require=true)`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatal(err)
	}
	probe := "toString(fields.`commandline`::String)"
	guard := "multiMatchAny(" + probe + ", (SELECT pats))"
	row := "if(" + guard + ", dictGetOrDefault('bifract.dict_tools_pattern', ('_match', 'tool'), " + probe + ", ('', '')), ('', ''))"
	for _, want := range []string{
		"tupleElement(" + row + ", 1) = '1'",
		"tupleElement(" + row + ", 2) AS tool",
	} {
		if !strings.Contains(res.SQL, want) {
			t.Errorf("want %s in:\n%s", want, res.SQL)
		}
	}
	if lookups, guards := strings.Count(res.SQL, "dictGetOrDefault('bifract.dict_tools_pattern'"), strings.Count(res.SQL, guard); guards < lookups {
		t.Errorf("%d lookups but %d guards:\n%s", lookups, guards, res.SQL)
	}
}

// A REGEXP_TREE walks its expressions on every dictGet, so match() reads the
// marker and every include column in one tuple lookup. ClickHouse computes an
// identical expression once per row, so the lookup must be spelled the same in
// the select list, the require filter and a later filter on an include column.
func TestPatternMatchLooksUpOncePerRow(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="tools", field=commandline, column=pattern, include=[tool, pattern, family, tool], require=true) | family="x"`)
	probe := "toString(fields.`commandline`::String)"
	row := "identity(dictGetOrDefault('bifract.dict_tools_pattern', ('_match', 'tool', 'family'), " + probe + ", ('', '', '')))"
	if lookups, rows := strings.Count(sql, "dictGetOrDefault("), strings.Count(sql, row); rows != lookups {
		t.Errorf("%d lookups, %d of them the shared row:\n%s", lookups, rows, sql)
	}
	for _, want := range []string{
		"tupleElement(" + row + ", 2) AS tool",
		"tupleElement(" + row + ", 3) AS family",
		"if(tupleElement(" + row + ", 1) = '1', " + probe + ", '') AS pattern",
		"tupleElement(" + row + ", 1) = '1'",
		"tupleElement(" + row + ", 3) = 'x'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("want %s in:\n%s", want, sql)
		}
	}
}

// Ordinary lists gain little from a tuple lookup and keep one dictGet per column.
func TestHashedMatchKeepsPerColumnLookups(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="names", field=user, column=name, include=[a, b])`)
	if strings.Contains(sql, "tupleElement") {
		t.Errorf("a hashed list is read per column: %s", sql)
	}
	for _, col := range []string{"a", "b"} {
		if !strings.Contains(sql, "dictGetOrDefault('bifract.dict_names_name', '"+col+"', ") {
			t.Errorf("want a lookup of %s, got: %s", col, sql)
		}
	}
}

// A list ClickHouse has not compiled for Hyperscan has no prefilter, and its
// lookups run unscreened.
func TestPatternWithoutPrefilterIsUnguarded(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="tools", field=commandline, column=pattern, include=[tool, pattern], require=true)`)
	if strings.Contains(sql, "multiMatchAny") {
		t.Errorf("no prefilter was given: %s", sql)
	}
}

// A key column is named after whatever field the list was built from, keywords
// included; the safety check must read it as a name.
func TestPatternPrefilterKeyNamedLikeAKeyword(t *testing.T) {
	opts := networkDictOpts()
	opts.PatternPrefilters = map[string]string{"dict_tools_pattern": "(SELECT groupArray(`last update time`) FROM `bifract`.`dict_t` FINAL WHERE notEmpty(`last update time`))"}
	pipeline, err := ParseQuery(`* | match(dict="tools", field=commandline, column=pattern, include=[tool], require=true)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TranslateToSQLWithOrder(pipeline, opts); err != nil {
		t.Fatal(err)
	}
}
