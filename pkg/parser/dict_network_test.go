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
			"corp":  {"network": "dict_corp_network"},
			"names": {"name": "dict_names_name"},
			"tools": {"pattern": "dict_tools_pattern"},
		},
		NetworkDicts:         map[string]bool{"corp": true},
		PatternDicts:         map[string]bool{"tools": true},
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
}

// A pattern is matched against the string as the log carries it, so the probe is
// unchanged. Only membership differs from a value list.
func TestPatternDictionaryProbesWithAString(t *testing.T) {
	sql := networkSQL(t, `* | match(dict="tools", field=commandline, column=pattern, include=[tool])`)
	if !strings.Contains(sql, "dictGetOrDefault('bifract.dict_tools_pattern', 'tool', toString(fields.`commandline`::String)") {
		t.Errorf("want a plain string probe, got: %s", sql)
	}
	if strings.Contains(sql, "toIPv6") {
		t.Errorf("a pattern list must not be probed with an address: %s", sql)
	}
}
