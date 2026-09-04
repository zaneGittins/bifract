package parser

import (
	"strings"
	"testing"
	"time"
)

func dictOpts() QueryOptions {
	return QueryOptions{
		StartTime:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:            100,
		TableName:          "logs_distributed",
		DictionaryDatabase: "logs",
		GeoIPEnabled:       true,
		Dictionaries: map[string]map[string]string{
			"sensitive_groups": {"group_name": "lookup_abc"},
		},
	}
}

// Every dictionary name must be database-qualified. A Distributed fan-out runs the
// remote half of the query with the shard connection's own current database, so an
// unqualified name resolves to nothing there and the shard fails the whole query
// with BAD_ARGUMENTS (36). Verified against a two-shard cluster.
func TestDictionaryNamesAreDatabaseQualified(t *testing.T) {
	cases := []struct {
		name, query string
		want        []string
	}{
		{
			name:  "match",
			query: `* | match(dict="sensitive_groups", field=target_user, column=group_name, include=[group_name])`,
			want:  []string{"dictGetOrDefault('logs.lookup_abc'"},
		},
		{
			name:  "match strict adds dictHas",
			query: `* | match(dict="sensitive_groups", field=target_user, column=group_name, include=[group_name], strict=true)`,
			want:  []string{"dictGetOrDefault('logs.lookup_abc'", "dictHas('logs.lookup_abc'"},
		},
		{
			name:  "lookupIP city and asn",
			query: `* | lookupIP(field=src_ip, include=[country,asn])`,
			want:  []string{"dictGetOrDefault('logs.geoip_city_lookup'", "dictGetOrDefault('logs.geoip_asn_lookup'"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := mustTranslate(t, tc.query, dictOpts())
			for _, w := range tc.want {
				if !strings.Contains(sql, w) {
					t.Fatalf("missing %q in SQL:\n%s", w, sql)
				}
			}
			if strings.Contains(sql, "dictGetOrDefault('lookup_") || strings.Contains(sql, "dictGetOrDefault('geoip_") ||
				strings.Contains(sql, "dictHas('lookup_") {
				t.Fatalf("unqualified dictionary name in SQL:\n%s", sql)
			}
		})
	}
}

// A single-node install leaves DictionaryDatabase empty; the name must then stay bare.
func TestDictionaryNameBareWithoutDatabase(t *testing.T) {
	opts := dictOpts()
	opts.DictionaryDatabase = ""
	sql := mustTranslate(t, `* | match(dict="sensitive_groups", field=target_user, column=group_name, include=[group_name])`, opts)
	if !strings.Contains(sql, "dictGetOrDefault('lookup_abc'") {
		t.Fatalf("expected a bare dictionary name, got:\n%s", sql)
	}
}

// match() and lookupIP() must look up the field as an earlier command left it. Both
// read the raw stored value before, so lowercase(x) | match(field=x) matched against
// the original casing and silently enriched nothing.
func TestDictionaryLookupUsesTransformedField(t *testing.T) {
	sql := mustTranslate(t, `* | lowercase(target_user) | match(dict="sensitive_groups", field=target_user, column=group_name, include=[group_name])`, dictOpts())
	if !strings.Contains(sql, "toString(lower(fields.`target_user`::String))") {
		t.Fatalf("match() did not look up the lowercased value:\n%s", sql)
	}

	sql = mustTranslate(t, `* | replace("^::ffff:", "", src_ip, src_ip) | lookupIP(field=src_ip, include=[country])`, dictOpts())
	if !strings.Contains(sql, "replaceRegexpAll(") || !strings.Contains(sql, "geoip_city_lookup") {
		t.Fatalf("lookupIP() did not look up the rewritten value:\n%s", sql)
	}
	if strings.Contains(sql, "toIPv4OrDefault(fields.`src_ip`::String)") {
		t.Fatalf("lookupIP() still reads the raw field:\n%s", sql)
	}
}
