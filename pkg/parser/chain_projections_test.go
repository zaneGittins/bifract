package parser

import (
	"strings"
	"testing"
)

// Projections are per-row scalars, so a step may compute one and filter on it. The
// step projects nothing itself: references fold in as the expression.
func TestChainStepProjections(t *testing.T) {
	dicts := map[string]map[string]string{
		"threatlist": {"sha256": "threatlist_sha256_dict"},
	}
	cases := []struct {
		name  string
		query string
		opts  QueryOptions
		want  []string
		deny  []string
	}{
		{
			name:  "len feeds a threshold in the same step",
			query: `* | chain(process_guid) { len(commandline) | _len > 500; event_id=3 }`,
			opts:  QueryOptions{FractalID: "f"},
			want:  []string{"length(fields.`commandline`::String) > 500"},
			deny:  []string{" AS _len", "_len >"},
		},
		{
			name:  "len with as= names its own output",
			query: `* | chain(process_guid) { len(commandline, as=clen) | clen > 500; event_id=3 }`,
			opts:  QueryOptions{FractalID: "f"},
			want:  []string{"length(fields.`commandline`::String) > 500"},
			deny:  []string{" AS clen"},
		},
		{
			name:  "match folds its dictionary lookup into the predicate",
			query: `* | chain(process_guid) { match(dict=threatlist, field=hash, column=sha256, include=[verdict]) | verdict="bad"; event_id=3 }`,
			opts:  QueryOptions{FractalID: "f", Dictionaries: dicts},
			want:  []string{"dictGetOrDefault('threatlist_sha256_dict', 'verdict'", "= 'bad'"},
			deny:  []string{" AS verdict"},
		},
		{
			name:  "lookupIP folds its geo lookup into the predicate",
			query: `* | chain(process_guid) { event_id=3 | lookupIP(field=dst_ip, include=[country]) | country="CN"; event_id=1 }`,
			opts:  QueryOptions{FractalID: "f", GeoIPEnabled: true},
			want:  []string{"dictGetOrDefault('geoip_city_lookup', 'country'", "= 'CN'"},
			deny:  []string{" AS country"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			res, err := TranslateToSQLWithOrder(pl, tc.opts)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(res.SQL, w) {
					t.Errorf("missing %q:\n%s", w, res.SQL)
				}
			}
			// A chain step projects nothing: an alias would name a column the
			// aggregate cannot see.
			for _, d := range tc.deny {
				if strings.Contains(res.SQL, d) {
					t.Errorf("step leaked a projection %q:\n%s", d, res.SQL)
				}
			}
		})
	}
}

// A step's projection is scoped to that step: it must not resolve in a later step
// or leak into the query containing the chain.
func TestChainStepProjectionDoesNotLeak(t *testing.T) {
	pl, err := ParseQuery(`* | chain(process_guid) { len(commandline) | _len > 500; _len > 10 }`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	// The second step never called len(), so its _len is an ordinary unknown field
	// and must resolve as JSON, not as the first step's length() expression.
	if strings.Count(res.SQL, "length(fields.`commandline`::String) > 10") != 0 {
		t.Errorf("first step's projection leaked into the second:\n%s", res.SQL)
	}
	if !strings.Contains(res.SQL, "fields.`_len`") {
		t.Errorf("second step's _len should resolve as a plain field:\n%s", res.SQL)
	}
}
