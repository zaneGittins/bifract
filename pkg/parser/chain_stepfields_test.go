package parser

import (
	"strings"
	"testing"
)

// StepFields must come from the query, not a fixed list, so any telemetry shape
// (endpoint, cloud, custom) surfaces the fields the detection actually used.
func TestChainStepFieldsDerivedFromQuery(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{
			"endpoint telemetry",
			`* | chain(process_guid) { dst_ip="1.1.1.1"; image="/usr/bin/curl" }`,
			[]string{"dst_ip", "image"},
		},
		{
			"cloud telemetry",
			`* | chain(account_id) { event_name="ConsoleLogin"; event_name="CreateAccessKey" | user_agent="aws-cli" }`,
			[]string{"event_name", "user_agent"},
		},
		{
			"nested boolean groups",
			`* | chain(user) { (event_id="4624" OR event_id="4625") AND logon_type="10"; image="cmd.exe" }`,
			[]string{"event_id", "logon_type", "image"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
			if err != nil {
				t.Fatal(err)
			}
			if res.Chain == nil {
				t.Fatal("expected chain meta")
			}
			got := strings.Join(res.Chain.StepFields, ",")
			if got != strings.Join(tc.want, ",") {
				t.Errorf("StepFields = %v, want %v", res.Chain.StepFields, tc.want)
			}
		})
	}
}
