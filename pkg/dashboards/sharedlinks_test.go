package dashboards

import (
	"encoding/json"
	"testing"

	"bifract/pkg/storage"
)

// An anonymous viewer cannot call /attack/matrix (viewer+), so a shared dashboard
// carrying a mitre() panel must ship the matrix in its own payload -- and no other
// dashboard should pay ~700 techniques for it.
func TestPublicDashboardAttachesAttackMatrixOnlyWhenNeeded(t *testing.T) {
	dash := &storage.Dashboard{Name: "wallboard", TimeRangeType: "last24h"}

	tests := []struct {
		name    string
		results string
		want    bool
	}{
		{"mitre widget", `{"chart_type":"mitre","results":[{"attack_tag":"attack.t1059.004","_count":3}]}`, true},
		{"other chart", `{"chart_type":"barchart","results":[]}`, false},
		{"table widget", `{"results":[]}`, false},
		{"no cached results", ``, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := storage.DashboardWidget{ID: "w1", ChartType: "mitre"}
			if tc.results != "" {
				w.LastResults = json.RawMessage(tc.results)
			}
			pub := buildPublicDashboard(dash, []storage.DashboardWidget{w})
			if got := pub.AttackMatrix != nil; got != tc.want {
				t.Errorf("attack matrix attached = %v, want %v", got, tc.want)
			}
		})
	}
}

// The renderer keys off the cached result's chart type, not the widget's stored
// one; reading the wrong field would ship the matrix for the wrong dashboards and
// omit it for the right ones.
func TestResultChartType(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{`{"chart_type":"mitre"}`, "mitre"},
		{`{"chart_type":""}`, ""},
		{`{"results":[]}`, ""},
		{`not json`, ""},
		{``, ""},
	}
	for _, tc := range tests {
		if got := resultChartType(json.RawMessage(tc.raw)); got != tc.want {
			t.Errorf("resultChartType(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}
