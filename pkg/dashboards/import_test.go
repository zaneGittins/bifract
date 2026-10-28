package dashboards

import "testing"

func TestValidateImport(t *testing.T) {
	name, err := ValidateImport([]byte(`
kind: Dashboard
grid_columns: 24
name: RMM activity
time_range_type: last7d
widgets:
  - title: Runs
    query: event_id=1 | count() | singleval(title="Runs")
    pos_x: 0
    pos_y: 0
    width: 6
    height: 5
`))
	if err != nil || name != "RMM activity" {
		t.Fatalf("ValidateImport = %q, %v", name, err)
	}

	bad := map[string]string{
		"no name":        "kind: Dashboard\nwidgets: []\n",
		"bad time range": "name: d\ntime_range_type: yesterday\nwidgets: []\n",
		"unknown field":  "name: d\ncolour: red\nwidgets: []\n",
		"bad query":      "name: d\nwidgets: [{query: '| | |', pos_x: 0, pos_y: 0, width: 6, height: 5}]\n",
		"off the grid":   "name: d\nwidgets: [{query: '*', pos_x: 20, pos_y: 0, width: 6, height: 5}]\n",
		"no widgets":     "name: d\n",
		"old grid":       "name: d\ngrid_columns: 12\nwidgets: [{query: '*', pos_x: 0, pos_y: 0, width: 6, height: 5}]\n",
		"overlap":        "name: d\nwidgets: [{query: '*', pos_x: 0, pos_y: 0, width: 6, height: 5}, {query: '*', pos_x: 3, pos_y: 2, width: 6, height: 5}]\n",
	}
	for label, body := range bad {
		if _, err := ValidateImport([]byte(body)); err == nil {
			t.Errorf("%s: ValidateImport succeeded, want an error", label)
		}
	}
}
