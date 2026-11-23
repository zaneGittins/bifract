package dashboards

import (
	"strings"
	"testing"

	"bifract/pkg/bqlvars"
	"bifract/pkg/storage"
)

func widget(query string, x, y, w, h int) CreateWidgetRequest {
	return CreateWidgetRequest{QueryContent: query, PosX: x, PosY: y, Width: w, Height: h}
}

func TestCheckCompositionAcceptsAWellFormedDashboard(t *testing.T) {
	vars := []bqlvars.Variable{{Name: "host", Value: "*"}}
	widgets := []CreateWidgetRequest{
		widget(`computer_name=@host | count() | singleval(title="Events")`, 0, 0, 6, 5),
		widget(`computer_name=@host | timechart(span=1h)`, 6, 0, 18, 5),
		widget(`* | groupby(image) | barchart()`, 0, 5, 24, 12),
	}
	if err := checkComposition(vars, widgets); err != nil {
		t.Fatalf("checkComposition: %v", err)
	}
}

func TestCheckCompositionRejects(t *testing.T) {
	ok := widget(`*`, 0, 0, 6, 5)
	many := make([]CreateWidgetRequest, maxWidgets+1)
	for i := range many {
		many[i] = widget(`*`, 0, i*5, 6, 5)
	}
	cases := map[string]struct {
		vars    []bqlvars.Variable
		widgets []CreateWidgetRequest
		want    string
	}{
		"bad query":        {nil, []CreateWidgetRequest{widget(`| | |`, 0, 0, 6, 5)}, "widget 1"},
		"empty query":      {nil, []CreateWidgetRequest{widget("  ", 0, 0, 6, 5)}, "query is required"},
		"off the grid":     {nil, []CreateWidgetRequest{widget(`*`, 20, 0, 6, 5)}, "off the 24-column grid"},
		"no size":          {nil, []CreateWidgetRequest{widget(`*`, 0, 0, 0, 5)}, "off the 24-column grid"},
		"overlap":          {nil, []CreateWidgetRequest{ok, widget(`*`, 3, 2, 6, 5)}, "overlaps widget 1"},
		"too many":         {nil, many, "at most"},
		"unknown variable": {nil, []CreateWidgetRequest{widget(`host=@host`, 0, 0, 6, 5)}, "widget 1"},
		"bad var name":     {[]bqlvars.Variable{{Name: "1x"}}, []CreateWidgetRequest{ok}, "variable name"},
		"duplicate var":    {[]bqlvars.Variable{{Name: "a"}, {Name: "a"}}, []CreateWidgetRequest{ok}, "more than once"},
		"long title": {nil, []CreateWidgetRequest{{
			Title: ptr(strings.Repeat("t", maxTitleLen+1)), QueryContent: "*", Width: 6, Height: 5,
		}}, "title"},
	}
	for label, c := range cases {
		err := checkComposition(c.vars, c.widgets)
		if err == nil {
			t.Errorf("%s: accepted, want an error", label)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not mention %q", label, err, c.want)
		}
	}
}

func ptr(s string) *string { return &s }

func TestCheckNewNormalizesAndRefuses(t *testing.T) {
	req := CreateDashboardRequest{Name: "  Overview  "}
	if err := CheckNew(&req); err != nil {
		t.Fatal(err)
	}
	if req.Name != "Overview" || req.TimeRangeType != "last1h" {
		t.Errorf("normalized to %q, %q", req.Name, req.TimeRangeType)
	}

	bad := map[string]CreateDashboardRequest{
		"no name":       {Name: "  "},
		"long name":     {Name: strings.Repeat("n", maxTitleLen+1)},
		"bad range":     {Name: "d", TimeRangeType: "yesterday"},
		"custom bare":   {Name: "d", TimeRangeType: "custom"},
		"bad zone":      {Name: "d", Timezone: "Mars/Olympus"},
		"broken widget": {Name: "d", Widgets: []CreateWidgetRequest{widget("| |", 0, 0, 6, 5)}},
	}
	for label, req := range bad {
		if err := CheckNew(&req); err == nil {
			t.Errorf("%s: accepted", label)
		}
	}
}

func TestCheckAddition(t *testing.T) {
	title := "Existing"
	existing := []storage.DashboardWidget{{Title: &title, QueryContent: "not | | parsed", PosX: 0, PosY: 0, Width: 24, Height: 10}}
	vars := []byte(`[{"name":"host","value":"*"}]`)

	if err := CheckAddition(vars, existing, []CreateWidgetRequest{widget(`computer_name=@host | count()`, 0, 10, 12, 5)}); err != nil {
		t.Fatalf("a widget below the board was refused: %v", err)
	}

	many := make([]storage.DashboardWidget, maxDashboardWidgets)
	cases := map[string]struct {
		existing []storage.DashboardWidget
		widgets  []CreateWidgetRequest
		want     string
	}{
		"none":             {existing, nil, "at least one"},
		"over an existing": {existing, []CreateWidgetRequest{widget(`*`, 0, 5, 6, 5)}, `existing widget "Existing"`},
		"unknown variable": {existing, []CreateWidgetRequest{widget(`user=@user`, 0, 10, 6, 5)}, "widget 1"},
		"board is full":    {many, []CreateWidgetRequest{widget(`*`, 0, 0, 6, 5)}, "at most"},
		"each other":       {existing, []CreateWidgetRequest{widget(`*`, 0, 10, 6, 5), widget(`*`, 2, 12, 6, 5)}, "overlaps widget 1"},
	}
	for label, c := range cases {
		err := CheckAddition(vars, c.existing, c.widgets)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %v, want it to mention %q", label, err, c.want)
		}
	}
}

func board() []storage.DashboardWidget {
	a, b := "Count", "Trend"
	return []storage.DashboardWidget{
		{ID: "w1", Title: &a, QueryContent: "* | count()", Width: 24, Height: 5},
		{ID: "w2", Title: &b, QueryContent: "* | timechart()", PosY: 5, Width: 24, Height: 10},
	}
}

func TestCheckEdits(t *testing.T) {
	vars := []byte(`[{"name":"host","value":"*"}]`)
	q := func(s string) *string { return &s }
	if err := CheckEdits(vars, board(), []storage.WidgetEdit{
		{ID: "w1", Title: q("Events")},
		{ID: "w2", QueryContent: q("computer_name=@host | timechart(span=1h)")},
	}); err != nil {
		t.Fatalf("valid edits refused: %v", err)
	}

	cases := map[string]struct {
		edits []storage.WidgetEdit
		want  string
	}{
		"none":             {nil, "at least one"},
		"not on the board": {[]storage.WidgetEdit{{ID: "w9", Title: q("x")}}, "not on this dashboard"},
		"twice":            {[]storage.WidgetEdit{{ID: "w1", Title: q("a")}, {ID: "w1", Title: q("b")}}, "more than once"},
		"nothing to do":    {[]storage.WidgetEdit{{ID: "w1"}}, "nothing to change"},
		"blank query":      {[]storage.WidgetEdit{{ID: "w1", QueryContent: q(" ")}}, "query is required"},
		"bad query":        {[]storage.WidgetEdit{{ID: "w1", QueryContent: q("| | |")}}, `"Count"`},
		"unknown variable": {[]storage.WidgetEdit{{ID: "w1", QueryContent: q("user=@user")}}, `"Count"`},
		"long title":       {[]storage.WidgetEdit{{ID: "w1", Title: q(strings.Repeat("t", maxTitleLen+1))}}, "title"},
	}
	for label, c := range cases {
		err := CheckEdits(vars, board(), c.edits)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %v, want it to mention %q", label, err, c.want)
		}
	}
}

func TestCheckRemovals(t *testing.T) {
	if err := CheckRemovals(board(), []string{"w2"}); err != nil {
		t.Fatalf("a valid removal was refused: %v", err)
	}
	for label, ids := range map[string][]string{
		"none":             nil,
		"not on the board": {"w9"},
		"twice":            {"w1", "w1"},
	} {
		if err := CheckRemovals(board(), ids); err == nil {
			t.Errorf("%s: accepted", label)
		}
	}
}
