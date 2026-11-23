package aitools

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

type placed struct{ x, y, w, h int }

func placements(widgets []dashboardWidgetArgs) []placed {
	out := make([]placed, len(widgets))
	for i, w := range widgets {
		out[i] = placed{*w.PosX, *w.PosY, w.Width, w.Height}
	}
	return out
}

func kinds(names ...string) []dashboardWidgetArgs {
	out := make([]dashboardWidgetArgs, len(names))
	for i, n := range names {
		out[i] = dashboardWidgetArgs{Title: n, Query: "*", ChartType: n}
	}
	return out
}

// Rows fill the grid edge to edge and share one height, so nothing is left
// ragged or overlapping whatever sizes the model asked for.
func TestLayoutPacksRowsEdgeToEdge(t *testing.T) {
	widgets := kinds("singleval", "singleval", "singleval", "singleval", "timechart", "piechart", "barchart", "")
	layoutWidgets(widgets, 0)
	want := []placed{
		{0, 0, 6, 5}, {6, 0, 6, 5}, {12, 0, 6, 5}, {18, 0, 6, 5},
		{0, 5, 24, 10},
		{0, 15, 8, 12}, {8, 15, 16, 12},
		{0, 27, 24, 12},
	}
	if got := placements(widgets); !reflect.DeepEqual(got, want) {
		t.Errorf("layout = %v\nwant     %v", got, want)
	}
}

func TestLayoutHonoursRequestedWidths(t *testing.T) {
	widgets := kinds("timechart", "barchart", "singleval")
	widgets[0].Width = 16
	widgets[1].Width = 8
	widgets[2].Width = 40 // out of range: clamped to the grid
	layoutWidgets(widgets, 0)
	want := []placed{{0, 0, 16, 12}, {16, 0, 8, 12}, {0, 12, 24, 5}}
	if got := placements(widgets); !reflect.DeepEqual(got, want) {
		t.Errorf("layout = %v\nwant     %v", got, want)
	}
}

// A caller that placed every widget itself keeps its placement; the server
// checks it for overlap.
func TestLayoutKeepsAnExplicitPlacement(t *testing.T) {
	x, y := 4, 2
	widgets := kinds("singleval")
	widgets[0].PosX, widgets[0].PosY, widgets[0].Width, widgets[0].Height = &x, &y, 6, 7
	layoutWidgets(widgets, 0)
	if got := placements(widgets); !reflect.DeepEqual(got, []placed{{4, 2, 6, 7}}) {
		t.Errorf("layout = %v", got)
	}
}

func TestCreateDashboardSendsTheWholeDashboardInOneRequest(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"id":"d1","name":"Logons"}}`)
	s.byPath = map[string]string{"/query/validate": `{"valid":true,"chart_type":"timechart"}`}

	out, err := call(t, s, createDashboardArgs{
		Name:      " Logons ",
		Variables: []dashboardVariableArgs{{Name: "host", Value: "*"}},
		Widgets: []dashboardWidgetArgs{
			{Title: "Logons", Query: "event_id=4624 computer_name=@host | timechart(span=1h)"},
			{Title: "Failures", Query: "event_id=4625 | timechart(span=1h)"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.last.Method != "POST" || s.last.Path != "/dashboards" {
		t.Fatalf("last call = %s %s, want POST /dashboards", s.last.Method, s.last.Path)
	}
	if validations := len(s.seen) - 1; validations != 2 {
		t.Errorf("validated %d queries, want 2", validations)
	}

	var sent struct {
		Name          string `json:"name"`
		TimeRangeType string `json:"time_range_type"`
		Variables     []map[string]string
		Widgets       []map[string]any
	}
	if err := json.Unmarshal(s.sent, &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Name != "Logons" || sent.TimeRangeType != "last24h" {
		t.Errorf("name %q, range %q", sent.Name, sent.TimeRangeType)
	}
	if len(sent.Variables) != 1 || sent.Variables[0]["name"] != "host" {
		t.Errorf("variables = %v", sent.Variables)
	}
	if len(sent.Widgets) != 2 {
		t.Fatalf("widgets = %v", sent.Widgets)
	}
	second := sent.Widgets[1]
	if second["query_content"] != "event_id=4625 | timechart(span=1h)" || second["chart_type"] != "timechart" ||
		second["pos_y"] != float64(10) || second["width"] != float64(24) {
		t.Errorf("second widget = %v", second)
	}
	if Field[string](out, "id") != "d1" {
		t.Errorf("result = %v", out)
	}
}

// Every broken widget is reported at once, so the model fixes them in one
// round, and nothing is created.
func TestCreateDashboardRefusesInvalidQueriesBeforeWriting(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"id":"d1"}}`)
	s.byPath = map[string]string{"/query/validate": `{"valid":false,"error":"Parse error: unexpected '|'"}`}

	_, err := call(t, s, createDashboardArgs{
		Name: "Broken",
		Widgets: []dashboardWidgetArgs{
			{Title: "One", Query: "| |"},
			{Title: "Two", Query: "| | |"},
			{Title: "Three", Query: "  "},
		},
	})
	if err == nil {
		t.Fatal("created a dashboard with broken queries")
	}
	for _, want := range []string{"widget 1 (One)", "widget 2 (Two)", "widget 3 (Three): a query is required", "unexpected '|'"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	for _, call := range s.seen {
		if call.Path == "/dashboards" {
			t.Error("the dashboard was created despite the errors")
		}
	}
}

// What the user approves is what runs: preparing prepared arguments changes
// nothing, so the second pass at run time cannot move or retype a widget.
func TestPreparingADashboardTwiceChangesNothing(t *testing.T) {
	tool := toolNamed(t, "create_dashboard")
	s := serve(t, 200, `{"valid":true,"chart_type":"barchart"}`)

	args := json.RawMessage(`{"name":"Hosts","widgets":[{"title":"Top","query":"* | groupby(host) | barchart()"},{"title":"Count","query":"* | count()"}]}`)
	once, err := tool.Prepare(context.Background(), s, args)
	if err != nil {
		t.Fatal(err)
	}
	twice, err := tool.Prepare(context.Background(), s, once)
	if err != nil {
		t.Fatal(err)
	}
	if string(once) != string(twice) {
		t.Errorf("second preparation changed the call:\n%s\n%s", once, twice)
	}
	if !strings.Contains(string(once), `"pos_x":0`) {
		t.Errorf("prepared arguments carry no layout: %s", once)
	}
}

func TestToolsWithoutPreparationPassTheirArgumentsThrough(t *testing.T) {
	tool := toolNamed(t, "create_notebook")
	args := json.RawMessage(`{"name":"n"}`)
	got, err := tool.Prepare(context.Background(), serve(t, 500, ""), args)
	if err != nil || string(got) != string(args) {
		t.Errorf("Prepare = %s, %v", got, err)
	}
}

// call runs create_dashboard the way both transports do: validated, prepared,
// then run.
func call(t *testing.T, c Client, in createDashboardArgs) (any, error) {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return toolNamed(t, "create_dashboard").Call(context.Background(), c, raw)
}

func toolNamed(t *testing.T, name string) Tool {
	t.Helper()
	for _, tool := range All() {
		if tool.Name() == name {
			return tool
		}
	}
	t.Fatalf("no tool %s", name)
	return Tool{}
}

// New panels go below everything already on the board, so nothing an analyst
// placed is moved or covered.
func TestLayoutStartsBelowTheExistingBoard(t *testing.T) {
	widgets := kinds("singleval", "barchart")
	layoutWidgets(widgets, 17)
	want := []placed{{0, 17, 6, 12}, {6, 17, 18, 12}}
	if got := placements(widgets); !reflect.DeepEqual(got, want) {
		t.Errorf("layout = %v\nwant     %v", got, want)
	}
}

const boardWithTwoWidgets = `{"success":true,"data":{"id":"d1","name":"Logons",
	"variables":[{"name":"host","value":"*"}],
	"widgets":[
		{"id":"w1","title":"Count","query_content":"* | count()","pos_x":0,"pos_y":0,"width":24,"height":5},
		{"id":"w2","title":"Trend","query_content":"* | timechart()","pos_x":0,"pos_y":5,"width":24,"height":10}]}}`

func TestAddDashboardWidgetsPlacesThemBelowAndSendsOneRequest(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":[{"id":"w3"}]}`)
	s.byPath = map[string]string{
		"/dashboards/d1":  boardWithTwoWidgets,
		"/query/validate": `{"valid":true,"chart_type":"barchart"}`,
	}
	raw, _ := json.Marshal(addDashboardWidgetsArgs{
		DashboardID: "d1",
		Widgets:     []dashboardWidgetArgs{{Title: "By host", Query: "computer_name=@host | groupby(computer_name) | barchart()"}},
	})
	out, err := toolNamed(t, "add_dashboard_widgets").Call(context.Background(), s, raw)
	if err != nil {
		t.Fatal(err)
	}

	var posted *recorded
	for i := range s.seen {
		if s.seen[i].Method == "POST" && s.seen[i].Path == "/dashboards/d1/widgets/batch" {
			posted = &s.seen[i]
		}
	}
	if posted == nil {
		t.Fatalf("no batch request; calls were %v", s.seen)
	}
	if Field[string](out, "id") != "d1" || Field[string](out, "name") != "Logons" {
		t.Errorf("result = %v", out)
	}
}

func TestAddDashboardWidgetsPreparesBelowTheBoard(t *testing.T) {
	s := serve(t, 200, "")
	s.byPath = map[string]string{
		"/dashboards/d1":  boardWithTwoWidgets,
		"/query/validate": `{"valid":true,"chart_type":"barchart"}`,
	}
	prepared, err := toolNamed(t, "add_dashboard_widgets").Prepare(context.Background(), s,
		json.RawMessage(`{"dashboard_id":"d1","widgets":[{"title":"By host","query":"* | groupby(computer_name) | barchart()"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	var got addDashboardWidgetsArgs
	if err := json.Unmarshal(prepared, &got); err != nil {
		t.Fatal(err)
	}
	if p := placements(got.Widgets); !reflect.DeepEqual(p, []placed{{0, 15, 24, 12}}) {
		t.Errorf("placed at %v, want below the board at row 15", p)
	}
	again, err := toolNamed(t, "add_dashboard_widgets").Prepare(context.Background(), s, prepared)
	if err != nil || string(again) != string(prepared) {
		t.Errorf("preparing again changed the call (%v):\n%s\n%s", err, prepared, again)
	}

	// A placement that lands on an existing panel is refused before the user
	// is asked.
	_, err = toolNamed(t, "add_dashboard_widgets").Prepare(context.Background(), s,
		json.RawMessage(`{"dashboard_id":"d1","widgets":[{"title":"Over","query":"*","pos_x":0,"pos_y":2,"width":6,"height":5}]}`))
	if err == nil || !strings.Contains(err.Error(), "existing widget") {
		t.Errorf("an overlapping placement was not refused: %v", err)
	}
}

func TestAddDashboardWidgetsUsesTheBoardsVariables(t *testing.T) {
	s := serve(t, 200, "")
	s.byPath = map[string]string{
		"/dashboards/d1":  boardWithTwoWidgets,
		"/query/validate": `{"valid":true}`,
	}
	if _, err := toolNamed(t, "add_dashboard_widgets").Prepare(context.Background(), s,
		json.RawMessage(`{"dashboard_id":"d1","widgets":[{"title":"T","query":"computer_name=@host"}]}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(s.sent), `"name":"host"`) {
		t.Errorf("validation was not given the dashboard's variables: %s", s.sent)
	}
}

func TestUpdateDashboardWidgetsChecksAndSendsOneRequest(t *testing.T) {
	s := serve(t, 200, `{"success":true}`)
	s.byPath = map[string]string{
		"/dashboards/d1":  boardWithTwoWidgets,
		"/query/validate": `{"valid":true,"chart_type":"piechart"}`,
	}
	tool := toolNamed(t, "update_dashboard_widgets")
	args := json.RawMessage(`{"dashboard_id":"d1","edits":[
		{"widget_id":"w1","title":"Events"},
		{"widget_id":"w2","query":"computer_name=@host | groupby(user) | piechart()"}]}`)

	prepared, err := tool.Prepare(context.Background(), s, args)
	if err != nil {
		t.Fatal(err)
	}
	var got updateDashboardWidgetsArgs
	if err := json.Unmarshal(prepared, &got); err != nil {
		t.Fatal(err)
	}
	if got.Edits[0].ChartType != "" || got.Edits[1].ChartType != "piechart" {
		t.Errorf("chart types = %q, %q: only a changed query sets one", got.Edits[0].ChartType, got.Edits[1].ChartType)
	}
	if again, _ := tool.Prepare(context.Background(), s, prepared); string(again) != string(prepared) {
		t.Errorf("preparing again changed the call:\n%s\n%s", prepared, again)
	}

	out, err := tool.Call(context.Background(), s, prepared)
	if err != nil {
		t.Fatal(err)
	}
	var put *recorded
	for i := range s.seen {
		if s.seen[i].Method == "PUT" && s.seen[i].Path == "/dashboards/d1/widgets/batch" {
			put = &s.seen[i]
		}
	}
	if put == nil {
		t.Fatalf("no batch edit; calls were %v", s.seen)
	}
	if Field[int](out, "updated") != 2 {
		t.Errorf("result = %v", out)
	}
}

func TestUpdateDashboardWidgetsRefusesBeforeAsking(t *testing.T) {
	s := serve(t, 200, "")
	s.byPath = map[string]string{
		"/dashboards/d1":  boardWithTwoWidgets,
		"/query/validate": `{"valid":false,"error":"Parse error: bad"}`,
	}
	tool := toolNamed(t, "update_dashboard_widgets")
	for label, args := range map[string]string{
		"unknown widget": `{"dashboard_id":"d1","edits":[{"widget_id":"w9","title":"x"}]}`,
		"broken query":   `{"dashboard_id":"d1","edits":[{"widget_id":"w1","query":"| |"}]}`,
		"no change":      `{"dashboard_id":"d1","edits":[{"widget_id":"w1"}]}`,
	} {
		if _, err := tool.Prepare(context.Background(), s, json.RawMessage(args)); err == nil {
			t.Errorf("%s: offered anyway", label)
		}
	}
}

// What was removed comes back in the result in add_dashboard_widgets' own
// shape, so a removal the user regrets can be undone.
func TestRemoveDashboardWidgetsReturnsWhatItRemoved(t *testing.T) {
	s := serve(t, 200, `{"success":true}`)
	s.byPath = map[string]string{"/dashboards/d1": boardWithTwoWidgets}
	tool := toolNamed(t, "remove_dashboard_widgets")

	if _, err := tool.Prepare(context.Background(), s, json.RawMessage(`{"dashboard_id":"d1","widget_ids":["w9"]}`)); err == nil {
		t.Error("removing a widget that is not on the board was offered")
	}

	out, err := tool.Call(context.Background(), s, json.RawMessage(`{"dashboard_id":"d1","widget_ids":["w2"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if s.last.Method != "POST" || s.last.Path != "/dashboards/d1/widgets/batch/delete" {
		t.Errorf("last call = %s %s", s.last.Method, s.last.Path)
	}
	removed := Field[[]dashboardWidgetArgs](out, "removed_widgets")
	if len(removed) != 1 || removed[0].Title != "Trend" || removed[0].Query != "* | timechart()" || *removed[0].PosY != 5 {
		t.Fatalf("removed_widgets = %+v", out)
	}

	// Restoring is add_dashboard_widgets with exactly those definitions: they
	// pass its schema and go back where they were.
	s.byPath["/dashboards/d1"] = `{"success":true,"data":{"id":"d1","name":"Logons","widgets":[
		{"id":"w1","title":"Count","query_content":"* | count()","pos_x":0,"pos_y":0,"width":24,"height":5}]}}`
	s.byPath["/query/validate"] = `{"valid":true,"chart_type":"timechart"}`
	restore, _ := json.Marshal(addDashboardWidgetsArgs{DashboardID: "d1", Widgets: removed})
	prepared, err := toolNamed(t, "add_dashboard_widgets").Prepare(context.Background(), s, restore)
	if err != nil {
		t.Fatalf("the removed widgets cannot be added back: %v", err)
	}
	var back addDashboardWidgetsArgs
	_ = json.Unmarshal(prepared, &back)
	if p := placements(back.Widgets); !reflect.DeepEqual(p, []placed{{0, 5, 24, 10}}) {
		t.Errorf("restored at %v, want where it was", p)
	}
}
