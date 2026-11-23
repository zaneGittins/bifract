package aitools

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"bifract/pkg/bqlvars"
	"bifract/pkg/dashboards"
	"bifract/pkg/storage"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerDashboardTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "create_dashboard",
		Annotations: mutates(),
		Description: "Create a dashboard of live BQL panels, built and laid out in one step.\n\n" +
			"Run every widget's query with query_logs first and keep only the ones that return " +
			"data: an empty panel is worse than none. Check list_dashboards so you do not " +
			"duplicate one that exists.\n\n" +
			"A widget's query decides how it is drawn, so end each with the command that suits " +
			"the question: singleval() for a headline number, timechart(span=...) for a trend, " +
			"barchart() or piechart() over a groupby() for a breakdown, worldmap() for " +
			"geography, and no rendering command for a table of top rows. A good dashboard " +
			"opens with up to four singleval() numbers, then the trends, then the breakdowns " +
			"and tables an analyst drills into. 4 to 10 widgets is usually right; at most 50.\n\n" +
			"Layout is automatic: widgets flow left to right in the order given and each row " +
			"is stretched to the full width. Set width only to pair widgets on a row.\n\n" +
			"Use a variable for a filter the viewer will want to change, such as a host or " +
			"user: declare {name: host, value: \"*\"} and write computer_name=@host in the " +
			"queries. Every query is checked before the user is asked, and errors name the " +
			"widget to fix.\n\n" +
			"Returns the created dashboard's id and name.",
	}, createDashboard, prepared(planDashboard))

	add(d, &mcp.Tool{
		Name:        "add_dashboard_widgets",
		Annotations: mutates(),
		Description: "Add panels to an existing dashboard, below the ones already on it.\n\n" +
			"Read the dashboard with get_dashboard first: reuse its variables (@name) rather " +
			"than hard-coding a value, and do not repeat a panel it already has. Run each new " +
			"query with query_logs and keep only the ones that return data.\n\n" +
			"Widgets take the same form as in create_dashboard and are laid out the same way, " +
			"starting on a new row below the existing panels, which are never moved. At most " +
			"50 at once. Every query is checked against the dashboard's variables before the " +
			"user is asked.\n\n" +
			"Returns the dashboard's id and name and how many panels were added.",
	}, addDashboardWidgets, prepared(planAddition))

	add(d, &mcp.Tool{
		Name:        "update_dashboard_widgets",
		Annotations: mutates(),
		Description: "Change the title or query of panels already on a dashboard.\n\n" +
			"Read the dashboard with get_dashboard first for the widget ids and their current " +
			"queries. Each edit names one widget and sets a new title, a new query, or both; " +
			"anything not given is left as it is. A new query decides the panel's chart, so " +
			"keep its rendering command unless the change is to the chart. Run a new query " +
			"with query_logs first. Panels are not moved or resized.\n\n" +
			"All edits are applied together or not at all. Every query is checked against " +
			"the dashboard's variables before the user is asked.\n\n" +
			"Returns the dashboard's id and name and how many panels changed.",
	}, updateDashboardWidgets, prepared(planEdits))

	add(d, &mcp.Tool{
		Name:        "remove_dashboard_widgets",
		Annotations: destroys(),
		Description: "Remove panels from a dashboard.\n\n" +
			"Read the dashboard with get_dashboard first for the widget ids, and remove only " +
			"what the user asked to remove. The other panels are not moved. All named panels " +
			"are removed together or not at all.\n\n" +
			"Returns the removed panels' definitions in removed_widgets. To undo, pass them " +
			"unchanged as the widgets of add_dashboard_widgets.",
	}, removeDashboardWidgets, prepared(planRemoval))
}

type widgetEditArgs struct {
	WidgetID  string  `json:"widget_id" jsonschema:"The widget UUID, from get_dashboard."`
	Title     *string `json:"title,omitempty" jsonschema:"New panel title. Omit to keep the current one."`
	Query     *string `json:"query,omitempty" jsonschema:"New BQL for the panel. Omit to keep the current one."`
	ChartType string  `json:"chart_type,omitempty" jsonschema:"Leave unset: filled in from the new query."`
}

type updateDashboardWidgetsArgs struct {
	DashboardID string           `json:"dashboard_id" jsonschema:"The dashboard UUID, from list_dashboards."`
	Edits       []widgetEditArgs `json:"edits" jsonschema:"One entry per panel to change."`
}

type removeDashboardWidgetsArgs struct {
	DashboardID string   `json:"dashboard_id" jsonschema:"The dashboard UUID, from list_dashboards."`
	WidgetIDs   []string `json:"widget_ids" jsonschema:"The widget UUIDs to remove, from get_dashboard."`
}

// planEdits checks edits against the dashboard they change, reading only.
func planEdits(ctx context.Context, c Client, in updateDashboardWidgetsArgs) (updateDashboardWidgetsArgs, error) {
	board, err := readDashboard(ctx, c, in.DashboardID)
	if err != nil {
		return in, err
	}
	vars := bqlvars.ParseVariables(board.Variables)
	var problems []string
	for i := range in.Edits {
		e := &in.Edits[i]
		if e.Title != nil {
			title := strings.TrimSpace(*e.Title)
			e.Title = &title
		}
		e.ChartType = ""
		if e.Query == nil {
			continue
		}
		chart, problem, err := validateQuery(ctx, c, *e.Query, vars)
		if err != nil {
			return in, fmt.Errorf("edit %d could not be checked: %w", i+1, err)
		}
		if problem != "" {
			problems = append(problems, fmt.Sprintf("edit %d (widget %s): %s", i+1, e.WidgetID, problem))
			continue
		}
		e.ChartType = chart
	}
	if len(problems) > 0 {
		return in, fmt.Errorf("nothing was changed; fix these edits and try again:\n%s", strings.Join(problems, "\n"))
	}
	if err := dashboards.CheckEdits(board.Variables, board.Widgets, widgetEdits(in.Edits)); err != nil {
		return in, err
	}
	return in, nil
}

func updateDashboardWidgets(ctx context.Context, c Client, in updateDashboardWidgetsArgs) (any, error) {
	path := "/dashboards/" + url.PathEscape(in.DashboardID)
	if _, err := c.Put(ctx, path+"/widgets/batch", map[string]any{"widgets": widgetEdits(in.Edits)}); err != nil {
		return nil, err
	}
	result := map[string]any{"id": in.DashboardID, "updated": len(in.Edits)}
	if board, err := readDashboard(ctx, c, in.DashboardID); err == nil {
		result["name"] = board.Name
	}
	return result, nil
}

func widgetEdits(edits []widgetEditArgs) []storage.WidgetEdit {
	out := make([]storage.WidgetEdit, len(edits))
	for i, e := range edits {
		out[i] = storage.WidgetEdit{ID: e.WidgetID, Title: e.Title, QueryContent: e.Query}
		if e.ChartType != "" {
			chart := e.ChartType
			out[i].ChartType = &chart
		}
	}
	return out
}

// planRemoval checks that every widget to remove is on the dashboard.
func planRemoval(ctx context.Context, c Client, in removeDashboardWidgetsArgs) (removeDashboardWidgetsArgs, error) {
	board, err := readDashboard(ctx, c, in.DashboardID)
	if err != nil {
		return in, err
	}
	if err := dashboards.CheckRemovals(board.Widgets, in.WidgetIDs); err != nil {
		return in, err
	}
	return in, nil
}

func removeDashboardWidgets(ctx context.Context, c Client, in removeDashboardWidgetsArgs) (any, error) {
	board, err := readDashboard(ctx, c, in.DashboardID)
	if err != nil {
		return nil, err
	}
	// Kept before the delete so the result can undo it.
	removing := map[string]bool{}
	for _, id := range in.WidgetIDs {
		removing[id] = true
	}
	var removed []dashboardWidgetArgs
	for _, w := range board.Widgets {
		if !removing[w.ID] {
			continue
		}
		x, y := w.PosX, w.PosY
		def := dashboardWidgetArgs{Query: w.QueryContent, ChartType: w.ChartType, PosX: &x, PosY: &y, Width: w.Width, Height: w.Height}
		if w.Title != nil {
			def.Title = *w.Title
		}
		removed = append(removed, def)
	}

	path := "/dashboards/" + url.PathEscape(in.DashboardID)
	if _, err := c.Post(ctx, path+"/widgets/batch/delete", map[string]any{"widget_ids": in.WidgetIDs}); err != nil {
		return nil, err
	}
	return map[string]any{
		"id":              in.DashboardID,
		"name":            board.Name,
		"removed":         len(removed),
		"removed_widgets": removed,
		"note":            "Removed. To undo, pass removed_widgets unchanged as the widgets of add_dashboard_widgets.",
	}, nil
}

type addDashboardWidgetsArgs struct {
	DashboardID string                `json:"dashboard_id" jsonschema:"The dashboard UUID, from list_dashboards."`
	Widgets     []dashboardWidgetArgs `json:"widgets" jsonschema:"The panels to add, in reading order."`
}

type dashboardVariableArgs struct {
	Name  string `json:"name" jsonschema:"Variable name, referenced in queries as @name. Letters, digits and underscores."`
	Value string `json:"value" jsonschema:"Default value, substituted as BQL. '*' matches everything."`
}

type dashboardWidgetArgs struct {
	Title string `json:"title" jsonschema:"Short panel title, for example 'Failed logons by host'."`
	Query string `json:"query" jsonschema:"BQL for the panel, ending in the rendering command that suits it (singleval, timechart, barchart, piechart, worldmap), or none for a table."`
	Width int    `json:"width,omitempty" jsonschema:"Columns of the 24-column grid: 6 is a quarter, 8 a third, 12 a half, 24 the full width. Omit to size by chart."`

	// Settled by preparation. They are in the schema because the prepared call
	// is what the user approves and what runs.
	ChartType string `json:"chart_type,omitempty" jsonschema:"Leave unset: filled in from the query."`
	PosX      *int   `json:"pos_x,omitempty" jsonschema:"Leave unset: filled in by the layout."`
	PosY      *int   `json:"pos_y,omitempty" jsonschema:"Leave unset: filled in by the layout."`
	Height    int    `json:"height,omitempty" jsonschema:"Leave unset: filled in by the layout."`
}

type createDashboardArgs struct {
	Name        string                  `json:"name" jsonschema:"Dashboard name, for example 'Authentication overview'."`
	Description string                  `json:"description,omitempty" jsonschema:"What the dashboard is for and how to read it."`
	TimeRange   string                  `json:"time_range,omitempty" jsonschema:"Window it opens with: lastN followed by m, h, d or w (last1h, last24h, last7d), or all. Default last24h."`
	Variables   []dashboardVariableArgs `json:"variables,omitempty" jsonschema:"Filters the viewer can change, referenced in queries as @name."`
	Widgets     []dashboardWidgetArgs   `json:"widgets" jsonschema:"The panels, in reading order."`
}

// planDashboard checks every query against the fractal and lays the widgets
// out, reading only. It reports every broken widget at once so the model can
// fix them in one round rather than one at a time.
func planDashboard(ctx context.Context, c Client, in createDashboardArgs) (createDashboardArgs, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.TimeRange = cmp.Or(strings.TrimSpace(in.TimeRange), "last24h")
	if in.TimeRange == "custom" || !dashboards.ValidTimeRangeType(in.TimeRange) {
		return in, fmt.Errorf("time_range must be all, or lastN followed by m, h, d or w (last15m, last24h, last7d)")
	}
	if len(in.Widgets) == 0 {
		return in, fmt.Errorf("a dashboard needs at least one widget")
	}

	if err := checkQueries(ctx, c, in.Widgets, newDashboardRequest(in).Variables); err != nil {
		return in, err
	}

	layoutWidgets(in.Widgets, 0)
	// The same check the server makes, so a call that would be refused after
	// approval is refused before the user is asked instead.
	if err := dashboards.CheckNew(newDashboardRequest(in)); err != nil {
		return in, err
	}
	return in, nil
}

// checkQueries validates every widget's query against the fractal, recording
// the chart each one renders as. It reports every broken widget at once so the
// model can fix them in one round rather than one at a time.
func checkQueries(ctx context.Context, c Client, widgets []dashboardWidgetArgs, vars []bqlvars.Variable) error {
	var problems []string
	for i := range widgets {
		w := &widgets[i]
		w.Title = strings.TrimSpace(w.Title)
		label := fmt.Sprintf("widget %d (%s)", i+1, w.Title)
		chart, problem, err := validateQuery(ctx, c, w.Query, vars)
		if err != nil {
			return fmt.Errorf("%s could not be checked: %w", label, err)
		}
		if problem != "" {
			problems = append(problems, label+": "+problem)
			continue
		}
		w.ChartType = chart
	}
	if len(problems) > 0 {
		return fmt.Errorf("nothing was changed; fix these widgets and try again:\n%s", strings.Join(problems, "\n"))
	}
	return nil
}

// validateQuery asks the server whether a query would run here, returning the
// chart it renders as, or the problem with it.
func validateQuery(ctx context.Context, c Client, query string, vars []bqlvars.Variable) (chart, problem string, err error) {
	if strings.TrimSpace(query) == "" {
		return "", "a query is required", nil
	}
	verdict, err := c.Post(ctx, "/query/validate", map[string]any{"query": query, "variables": vars})
	if err != nil {
		return "", "", err
	}
	if !Field[bool](verdict, "valid") {
		return "", Field[string](verdict, "error"), nil
	}
	return Field[string](verdict, "chart_type"), "", nil
}

// planAddition checks new widgets against the dashboard they join and places
// them below it, reading only.
func planAddition(ctx context.Context, c Client, in addDashboardWidgetsArgs) (addDashboardWidgetsArgs, error) {
	if len(in.Widgets) == 0 {
		return in, fmt.Errorf("give at least one widget to add")
	}
	board, err := readDashboard(ctx, c, in.DashboardID)
	if err != nil {
		return in, err
	}
	if err := checkQueries(ctx, c, in.Widgets, bqlvars.ParseVariables(board.Variables)); err != nil {
		return in, err
	}
	bottom := 0
	for _, w := range board.Widgets {
		bottom = max(bottom, w.PosY+w.Height)
	}
	layoutWidgets(in.Widgets, bottom)
	if err := dashboards.CheckAddition(board.Variables, board.Widgets, widgetRequests(in.Widgets)); err != nil {
		return in, err
	}
	return in, nil
}

func addDashboardWidgets(ctx context.Context, c Client, in addDashboardWidgetsArgs) (any, error) {
	path := "/dashboards/" + url.PathEscape(in.DashboardID)
	if _, err := c.Post(ctx, path+"/widgets/batch", map[string]any{"widgets": widgetRequests(in.Widgets)}); err != nil {
		return nil, err
	}
	result := map[string]any{
		"id":    in.DashboardID,
		"added": len(in.Widgets),
		"note":  "Added. Anyone viewing the dashboard sees the new panels appear.",
	}
	if board, err := readDashboard(ctx, c, in.DashboardID); err == nil {
		result["name"] = board.Name
		result["widgets"] = len(board.Widgets)
	}
	return result, nil
}

// readDashboard reads a dashboard and its widgets in the server's own shape.
func readDashboard(ctx context.Context, c Client, id string) (*storage.Dashboard, error) {
	payload, err := c.Get(ctx, "/dashboards/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var board storage.Dashboard
	if err := json.Unmarshal(encoded, &board); err != nil {
		return nil, fmt.Errorf("the dashboard could not be read: %w", err)
	}
	return &board, nil
}

// newDashboardRequest is the create request a prepared call sends.
func newDashboardRequest(in createDashboardArgs) *dashboards.CreateDashboardRequest {
	vars := make([]bqlvars.Variable, len(in.Variables))
	for i, v := range in.Variables {
		vars[i] = bqlvars.Variable{Name: v.Name, Value: v.Value}
	}
	return &dashboards.CreateDashboardRequest{
		Name:          in.Name,
		Description:   in.Description,
		TimeRangeType: in.TimeRange,
		Variables:     vars,
		Widgets:       widgetRequests(in.Widgets),
	}
}

func createDashboard(ctx context.Context, c Client, in createDashboardArgs) (any, error) {
	created, err := c.Post(ctx, "/dashboards", newDashboardRequest(in))
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"id":      Field[string](created, "id"),
		"name":    Field[string](created, "name"),
		"widgets": len(in.Widgets),
		"note":    "Created. The user can open it from the Dashboards tab; its panels run when it is opened.",
	}, nil
}

func widgetRequests(widgets []dashboardWidgetArgs) []dashboards.CreateWidgetRequest {
	out := make([]dashboards.CreateWidgetRequest, len(widgets))
	for i, w := range widgets {
		out[i] = dashboards.CreateWidgetRequest{
			QueryContent: w.Query,
			ChartType:    w.ChartType,
			Width:        w.Width,
			Height:       w.Height,
		}
		if w.Title != "" {
			title := w.Title
			out[i].Title = &title
		}
		if w.PosX != nil && w.PosY != nil {
			out[i].PosX, out[i].PosY = *w.PosX, *w.PosY
		}
	}
	return out
}

// panelSize is the width and height a chart reads best at when nothing asks
// for another width. Heights are in 26px grid rows.
func panelSize(chartType string) (width, height int) {
	switch chartType {
	case "singleval":
		return 6, 5
	case "timechart":
		return 24, 10
	case "piechart":
		return 8, 12
	case "barchart", "histogram", "heatmap", "boxplot", "scatter":
		return 12, 12
	case "worldmap", "graph", "pgraph", "mesh", "mitre":
		return 24, 16
	}
	return 24, 12
}

// layoutWidgets flows widgets onto the grid in order, from row top. A row closes when the
// next widget would not fit; its last widget takes up the slack so the row
// spans the grid, and every widget in it takes the tallest one's height. A set
// of widgets that already carries a full placement is left as it is.
func layoutWidgets(widgets []dashboardWidgetArgs, top int) {
	placed := true
	for _, w := range widgets {
		placed = placed && w.PosX != nil && w.PosY != nil && w.Width > 0 && w.Height > 0
	}
	if placed {
		return
	}

	cols := storage.DashboardGridColumns
	y, x, rowStart, rowHeight := top, 0, 0, 0
	closeRow := func(end int) {
		if end == rowStart {
			return
		}
		widgets[end-1].Width += cols - x
		for i := rowStart; i < end; i++ {
			widgets[i].Height = rowHeight
		}
		y += rowHeight
		x, rowStart, rowHeight = 0, end, 0
	}
	for i := range widgets {
		w := &widgets[i]
		width, height := panelSize(w.ChartType)
		if w.Width > 0 {
			width = min(w.Width, cols)
		}
		if x+width > cols {
			closeRow(i)
		}
		posX, posY := x, y
		w.PosX, w.PosY, w.Width = &posX, &posY, width
		x += width
		rowHeight = max(rowHeight, height)
	}
	closeRow(len(widgets))
}
