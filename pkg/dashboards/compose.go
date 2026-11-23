package dashboards

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"bifract/pkg/bqlvars"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// Bounds on a dashboard built in one request. They keep a single call, which
// may come from an AI tool acting on injected text, from writing an unbounded
// amount of state.
const (
	maxWidgets = 50
	// maxDashboardWidgets bounds what additions may grow a dashboard to.
	maxDashboardWidgets = 200
	maxTitleLen         = 200
	maxQueryLen         = 16000
	maxVariables        = 32
	maxVariableLen      = 1000
)

var variableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// widgetShape is what checkWidgets needs to know about a widget, whether it
// arrived as JSON or in an imported file.
type widgetShape struct {
	title, query        string
	x, y, width, height int
}

func (w widgetShape) overlaps(o widgetShape) bool {
	return w.x < o.x+o.width && o.x < w.x+w.width && w.y < o.y+o.height && o.y < w.y+w.height
}

// CheckNew normalizes and validates a request to create a dashboard. It is the
// one check a create must pass, so a caller that prepares a request elsewhere
// (an AI tool asking a user first) can refuse exactly what the server would.
func CheckNew(req *CreateDashboardRequest) error {
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(req.Name) > maxTitleLen {
		return fmt.Errorf("name is longer than %d characters", maxTitleLen)
	}
	if req.TimeRangeType == "" {
		req.TimeRangeType = "last1h"
	}
	if !ValidTimeRangeType(req.TimeRangeType) {
		return fmt.Errorf("%s", TimeRangeTypeHelp)
	}
	if req.TimeRangeType == "custom" && (req.TimeRangeStart == nil || req.TimeRangeEnd == nil) {
		return fmt.Errorf("time_range_start and time_range_end are required for custom time range")
	}
	req.Timezone = strings.TrimSpace(req.Timezone)
	if req.Timezone != "" && !storage.ValidTimezone(req.Timezone) {
		return fmt.Errorf("Unknown timezone")
	}
	return checkComposition(req.Variables, req.Widgets)
}

// checkComposition validates the variables and any widgets of a dashboard
// created in one request. Every widget must parse with the variables substituted and
// sit on the grid without overlapping another, so nothing is clamped or moved
// from what the caller asked for.
func checkComposition(vars []bqlvars.Variable, widgets []CreateWidgetRequest) error {
	if len(vars) > maxVariables {
		return fmt.Errorf("a dashboard takes at most %d variables", maxVariables)
	}
	seen := map[string]bool{}
	for _, v := range vars {
		if !variableName.MatchString(v.Name) {
			return fmt.Errorf("variable name %q must be a letter or underscore followed by letters, digits or underscores", v.Name)
		}
		if seen[v.Name] {
			return fmt.Errorf("variable %q is declared more than once", v.Name)
		}
		seen[v.Name] = true
		if len(v.Value) > maxVariableLen {
			return fmt.Errorf("variable %q is longer than %d characters", v.Name, maxVariableLen)
		}
	}

	if len(widgets) == 0 {
		return nil
	}
	return checkWidgets(shapesOf(widgets), nil, bqlvars.ToValueMap(vars))
}

// CheckAddition validates widgets to be added to a dashboard holding existing,
// with the dashboard's own variables. The new widgets must parse and must not
// overlap each other or anything already on the board, which is not moved.
func CheckAddition(variables json.RawMessage, existing []storage.DashboardWidget, widgets []CreateWidgetRequest) error {
	if len(widgets) == 0 {
		return fmt.Errorf("widgets must hold at least one widget")
	}
	if len(widgets) > maxWidgets {
		return fmt.Errorf("at most %d widgets can be added at once", maxWidgets)
	}
	if len(existing)+len(widgets) > maxDashboardWidgets {
		return fmt.Errorf("a dashboard holds at most %d widgets; this one has %d", maxDashboardWidgets, len(existing))
	}
	fixed := make([]widgetShape, len(existing))
	for i, w := range existing {
		fixed[i] = widgetShape{query: w.QueryContent, x: w.PosX, y: w.PosY, width: w.Width, height: w.Height}
		if w.Title != nil {
			fixed[i].title = *w.Title
		}
	}
	return checkWidgets(shapesOf(widgets), fixed, bqlvars.ToValueMap(bqlvars.ParseVariables(variables)))
}

func shapesOf(widgets []CreateWidgetRequest) []widgetShape {
	shapes := make([]widgetShape, len(widgets))
	for i, w := range widgets {
		shapes[i] = widgetShape{query: w.QueryContent, x: w.PosX, y: w.PosY, width: w.Width, height: w.Height}
		if w.Title != nil {
			shapes[i].title = *w.Title
		}
	}
	return shapes
}

// checkWidgets requires each widget to parse and to sit on the 24-column grid
// without overlapping another or any of fixed, which are already placed and
// are not checked themselves. Errors name the widget by its 1-based position
// so a caller can fix exactly that one.
func checkWidgets(widgets, fixed []widgetShape, values map[string]string) error {
	if len(widgets) == 0 {
		return fmt.Errorf("a dashboard needs widgets")
	}
	if len(widgets) > maxWidgets {
		return fmt.Errorf("a dashboard takes at most %d widgets", maxWidgets)
	}
	cols := storage.DashboardGridColumns
	for i, w := range widgets {
		label := fmt.Sprintf("widget %d (%s)", i+1, w.title)
		if len(w.title) > maxTitleLen {
			return fmt.Errorf("%s: title is longer than %d characters", label, maxTitleLen)
		}
		if strings.TrimSpace(w.query) == "" {
			return fmt.Errorf("%s: query is required", label)
		}
		if len(w.query) > maxQueryLen {
			return fmt.Errorf("%s: query is longer than %d characters", label, maxQueryLen)
		}
		if _, err := parser.ParseQuery(bqlvars.SubstituteMap(w.query, values)); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		if w.x < 0 || w.y < 0 || w.width < 1 || w.height < 1 || w.x+w.width > cols || w.y+w.height > maxGridRows {
			return fmt.Errorf("%s is off the %d-column grid", label, cols)
		}
		for j, o := range widgets[:i] {
			if w.overlaps(o) {
				return fmt.Errorf("%s overlaps widget %d (%s)", label, j+1, o.title)
			}
		}
		for _, o := range fixed {
			if w.overlaps(o) {
				return fmt.Errorf("%s overlaps the existing widget %q", label, o.title)
			}
		}
	}
	return nil
}

// CheckEdits validates edits to widgets already on a dashboard: each names a
// widget on it once, changes something, and any new query parses with the
// dashboard's variables.
func CheckEdits(variables json.RawMessage, existing []storage.DashboardWidget, edits []storage.WidgetEdit) error {
	if len(edits) == 0 {
		return fmt.Errorf("widgets must hold at least one edit")
	}
	byID := widgetsByID(existing)
	values := bqlvars.ToValueMap(bqlvars.ParseVariables(variables))
	seen := map[string]bool{}
	for i, e := range edits {
		current, ok := byID[e.ID]
		if !ok {
			return fmt.Errorf("edit %d: widget %q is not on this dashboard", i+1, e.ID)
		}
		if seen[e.ID] {
			return fmt.Errorf("edit %d: widget %q is edited more than once", i+1, e.ID)
		}
		seen[e.ID] = true
		label := fmt.Sprintf("edit %d (%q)", i+1, titleOf(current))
		if e.Title == nil && e.QueryContent == nil {
			return fmt.Errorf("%s: nothing to change", label)
		}
		if e.Title != nil && len(*e.Title) > maxTitleLen {
			return fmt.Errorf("%s: title is longer than %d characters", label, maxTitleLen)
		}
		if e.QueryContent != nil {
			if strings.TrimSpace(*e.QueryContent) == "" {
				return fmt.Errorf("%s: query is required", label)
			}
			if len(*e.QueryContent) > maxQueryLen {
				return fmt.Errorf("%s: query is longer than %d characters", label, maxQueryLen)
			}
			if _, err := parser.ParseQuery(bqlvars.SubstituteMap(*e.QueryContent, values)); err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
		}
	}
	return nil
}

// CheckRemovals validates removing widgets from a dashboard: each id names a
// widget on it, once.
func CheckRemovals(existing []storage.DashboardWidget, widgetIDs []string) error {
	if len(widgetIDs) == 0 {
		return fmt.Errorf("widget_ids must name at least one widget")
	}
	byID := widgetsByID(existing)
	seen := map[string]bool{}
	for _, id := range widgetIDs {
		if _, ok := byID[id]; !ok {
			return fmt.Errorf("widget %q is not on this dashboard", id)
		}
		if seen[id] {
			return fmt.Errorf("widget %q is named more than once", id)
		}
		seen[id] = true
	}
	return nil
}

func widgetsByID(widgets []storage.DashboardWidget) map[string]storage.DashboardWidget {
	out := make(map[string]storage.DashboardWidget, len(widgets))
	for _, w := range widgets {
		out[w.ID] = w
	}
	return out
}

func titleOf(w storage.DashboardWidget) string {
	if w.Title != nil {
		return *w.Title
	}
	return ""
}
