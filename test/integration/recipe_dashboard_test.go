//go:build integration

// Building a dashboard in one request. The widgets travel with the dashboard
// and are stored in one transaction, so a client (or an AI tool) either gets
// the whole dashboard or nothing.
//
//	go test -tags integration ./test/integration/ -run TestDashboardInOneRequest -v

package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestDashboardInOneRequest(t *testing.T) {
	t.Parallel()
	c := New(t)
	_, scoped := newFractal(t, c, "dashboard")
	name := fmt.Sprintf("api-suite-%d", time.Now().UnixNano())

	// 1. A dashboard with a variable and two widgets on the 24-column grid.
	var created struct {
		ID      string `json:"id"`
		Widgets []struct {
			QueryContent string `json:"query_content"`
			PosY         int    `json:"pos_y"`
		} `json:"widgets"`
	}
	scoped.Do(t, "POST", "/dashboards", map[string]any{
		"name":            name,
		"time_range_type": "last24h",
		"variables":       []map[string]string{{"name": "lvl", "value": "*"}},
		"widgets": []map[string]any{
			{"title": "Events", "query_content": "level=@lvl | count() | singleval()", "pos_x": 0, "pos_y": 0, "width": 6, "height": 5},
			{"title": "By host", "query_content": "level=@lvl | groupby(host) | barchart()", "pos_x": 0, "pos_y": 5, "width": 24, "height": 12},
		},
	}, &created)
	if created.ID == "" {
		t.Fatal("the created dashboard came back without an id")
	}
	t.Cleanup(func() { scoped.Status(t, "DELETE", "/dashboards/"+created.ID, nil) })
	if len(created.Widgets) != 2 || created.Widgets[1].PosY != 5 {
		t.Fatalf("widgets = %+v", created.Widgets)
	}

	// 2. Read back, the widgets are part of the dashboard.
	var read struct {
		Widgets []struct{ ID string } `json:"widgets"`
	}
	scoped.Do(t, "GET", "/dashboards/"+created.ID, nil, &read)
	if len(read.Widgets) != 2 {
		t.Errorf("read back %d widgets, want 2", len(read.Widgets))
	}

	// 3. Add two more below the existing ones in one request. They use the
	//    dashboard's own variable.
	scoped.Do(t, "POST", "/dashboards/"+created.ID+"/widgets/batch", map[string]any{
		"widgets": []map[string]any{
			{"title": "Trend", "query_content": "level=@lvl | timechart(span=1h)", "pos_x": 0, "pos_y": 17, "width": 12, "height": 10},
			{"title": "Top", "query_content": "level=@lvl | groupby(host)", "pos_x": 12, "pos_y": 17, "width": 12, "height": 10},
		},
	}, nil)
	scoped.Do(t, "GET", "/dashboards/"+created.ID, nil, &read)
	if len(read.Widgets) != 4 {
		t.Errorf("after the batch the dashboard holds %d widgets, want 4", len(read.Widgets))
	}

	// 4. A batch that lands on an existing widget is refused whole.
	status, _ := scoped.Failure(t, "POST", "/dashboards/"+created.ID+"/widgets/batch", map[string]any{
		"widgets": []map[string]any{
			{"query_content": "*", "pos_x": 0, "pos_y": 40, "width": 6, "height": 5},
			{"query_content": "*", "pos_x": 0, "pos_y": 2, "width": 6, "height": 5},
		},
	})
	if status != http.StatusBadRequest {
		t.Errorf("an overlapping batch answered %d, want 400", status)
	}
	scoped.Do(t, "GET", "/dashboards/"+created.ID, nil, &read)
	if len(read.Widgets) != 4 {
		t.Errorf("a refused batch left %d widgets, want 4", len(read.Widgets))
	}

	// 5. Edit two widgets at once. An edit naming a widget that is not on the
	//    board refuses the whole set.
	var board struct {
		Widgets []struct {
			ID           string `json:"id"`
			Title        string `json:"title"`
			QueryContent string `json:"query_content"`
		} `json:"widgets"`
	}
	scoped.Do(t, "GET", "/dashboards/"+created.ID, nil, &board)
	first, second := board.Widgets[0].ID, board.Widgets[1].ID
	scoped.Do(t, "PUT", "/dashboards/"+created.ID+"/widgets/batch", map[string]any{
		"widgets": []map[string]any{
			{"id": first, "title": "Renamed"},
			{"id": second, "query_content": "level=@lvl | groupby(host) | piechart()"},
		},
	}, nil)
	status, _ = scoped.Failure(t, "PUT", "/dashboards/"+created.ID+"/widgets/batch", map[string]any{
		"widgets": []map[string]any{
			{"id": first, "title": "Not applied"},
			{"id": "00000000-0000-0000-0000-000000000000", "title": "x"},
		},
	})
	if status != http.StatusBadRequest {
		t.Errorf("an edit of a missing widget answered %d, want 400", status)
	}
	scoped.Do(t, "GET", "/dashboards/"+created.ID, nil, &board)
	for _, w := range board.Widgets {
		if w.ID == first && w.Title != "Renamed" {
			t.Errorf("first widget title = %q, want Renamed", w.Title)
		}
		if w.ID == second && w.QueryContent != "level=@lvl | groupby(host) | piechart()" {
			t.Errorf("second widget query = %q", w.QueryContent)
		}
	}

	// 6. Remove two widgets at once.
	scoped.Do(t, "POST", "/dashboards/"+created.ID+"/widgets/batch/delete", map[string]any{
		"widget_ids": []string{first, second},
	}, nil)
	scoped.Do(t, "GET", "/dashboards/"+created.ID, nil, &read)
	if len(read.Widgets) != 2 {
		t.Errorf("after removing two of four, %d widgets remain", len(read.Widgets))
	}

	// 7. One bad widget refuses the whole dashboard: nothing is left behind.
	broken := name + "-broken"
	status, _ = scoped.Failure(t, "POST", "/dashboards", map[string]any{
		"name": broken,
		"widgets": []map[string]any{
			{"query_content": "*", "pos_x": 0, "pos_y": 0, "width": 12, "height": 5},
			{"query_content": "*", "pos_x": 6, "pos_y": 2, "width": 12, "height": 5},
		},
	})
	if status != http.StatusBadRequest {
		t.Errorf("overlapping widgets answered %d, want 400", status)
	}
	var listed []struct{ Name string }
	scoped.Do(t, "GET", "/dashboards?limit=100&search="+broken, nil, &listed)
	for _, d := range listed {
		if d.Name == broken {
			t.Error("a refused dashboard was stored anyway")
		}
	}
}
