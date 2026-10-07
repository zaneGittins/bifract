package dashboards

import (
	"regexp"
	"strconv"
	"time"

	"bifract/pkg/storage"
)

// relativeRange matches the rolling windows a dashboard can default to, e.g.
// last15m, last4h, last7d, last2w.
var relativeRange = regexp.MustCompile(`^last([1-9][0-9]{0,4})([mhdw])$`)

var rangeUnits = map[string]time.Duration{
	"m": time.Minute,
	"h": time.Hour,
	"d": 24 * time.Hour,
	"w": 7 * 24 * time.Hour,
}

func relativeDuration(rangeType string) (time.Duration, bool) {
	m := relativeRange.FindStringSubmatch(rangeType)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return time.Duration(n) * rangeUnits[m[2]], true
}

func validTimeRangeType(rangeType string) bool {
	if rangeType == "all" || rangeType == "custom" {
		return true
	}
	_, ok := relativeDuration(rangeType)
	return ok
}

const timeRangeTypeHelp = "time_range_type must be all, custom, or lastN followed by m, h, d or w (e.g. last15m, last24h)"

// clampLayout keeps a widget inside the grid and at least one cell in size.
func clampLayout(x, y, w, h int) (int, int, int, int) {
	const maxRows = 2000
	w = min(max(w, 1), storage.DashboardGridColumns)
	h = min(max(h, 1), maxRows)
	x = min(max(x, 0), storage.DashboardGridColumns-w)
	y = min(max(y, 0), maxRows)
	return x, y, w, h
}
