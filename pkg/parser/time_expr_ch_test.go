package parser_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bifract/db"
	"bifract/pkg/parser"
)

// timeRows are the logs the time expression tests read, as (timestamp, fields).
// 2026-03-08 is a Sunday, and the day New York moves to daylight time at 07:00 UTC.
var timeRows = [][2]string{
	// 25 failed logons from one address inside ten minutes, 5 from another.
	{"2026-03-04 10:00:00.000", `{"event_id":"4625","src_ip":"10.0.0.1"}`},
	{"2026-03-04 10:05:00.000", `{"event_id":"4625","src_ip":"10.0.0.2"}`},
	// Logons at 03:00 and 12:00 on a Wednesday, and at noon on the Sunday.
	{"2026-03-04 03:00:00.000", `{"event_id":"4624","user":"night"}`},
	{"2026-03-04 12:00:00.000", `{"event_id":"4624","user":"day"}`},
	{"2026-03-08 12:00:00.000", `{"event_id":"4624","user":"sunday"}`},
	// Around New York's change to daylight time: 23:30 EST on the 7th, then
	// 01:30 EST and 03:30 EDT on the 8th.
	{"2026-03-08 04:30:00.000", `{"event_id":"9","host":"ny"}`},
	{"2026-03-08 06:30:00.000", `{"event_id":"9","host":"ny"}`},
	{"2026-03-08 07:30:00.000", `{"event_id":"9","host":"ny"}`},
	// Process start times 30 seconds and 5 minutes before the event, and one that
	// is no time at all.
	{"2026-03-04 11:00:30.000", `{"event_id":"1","pid":"a","process_start":"2026-03-04 11:00:00"}`},
	{"2026-03-04 11:05:00.000", `{"event_id":"1","pid":"b","process_start":"2026-03-04 11:00:00"}`},
	{"2026-03-04 11:06:00.000", `{"event_id":"1","pid":"c","process_start":"not a time"}`},
	// Connection durations in seconds.
	{"2026-03-04 13:00:00.000", `{"event_id":"3","conn":"long","duration":"150"}`},
	{"2026-03-04 13:00:01.000", `{"event_id":"3","conn":"short","duration":"60"}`},
}

func timeRowsSQL() string {
	var rows []string
	add := func(i int, ts, fields string) {
		rows = append(rows, fmt.Sprintf("('%s', 'l%d', '%s', 'f1')", ts, i, strings.ReplaceAll(fields, "'", "\\'")))
	}
	for i, r := range timeRows {
		add(i, r[0], r[1])
	}
	// The rest of the burst from 10.0.0.1.
	for i := 1; i < 25; i++ {
		add(1000+i, fmt.Sprintf("2026-03-04 10:%02d:%02d.000", i*20/60, i*20%60), `{"event_id":"4625","src_ip":"10.0.0.1"}`)
	}
	for i := 1; i < 5; i++ {
		add(2000+i, fmt.Sprintf("2026-03-04 10:06:%02d.000", i), `{"event_id":"4625","src_ip":"10.0.0.2"}`)
	}
	return "INSERT INTO logs (timestamp, log_id, fields, fractal_id) VALUES " + strings.Join(rows, ", ") + ";\n"
}

// TestTimeExpressionsExecute runs time expressions against rows in ClickHouse and
// checks what they return, which a check of the SQL text or of its types cannot.
func TestTimeExpressionsExecute(t *testing.T) {
	bin := clickhouseBin(t)
	cases := []struct {
		query, tz string
		want      []string
	}{
		{`event_id=4625 | groupBy([bin(timestamp, 10m), src_ip]) | _count > 20 | table(bin_timestamp_10m, src_ip, _count)`, "",
			[]string{"2026-03-04 10:00:00\t10.0.0.1\t25"}},
		{`event_id=4624 | hourOfDay(timestamp) < 6 OR dayOfWeek(timestamp) >= 6 | table(user)`, "",
			[]string{"night", "sunday"}},
		// Calendar days and hours follow the viewer's zone, across the change to
		// daylight time, which skips 02:00.
		{`event_id=9 | groupBy(bin(timestamp, 1d)) | table(bin_timestamp_1d, _count)`, "America/New_York",
			[]string{"2026-03-07 00:00:00\t1", "2026-03-08 00:00:00\t2"}},
		{`event_id=9 | groupBy(bin(timestamp, 1h)) | table(bin_timestamp_1h, _count)`, "America/New_York",
			[]string{"2026-03-07 23:00:00\t1", "2026-03-08 01:00:00\t1", "2026-03-08 03:00:00\t1"}},
		{`event_id=9 | h := hourOfDay(timestamp, timezone="America/New_York") | table(h)`, "",
			[]string{"23", "1", "3"}},
		// Computed in the final SELECT, beside toString(timestamp) AS timestamp.
		{`event_id=1 | age := timestamp - process_start | table(pid, age)`, "",
			[]string{"a\t30", "b\t300", "c\t\\N"}},
		{`event_id=1 | (timestamp - process_start) < 1m | table(pid)`, "",
			[]string{"a"}},
		{`event_id=1 | started := timestamp - 30s | table(pid, started)`, "",
			[]string{"a\t2026-03-04 11:00:00.000", "b\t2026-03-04 11:04:30.000", "c\t2026-03-04 11:05:30.000"}},
		{`event_id=1 | gap := timestamp - process_start | gap > 1m | table(pid)`, "",
			[]string{"b"}},
		{`event_id=3 | duration > 2m | table(conn)`, "",
			[]string{"long"}},
		{`event_id=4624 | groupBy(event_id, function=[min(timestamp), max(bin(timestamp, 1d))]) | table(_min, _max)`, "",
			[]string{"2026-03-04 03:00:00.000\t2026-03-08 00:00:00"}},
		{`event_id=1 | b := bin(process_start, 1h) | table(pid, b)`, "",
			[]string{"a\t2026-03-04 11:00:00", "b\t2026-03-04 11:00:00", "c\t\\N"}},
	}

	dir := t.TempDir()
	var script strings.Builder
	script.WriteString(strings.TrimRight(strings.TrimSpace(db.ClickHouseSQL), ";") + ";\nUSE logs;\n")
	script.WriteString(timeRowsSQL())
	for i, c := range cases {
		pl, err := parser.ParseQuery(c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		opts := parser.QueryOptions{StartTime: start, EndTime: start.Add(30 * 24 * time.Hour), MaxRows: 100, FractalID: "f1", DisplayTimezone: c.tz}
		r, err := parser.TranslateToSQLWithOrder(pl, opts)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		fmt.Fprintf(&script, "SELECT 'CASE %d' FORMAT TSV;\n%s FORMAT TSV;\n", i, strings.TrimRight(r.SQL, "; \n"))
	}
	path := filepath.Join(dir, "time.sql")
	if err := os.WriteFile(path, []byte(script.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(bin, "local", "--path", filepath.Join(dir, "data"), "--multiquery", "--queries-file", path).CombinedOutput()
	if err != nil {
		t.Fatalf("clickhouse local: %v\n%s", err, out)
	}
	got := map[int][]string{}
	current := -1
	for _, line := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
		if n, ok := strings.CutPrefix(line, "CASE "); ok {
			fmt.Sscan(n, &current)
			continue
		}
		if current >= 0 {
			got[current] = append(got[current], line)
		}
	}
	for i, c := range cases {
		// Row queries also return the identity columns; compare the ones asked for.
		width := strings.Count(c.want[0], "\t") + 1
		for j, row := range got[i] {
			got[i][j] = strings.Join(strings.SplitN(row, "\t", width+1)[:min(width, strings.Count(row, "\t")+1)], "\t")
		}
		rows, want := slices.Sorted(slices.Values(got[i])), slices.Sorted(slices.Values(c.want))
		if !slices.Equal(rows, want) {
			t.Errorf("%s (zone %q)\n  got  %q\n  want %q", c.query, c.tz, rows, want)
		}
	}
}
