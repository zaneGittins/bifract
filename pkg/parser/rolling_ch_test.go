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

// rollingRows are logons and connections, as (timestamp, log_id, fields).
var rollingRows = [][3]string{
	// alice logs on twice from the US, then from RU 30 minutes later.
	{"2026-03-04 10:00:00.000", "a1", `{"event_id":"4624","user":"alice","country":"US","bytes":"10"}`},
	{"2026-03-04 11:00:00.000", "a2", `{"event_id":"4624","user":"alice","country":"US","bytes":"30"}`},
	{"2026-03-04 11:30:00.000", "a3", `{"event_id":"4624","user":"alice","country":"RU","bytes":"5"}`},
	// bob moves from DE to FR, five hours apart.
	{"2026-03-04 09:00:00.000", "b1", `{"event_id":"4624","user":"bob","country":"DE","bytes":"7"}`},
	{"2026-03-04 14:00:00.000", "b2", `{"event_id":"4624","user":"bob","country":"FR","bytes":"7"}`},
	// carol's two logons share a timestamp; log_id orders them.
	{"2026-03-04 12:00:00.000", "c2", `{"event_id":"4624","user":"carol","country":"SE","bytes":"2"}`},
	{"2026-03-04 12:00:00.000", "c1", `{"event_id":"4624","user":"carol","country":"NO","bytes":"1"}`},
}

func rollingRowsSQL() string {
	var rows []string
	for _, r := range rollingRows {
		rows = append(rows, fmt.Sprintf("('%s', '%s', '%s', 'f1')", r[0], r[1], r[2]))
	}
	// A beacon every 60s from .5, and an irregular talker from .6.
	for i, gap := range []int{0, 60, 120, 180, 240} {
		rows = append(rows, fmt.Sprintf("('2026-03-04 13:%02d:%02d.000', 'n5%d', '{\"event_id\":\"3\",\"src_ip\":\"10.0.0.5\",\"dst_ip\":\"1.2.3.4\"}', 'f1')", gap/60, gap%60, i))
	}
	for i, at := range []int{0, 10, 200, 230} {
		rows = append(rows, fmt.Sprintf("('2026-03-04 13:%02d:%02d.000', 'n6%d', '{\"event_id\":\"3\",\"src_ip\":\"10.0.0.6\",\"dst_ip\":\"1.2.3.4\"}', 'f1')", at/60, at%60, i))
	}
	return "INSERT INTO logs (timestamp, log_id, fields, fractal_id) VALUES " + strings.Join(rows, ", ") + ";\n"
}

// rollingCases are queries over rollingRows and the rows each must return.
var rollingCases = []struct {
	query string
	want  []string
}{
	// A group's first event has no previous one: NULL for a time, '' for text,
	// never 1970-01-01.
	{`event_id=4624 | rolling(user, function=[previous(timestamp), previous(country, as=pc)]) | table(log_id, _previous, pc)`,
		[]string{"a1\t\\N\t", "a2\t2026-03-04 10:00:00.000\tUS", "a3\t2026-03-04 11:00:00.000\tUS",
			"b1\t\\N\t", "b2\t2026-03-04 09:00:00.000\tDE", "c1\t\\N\t", "c2\t2026-03-04 12:00:00.000\tNO"}},
	{`event_id=4624 | rolling(user, function=previous(country)) | isEmpty(_previous) | table(log_id)`,
		[]string{"a1", "b1", "c1"}},
	// Running aggregates include the current event.
	{`event_id=4624 | rolling(user, function=[count(), sum(bytes), avg(bytes), min(bytes), max(timestamp), count(country, unique=true, as=countries)]) | table(log_id, _count, _sum, _avg, _min, _max, countries)`,
		[]string{"a1\t1\t10\t10\t10\t2026-03-04 10:00:00.000\t1", "a2\t2\t40\t20\t10\t2026-03-04 11:00:00.000\t1", "a3\t3\t45\t15\t5\t2026-03-04 11:30:00.000\t2",
			"b1\t1\t7\t7\t7\t2026-03-04 09:00:00.000\t1", "b2\t2\t14\t7\t7\t2026-03-04 14:00:00.000\t2",
			"c1\t1\t1\t1\t1\t2026-03-04 12:00:00.000\t1", "c2\t2\t3\t1.5\t1\t2026-03-04 12:00:00.000\t2"}},
	// Without keys every event is one group.
	{`event_id=4624 | rolling() | _count = 7 | table(log_id)`, []string{"b2"}},
	// A filter before rolling() selects what it reads; one after filters its output.
	{`event_id=4624 country=US | rolling(user) | table(log_id, _count)`, []string{"a1\t1", "a2\t2"}},
	{`event_id=4624 | rolling(user) | country=RU | table(log_id, _count)`, []string{"a3\t3"}},
	{`event_id=4624 | rolling(user) | _count > 1 | groupBy(user) | table(user, _count)`, []string{"alice\t2", "bob\t1", "carol\t1"}},
	{`event_id=4624 | rolling(user) | groupBy(user, function=max(_count)) | table(user, _max)`, []string{"alice\t3", "bob\t2", "carol\t2"}},
	// A row cap before it bounds the rows it reads; after it, the rows it returns.
	{`event_id=4624 | head(2) | rolling(user) | table(log_id, _count)`, []string{"b1\t1", "a1\t1"}},
	{`event_id=4624 | rolling(user) | head(2) | table(log_id, _count)`, []string{"b1\t1", "a1\t1"}},
	{`event_id=4624 | rolling(user) | rolling(user, function=sum(_count, as=s)) | table(log_id, s)`,
		[]string{"a1\t1", "a2\t3", "a3\t6", "b1\t1", "b2\t3", "c1\t1", "c2\t3"}},
	// Impossible travel.
	{`event_id=4624
	  | rolling(user, function=[previous(country, as=prev_country), previous(timestamp, as=prev_time)])
	  | !isEmpty(prev_country) AND country != field(prev_country)
	  | minutes := dateDiff("minute", prev_time, timestamp)
	  | minutes < 120
	  | table(user, prev_country, country, minutes)`,
		[]string{"alice\tUS\tRU\t30", "carol\tNO\tSE\t0"}},
	// Beaconing: a steady gap has no spread.
	{`event_id=3 | rolling([src_ip, dst_ip], function=previous(timestamp))
	  | gap := dateDiff("second", _previous, timestamp)
	  | groupBy([src_ip, dst_ip], function=[stddev(gap), count()])
	  | table(src_ip, _stddev, _count)`,
		[]string{"10.0.0.5\t0\t5", "10.0.0.6\t80.55363982396382\t4"}},
}

// TestRollingExecute runs rolling() over rows in ClickHouse: each running value is
// checked against what it must be, which a check of the SQL text cannot do.
func TestRollingExecute(t *testing.T) {
	bin := clickhouseBin(t)
	cases := rollingCases

	dir := t.TempDir()
	var script strings.Builder
	script.WriteString(strings.TrimRight(strings.TrimSpace(db.ClickHouseSQL), ";") + ";\nUSE logs;\n")
	script.WriteString(rollingRowsSQL())
	for i, c := range cases {
		pl, err := parser.ParseQuery(c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		opts := parser.QueryOptions{StartTime: start, EndTime: start.Add(30 * 24 * time.Hour), MaxRows: 100, FractalID: "f1"}
		r, err := parser.TranslateToSQLWithOrder(pl, opts)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		fmt.Fprintf(&script, "SELECT 'CASE %d' FORMAT TSV;\n%s FORMAT TSV;\n", i, strings.TrimRight(r.SQL, "; \n"))
	}
	path := filepath.Join(dir, "rolling.sql")
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
			t.Errorf("%s\n  got  %q\n  want %q", c.query, rows, want)
		}
	}
}
