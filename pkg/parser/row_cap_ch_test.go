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

// rowCapRows are (timestamp, log_id, fields) with nested, hyphenated and
// sometimes missing fields.
var rowCapRows = [][3]string{
	{"2026-03-04 10:00:00.000", "r1", `{"user":"alice","src":{"ip":"10.0.0.1"},"event-id":"1","bytes":"10"}`},
	{"2026-03-04 10:01:00.000", "r2", `{"user":"alice","src":{"ip":"10.0.0.2"},"event-id":"2","bytes":"20"}`},
	{"2026-03-04 10:02:00.000", "r3", `{"user":"bob","src":{"ip":"10.0.0.1"},"event-id":"1"}`},
	{"2026-03-04 10:03:00.000", "r4", `{"user":"bob","event-id":"3","bytes":"5"}`},
	{"2026-03-04 10:04:00.000", "r5", `{"user":"carol","src":{"ip":"10.0.0.3"},"bytes":"7"}`},
	{"2026-03-04 10:05:00.000", "r6", `{"src":{"ip":"10.0.0.1"},"event-id":"1","bytes":"1"}`},
}

// rowCapCases read stored fields through head, tail, limit and dedup. Each must
// return what it does over the table itself.
var rowCapCases = []struct {
	query string
	want  []string
}{
	{`* | head(3) | table(log_id, user, src.ip)`,
		[]string{"r1\talice\t10.0.0.1", "r2\talice\t10.0.0.2", "r3\tbob\t10.0.0.1"}},
	{`* | tail(2) | table(log_id, event-id, nosuch)`,
		[]string{"r5\t\t", "r6\t1\t"}},
	{`* | head(4) | groupby(src.ip)`,
		[]string{"\t1", "10.0.0.1\t2", "10.0.0.2\t1"}},
	{`* | dedup(user) | table(log_id, bytes)`,
		[]string{"r2\t20", "r4\t5", "r5\t7", "r6\t1"}},
	{`* | dedup(user) | count()`,
		[]string{"4"}},
	{`* | dedup(src.ip) | sum(bytes)`,
		[]string{"33"}},
	{`* | sort(bytes) | limit(2) | user=bob | table(log_id, user)`,
		[]string{"r3\tbob"}},
	{`* | head(5) | event-id=1 | head(1) | table(log_id, src.ip)`,
		[]string{"r1\t10.0.0.1"}},
	{`* | head(6) | sum(bytes, as=total) | table(total)`,
		[]string{"43"}},
}

func rowCapRowsSQL() string {
	var rows []string
	for _, r := range rowCapRows {
		rows = append(rows, fmt.Sprintf("('%s', '%s', '%s', 'f1')", r[0], r[1], r[2]))
	}
	return "INSERT INTO logs (timestamp, log_id, fields, fractal_id) VALUES " + strings.Join(rows, ", ") + ";\n"
}

// TestRowCapExecute runs the row caps in ClickHouse: the paths each projects under
// its subquery must be every path the query reads above it, or ClickHouse rejects
// the query.
func TestRowCapExecute(t *testing.T) {
	bin := clickhouseBin(t)
	dir := t.TempDir()
	var script strings.Builder
	script.WriteString(strings.TrimRight(strings.TrimSpace(db.ClickHouseSQL), ";") + ";\nUSE logs;\n")
	script.WriteString(rowCapRowsSQL())
	for i, c := range rowCapCases {
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
		if strings.Contains(r.SQL, "(SELECT * FROM") {
			t.Errorf("%s: the cap reads the whole fields column:\n%s", c.query, r.SQL)
		}
		fmt.Fprintf(&script, "SELECT 'CASE %d' FORMAT TSV;\n%s FORMAT TSV;\n", i, strings.TrimRight(r.SQL, "; \n"))
	}
	path := filepath.Join(dir, "rowcap.sql")
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
	for i, c := range rowCapCases {
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
