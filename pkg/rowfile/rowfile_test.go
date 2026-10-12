package rowfile

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, format, body, listColumn string) ([]map[string]string, []string) {
	t.Helper()
	r, err := NewReader(format, bufio.NewReader(strings.NewReader(body)), listColumn)
	if err != nil {
		t.Fatalf("NewReader(%s): %v", format, err)
	}
	var rows []map[string]string
	var skips []string
	for {
		row, skip, err := r.Next()
		if errors.Is(err, io.EOF) {
			return rows, skips
		}
		if err != nil {
			t.Fatalf("Next(%s): %v", format, err)
		}
		if skip != "" {
			skips = append(skips, skip)
			continue
		}
		rows = append(rows, row)
	}
}

func TestFormats(t *testing.T) {
	cases := []struct {
		format, body string
		want         []string // value of column "k" per row
		skips        int
	}{
		{"csv", BOM + "k,v\r\na,1\r\n\"b,c\",2\n", []string{"a", "b,c"}, 0},
		{"tsv", "k\tv\na\t1\n", []string{"a"}, 0},
		{"json", BOM + `[{"k":"a","v":1},{"k":"b"},3]`, []string{"a", "b"}, 1},
		{"ndjson", BOM + "{\"k\":\"a\"}\n\nnot json\n{\"k\":\"b\"}", []string{"a", "b"}, 1},
	}
	for _, c := range cases {
		rows, skips := readAll(t, c.format, c.body, "")
		if len(rows) != len(c.want) || len(skips) != c.skips {
			t.Fatalf("%s: rows %v skips %v", c.format, rows, skips)
		}
		for i, w := range c.want {
			if rows[i]["k"] != w {
				t.Errorf("%s row %d: k = %q, want %q", c.format, i, rows[i]["k"], w)
			}
		}
	}
}

// A lines file fills the column exactly as named, and a first line repeating
// that name is a header, not a value.
func TestLinesKeepTheColumnNameAsGiven(t *testing.T) {
	rows, _ := readAll(t, "lines", BOM+"IP address\n# comment\n1.2.3.4\r\n\n5.6.7.8", "IP address")
	if len(rows) != 2 || rows[0]["IP address"] != "1.2.3.4" || rows[1]["IP address"] != "5.6.7.8" {
		t.Fatalf("rows = %v", rows)
	}
}

func TestSniff(t *testing.T) {
	for body, want := range map[string]string{
		BOM + "[{}]": "json",
		" {\"a\":1}": "ndjson",
		"a\tb\n":     "tsv",
		"a,b\n":      "csv",
		"1.2.3.4\n":  "lines",
		"":           "lines",
	} {
		if got := Sniff([]byte(body)); got != want {
			t.Errorf("Sniff(%q) = %s, want %s", body, got, want)
		}
	}
}

func TestLimitLines(t *testing.T) {
	ok := strings.Repeat("a", 10) + "\n" + strings.Repeat("b", 10) + "\n"
	if _, err := io.ReadAll(LimitLines(strings.NewReader(ok), 10)); err != nil {
		t.Fatalf("lines at the limit refused: %v", err)
	}
	long := "short\n" + strings.Repeat("x", 11)
	if _, err := io.ReadAll(LimitLines(strings.NewReader(long), 10)); !errors.Is(err, ErrLineTooLong) {
		t.Fatalf("over-long final line: got %v", err)
	}
	// Small reads must not reset the count part way through a line.
	r := LimitLines(io.MultiReader(strings.NewReader(strings.Repeat("y", 6)), strings.NewReader(strings.Repeat("y", 6))), 10)
	if _, err := io.ReadAll(r); !errors.Is(err, ErrLineTooLong) {
		t.Fatalf("line split across reads: got %v", err)
	}
}

func TestColumnName(t *testing.T) {
	for raw, want := range map[string]string{
		"Name":       "Name",
		"IP address": "IP_address",
		"1st":        "_1st",
		"_bf_seq":    "f_bf_seq",
		"  ":         "column",
	} {
		if got := ColumnName(raw); got != want {
			t.Errorf("ColumnName(%q) = %q, want %q", raw, got, want)
		}
	}
}
