package query

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

func TestExportLine(t *testing.T) {
	ts := "2026-10-12 08:30:00.123"
	cases := []struct {
		name       string
		row        map[string]interface{}
		fieldOrder []string
		want       string
	}{
		{
			name: "raw search nests norm_log verbatim",
			row: map[string]interface{}{
				"timestamp": ts, "log_id": "abc", "fractal_id": "f1",
				"norm_log": `{"user":"a<b>","n":1}`,
			},
			fieldOrder: []string{"timestamp", "norm_log", "log_id", "fractal_id"},
			want:       `{"timestamp":"2026-10-12T08:30:00.123Z","log_id":"abc","fractal_id":"f1","fields":{"user":"a<b>","n":1}}`,
		},
		{
			name: "only selected columns are written",
			row: map[string]interface{}{
				"timestamp": ts, "log_id": "abc", "fractal_id": "f1", "norm_log": `{}`,
				"lowered": "x<y", "_join__count": 3, "fields": `{}`,
			},
			fieldOrder: []string{"timestamp", "norm_log", "log_id", "fractal_id", "lowered"},
			want:       `{"timestamp":"2026-10-12T08:30:00.123Z","log_id":"abc","fractal_id":"f1","lowered":"x<y","fields":{}}`,
		},
		{
			name: "explicit table columns have no fields object",
			row: map[string]interface{}{
				"timestamp": ts, "log_id": "abc", "fractal_id": "f1", "user": "bob",
			},
			fieldOrder: []string{"user", "timestamp", "fractal_id"},
			want:       `{"timestamp":"2026-10-12T08:30:00.123Z","log_id":"abc","fractal_id":"f1","user":"bob"}`,
		},
		{
			name: "invalid UTF-8 in norm_log is replaced",
			row: map[string]interface{}{
				"timestamp": ts, "log_id": "abc", "fractal_id": "f1", "norm_log": "{\"msg\":\"a\xffb\"}",
			},
			want: `{"timestamp":"2026-10-12T08:30:00.123Z","log_id":"abc","fractal_id":"f1","fields":{"msg":"a` + "�" + `b"}}`,
		},
		{
			name: "non-object norm_log keeps fields absent",
			row: map[string]interface{}{
				"timestamp": ts, "log_id": "abc", "fractal_id": "f1", "norm_log": "plain text",
			},
			want: `{"timestamp":"2026-10-12T08:30:00.123Z","log_id":"abc","fractal_id":"f1","norm_log":"plain text"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			out := bufio.NewWriter(&buf)
			var line exportLine
			line.init(tc.row, tc.fieldOrder)
			if err := line.write(out, tc.row); err != nil {
				t.Fatal(err)
			}
			out.Flush()
			if got := buf.String(); got != tc.want+"\n" {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestAcceptsGzip(t *testing.T) {
	cases := map[string]bool{
		"":                      false,
		"gzip":                  true,
		"GZIP":                  true,
		"br, gzip;q=0.5":        true,
		"identity, gzip;q=0":    false,
		"gzip; q=0.0":           false,
		"deflate, x-gzip":       false,
		"gzip;q=1.0, identity":  true,
		"identity;q=1, *;q=0.1": false,
	}
	for header, want := range cases {
		if got := acceptsGzip([]string{header}); got != want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", header, got, want)
		}
	}
}

func TestParseTimeRangeNormalizesToUTC(t *testing.T) {
	var h QueryHandler
	start, end, err := h.parseTimeRange("2026-09-09T03:00:00+02:00", "2026-09-09T05:00:00+02:00")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC); !start.Equal(want) || start.Location() != time.UTC {
		t.Errorf("start = %v, want %v in UTC", start, want)
	}
	if end.Location() != time.UTC {
		t.Errorf("end = %v, want UTC", end)
	}
}
