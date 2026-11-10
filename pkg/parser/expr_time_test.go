package parser

import (
	"strings"
	"testing"
)

// translateTZ compiles a query for a viewer in zone tz.
func translateTZ(t *testing.T, query, tz string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	opts := exprOpts()
	opts.DisplayTimezone = tz
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return result.SQL
}

func wantSQL(t *testing.T, query, sql string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(sql, p) {
			t.Errorf("%s\n  want %q in:\n  %s", query, p, sql)
		}
	}
}

func TestDurationLiterals(t *testing.T) {
	for q, want := range map[string]string{
		`* | x := 30s`:             "30 AS x",
		`* | x := 5m`:              "300 AS x",
		`* | x := 1h`:              "3600 AS x",
		`* | x := 7d`:              "604800 AS x",
		`* | x := 2w`:              "1209600 AS x",
		`* | x := 2 * 5m`:          "2 * 300",
		`* | x := 5m * 2`:          "300 * 2",
		`* | x := (1h + 30m) / 1m`: "(3600 + 1800) / 60",
	} {
		wantSQL(t, q, translateExpr(t, q), want)
	}
}

func TestDurationLiteralErrors(t *testing.T) {
	for q, want := range map[string]string{
		`* | x := timestamp - 5y`:   "durations are a whole number and a unit",
		`* | x := timestamp - 1.5h`: "durations are a whole number and a unit",
		`* | x := 5m * 5m`:          "a duration times a duration",
		`* | x := 5m + 3`:           "write 3 with a unit",
		`* | x := 5m > 3`:           "write 3 with a unit",
	} {
		if got := exprError(t, q); !strings.Contains(got, want) {
			t.Errorf("%s\n  error %q, want it to contain %q", q, got, want)
		}
	}
}

func TestTimeArithmetic(t *testing.T) {
	cases := []struct {
		q    string
		want []string
	}{
		{`* | gap := timestamp - ingest_timestamp`, []string{"dateDiff('millisecond', toDateTime64(ingest_timestamp, 3, 'UTC'), toDateTime64(timestamp, 3, 'UTC')) / 1000 AS gap"}},
		{`* | t := timestamp - 1h`, []string{"addMilliseconds(toDateTime64(timestamp, 3, 'UTC'), -3600000) AS t"}},
		{`* | t := timestamp + 90m`, []string{"addMilliseconds(toDateTime64(timestamp, 3, 'UTC'), 5400000) AS t"}},
		{`* | t := 1h + timestamp`, []string{"addMilliseconds(toDateTime64(timestamp, 3, 'UTC'), 3600000) AS t"}},
		{`* | age := now() - timestamp`, []string{"dateDiff('millisecond', toDateTime64(timestamp, 3, 'UTC'), now64(3)) / 1000 AS age"}},
		// A stored field holding a time is parsed the way dateDiff() parses it.
		{`* | (timestamp - process_start) < 1m`, []string{"dateDiff('millisecond', multiIf(match(fields.`process_start`::String", ", toDateTime64(timestamp, 3, 'UTC')) / 1000 < 60"}},
		// A computed duration shifts a time by its seconds.
		{`* | gap := timestamp - ingest_timestamp | t := timestamp - gap`, []string{"addMilliseconds(toDateTime64(timestamp, 3, 'UTC'), -toInt64(round("}},
		{`* | (timestamp - ingest_timestamp) > 5m`, []string{"dateDiff('millisecond', toDateTime64(ingest_timestamp, 3, 'UTC'), toDateTime64(timestamp, 3, 'UTC')) / 1000 > 300"}},
		{`* | recent := timestamp > now() - 7d`, []string{"toDateTime64(timestamp, 3, 'UTC') > addMilliseconds(now64(3), -604800000) AS recent"}},
		{`* | now() - timestamp < 7d`, []string{"dateDiff('millisecond', toDateTime64(timestamp, 3, 'UTC'), now64(3)) / 1000 < 604800"}},
		{`* | (now() - timestamp) < 7d`, []string{"dateDiff('millisecond', toDateTime64(timestamp, 3, 'UTC'), now64(3)) / 1000 < 604800"}},
		{`* | x := (timestamp - ingest_timestamp) / 1h`, []string{"(dateDiff('millisecond', toDateTime64(ingest_timestamp, 3, 'UTC'), toDateTime64(timestamp, 3, 'UTC')) / 1000) / 3600"}},
		{`* | x := -(timestamp - ingest_timestamp)`, []string{"-(dateDiff('millisecond', toDateTime64(ingest_timestamp, 3, 'UTC'), toDateTime64(timestamp, 3, 'UTC')) / 1000)"}},
	}
	for _, c := range cases {
		wantSQL(t, c.q, translateExpr(t, c.q), c.want...)
	}
}

func TestTimeArithmeticErrors(t *testing.T) {
	for q, want := range map[string]string{
		`* | x := timestamp + ingest_timestamp`:   "cannot add two times",
		`* | x := timestamp * 2`:                  "expected a number, got the time timestamp",
		`* | x := timestamp - 30`:                 "write 30 with a unit",
		`* | (timestamp - ingest_timestamp) > 30`: "write 30 with a unit",
		`* | timestamp > 30`:                      "compares a time with a number",
		`* | timestamp > 5m`:                      "compares a time with a duration",
		`* | x := timestamp > len(user)`:          "compares a time with a number",
		`* | x := if(user = "a", timestamp, 5m)`:  "branches must have the same type, got time and duration",
	} {
		if got := exprError(t, q); !strings.Contains(got, want) {
			t.Errorf("%s\n  error %q, want it to contain %q", q, got, want)
		}
	}
}

// A comparison against text parses the text as a time, as dateDiff() does, rather
// than comparing the time's rendering as a string.
func TestTimeComparedWithText(t *testing.T) {
	q := `* | x := timestamp > "2026-01-01 12:00:00"`
	wantSQL(t, q, translateExpr(t, q), "toDateTime64(timestamp, 3, 'UTC') > multiIf(match('2026-01-01 12:00:00'")
	if got := exprError(t, `* | x := timestamp > "yesterday"`); !strings.Contains(got, `"yesterday" is not a time`) {
		t.Errorf("error %q, want it to name the bad time", got)
	}
}

func TestDateDiffReadsTimesDirectly(t *testing.T) {
	q := `* | x := dateDiff("hour", ingest_timestamp, timestamp)`
	sql := translateExpr(t, q)
	wantSQL(t, q, sql, "dateDiff('hour', toDateTime64(ingest_timestamp, 3, 'UTC'), toDateTime64(timestamp, 3, 'UTC')) AS x")
	if strings.Contains(sql, "match(toString(timestamp)") {
		t.Errorf("dateDiff() round-tripped a time through text: %s", sql)
	}
}

func TestBin(t *testing.T) {
	cases := []struct {
		q, tz string
		want  []string
	}{
		{`* | groupBy([bin(timestamp, 5m), host])`, "", []string{"toStartOfFiveMinutes(toDateTime64(timestamp, 3, 'UTC')) AS bin_timestamp_5m", "GROUP BY bin_timestamp_5m, host"}},
		{`* | groupBy(bin(timestamp, 1h))`, "", []string{"toStartOfHour(toDateTime64(timestamp, 3, 'UTC')) AS bin_timestamp_1h"}},
		{`* | groupBy(bin(timestamp, 10m))`, "", []string{"toStartOfInterval(toDateTime64(timestamp, 3, 'UTC'), INTERVAL 10 MINUTE) AS bin_timestamp_10m"}},
		{`* | groupBy(bin(timestamp, 1d))`, "America/New_York", []string{"toStartOfDay(toDateTime64(timestamp, 3, 'UTC'), 'America/New_York')"}},
		{`* | groupBy(bin(timestamp, 1d, timezone="Europe/Berlin"))`, "America/New_York", []string{"toStartOfDay(toDateTime64(timestamp, 3, 'UTC'), 'Europe/Berlin')"}},
		{`* | groupBy(bin(timestamp, 1d, timezone="UTC"))`, "America/New_York", []string{"toStartOfDay(toDateTime64(timestamp, 3, 'UTC')) AS"}},
		{`* | groupBy(bin(process_start, 1h))`, "", []string{"toStartOfHour(multiIf(match(fields.`process_start`::String"}},
		{`* | b := bin(timestamp, 15m) | groupBy(b)`, "", []string{"toStartOfFifteenMinutes(toDateTime64(timestamp, 3, 'UTC'))"}},
		{`event_id=4625 | groupBy([bin(timestamp, 10m), src_ip]) | _count > 20`, "", []string{"toStartOfInterval(toDateTime64(timestamp, 3, 'UTC'), INTERVAL 10 MINUTE) AS bin_timestamp_10m", "HAVING _count > 20"}},
	}
	for _, c := range cases {
		wantSQL(t, c.q, translateTZ(t, c.q, c.tz), c.want...)
	}
}

func TestBinErrors(t *testing.T) {
	for q, want := range map[string]string{
		`* | x := bin(timestamp, span)`:                     "bin(): span must be a duration such as 5m",
		`* | x := bin(timestamp, 0m)`:                       "bin(): span must be longer than zero",
		`* | x := bin(timestamp, 5)`:                        "bin(): span must be a duration such as 5m",
		`* | x := bin(timestamp, 1h, timezone="Mars/Base")`: `unknown time zone "Mars/Base"`,
		`* | x := bin(timestamp, 1h, timezone=user)`:        "timezone must be a quoted zone name",
		`* | x := bin(user = "a", 1h)`:                      "bin(): field: expected a time, got boolean",
		`* | x := bin(5m, 1h)`:                              "bin(): field: expected a time, got duration",
	} {
		if got := exprError(t, q); !strings.Contains(got, want) {
			t.Errorf("%s\n  error %q, want it to contain %q", q, got, want)
		}
	}
}

func TestHourOfDayAndDayOfWeek(t *testing.T) {
	cases := []struct {
		q, tz string
		want  []string
	}{
		{`event_id=4624 | hourOfDay(timestamp) < 6`, "", []string{"toHour(toDateTime64(timestamp, 3, 'UTC')) < 6"}},
		{`event_id=4624 | dayOfWeek(timestamp) >= 6`, "", []string{"toDayOfWeek(toDateTime64(timestamp, 3, 'UTC'), 0) >= 6"}},
		{`event_id=4624 | hourOfDay(timestamp) < 6 OR dayOfWeek(timestamp) >= 6`, "", []string{"toHour(toDateTime64(timestamp, 3, 'UTC')) < 6", "toDayOfWeek(toDateTime64(timestamp, 3, 'UTC'), 0) >= 6"}},
		{`* | h := hourOfDay(timestamp)`, "Australia/Sydney", []string{"toHour(toDateTime64(timestamp, 3, 'UTC'), 'Australia/Sydney') AS h"}},
		{`* | d := dayOfWeek(timestamp, timezone="Asia/Tokyo")`, "", []string{"toDayOfWeek(toDateTime64(timestamp, 3, 'UTC'), 0, 'Asia/Tokyo') AS d"}},
		{`* | groupBy(hourOfDay(timestamp))`, "", []string{"toHour(toDateTime64(timestamp, 3, 'UTC')) AS hourOfDay_timestamp"}},
		{`* | t := timestamp + 1h | hourOfDay(t) = 1`, "", []string{"toHour((addMilliseconds(toDateTime64(timestamp, 3, 'UTC'), 3600000))) = 1"}},
	}
	for _, c := range cases {
		wantSQL(t, c.q, translateTZ(t, c.q, c.tz), c.want...)
	}
}

// A plain filter on a field reads a duration value as its seconds, never as the
// text "5m".
func TestFilterComparesDurationValueAsSeconds(t *testing.T) {
	q := `* | gap := timestamp - ingest_timestamp | gap > 5m`
	sql := translateExpr(t, q)
	wantSQL(t, q, sql, "> 300")
	if strings.Contains(sql, "'5m'") {
		t.Errorf("compared a duration as text: %s", sql)
	}
	q = `* | duration > 2m`
	wantSQL(t, q, translateExpr(t, q), "toFloat64OrDefault(fields.`duration`::String, nan) > 120")
}

// An invalid DisplayTimezone falls back to UTC, as timechart does, so a bad
// preference never fails a query.
func TestBinIgnoresInvalidDisplayTimezone(t *testing.T) {
	q := `* | groupBy(bin(timestamp, 1d))`
	sql := translateTZ(t, q, "Not/AZone")
	wantSQL(t, q, sql, "toStartOfDay(toDateTime64(timestamp, 3, 'UTC')) AS")
}

// A column computed in the final SELECT reads the log's other columns through the
// inner one, which carries them without showing them.
func TestDeferredAssignmentCarriesBaseColumns(t *testing.T) {
	for _, q := range []string{`* | gap := timestamp - ingest_timestamp`, `* | x := concat(ingest_timestamp, "a")`} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		r, err := TranslateToSQLWithOrder(pl, exprOpts())
		if err != nil {
			t.Fatal(err)
		}
		wantSQL(t, q, r.SQL, "fields, ingest_timestamp FROM logs")
		for _, f := range r.FieldOrder {
			if f == "ingest_timestamp" {
				t.Errorf("%s shows the carried column: %v", q, r.FieldOrder)
			}
		}
	}
}
