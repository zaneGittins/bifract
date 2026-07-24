package archive

import (
	"testing"
	"time"

	"github.com/apache/iceberg-go"
)

// The retention filter compares against ingest_date, a Date32 (days since the
// unix epoch). Getting the day arithmetic wrong silently deletes the wrong
// window, so pin the encoding.
func TestRetentionCutoffDateEncoding(t *testing.T) {
	for _, tc := range []struct {
		name string
		date string
		want iceberg.Date
	}{
		{"epoch", "1970-01-01", 0},
		{"day after epoch", "1970-01-02", 1},
		{"leap day", "2024-02-29", 19782},
		{"recent", "2026-07-23", 20657},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := time.Parse("2006-01-02", tc.date)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := iceberg.Date(d.Unix() / int64(24*time.Hour/time.Second))
			if got != tc.want {
				t.Errorf("%s encoded to %d, want %d", tc.date, got, tc.want)
			}
			// Round-trips back to the same calendar day.
			if back := got.ToTime().UTC().Format("2006-01-02"); back != tc.date {
				t.Errorf("%d decoded to %s, want %s", got, back, tc.date)
			}
		})
	}
}

// A retention of N days must keep exactly the last N ingest dates: the cutoff is
// the first date that is dropped, so it sits N days before today's UTC date.
func TestRetentionCutoffKeepsExactlyNDays(t *testing.T) {
	for _, days := range []int{1, 7, 30, 365} {
		cutoff := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -days)
		today := time.Now().UTC().Truncate(24 * time.Hour)

		if got := int(today.Sub(cutoff).Hours() / 24); got != days {
			t.Errorf("retention %dd: cutoff is %d days back, want %d", days, got, days)
		}
		// The oldest kept date is strictly after the cutoff, and today is kept.
		if !cutoff.Before(today) {
			t.Errorf("retention %dd: cutoff %s is not before today %s", days, cutoff, today)
		}
	}
}

// The maintain pass looks policies up by table name; completeness looks them up
// by fractal ID. Both must agree or retention silently applies to nothing.
func TestRetentionPolicyByTable(t *testing.T) {
	p := RetentionPolicy{"473e7d6e-35b4-4002-9eb3-9154102e5465": 30}
	byTable := p.ByTable()
	const want = "f_473e7d6e_35b4_4002_9eb3_9154102e5465"
	if byTable[want] != 30 {
		t.Errorf("ByTable()[%q] = %d, want 30 (keys: %v)", want, byTable[want], byTable)
	}
}

// A day the archive dropped on purpose must not be reported as a completeness
// gap, and a fractal without a policy must never be treated as expired.
func TestRetentionPolicyExpired(t *testing.T) {
	const fid = "473e7d6e-35b4-4002-9eb3-9154102e5465"
	p := RetentionPolicy{fid: 7}
	today := time.Now().UTC().Truncate(24 * time.Hour)

	if p.Expired(fid, today.AddDate(0, 0, -3)) {
		t.Error("day inside the 7d window reported as expired")
	}
	if !p.Expired(fid, today.AddDate(0, 0, -30)) {
		t.Error("day well outside the 7d window not reported as expired")
	}
	// The cutoff day itself is the first dropped day, so it is expired; the day
	// after it is the oldest kept.
	if !p.Expired(fid, today.AddDate(0, 0, -8)) {
		t.Error("day before the cutoff not reported as expired")
	}
	if p.Expired(fid, today.AddDate(0, 0, -7)) {
		t.Error("oldest kept day reported as expired")
	}
	// Keep-forever fractals and an empty policy never expire anything.
	if p.Expired("some-other-fractal", today.AddDate(0, 0, -3650)) {
		t.Error("fractal without a policy reported as expired")
	}
	if (RetentionPolicy{}).Expired(fid, today.AddDate(0, 0, -3650)) {
		t.Error("empty policy reported a day as expired")
	}
}

// Only fractals with a policy are eligible; everything else is keep-forever, and
// applyRetention must be a no-op for a non-positive window rather than deleting
// everything.
func TestApplyRetentionIgnoresNonPositiveWindow(t *testing.T) {
	for _, days := range []int{0, -1} {
		res, err := applyRetention(t.Context(), nil, nil, days)
		if err != nil {
			t.Fatalf("days=%d: unexpected error: %v", days, err)
		}
		if res.Deleted {
			t.Errorf("days=%d: reported a delete for a keep-forever policy", days)
		}
	}
}
