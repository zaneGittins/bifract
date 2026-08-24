package deeplink

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// The SPA decodes q with decodeURIComponent(atob(q)). This mirrors that so the
// tests assert on the query a browser would actually run, not on an opaque blob.
func decodeShareValue(t *testing.T, s string) string {
	t.Helper()
	raw, err := decodeBase64Any(s)
	if err != nil {
		t.Fatalf("share value is not base64: %v", err)
	}
	out, err := url.QueryUnescape(strings.ReplaceAll(string(raw), "+", "%2B"))
	if err != nil {
		t.Fatalf("share value is not percent-encoded: %v", err)
	}
	return out
}

func TestEncodeShareValueRoundTrips(t *testing.T) {
	queries := []string{
		`fractal_id="x" | stats count() by host`,
		`process_name="powershell.exe" AND cmdline~"-enc"`,
		`pgr(start="{1a2b-3c4d}") | pgraph()`,
		"user=\"o'brien\" | table user, host",
		`message~"100% cpu" | sort -timestamp`,
		`host="wörk-01"`,
		`a="b+c&d=e" | head 10`,
	}
	for _, q := range queries {
		if got := decodeShareValue(t, encodeShareValue(q)); got != q {
			t.Errorf("round trip changed query:\n want %q\n got  %q", q, got)
		}
	}
}

func TestEncodeURIComponentMatchesJavaScript(t *testing.T) {
	// Values verified against encodeURIComponent in a browser.
	cases := map[string]string{
		"a b":            "a%20b",
		"a+b":            "a%2Bb",
		"100%":           "100%25",
		"a/b?c=d&e":      "a%2Fb%3Fc%3Dd%26e",
		"-_.!~*'()":      "-_.!~*'()",
		"\"quoted\"":     "%22quoted%22",
		"ä":              "%C3%A4",
		"tab\there":      "tab%09here",
		"{brace}":        "%7Bbrace%7D",
		"AZaz09":         "AZaz09",
		"back\\slash":    "back%5Cslash",
		"semi;colon:pip": "semi%3Bcolon%3Apip",
	}
	for in, want := range cases {
		if got := encodeURIComponent(in); got != want {
			t.Errorf("encodeURIComponent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReadQuery(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		got, err := readQuery(url.Values{"q": {` host="a" | head 5 `}})
		if err != nil {
			t.Fatal(err)
		}
		if got != `host="a" | head 5` {
			t.Errorf("got %q", got)
		}
	})

	t.Run("base64 variants", func(t *testing.T) {
		// Same payload in standard (padded) and URL-safe (unpadded) base64.
		for _, enc := range []string{"aG9zdD0iYT8+YiI=", "aG9zdD0iYT8-YiI"} {
			got, err := readQuery(url.Values{"q64": {enc}})
			if err != nil {
				t.Fatalf("%s: %v", enc, err)
			}
			if got != `host="a?>b"` {
				t.Errorf("%s decoded to %q", enc, got)
			}
		}
	})

	t.Run("rejections", func(t *testing.T) {
		cases := map[string]url.Values{
			"missing":     {},
			"both":        {"q": {"a"}, "q64": {"YQ"}},
			"bad base64":  {"q64": {"not base64!!"}},
			"empty b64":   {"q64": {"ICAg"}},
			"too long":    {"q": {strings.Repeat("a", maxQueryLen+1)}},
			"control cha": {"q": {"host=\"a\x00b\""}},
		}
		for name, v := range cases {
			if _, err := readQuery(v); err == nil {
				t.Errorf("%s: expected an error", name)
			}
		}
	})
}

func TestParseTimeRange(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

	t.Run("relative windows collapse to presets", func(t *testing.T) {
		for in, want := range map[string]string{
			"-24h":    "24h",
			"now-24h": "24h",
			"-15m":    "15m",
			"-7d":     "7d",
			"-30d":    "30d",
			"-1h":     "1h",
			"-60m":    "1h", // same duration, same preset
			"-1440m":  "24h",
			"-168h":   "7d",
		} {
			sel, err := parseTimeRange(in, "", now)
			if err != nil {
				t.Fatalf("%s: %v", in, err)
			}
			if sel.tr != want {
				t.Errorf("from=%s gave tr=%s, want %s", in, sel.tr, want)
			}
		}
	})

	t.Run("non-preset windows stay relative", func(t *testing.T) {
		sel, err := parseTimeRange("-90m", "now", now)
		if err != nil {
			t.Fatal(err)
		}
		if sel.tr != "relative" || sel.rn != 90 || sel.ru != "minutes" {
			t.Errorf("got %+v", sel)
		}

		sel, err = parseTimeRange("-3w", "", now)
		if err != nil {
			t.Fatal(err)
		}
		if sel.tr != "relative" || sel.rn != 3 || sel.ru != "weeks" {
			t.Errorf("got %+v", sel)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		sel, err := parseTimeRange("", "", now)
		if err != nil {
			t.Fatal(err)
		}
		if sel.tr != "24h" {
			t.Errorf("default should be 24h, got %s", sel.tr)
		}
	})

	t.Run("all time", func(t *testing.T) {
		sel, err := parseTimeRange("all", "", now)
		if err != nil || sel.tr != "all" {
			t.Errorf("got %+v err %v", sel, err)
		}
		if _, err := parseTimeRange("all", "now", now); err != nil {
			t.Errorf("to=now is redundant but not wrong: %v", err)
		}
		// Honouring from=all while ignoring to would search wider than asked.
		if _, err := parseTimeRange("all", "2026-08-01T00:00:00Z", now); err == nil {
			t.Error("from=all with an explicit end should be rejected, not silently widened")
		}
	})

	t.Run("absolute", func(t *testing.T) {
		sel, err := parseTimeRange("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", now)
		if err != nil {
			t.Fatal(err)
		}
		if sel.tr != "custom" || sel.ts != "2026-08-01T00:00:00Z" || sel.te != "2026-08-02T00:00:00Z" {
			t.Errorf("got %+v", sel)
		}
	})

	t.Run("epoch seconds and millis", func(t *testing.T) {
		sel, err := parseTimeRange("1754006400", "1754092800000", now)
		if err != nil {
			t.Fatal(err)
		}
		if sel.tr != "custom" {
			t.Fatalf("got %+v", sel)
		}
		if sel.ts != "2025-08-01T00:00:00Z" || sel.te != "2025-08-02T00:00:00Z" {
			t.Errorf("epoch parsed to %s .. %s", sel.ts, sel.te)
		}
	})

	t.Run("relative from with absolute to anchors on to", func(t *testing.T) {
		sel, err := parseTimeRange("-2h", "2026-08-01T12:00:00Z", now)
		if err != nil {
			t.Fatal(err)
		}
		if sel.tr != "custom" || sel.ts != "2026-08-01T10:00:00Z" {
			t.Errorf("got %+v", sel)
		}
	})

	t.Run("absolute from with implicit now end", func(t *testing.T) {
		sel, err := parseTimeRange("2026-08-01T00:00:00Z", "", now)
		if err != nil {
			t.Fatal(err)
		}
		if sel.tr != "custom" || sel.te != "2026-08-18T12:00:00Z" {
			t.Errorf("got %+v", sel)
		}
	})

	t.Run("rejections", func(t *testing.T) {
		cases := [][2]string{
			{"yesterday", ""},
			{"-24", ""},
			{"-0h", ""},
			{"-30s", "now"}, // no seconds unit in the picker
			{"2026-08-02T00:00:00Z", "2026-08-01T00:00:00Z"},
			{"2026-08-01T00:00:00Z", "2026-08-01T00:00:00Z"},
		}
		for _, c := range cases {
			if _, err := parseTimeRange(c[0], c[1], now); err == nil {
				t.Errorf("from=%q to=%q should have failed", c[0], c[1])
			}
		}
	})
}

func TestEncodeVars(t *testing.T) {
	got, err := encodeVars(url.Values{
		"var.host": {"web-01"},
		"var.user": {"svc_backup"},
		"q":        {"ignored"},
		"fractal":  {"ignored"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Sorted by name so a given link always redirects to the same URL.
	if want := `[{"name":"host","value":"web-01"},{"name":"user","value":"svc_backup"}]`; decodeShareValue(t, got) != want {
		t.Errorf("got %s", decodeShareValue(t, got))
	}

	if got, err := encodeVars(url.Values{"q": {"x"}}); err != nil || got != "" {
		t.Errorf("no vars should encode to empty, got %q err %v", got, err)
	}

	for name, v := range map[string]url.Values{
		"bad name":   {"var.1bad": {"x"}},
		"dotted":     {"var.a.b": {"x"}},
		"empty name": {"var.": {"x"}},
		"long value": {"var.a": {strings.Repeat("x", maxVarLen+1)}},
		"control":    {"var.a": {"x\x00y"}},
	} {
		if _, err := encodeVars(v); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestBuildShareURL(t *testing.T) {
	q := `host="a b" | head 5`

	t.Run("preset fractal", func(t *testing.T) {
		got := buildShareURL(q, scope{id: "frac-1"}, timeSel{tr: "24h"}, "")
		v := parseTarget(t, got)
		if v.Get("f") != "frac-1" || v.Get("tr") != "24h" || v.Has("p") {
			t.Errorf("got %s", got)
		}
		if decodeShareValue(t, v.Get("q")) != q {
			t.Errorf("query did not survive: %s", got)
		}
	})

	t.Run("prism", func(t *testing.T) {
		v := parseTarget(t, buildShareURL(q, scope{id: "prism-1", isPrism: true}, timeSel{tr: "all"}, ""))
		if v.Get("p") != "prism-1" || v.Has("f") {
			t.Errorf("prism links must set p and not f")
		}
	})

	t.Run("custom carries ts and te", func(t *testing.T) {
		v := parseTarget(t, buildShareURL(q, scope{id: "f"}, timeSel{tr: "custom", ts: "2026-08-01T00:00:00Z", te: "2026-08-02T00:00:00Z"}, ""))
		if v.Get("ts") == "" || v.Get("te") == "" {
			t.Errorf("custom range lost its bounds")
		}
	})

	t.Run("relative carries rn and ru", func(t *testing.T) {
		v := parseTarget(t, buildShareURL(q, scope{id: "f"}, timeSel{tr: "relative", rn: 90, ru: "minutes"}, ""))
		if v.Get("rn") != "90" || v.Get("ru") != "minutes" {
			t.Errorf("relative range lost its magnitude")
		}
	})

	t.Run("share gate is satisfied", func(t *testing.T) {
		// The SPA ignores a link unless q, tr and one of f/p are all present.
		v := parseTarget(t, buildShareURL(q, scope{id: "f"}, timeSel{tr: "24h"}, ""))
		if !v.Has("q") || !v.Has("tr") || (!v.Has("f") && !v.Has("p")) {
			t.Errorf("redirect would be ignored by the share-link reader")
		}
	})
}

func parseTarget(t *testing.T, target string) url.Values {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatalf("target is not a URL: %v", err)
	}
	if u.Path != "/" || u.IsAbs() || u.Host != "" {
		t.Fatalf("redirect must stay same-origin at /, got %q", target)
	}
	return u.Query()
}
