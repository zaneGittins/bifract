package alerts

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
	"time"
)

// jsDecodeURIComponent mirrors the browser's decodeURIComponent, which only
// decodes %XX sequences (not '+' to space). url.QueryUnescape would do the
// latter, so we use url.PathUnescape which matches decodeURIComponent semantics.
func jsDecodeURIComponent(s string) (string, error) {
	return url.PathUnescape(s)
}

func roundTrip(t *testing.T, originalQuery string) {
	t.Helper()
	encoded := encodeQueryForShareLink(originalQuery)

	// The produced string must be valid base64
	decodedBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("base64 decode failed: %v", err)
	}

	// The base64-decoded string must contain no raw '+' characters
	// (spaces must have been converted to %20, literal '+' to %2B).
	if strings.Contains(string(decodedBytes), "+") {
		t.Errorf("encoded query must not contain raw '+'; got: %s", string(decodedBytes))
	}

	// Now apply the JS decodeURIComponent equivalent
	decoded, err := jsDecodeURIComponent(string(decodedBytes))
	if err != nil {
		t.Fatalf("decodeURIComponent-equivalent failed: %v", err)
	}

	if decoded != originalQuery {
		t.Errorf("round-trip mismatch:\n  original: %q\n  got:      %q", originalQuery, decoded)
	}
}

func TestEncodeQueryForShareLinkRoundTrip(t *testing.T) {
	cases := []string{
		`artifact="Linux.Detection.Honeyfiles"`,
		"artifact=\"Linux.Detection.Honeyfiles\"\n| table(computer_name,image,user,commandline,file_name,call_chain)",
		"artifact=\"Linux.Detection.Honeyfiles\"\n| concat([computer_name,file_name], as=suppression)",
		"simple space test",
		"has+literal+pluses",
		"has a mix of spaces and +pluses",
		"unicode: café, naïve, emoji here",
		"special: !@#$%^&*()[]{}",
		"",
		"\t\n\r",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			roundTrip(t, c)
		})
	}
}

func TestBuildShareLinkFractal(t *testing.T) {
	alert := &Alert{
		QueryString: "artifact=\"x\"\n| table(a)",
		FractalID:   "frac-123",
	}
	triggered := time.Date(2026, 4, 10, 12, 30, 0, 0, time.UTC)
	link := buildShareLink("https://bifract.example.com", alert, triggered)
	if link == "" {
		t.Fatal("expected link, got empty string")
	}
	if !strings.HasPrefix(link, "https://bifract.example.com/?") {
		t.Errorf("unexpected prefix: %s", link)
	}
	u, err := url.Parse(link)
	if err != nil {
		t.Fatalf("invalid URL: %v", err)
	}
	q := u.Query()
	if q.Get("f") != "frac-123" {
		t.Errorf("f param = %q, want frac-123", q.Get("f"))
	}
	if q.Get("p") != "" {
		t.Errorf("p param should be empty for fractal alert, got %q", q.Get("p"))
	}
	if q.Get("tr") != "custom" {
		t.Errorf("tr param = %q, want custom", q.Get("tr"))
	}
	if got, want := q.Get("ts"), "2026-04-10T11:30:00.000Z"; got != want {
		t.Errorf("ts param = %q, want %q", got, want)
	}
	if got, want := q.Get("te"), "2026-04-10T13:30:00.000Z"; got != want {
		t.Errorf("te param = %q, want %q", got, want)
	}
	if q.Get("q") == "" {
		t.Error("q param should be set")
	}
}

func TestBuildShareLinkPrism(t *testing.T) {
	alert := &Alert{
		QueryString: "artifact=\"x\"",
		PrismID:     "prism-456",
	}
	triggered := time.Date(2026, 4, 10, 12, 30, 0, 0, time.UTC)
	link := buildShareLink("https://bifract.example.com", alert, triggered)
	u, err := url.Parse(link)
	if err != nil {
		t.Fatalf("invalid URL: %v", err)
	}
	q := u.Query()
	if q.Get("p") != "prism-456" {
		t.Errorf("p param = %q, want prism-456", q.Get("p"))
	}
	if q.Get("f") != "" {
		t.Errorf("f param should be empty for prism alert, got %q", q.Get("f"))
	}
}

func TestBuildShareLinkPrismPreferredOverFractal(t *testing.T) {
	// A prism alert should never set f= even if FractalID is somehow populated.
	alert := &Alert{
		QueryString: "x",
		FractalID:   "frac-123",
		PrismID:     "prism-456",
	}
	link := buildShareLink("https://bifract.example.com", alert, time.Now())
	u, _ := url.Parse(link)
	q := u.Query()
	if q.Get("p") != "prism-456" {
		t.Errorf("p param = %q, want prism-456", q.Get("p"))
	}
	if q.Get("f") != "" {
		t.Errorf("f param must not be set when PrismID is present, got %q", q.Get("f"))
	}
}

func TestBuildShareLinkEmptyBaseURL(t *testing.T) {
	alert := &Alert{QueryString: "x", FractalID: "f"}
	if got := buildShareLink("", alert, time.Now()); got != "" {
		t.Errorf("expected empty link for empty baseURL, got %q", got)
	}
}

func TestBuildShareLinkNoScope(t *testing.T) {
	alert := &Alert{QueryString: "x"}
	if got := buildShareLink("https://bifract.example.com", alert, time.Now()); got != "" {
		t.Errorf("expected empty link when neither fractal nor prism is set, got %q", got)
	}
}

// TestBuildShareLinkZeroTime falls back to a relative time range when no
// trigger timestamp is available (shouldn't happen in practice, but defensive).
func TestBuildShareLinkZeroTime(t *testing.T) {
	alert := &Alert{QueryString: "x", FractalID: "frac-123"}
	link := buildShareLink("https://bifract.example.com", alert, time.Time{})
	u, err := url.Parse(link)
	if err != nil {
		t.Fatalf("invalid URL: %v", err)
	}
	q := u.Query()
	if q.Get("tr") != "1h" {
		t.Errorf("tr param = %q, want 1h fallback", q.Get("tr"))
	}
	if q.Get("ts") != "" || q.Get("te") != "" {
		t.Errorf("ts/te should not be set for fallback, got ts=%q te=%q", q.Get("ts"), q.Get("te"))
	}
}

// TestBuildShareLinkTimezoneConversion confirms that non-UTC trigger times are
// correctly converted to UTC in the ts/te params (frontend expects Z suffix).
func TestBuildShareLinkTimezoneConversion(t *testing.T) {
	pst := time.FixedZone("PST", -8*60*60)
	triggered := time.Date(2026, 4, 10, 4, 30, 0, 0, pst) // = 12:30 UTC
	alert := &Alert{QueryString: "x", FractalID: "frac-123"}
	link := buildShareLink("https://bifract.example.com", alert, triggered)
	u, _ := url.Parse(link)
	q := u.Query()
	if got, want := q.Get("ts"), "2026-04-10T11:30:00.000Z"; got != want {
		t.Errorf("ts param = %q, want %q (UTC conversion)", got, want)
	}
	if got, want := q.Get("te"), "2026-04-10T13:30:00.000Z"; got != want {
		t.Errorf("te param = %q, want %q (UTC conversion)", got, want)
	}
}

// TestBuildShareLinkEndToEnd verifies the full round-trip that the browser will
// perform: URL-parse the link, atob the q param, decodeURIComponent the result,
// and confirm the original query comes back intact.
func TestBuildShareLinkEndToEnd(t *testing.T) {
	original := "artifact=\"Linux.Detection.Honeyfiles\"\n| concat([computer_name,file_name], as=suppression)\n| table(computer_name,suppression,image,user,commandline,file_name,call_chain)"
	alert := &Alert{QueryString: original, FractalID: "frac-123"}
	link := buildShareLink("https://bifract.example.com", alert, time.Now())

	u, err := url.Parse(link)
	if err != nil {
		t.Fatalf("parse link: %v", err)
	}
	qParam := u.Query().Get("q")
	if qParam == "" {
		t.Fatal("q param missing")
	}

	// atob
	rawBytes, err := base64.StdEncoding.DecodeString(qParam)
	if err != nil {
		t.Fatalf("atob failed: %v", err)
	}

	// decodeURIComponent-equivalent (strict %XX parsing)
	decoded, err := url.PathUnescape(string(rawBytes))
	if err != nil {
		t.Fatalf("decodeURIComponent-equivalent failed: %v", err)
	}
	if decoded != original {
		t.Errorf("end-to-end mismatch:\n  want: %q\n  got:  %q", original, decoded)
	}
	// Specifically: the reported bug was a stray '+' appearing after '|'.
	if strings.Contains(decoded, "|+") {
		t.Errorf("decoded query contains '|+' (regression of the original bug): %q", decoded)
	}
}
