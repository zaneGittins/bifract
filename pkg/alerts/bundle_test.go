package alerts

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestBundleSlug(t *testing.T) {
	cases := map[string]string{
		"Suspicious PowerShell Download": "Suspicious-PowerShell-Download",
		"  ../../etc/passwd ":            "etc-passwd",
		"cmd.exe /c whoami":              "cmd.exe-c-whoami",
		"!!!":                            "alert",
		"":                               "alert",
		"Ünïcode name":                   "n-code-name",
	}
	for in, want := range cases {
		if got := bundleSlug(in); got != want {
			t.Errorf("bundleSlug(%q) = %q, want %q", in, got, want)
		}
	}
	long := bundleSlug(strings.Repeat("a", 200))
	if len(long) != 80 {
		t.Errorf("a slug is capped at 80 bytes, got %d", len(long))
	}
}

// Export is the inverse of import: a plain event alert must not grow a window or a
// schedule it does not have, and actions travel by name, sorted, with deleted ones
// dropped rather than exported as bare ids.
func TestAlertToYAML(t *testing.T) {
	zero, empty := 0, ""
	alert := &Alert{
		Name: "n", Description: "d", QueryString: `a="b"`, AlertType: "event", Severity: "high",
		Labels: []string{"l"}, References: []string{"r"}, Enabled: true,
		ThrottleTimeSeconds: 60, ThrottleField: "host", MaxEventLagSeconds: 30,
		WindowDuration: &zero, ScheduleCron: &empty, QueryWindowSeconds: &zero,
		WebhookActions:      []WebhookAction{{ID: "w1"}, {ID: "w-deleted"}},
		FractalActions:      []FractalAction{{ID: "f1"}},
		DictionaryActionIDs: []string{"d1"},
		EmailActionIDs:      []string{"e1"},
	}
	names := map[string]string{"w1": "Zeta webhook", "f1": "Copy to SOC", "d1": "IOC list", "e1": "Page oncall"}

	y := alertToYAML(alert, names)
	if y.WindowDuration != nil || y.ScheduleCron != nil || y.QueryWindowSeconds != nil {
		t.Errorf("unset windows exported as values: %v %v %v", y.WindowDuration, y.ScheduleCron, y.QueryWindowSeconds)
	}
	if strings.Join(y.ActionNames, "|") != "Copy to SOC|IOC list|Page oncall|Zeta webhook" {
		t.Errorf("action names = %v", y.ActionNames)
	}
	if y.Name != "n" || y.QueryString != `a="b"` || y.Severity != "high" || !y.Enabled || y.ThrottleField != "host" ||
		y.ThrottleTimeSeconds != 60 || y.MaxEventLagSeconds != 30 {
		t.Errorf("fields not carried: %+v", y)
	}

	window, sched, qw := 300, "@hourly", 600
	alert.WindowDuration, alert.ScheduleCron, alert.QueryWindowSeconds = &window, &sched, &qw
	y = alertToYAML(alert, names)
	if *y.WindowDuration != 300 || *y.ScheduleCron != "@hourly" || *y.QueryWindowSeconds != 600 {
		t.Error("real windows must be exported")
	}

	// The document round-trips through YAML with the import's own keys.
	raw, err := yaml.Marshal(y)
	if err != nil {
		t.Fatal(err)
	}
	var back YAMLAlert
	if err := yaml.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.QueryString != y.QueryString || len(back.ActionNames) != 4 || *back.ScheduleCron != "@hourly" {
		t.Errorf("round trip lost fields: %+v", back)
	}
}

func TestActionIDsCoverEveryKind(t *testing.T) {
	ids := actionIDs(&Alert{
		WebhookActions: []WebhookAction{{ID: "w"}}, FractalActions: []FractalAction{{ID: "f"}},
		DictionaryActionIDs: []string{"d"}, EmailActionIDs: []string{"e"},
	})
	for _, kind := range alertActionKinds {
		if len(ids[kind.table]) != 1 {
			t.Errorf("%s: %v", kind.table, ids[kind.table])
		}
	}
}

type bundleFile struct {
	name string
	body string
}

func makeBundle(t *testing.T, files ...bundleFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range files {
		w, err := zw.Create(f.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(f.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func manifestJSON(format string, version int) string {
	b, _ := json.Marshal(BundleManifest{Format: format, Version: version})
	return string(b)
}

// An archive is refused whole for what is wrong with the archive itself, before any
// alert in it is looked at. The manager has no database: reaching one would panic.
func TestImportBundleRefusesBadArchives(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()
	alertYAML := bundleFile{"alerts/a.yaml", "name: a\nqueryString: status=500\n"}

	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"not a zip", []byte("plain text"), "not a readable archive"},
		{"traversal", makeBundle(t, bundleFile{"../escape.yaml", "x"}, alertYAML), "unsafe path"},
		{"traversal inside a folder", makeBundle(t, bundleFile{"alerts/../../x.yaml", "x"}), "unsafe path"},
		{"absolute path", makeBundle(t, bundleFile{"/etc/alerts/a.yaml", "x"}), "unsafe path"},
		{"foreign format", makeBundle(t, bundleFile{"manifest.json", manifestJSON("other-tool", 1)}, alertYAML), `"other-tool", not a Bifract alert bundle`},
		{"newer version", makeBundle(t, bundleFile{"manifest.json", manifestJSON(BundleFormat, BundleVersion+1)}, alertYAML), "newer Bifract"},
		{"no alerts", makeBundle(t, bundleFile{"manifest.json", manifestJSON(BundleFormat, 1)}, bundleFile{"tests/a.json", "{}"}), "no alert definitions"},
		{"alerts outside the folder", makeBundle(t, bundleFile{"a.yaml", alertYAML.body}), "no alert definitions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := m.ImportBundle(ctx, tc.data, "u", "f1", "", false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

// Archive limits are judged from what the archive declares, before reading any of it.
func TestImportBundleBoundsTheArchive(t *testing.T) {
	m := &Manager{}
	files := make([]bundleFile, MaxBundleEntries+1)
	for i := range files {
		files[i] = bundleFile{name: "tests/" + strconv.Itoa(i) + ".json"}
	}
	if _, err := m.ImportBundle(context.Background(), makeBundle(t, files...), "u", "f1", "", false); err == nil ||
		!strings.Contains(err.Error(), "more than 5000 files") {
		t.Errorf("too many entries: got %v", err)
	}

	// One entry claiming an expanded size past the cap. The header is what is judged,
	// so nothing has to be inflated to refuse it.
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.CreateRaw(&zip.FileHeader{Name: "alerts/bomb.yaml", Method: zip.Store, CompressedSize64: 1, UncompressedSize64: MaxBundleBytes + 1})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("x"))
	_ = zw.Close()
	if _, err := m.ImportBundle(context.Background(), buf.Bytes(), "u", "f1", "", false); err == nil ||
		!strings.Contains(err.Error(), "expands to more than") {
		t.Errorf("declared expansion: got %v", err)
	}
}

// One bad alert is reported and skipped, never failing the rest. These all fail to
// parse, so none reaches the database.
func TestImportBundleReportsEachAlert(t *testing.T) {
	m := &Manager{}
	data := makeBundle(t,
		bundleFile{"manifest.json", manifestJSON(BundleFormat, BundleVersion)},
		bundleFile{"alerts/b-bad-yaml.yaml", "name: [unclosed"},
		bundleFile{"alerts/a-no-query.yaml", "name: a\n"},
		bundleFile{"alerts/c-bad-query.yaml", "name: c\nqueryString: 'status=500 |'\n"},
		bundleFile{"alerts/notes.txt", "ignored"},
		bundleFile{"alerts/", ""},
	)
	result, err := m.ImportBundle(context.Background(), data, "u", "f1", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 3 || result.Imported != 0 || result.Skipped != 0 || len(result.Alerts) != 3 {
		t.Fatalf("result = %+v", result)
	}
	// Reported in file order so a re-run reads the same.
	wantFiles := []string{"alerts/a-no-query.yaml", "alerts/b-bad-yaml.yaml", "alerts/c-bad-query.yaml"}
	for i, a := range result.Alerts {
		if a.File != wantFiles[i] || a.Status != "failed" || a.Message == "" {
			t.Errorf("entry %d = %+v", i, a)
		}
	}
	if !strings.Contains(result.Alerts[0].Message, "query string is required") {
		t.Errorf("the reason must reach the operator: %q", result.Alerts[0].Message)
	}
}

// A manifest that is unreadable or says nothing about its format is not a reason to
// refuse an archive of otherwise readable alerts.
func TestImportBundleToleratesAMissingOrBlankManifest(t *testing.T) {
	m := &Manager{}
	for _, manifest := range []string{"not json", "{}"} {
		data := makeBundle(t, bundleFile{"manifest.json", manifest}, bundleFile{"alerts/a.yaml", "name: a\n"})
		result, err := m.ImportBundle(context.Background(), data, "u", "f1", "", false)
		if err != nil || result.Failed != 1 {
			t.Errorf("manifest %q: %+v %v", manifest, result, err)
		}
	}
}

// Tests travel without identity: ids belong to the instance that stored them, and a
// position is just the order in the file.
func TestReadBundleTestsReMintsIdentity(t *testing.T) {
	doc := `{"alert":"a","tests":[
		{"id":"old-1","name":"fires","expectation":"match","events":[{"a":"b"}],"position":7},
		{"id":"old-2","name":"quiet","expectation":"no_match","events":[{"a":"c"}],"position":3}]}`
	data := makeBundle(t, bundleFile{"tests/a.json", doc}, bundleFile{"tests/bad.json", "{"})
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	tests, err := readBundleTests(zr.File[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(tests) != 2 || tests[0].ID != "" || tests[1].ID != "" || tests[0].Position != 0 || tests[1].Position != 1 {
		t.Errorf("tests = %+v", tests)
	}
	if tests[0].Name != "fires" || tests[1].Expectation != "no_match" {
		t.Errorf("content lost: %+v", tests)
	}

	if _, err := readBundleTests(zr.File[1]); err == nil || !strings.Contains(err.Error(), "unreadable tests") {
		t.Errorf("a malformed corpus: got %v", err)
	}
}

func TestBundleOriginIsProvenanceOnly(t *testing.T) {
	if origin("f1", "") != "fractal:f1" || origin("", "p1") != "prism:p1" || origin("f1", "p1") != "prism:p1" || origin("", "") != "" {
		t.Error("origin labels the scope an archive came from")
	}
}
