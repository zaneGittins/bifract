package dictionaries

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestGlobToRegexp(t *testing.T) {
	cases := []struct {
		glob    string
		match   []string
		noMatch []string
	}{
		{`aeroadmin.exe`,
			[]string{`aeroadmin.exe`, `C:\Tools\AeroAdmin.exe`, `/opt/x/aeroadmin.exe`},
			[]string{`notaeroadmin.exe`, `aeroadmin.exe.bak`, `aeroadminXexe`}},
		{`tigervnc*.exe`,
			[]string{`C:\bin\tigervnc-1.12.exe`, `tigervnc.exe`},
			[]string{`C:\bin\mytigervnc.exe`}},
		{`*\TightVNC\*`,
			[]string{`C:\Program Files\TightVNC\tvnserver.exe`},
			[]string{`C:\Program Files\TightVNC`}},
		{`C:\Users\*\AppData\Local\Programs\RemSupp\RemSupp.exe`,
			[]string{`c:\users\bob\appdata\local\programs\remsupp\remsupp.exe`},
			[]string{`D:\Users\bob\AppData\Local\Programs\RemSupp\RemSupp.exe`}},
		{`/opt/komari/*`,
			[]string{`/opt/komari/agent`},
			[]string{`/usr/opt/komari/agent`}},
		{`file?.txt`,
			[]string{`file1.txt`},
			[]string{`file12.txt`}},
		{`*.level.io`,
			[]string{`agent.level.io`},
			[]string{`level.io`, `agent.level.io.evil.com`}},
	}
	for _, c := range cases {
		expr, err := globToRegexp(c.glob)
		if err != nil {
			t.Fatalf("globToRegexp(%q): %v", c.glob, err)
		}
		re := regexp.MustCompile(expr)
		for _, s := range c.match {
			if !re.MatchString(s) {
				t.Errorf("%q (%s) should match %q", c.glob, expr, s)
			}
		}
		for _, s := range c.noMatch {
			if re.MatchString(s) {
				t.Errorf("%q (%s) should not match %q", c.glob, expr, s)
			}
		}
	}
	for _, all := range []string{"", "*", "**", "?*"} {
		if _, err := globToRegexp(all); err == nil {
			t.Errorf("globToRegexp(%q) accepted a glob that matches everything", all)
		}
	}
}

// The shape of the LOLRMM tools CSV: the key cell holds several comma-separated
// globs, and every one becomes its own row carrying the record's other columns.
func TestSourceSplitsGlobKeysIntoRows(t *testing.T) {
	body := "Name,Description,InstallationPaths,Website\n" +
		`TigerVNC,VNC server,"tigervnc*.exe, *\TightVNC\*",https://tigervnc.org` + "\n" +
		"NordLocker,Locker,,https://nordlocker.com/\n" +
		"Everything,Bad,*,x\n"
	src := &Source{URL: "https://example.com/a.csv", KeyField: "InstallationPaths",
		Columns: []string{"Name", "Description"}, SplitKey: ",", KeyFormat: keyFormatGlob}
	if err := src.normalize(KindPattern); err != nil {
		t.Fatal(err)
	}
	f, err := parseSource([]byte(body), src, KindPattern)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range f.columns {
		names = append(names, c.Name)
	}
	if got := strings.Join(names, ","); got != "InstallationPaths,Name,Description" {
		t.Fatalf("columns = %s", got)
	}

	var rows []DictionaryRow
	if err := f.each(func(r DictionaryRow) error { rows = append(rows, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(rows), rows)
	}
	for _, r := range rows {
		if r.Fields["Name"] != "TigerVNC" || r.Fields["Website"] != "" {
			t.Errorf("row carries the wrong columns: %+v", r.Fields)
		}
		if !regexp.MustCompile(r.Key).MatchString(`C:\Program Files\TightVNC\tigervnc-1.exe`) {
			t.Errorf("key %q does not match a TightVNC path", r.Key)
		}
	}
	if f.rows != 2 || f.skipped != 2 || f.sample == "" {
		t.Errorf("rows = %d, skipped = %d (%q), want the empty and match-everything records skipped", f.rows, f.skipped, f.sample)
	}
}

func TestSourceKeepsEveryColumnByDefault(t *testing.T) {
	src := &Source{URL: "https://example.com/a", KeyField: "ip"}
	if err := src.normalize(KindNetwork); err != nil {
		t.Fatal(err)
	}
	f, err := parseSource([]byte(`[{"ip":"10.0.0.0/8","site":"hq","rack":1},{"ip":"bogus","site":"x"}]`), src, KindNetwork)
	if err != nil {
		t.Fatal(err)
	}
	if f.format != "json" || len(f.columns) != 3 || f.columns[0].Name != "ip" {
		t.Fatalf("format %q columns %+v", f.format, f.columns)
	}
	var rows []DictionaryRow
	if err := f.each(func(r DictionaryRow) error { rows = append(rows, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Fields["rack"] != "1" || f.skipped != 1 {
		t.Fatalf("rows %+v skipped %d", rows, f.skipped)
	}
}

func TestSourcePlainList(t *testing.T) {
	src := &Source{URL: "https://example.com/tor.txt", KeyField: "ip", Format: "lines"}
	if err := src.normalize(KindValue); err != nil {
		t.Fatal(err)
	}
	f, err := parseSource([]byte("# exit nodes\n1.2.3.4\n\n5.6.7.8\n"), src, KindValue)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	if err := f.each(func(DictionaryRow) error { n++; return nil }); err != nil || n != 2 {
		t.Fatalf("got %d rows, err %v", n, err)
	}
}

// A lines source fills the column the key field names, even one that is not an
// identifier: the list's column is the cleaned name, the file's the raw one.
func TestSourcePlainListWithSpacedName(t *testing.T) {
	src := &Source{URL: "https://example.com/l.txt", KeyField: "IP address", Format: "lines"}
	_ = src.normalize(KindValue)
	f, err := parseSource([]byte("IP address\n1.2.3.4\n"), src, KindValue)
	if err != nil {
		t.Fatal(err)
	}
	var rows []DictionaryRow
	_ = f.each(func(r DictionaryRow) error { rows = append(rows, r); return nil })
	if f.columns[0].Name != "IP_address" || len(rows) != 1 || rows[0].Fields["IP_address"] != "1.2.3.4" {
		t.Fatalf("columns %+v rows %+v", f.columns, rows)
	}
}

// Blank header cells carry nothing, and two of them must not declare the same
// column twice.
func TestSourceIgnoresBlankHeaders(t *testing.T) {
	src := &Source{URL: "https://example.com/a.csv", KeyField: "k"}
	_ = src.normalize(KindValue)
	f, err := parseSource([]byte("k,v,,\na,1,,\n"), src, KindValue)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.columns) != 2 {
		t.Fatalf("columns = %+v, want k and v only", f.columns)
	}
}

// Everything that would fail after the list changed is refused while reading.
func TestSourceRefusedBeforeAnythingChanges(t *testing.T) {
	cases := []struct {
		name, body, kind string
		src              Source
		want             string
	}{
		{"reserved column", "p,_match\nabc,1\n", KindPattern, Source{KeyField: "p"}, "reserved"},
		{"unknown kept column", "k,v\na,1\n", KindValue, Source{KeyField: "k", Columns: []string{"w"}}, `"w"`},
		{"no usable rows", "net,site\nnot-a-range,hq\n", KindNetwork, Source{KeyField: "net"}, "no usable rows"},
	}
	for _, c := range cases {
		c.src.URL = "https://example.com/a.csv"
		_ = c.src.normalize(c.kind)
		if _, err := parseSource([]byte(c.body), &c.src, c.kind); err == nil || !strings.Contains(err.Error(), c.want) || !IsInvalidInput(err) {
			t.Errorf("%s: err = %v, want a refusal naming %s", c.name, err, c.want)
		}
	}
}

// A line longer than a source may hold fails the read instead of being held.
func TestSourceLineCap(t *testing.T) {
	src := &Source{URL: "https://example.com/a", KeyField: "k", Format: "lines"}
	_ = src.normalize(KindValue)
	if _, err := parseSource([]byte(strings.Repeat("x", maxSourceLine+1)), src, KindValue); err == nil {
		t.Fatal("an over-long line was read")
	}
}

func TestSourceHashIgnoresInterval(t *testing.T) {
	a := Source{URL: "https://x.com/a", KeyField: "k", RefreshMinutes: 60}
	b := a
	b.RefreshMinutes = 1440
	if a.hash([]byte("x")) != b.hash([]byte("x")) {
		t.Error("changing only the interval changed the hash, which forces a reload")
	}
	b.KeyField = "j"
	if a.hash([]byte("x")) == b.hash([]byte("x")) {
		t.Error("changing the key field kept the hash, so the file would not reload")
	}
}

func TestSourceMissingKeyColumnIsNamed(t *testing.T) {
	src := &Source{URL: "https://example.com/a.csv", KeyField: "Path"}
	_ = src.normalize(KindValue)
	_, err := parseSource([]byte("Name,Paths\na,b\n"), src, KindValue)
	if err == nil || !strings.Contains(err.Error(), `"Path"`) {
		t.Fatalf("err = %v, want it to name the missing column", err)
	}
}

func TestSourceNormalize(t *testing.T) {
	bad := []struct {
		src  Source
		kind string
	}{
		{Source{URL: "file:///etc/passwd", KeyField: "k"}, KindValue},
		{Source{URL: "https://x.com/a", KeyField: ""}, KindValue},
		{Source{URL: "https://x.com/a", KeyField: "k", Format: "xml"}, KindValue},
		{Source{URL: "https://x.com/a", KeyField: "k", KeyFormat: keyFormatGlob}, KindValue},
		{Source{URL: "https://x.com/a", KeyField: "k", RefreshMinutes: 1}, KindValue},
	}
	for _, c := range bad {
		if err := c.src.normalize(c.kind); err == nil {
			t.Errorf("accepted %+v for a %s list", c.src, c.kind)
		}
	}
	ok := Source{URL: " https://x.com/a ", KeyField: "k", Format: "auto", Columns: []string{"k", "a", " a ", ""}}
	if err := ok.normalize(KindValue); err != nil {
		t.Fatal(err)
	}
	if ok.URL != "https://x.com/a" || ok.Format != "" || len(ok.Columns) != 1 || ok.RefreshMinutes != defaultRefreshMinutes {
		t.Errorf("normalized to %+v", ok)
	}
}

func TestSourceDue(t *testing.T) {
	now := time.Now()
	checked := now.Add(-30 * time.Minute)
	d := &Dictionary{Source: &Source{RefreshMinutes: 60}, SourceStatus: &SourceStatus{}}
	if !sourceDue(d, now) {
		t.Error("a list never checked is due")
	}
	d.SourceStatus.CheckedAt = &checked
	if sourceDue(d, now) {
		t.Error("due 30 minutes into a 60 minute interval")
	}
	if !sourceDue(d, now.Add(31*time.Minute)) {
		t.Error("not due once the interval passed")
	}
}

func TestSourcedListRefusesHandEdits(t *testing.T) {
	if requireEditable(&Dictionary{Name: "x", Source: &Source{}}) == nil {
		t.Error("a sourced list accepted a hand edit")
	}
	if requireEditable(&Dictionary{Name: "x"}) != nil {
		t.Error("a plain list refused a hand edit")
	}
}

// A failed refresh is retried after five minutes, doubling per failure, and
// never later than the list's own interval.
func TestSourceRetryBacksOff(t *testing.T) {
	checked := time.Now()
	d := &Dictionary{Source: &Source{RefreshMinutes: 7 * 24 * 60},
		SourceStatus: &SourceStatus{CheckedAt: &checked, Error: "fetch failed", Failures: 1}}
	if sourceDue(d, checked.Add(4*time.Minute)) || !sourceDue(d, checked.Add(5*time.Minute)) {
		t.Error("first retry is not at five minutes")
	}
	d.SourceStatus.Failures = 3
	if sourceDue(d, checked.Add(19*time.Minute)) || !sourceDue(d, checked.Add(20*time.Minute)) {
		t.Error("third retry is not at twenty minutes")
	}
	d.SourceStatus.Failures = 40
	if !sourceDue(d, checked.Add(7*24*time.Hour)) {
		t.Error("retries backed off past the list's own interval")
	}
	d.SourceStatus.Error, d.SourceStatus.Failures = "", 0
	if sourceDue(d, checked.Add(time.Hour)) {
		t.Error("a healthy weekly list was due after an hour")
	}
}

func TestManualRefreshCooldown(t *testing.T) {
	checked := time.Now()
	d := &Dictionary{SourceStatus: &SourceStatus{CheckedAt: &checked}}
	if !coolingDown(d, checked.Add(30*time.Second)) || coolingDown(d, checked.Add(61*time.Second)) {
		t.Error("cooldown is not one minute")
	}
}

// Every row of a Patterns list is an expression compiled on every node, so a
// Patterns source is held to far fewer rows than a Values one.
func TestPatternSourceRowCap(t *testing.T) {
	body := strings.Repeat("abc\n", maxPatternSourceRows+1)
	src := &Source{URL: "https://example.com/p.txt", KeyField: "p", Format: "lines"}
	_ = src.normalize(KindPattern)
	if _, err := parseSource([]byte(body), src, KindPattern); err == nil || !strings.Contains(err.Error(), "pattern list") {
		t.Fatalf("err = %v, want the pattern row cap", err)
	}
	_ = src.normalize(KindValue)
	if _, err := parseSource([]byte(body), src, KindValue); err != nil {
		t.Fatalf("a values list hit the pattern cap: %v", err)
	}
}
