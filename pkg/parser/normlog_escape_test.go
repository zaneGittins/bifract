package parser

import (
	"regexp"
	"strings"
	"testing"
)

// TestEscapeLiteralForNormLog pins the serialization convention of each store.
// The expectations were measured against ClickHouse 26.6 (toString(fields)) and
// the archiver's marshalFields; see normlog_escape.go.
func TestEscapeLiteralForNormLog(t *testing.T) {
	tests := []struct {
		name        string
		literal     string
		wantHot     string
		wantIceberg string
	}{
		{"plain", "curl", "curl", "curl"},
		{"domain", "example.com", "example.com", "example.com"},
		{"forward slash", "/usr/bin/curl", `\/usr\/bin\/curl`, "/usr/bin/curl"},
		{"url", "https://example.com/a", `https:\/\/example.com\/a`, "https://example.com/a"},
		{"backslash", `C:\Windows`, `C:\\Windows`, `C:\\Windows`},
		{"double quote", `say "hi"`, `say \"hi\"`, `say \"hi\"`},
		{"newline", "a\nb", `a\nb`, `a\nb`},
		{"tab", "a\tb", `a\tb`, `a\tb`},
		{"carriage return", "a\rb", `a\rb`, `a\rb`},
		{"control 0x01", "a\x01b", `a\u0001b`, `a\u0001b`},
		{"del 0x7f passes through", "a\x7fb", "a\x7fb", "a\x7fb"},
		{"html chars pass through", "a<b>&c", "a<b>&c", "a<b>&c"},
		{"single quote passes through", "a'b", "a'b", "a'b"},
		{"non-ascii passes through", "café привет 日本語", "café привет 日本語", "café привет 日本語"},
		{"empty", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeLiteralForNormLog(tt.literal, SourceHot); got != tt.wantHot {
				t.Errorf("hot: got %q, want %q", got, tt.wantHot)
			}
			if got := escapeLiteralForNormLog(tt.literal, SourceIceberg); got != tt.wantIceberg {
				t.Errorf("iceberg: got %q, want %q", got, tt.wantIceberg)
			}
		})
	}
}

// TestNormLogLiteralPatternMatchesSerializedForm is the property that actually
// matters: the generated RE2 pattern must match the serialized document, and the
// escaping must not be so aggressive that it stops matching plain text.
func TestNormLogLiteralPatternMatchesSerializedForm(t *testing.T) {
	literals := []string{
		"curl", "example.com", "/usr/bin/curl", "https://example.com/a/b",
		`C:\Windows\System32\cmd.exe`, `say "hi"`, "a<b>&c", "café", "a'b",
		"1.2.3.4", "a+b", "x(y)z", "a[0]", "a|b", "a?b", "a*b", "a$b", "a^b",
	}
	for _, mode := range []SourceMode{SourceHot, SourceIceberg} {
		for _, lit := range literals {
			// The document as the store would serialize it.
			doc := `{"v":"` + escapeLiteralForNormLog(lit, mode) + `"}`
			pat := normLogLiteralPattern(lit, mode)
			re, err := regexp.Compile(strings.ToLower(pat))
			if err != nil {
				t.Fatalf("mode=%d literal=%q: pattern %q does not compile: %v", mode, lit, pat, err)
			}
			if !re.MatchString(strings.ToLower(doc)) {
				t.Errorf("mode=%d literal=%q: pattern %q did not match serialized doc %q", mode, lit, pat, doc)
			}
		}
	}
}

// TestNormLogLiteralPatternNoFalsePositives guards the other direction: escaping
// must not turn a literal into something that matches unrelated text.
func TestNormLogLiteralPatternNoFalsePositives(t *testing.T) {
	cases := []struct{ literal, shouldNotMatch string }{
		{"/usr/bin", `{"v":"usr\/bin"}`},       // leading slash is real
		{`C:\Windows`, `{"v":"C:Windows"}`},    // backslash is real
		{"example.com", `{"v":"exampleXcom"}`}, // '.' must stay literal
		{"a+b", `{"v":"aaab"}`},                // '+' must stay literal
	}
	for _, c := range cases {
		pat := normLogLiteralPattern(c.literal, SourceHot)
		re := regexp.MustCompile(strings.ToLower(pat))
		if re.MatchString(strings.ToLower(c.shouldNotMatch)) {
			t.Errorf("literal %q pattern %q wrongly matched %q", c.literal, pat, c.shouldNotMatch)
		}
	}
}

// TestBareTermTranslationBySourceMode asserts the end-to-end translator output
// diverges on '/' and only on '/'.
func TestBareTermTranslationBySourceMode(t *testing.T) {
	tests := []struct {
		query       string
		wantHot     string
		wantIceberg string
	}{
		{`"/usr/bin/curl"`, `match(lower(norm_log), '\\\\/usr\\\\/bin\\\\/curl')`, `match(lower(norm_log), '/usr/bin/curl')`},
		{`"C:\\Windows"`, `match(lower(norm_log), 'c:\\\\\\\\windows')`, `match(lower(norm_log), 'c:\\\\\\\\windows')`},
		{`"example.com"`, `match(lower(norm_log), 'example\\.com')`, `match(lower(norm_log), 'example\\.com')`},
		{`"curl"`, `match(lower(norm_log), 'curl')`, `match(lower(norm_log), 'curl')`},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			for _, m := range []struct {
				mode SourceMode
				want string
			}{{SourceHot, tt.wantHot}, {SourceIceberg, tt.wantIceberg}} {
				p, err := ParseQuery(tt.query)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				sql, err := TranslateToSQL(p, QueryOptions{TableName: "logs", MaxRows: 10, SourceMode: m.mode})
				if err != nil {
					t.Fatalf("translate: %v", err)
				}
				if !strings.Contains(sql, m.want) {
					t.Errorf("mode=%d: expected %s in SQL, got: %s", m.mode, m.want, sql)
				}
			}
		})
	}
}

// TestFieldQualifiedOperatorsUnescaped is the guard that matters most for
// regressions: a field-qualified operator reads the JSON sub-column, which holds
// the raw unescaped value. Serialization escaping must NEVER be applied there.
func TestFieldQualifiedOperatorsUnescaped(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{`image=~"/usr/bin"`, "multiSearchAnyCaseInsensitive(fields.`image`::String, ['/usr/bin'])"},
		{`image=~"C:\\Windows"`, "multiSearchAnyCaseInsensitive(fields.`image`::String, ['C:\\\\Windows'])"},
		{`image=~"a","b"`, "multiSearchAnyCaseInsensitive(fields.`image`::String, ['a', 'b'])"},
		{`image=^"/usr"`, "startsWith(lower(fields.`image`::String), '/usr')"},
		{`image=$"/curl"`, "endsWith(lower(fields.`image`::String), '/curl')"},
		{`image="/usr/bin/curl"`, "fields.`image`::String = '/usr/bin/curl'"},
		{`commandline=~"https://example.com"`, "multiSearchAnyCaseInsensitive(fields.`commandline`::String, ['https://example.com'])"},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			p, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			sql, err := TranslateToSQL(p, QueryOptions{TableName: "logs", MaxRows: 10})
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			if !strings.Contains(sql, tt.want) {
				t.Errorf("expected %s\n     got: %s", tt.want, sql)
			}
			// A field-qualified predicate must never carry a norm_log escape.
			if strings.Contains(sql, `\\/`) {
				t.Errorf("field-qualified query picked up norm_log slash escaping: %s", sql)
			}
		})
	}
}

// TestNormLogOperatorsEscaped covers the norm_log-targeted forms of the same
// operators, which DO need serialization escaping.
func TestNormLogOperatorsEscaped(t *testing.T) {
	tests := []struct {
		query       string
		wantHot     string
		wantIceberg string
	}{
		{`norm_log=~"/usr/bin"`, `multiSearchAnyCaseInsensitive(norm_log, ['\\/usr\\/bin'])`, `multiSearchAnyCaseInsensitive(norm_log, ['/usr/bin'])`},
		{`norm_log=~"C:\\Windows"`, `multiSearchAnyCaseInsensitive(norm_log, ['C:\\\\Windows'])`, `multiSearchAnyCaseInsensitive(norm_log, ['C:\\\\Windows'])`},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			for _, m := range []struct {
				mode SourceMode
				want string
			}{{SourceHot, tt.wantHot}, {SourceIceberg, tt.wantIceberg}} {
				p, err := ParseQuery(tt.query)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				sql, err := TranslateToSQL(p, QueryOptions{TableName: "logs", MaxRows: 10, SourceMode: m.mode})
				if err != nil {
					t.Fatalf("translate: %v", err)
				}
				if !strings.Contains(sql, m.want) {
					t.Errorf("mode=%d expected %s\n     got: %s", m.mode, m.want, sql)
				}
			}
		})
	}
}

// TestUserRegexLiteralsUnchanged confirms author-written /regex/ still passes
// through without serialization escaping applied.
func TestUserRegexLiteralsUnchanged(t *testing.T) {
	p, err := ParseQuery(`/usr\/bin/`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sql, err := TranslateToSQL(p, QueryOptions{TableName: "logs", MaxRows: 10})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(sql, `match(norm_log, 'usr\\/bin')`) {
		t.Errorf("user regex was rewritten, got: %s", sql)
	}
}
