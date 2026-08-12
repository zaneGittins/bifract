package parser

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func mitreOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}
}

func TestMitreCommand(t *testing.T) {
	opts := mitreOpts()

	t.Run("no args reads the default tag field", func(t *testing.T) {
		sql := mustTranslate(t, `computer_name="wks01" | mitre()`, opts)
		if !strings.Contains(sql, "extractAll(lower(toString(fields.`rule_tags`::String))") {
			t.Errorf("expected the rule_tags default, got: %s", sql)
		}
		if !strings.Contains(sql, `\\bt[0-9]{4}`) {
			t.Errorf("a dedicated tag field should also accept bare ids: %s", sql)
		}
		if !strings.Contains(sql, "AS attack_tag") || !strings.Contains(sql, "GROUP BY attack_tag") {
			t.Errorf("expected attack_tag group key, got: %s", sql)
		}
		if !strings.Contains(sql, "COUNT(*) AS _count") || !strings.Contains(sql, "_count DESC") {
			t.Errorf("expected counted, count-ordered output, got: %s", sql)
		}
	})

	t.Run("any named field is read the same way", func(t *testing.T) {
		sql := mustTranslate(t, `* | mitre(tags=detect_mtd_tags)`, opts)
		if !strings.Contains(sql, "fields.`detect_mtd_tags`") {
			t.Errorf("expected detect_mtd_tags field reference, got: %s", sql)
		}
		if !strings.Contains(sql, `\\bt[0-9]{4}`) {
			t.Errorf("expected bare technique pattern for a tag field, got: %s", sql)
		}
	})

	t.Run("tags=norm_log scans the whole event without bare ids", func(t *testing.T) {
		sql := mustTranslate(t, `* | mitre(tags=norm_log)`, opts)
		if !strings.Contains(sql, "extractAll(lower(toString(norm_log))") {
			t.Errorf("expected a norm_log scan, got: %s", sql)
		}
		// Every hash containing a t-token would otherwise become a technique.
		if strings.Contains(sql, `\\bt[0-9]{4}`) {
			t.Errorf("bare technique pattern must not apply to a whole-event scan: %s", sql)
		}
	})

	t.Run("bare argument is the tag field", func(t *testing.T) {
		sql := mustTranslate(t, `* | mitre(rule_tags)`, opts)
		if !strings.Contains(sql, "fields.`rule_tags`") {
			t.Errorf("expected rule_tags field reference, got: %s", sql)
		}
	})

	t.Run("by adds a second group key", func(t *testing.T) {
		sql := mustTranslate(t, `* | mitre(tags=rule_tags, by=computer_name)`, opts)
		if !strings.Contains(sql, "AS computer_name") {
			t.Errorf("expected by= column, got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY attack_tag,") {
			t.Errorf("expected both group keys, got: %s", sql)
		}
	})

	t.Run("limit caps rows", func(t *testing.T) {
		sql := mustTranslate(t, `* | mitre(limit=10)`, opts)
		if !strings.Contains(sql, "LIMIT 10") {
			t.Errorf("expected LIMIT 10, got: %s", sql)
		}
	})

	t.Run("duplicate tags on one event count once", func(t *testing.T) {
		sql := mustTranslate(t, `* | mitre()`, opts)
		if !strings.Contains(sql, "arrayDistinct(") {
			t.Errorf("expected arrayDistinct to dedupe per-event tags, got: %s", sql)
		}
	})

	t.Run("rejects unknown argument", func(t *testing.T) {
		if _, err := TranslateToSQL(mustParse(t, `* | mitre(foo=bar)`), opts); err == nil {
			t.Error("expected an error for an unknown argument")
		}
	})

	t.Run("rejects use after groupby", func(t *testing.T) {
		if _, err := TranslateToSQL(mustParse(t, `* | groupby(computer_name) | mitre()`), opts); err == nil {
			t.Error("expected an error when mitre() follows an aggregation")
		}
	})

	t.Run("downstream filter routes to HAVING", func(t *testing.T) {
		sql := mustTranslate(t, `* | mitre() | _count > 5`, opts)
		if !strings.Contains(sql, "HAVING") {
			t.Errorf("expected HAVING for a post-aggregation filter, got: %s", sql)
		}
	})
}

// The tag extractor is a regex on purpose: real sources ship ATT&CK tags in a
// JSON-array string, a comma list, or bare in free text, and the same value
// arrives backslash-escaped once it is inside norm_log. Swapping the regex for
// JSON parsing would pass a unit test on the clean shape and drop every escaped
// one, so the accepted shapes are pinned here.
//
// ClickHouse strips one level of backslashes from a string literal, so the
// pattern is unescaped the same way before compiling.
func TestMitreTagPatternValueShapes(t *testing.T) {
	unescape := strings.NewReplacer(`\\`, `\`)
	fieldRe := regexp.MustCompile(unescape.Replace(mitreTagPattern + "|" + mitreBareTagPattern))
	scanRe := regexp.MustCompile(unescape.Replace(mitreTagPattern))

	tests := []struct {
		name  string
		value string
		field []string // matches when the operator named the tag field
		scan  []string // matches when scanning the whole event
	}{
		{
			name:  "limacharlie detect_mtd_tags",
			value: `["attack.t1059.004","attack.execution"]`,
			field: []string{"attack.t1059.004", "attack.execution"},
			scan:  []string{"attack.t1059.004", "attack.execution"},
		},
		{
			name:  "same value escaped inside norm_log",
			value: `{"detect_mtd_tags":"[\"attack.t1059.004\",\"attack.execution\"]","detect_mtd_level":"low"}`,
			field: []string{"attack.t1059.004", "attack.execution"},
			scan:  []string{"attack.t1059.004", "attack.execution"},
		},
		{
			name:  "sigma list with spaces",
			value: `["attack.t1218.011", "attack.defense-evasion"]`,
			field: []string{"attack.t1218.011", "attack.defense-evasion"},
			scan:  []string{"attack.t1218.011", "attack.defense-evasion"},
		},
		{
			name:  "comma separated",
			value: `attack.t1053.003,attack.persistence`,
			field: []string{"attack.t1053.003", "attack.persistence"},
			scan:  []string{"attack.t1053.003", "attack.persistence"},
		},
		{
			name:  "bare ids only in a named field",
			value: `T1059.004, T1105`,
			field: []string{"t1059.004", "t1105"},
			scan:  nil,
		},
		{
			name:  "platform tags are not attack tags",
			value: `["linux"]`,
		},
		{
			name:  "hex ids do not look like techniques",
			value: `a972bc0383f4a66cdfdfd1a3bb885a6b1dd93f46t1059c999d4adc505ab127dc0`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := fieldRe.FindAllString(strings.ToLower(tc.value), -1); !equalStrings(got, tc.field) {
				t.Errorf("named field: got %q, want %q", got, tc.field)
			}
			if got := scanRe.FindAllString(strings.ToLower(tc.value), -1); !equalStrings(got, tc.scan) {
				t.Errorf("norm_log scan: got %q, want %q", got, tc.scan)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMitreRejectedInsideCase(t *testing.T) {
	_, err := TranslateToSQL(mustParse(t, `* | case { user="root" | mitre(); * | x:="other"; }`), mitreOpts())
	if err == nil {
		t.Error("expected mitre() inside case() to be rejected as structural")
	}
}
