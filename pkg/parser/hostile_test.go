package parser

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A hostile value can change only what a string literal or a quoted identifier
// holds, never the shape of the SQL around it. Each template is translated twice:
// with the hostile value, and with its benign twin, the same text with every
// character that could end a literal replaced. Both are lexed as ClickHouse lexes
// SQL; outside literal contents the token streams must be identical, so a value
// that broke out of its quotes (closing one, commenting out the rest, starting a
// statement) shows up as a different stream.

// hostileTemplates put a value in every position a query takes text from the user.
// {v} is a BQL string, {r} a regex body, {f} a quoted field name.
var hostileTemplates = []string{
	`user={v}`,
	`user!={v}`,
	`user=~{v}`,
	`user=^{v}`,
	`user=${v}`,
	`image=/{r}/`,
	`* | in({f}, values=[{v}])`,
	`* | cidr({f}, "10.0.0.0/8")`,
	`* | lowercase({f})`,
	`* | dedup({f})`,
	`* | in(user, values=[{v}, "b"])`,
	`* | !in(user, values=[{v}])`,
	`* | regex({v}, field=image)`,
	`* | replace(image, pattern={v}, replacement={v})`,
	`* | split(image, delimiter={v}, index=1)`,
	`* | sprintf({v}, user)`,
	`* | strftime(format={v})`,
	`* | x := {v} | x={v}`,
	`* | eval(y = concat(user, {v})) | y={v}`,
	`* | case { user={v} | k := {v} ; * | k := "o" }`,
	`* | groupby(user) | user={v}`,
	`* | lowercase(user) | user=~{v}`,
	`* | user=field({f})`,
	`* | table(user, {f})`,
	`* | sort({f})`,
	`* | groupby({f})`,
	`* | chain(user) { user={v} ; host={v} }`,
	`user={v} | modelLookup(model="fs", key=[user]) | is_new={v}`,
	`* | timechart(span=1h, function=groupby(user)) | user={v}`,
}

// hostileValues are inputs built to escape a literal or an identifier.
var hostileValues = []string{
	`'`, `''`, `\`, `\'`, `\\'`, "`", "\\`", `"`, `' OR 1=1 --`, `') OR ('1'='1`,
	`'; DROP TABLE logs; --`, `\'; SELECT 1; --`, `*/ SELECT 1 /*`, `-- `, `/*`,
	"a\nb", "a\rb", "a\tb", "\\\n", `%s`, `%%`, `${x}`, `{v}`, `'||'`, "x`; DROP TABLE logs; --",
	`fractal_id`, `' OR fractal_id != 'f1`, `\x27`, "’", `é'ü`, strings.Repeat(`\'`, 20),
}

// benignTwin is v with every character that could end a literal or identifier
// replaced: the same value in every other respect. A comment or a statement
// separator only matters once a value is out of its quotes, so - / * ; stay: *
// alone means "present", and replacing it would change what the query means.
func benignTwin(v string) string {
	return strings.Map(func(r rune) rune {
		if strings.ContainsRune("'\"`\\\n\r\t", r) {
			return 'q'
		}
		return r
	}, v)
}

func hostileBQL(v string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range v {
		if r == '"' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
}

// fillTemplate writes v into the template's positions, or reports that it cannot
// be written there (a regex cannot hold an unescaped slash or a newline).
func fillTemplate(tmpl, v string) (string, bool) {
	if strings.Contains(tmpl, "{r}") && strings.ContainsAny(v, "/\n\r") {
		return "", false
	}
	q := strings.NewReplacer("{v}", hostileBQL(v), "{f}", hostileBQL("f"+v), "{r}", v)
	return q.Replace(tmpl), true
}

// sqlShape lexes SQL as ClickHouse does and returns its tokens with the contents
// of string literals and quoted identifiers blanked, so two translations differing
// only in what a value held lex the same.
func sqlShape(sql string) ([]string, error) {
	var toks []string
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\n' || c == '\t' || c == '\r':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			return nil, fmt.Errorf("comment at %d", i) // generated SQL never comments
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			return nil, fmt.Errorf("comment at %d", i)
		case c == '\'' || c == '`' || c == '"':
			j := i + 1
			for ; j < len(sql); j++ {
				if sql[j] == '\\' {
					j++
					continue
				}
				if sql[j] == c {
					if j+1 < len(sql) && sql[j+1] == c {
						j++
						continue
					}
					break
				}
			}
			if j >= len(sql) {
				return nil, fmt.Errorf("unterminated %c at %d", c, i)
			}
			toks = append(toks, string(c)+"…"+string(c))
			i = j + 1
		case c == ';':
			return nil, fmt.Errorf("statement separator at %d", i)
		case isWord(c) || c >= 0x80:
			j := i
			for j < len(sql) && (isWord(sql[j]) || sql[j] >= 0x80) {
				j++
			}
			toks = append(toks, sql[i:j])
			i = j
		default:
			toks = append(toks, string(c))
			i++
		}
	}
	return toks, nil
}

// checkHostile translates tmpl with v and with its benign twin and compares their
// shapes. It returns the hostile SQL, or "" when neither translates.
func checkHostile(t *testing.T, tmpl, v string) string {
	t.Helper()
	q, ok := fillTemplate(tmpl, v)
	if !ok {
		return ""
	}
	twin, _ := fillTemplate(tmpl, benignTwin(v))
	sql, err := hostileTranslate(q)
	twinSQL, twinErr := hostileTranslate(twin)
	if err != nil || twinErr != nil {
		// Refusing a value is safe; refusing only one of the pair is fine too, as
		// long as nothing was emitted.
		return ""
	}
	shape, err := sqlShape(sql)
	if err != nil {
		t.Errorf("%s with %q: %v\n%s", tmpl, v, err, sql)
		return ""
	}
	twinShape, err := sqlShape(twinSQL)
	if err != nil {
		t.Fatalf("benign twin of %s does not lex: %v\n%s", tmpl, err, twinSQL)
	}
	if strings.Join(shape, " ") != strings.Join(twinShape, " ") {
		t.Errorf("%s with %q changed the SQL around it:\n%s\nbenign:\n%s", tmpl, v, sql, twinSQL)
		return ""
	}
	return sql
}

func hostileTranslate(q string) (string, error) {
	pl, err := ParseQuery(q)
	if err != nil {
		return "", err
	}
	opts := goldenBase()
	opts.Models = GoldenModels(q)
	r, err := TranslateToSQLWithOrder(pl, opts)
	if err != nil {
		return "", err
	}
	return r.SQL, nil
}

func TestHostileValuesStayInsideTheirQuotes(t *testing.T) {
	for _, tmpl := range hostileTemplates {
		for _, v := range hostileValues {
			checkHostile(t, tmpl, v)
		}
	}
}

func FuzzHostileValue(f *testing.F) {
	for _, v := range hostileValues {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, v string) {
		if strings.ContainsRune(v, 0) {
			return // NUL ends a BQL query; it never reaches the translator
		}
		for _, tmpl := range hostileTemplates {
			checkHostile(t, tmpl, v)
		}
	})
}

// ClickHouse's own parser agrees: every hostile translation is one SELECT, and
// its syntax tree with literals blanked is the benign twin's.
func TestHostileValuesParseAsOneSelectInClickHouse(t *testing.T) {
	bin := os.Getenv("BIFRACT_TEST_CLICKHOUSE_BIN")
	if bin == "" {
		if os.Getenv("BIFRACT_REQUIRE_CLICKHOUSE") != "" {
			t.Fatal("BIFRACT_TEST_CLICKHOUSE_BIN is not set")
		}
		t.Skip("BIFRACT_TEST_CLICKHOUSE_BIN not set")
	}
	type pair struct{ name, sql, twin string }
	var pairs []pair
	for _, tmpl := range hostileTemplates {
		for _, v := range hostileValues {
			sql := checkHostile(t, tmpl, v)
			if sql == "" {
				continue
			}
			twin, _ := fillTemplate(tmpl, benignTwin(v))
			twinSQL, _ := hostileTranslate(twin)
			pairs = append(pairs, pair{fmt.Sprintf("%s with %q", tmpl, v), sql, twinSQL})
		}
	}
	var batch strings.Builder
	for i, p := range pairs {
		fmt.Fprintf(&batch, "SELECT 'MARK %d';\nEXPLAIN AST %s;\nSELECT 'TWIN %d';\nEXPLAIN AST %s;\n", i, p.sql, i, p.twin)
	}
	file := filepath.Join(t.TempDir(), "hostile.sql")
	if err := os.WriteFile(file, []byte(batch.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(bin, "local", "--multiquery", "--format", "TabSeparated", "--queries-file", file).CombinedOutput()
	if err != nil {
		t.Fatalf("clickhouse: %v\n%s", err, out)
	}
	// A tree is one node per row, indented by depth under its root. A row starting
	// at the margin that is not a root continues the row before it: a newline an
	// identifier holds.
	rows := map[string][]string{}
	var cur string
	for _, line := range strings.Split(string(out), "\n") {
		switch {
		case strings.HasPrefix(line, "MARK ") || strings.HasPrefix(line, "TWIN "):
			cur = line
		case line == "":
		case !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "SelectWithUnionQuery") && len(rows[cur]) > 0:
			rows[cur][len(rows[cur])-1] += "\n" + line
		default:
			rows[cur] = append(rows[cur], line)
		}
	}
	tree := func(key string) string {
		var b strings.Builder
		for _, r := range rows[key] {
			b.WriteString(maskLiterals(r) + "\n")
		}
		return b.String()
	}
	for i, p := range pairs {
		hostile, twin := tree(fmt.Sprintf("MARK %d", i)), tree(fmt.Sprintf("TWIN %d", i))
		if !strings.Contains(hostile, "SelectWithUnionQuery") || strings.Count(hostile, "SelectWithUnionQuery (children") < 1 {
			t.Errorf("%s: not a SELECT:\n%s", p.name, hostile)
		}
		if hostile != twin {
			t.Errorf("%s: ClickHouse parses it differently from its benign twin:\n%s\nbenign:\n%s", p.name, hostile, twin)
		}
	}
	if len(pairs) == 0 {
		t.Fatal("no translations to check")
	}
}

// maskLiterals blanks the values in an EXPLAIN AST line: string literals and the
// names of identifiers, which are what a value is allowed to change.
func maskLiterals(line string) string {
	if i := strings.Index(line, "Literal "); i >= 0 {
		return line[:i] + "Literal …"
	}
	if i := strings.Index(line, "Identifier "); i >= 0 {
		return line[:i] + "Identifier …"
	}
	if i := strings.Index(line, " (alias "); i >= 0 {
		return line[:i] + " (alias …)"
	}
	return line
}
