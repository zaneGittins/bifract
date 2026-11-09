// Command bifract-bqlgolden compares the SQL the BQL translator generates for the
// golden corpus (pkg/parser/testdata/golden.jsonl) at another git ref with the
// working tree's, and says which changes keep the query's meaning.
//
//	go run ./cmd/bifract-bqlgolden -base origin/main
//
// Each changed translation is parsed by ClickHouse (clickhouse local, EXPLAIN AST).
// Nested AND and OR are flattened and their operands sorted, so a change of brackets
// or conjunct order alone is reported as same meaning; anything else, a changed
// error, or changed result metadata is listed in full for review. It exits 1 when any
// change needs review.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func main() {
	base := flag.String("base", "origin/main", "git ref to compare the working tree with")
	clickhouse := flag.String("clickhouse", "clickhouse", "clickhouse binary, run as clickhouse local")
	show := flag.Int("show", 20, "changes needing review to print in full")
	flag.Parse()
	if err := run(*base, *clickhouse, *show); err != nil {
		fmt.Fprintln(os.Stderr, "bifract-bqlgolden:", err)
		os.Exit(2)
	}
}

func run(base, clickhouse string, show int) error {
	root, err := output("", "git", "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root = strings.TrimSpace(root)
	tmp, err := os.MkdirTemp("", "bqlgolden-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	baseTree := filepath.Join(tmp, "base")
	if _, err := output(root, "git", "worktree", "add", "--detach", baseTree, base); err != nil {
		return err
	}
	defer output(root, "git", "worktree", "remove", "--force", baseTree)

	before, err := dump(baseTree, filepath.Join(tmp, "base.jsonl"))
	if err != nil {
		return fmt.Errorf("%s: %w", base, err)
	}
	after, err := dump(root, filepath.Join(tmp, "work.jsonl"))
	if err != nil {
		return fmt.Errorf("working tree: %w", err)
	}

	var pairs []pair
	added, removed := 0, 0
	for q, outs := range after {
		was, ok := before[q]
		if !ok {
			added++
			continue
		}
		for v, sql := range outs {
			if old := was[v]; old != sql {
				pairs = append(pairs, pair{q: q, variant: v, old: old, new: sql})
			}
		}
	}
	for q := range before {
		if _, ok := after[q]; !ok {
			removed++
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].q != pairs[j].q {
			return pairs[i].q < pairs[j].q
		}
		return pairs[i].variant < pairs[j].variant
	})
	if err := classify(pairs, clickhouse, tmp); err != nil {
		return err
	}

	counts := map[string]int{}
	var review []pair
	for _, p := range pairs {
		counts[p.verdict]++
		if p.verdict != sameMeaning {
			review = append(review, p)
		}
	}
	fmt.Printf("%d translations changed against %s (%d queries added, %d removed):\n", len(pairs), base, added, removed)
	for _, v := range []string{sameMeaning, changedMeaning, changedError, changedMetadata} {
		fmt.Printf("  %-34s %d\n", v, counts[v])
	}
	for i, p := range review {
		if i == show {
			fmt.Printf("\n... and %d more\n", len(review)-show)
			break
		}
		fmt.Printf("\n%s: %s [%s]\n  before: %s\n  after:  %s\n", p.verdict, p.q, p.variant, oneLine(p.old), oneLine(p.new))
	}
	if len(review) > 0 {
		os.Exit(1)
	}
	return nil
}

const (
	sameMeaning     = "same meaning (brackets or order)"
	changedMeaning  = "changed SQL meaning"
	changedError    = "changed error"
	changedMetadata = "changed result metadata"
)

type pair struct {
	q, variant, old, new string
	verdict              string
}

// dump runs the golden test in tree with -golden.dump and reads what it wrote: each
// query's translation per variant. The test fails when the tree's translations differ
// from its own snapshot, which is expected for the working tree, so only a missing
// dump is an error.
func dump(tree, path string) (map[string]map[string]string, error) {
	cmd := exec.Command("go", "test", "./pkg/parser", "-run", "^TestGolden$", "-count=1", "-golden.dump="+path)
	cmd.Dir = tree
	out, _ := cmd.CombinedOutput()
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("no golden dump (does this ref have pkg/parser/golden_test.go?): %s", lastLines(out, 5))
	}
	defer f.Close()
	recs := map[string]map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		rec := map[string]string{}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return nil, err
		}
		q := rec["q"]
		delete(rec, "q")
		recs[q] = rec
	}
	return recs, sc.Err()
}

// classify sets each pair's verdict, parsing every changed SQL statement in one
// clickhouse local run.
func classify(pairs []pair, clickhouse, tmp string) error {
	var batch strings.Builder
	var asked []int
	for i := range pairs {
		p := &pairs[i]
		was, now := split(p.old), split(p.new)
		switch {
		case was.isErr || now.isErr:
			p.verdict = changedError
		case was.meta != now.meta:
			p.verdict = changedMetadata
		default:
			asked = append(asked, i)
			fmt.Fprintf(&batch, "SELECT 'PAIR %d OLD';\nEXPLAIN AST %s;\nSELECT 'PAIR %d NEW';\nEXPLAIN AST %s;\n", i, was.statement(), i, now.statement())
		}
	}
	if len(asked) == 0 {
		return nil
	}
	path := filepath.Join(tmp, "ast.sql")
	if err := os.WriteFile(path, []byte(batch.String()), 0o644); err != nil {
		return err
	}
	cmd := exec.Command(clickhouse, "local", "--multiquery", "--ignore-error", "--queries-file", path)
	out, err := cmd.CombinedOutput()
	if err != nil && len(out) == 0 {
		return fmt.Errorf("clickhouse local: %w", err)
	}
	trees := parseBatch(string(out))
	for _, i := range asked {
		o, n := trees[fmt.Sprintf("%d OLD", i)], trees[fmt.Sprintf("%d NEW", i)]
		if o != "" && o == n {
			pairs[i].verdict = sameMeaning
		} else {
			pairs[i].verdict = changedMeaning
		}
	}
	return nil
}

// translation is one golden output: its SQL, the result metadata the golden test
// appends, and the source WHERE entries, which are SQL as well.
type translation struct {
	sql, meta   string
	sourceWhere []string
	isErr       bool
}

func split(out string) translation {
	if strings.HasPrefix(out, "error: ") || out == "" {
		return translation{meta: out, isErr: true}
	}
	t := translation{sql: out}
	if i := strings.LastIndex(out, "\nsw="); i >= 0 {
		if json.Unmarshal([]byte(out[i+len("\nsw="):]), &t.sourceWhere) == nil {
			t.sql = out[:i]
		}
	}
	if i := strings.LastIndex(t.sql, "\nfields="); i >= 0 {
		t.sql, t.meta = t.sql[:i], t.sql[i:]
	}
	return t
}

// statement is the translation as one statement for ClickHouse to parse: its SQL,
// with the source WHERE entries as a second query beside it.
func (t translation) statement() string {
	if len(t.sourceWhere) == 0 {
		return t.sql
	}
	return t.sql + " UNION ALL SELECT 1 WHERE (" + strings.Join(t.sourceWhere, ") AND (") + ")"
}

var markerRe = regexp.MustCompile(`(?m)^PAIR (\d+ (?:OLD|NEW))$`)

// parseBatch maps each marker to the normalized AST printed after it, or "" when
// ClickHouse could not parse that statement.
func parseBatch(out string) map[string]string {
	trees := map[string]string{}
	locs := markerRe.FindAllStringSubmatchIndex(out, -1)
	for k, loc := range locs {
		end := len(out)
		if k+1 < len(locs) {
			end = locs[k+1][0]
		}
		body := out[loc[1]:end]
		key := out[loc[2]:loc[3]]
		if strings.Contains(body, "DB::Exception") {
			trees[key] = ""
			continue
		}
		trees[key] = normalize(body)
	}
	return trees
}

// node is one line of EXPLAIN AST output and its children.
type node struct {
	label string
	kids  []*node
}

var childCountRe = regexp.MustCompile(` \(children \d+\)$`)

// normalize parses an indented AST and renders it with nested AND and OR flattened
// and their operands sorted.
func normalize(text string) string {
	root := &node{label: "root"}
	type frame struct {
		indent int
		n      *node
	}
	stack := []frame{{-1, root}}
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		n := &node{label: childCountRe.ReplaceAllString(strings.TrimSpace(line), "")}
		for stack[len(stack)-1].indent >= indent {
			stack = stack[:len(stack)-1]
		}
		parent := stack[len(stack)-1].n
		parent.kids = append(parent.kids, n)
		stack = append(stack, frame{indent, n})
	}
	return render(flatten(root))
}

func flatten(n *node) *node {
	for i, k := range n.kids {
		n.kids[i] = flatten(k)
	}
	if (n.label == "Function and" || n.label == "Function or") && len(n.kids) > 0 && n.kids[0].label == "ExpressionList" {
		var args []*node
		for _, a := range n.kids[0].kids {
			if a.label == n.label && len(a.kids) > 0 && a.kids[0].label == "ExpressionList" {
				args = append(args, a.kids[0].kids...)
			} else {
				args = append(args, a)
			}
		}
		sort.Slice(args, func(i, j int) bool { return render(args[i]) < render(args[j]) })
		n.kids[0].kids = args
	}
	return n
}

func render(n *node) string {
	var b strings.Builder
	b.WriteString(n.label)
	b.WriteByte('[')
	for i, k := range n.kids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(render(k))
	}
	b.WriteByte(']')
	return b.String()
}

func output(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, lastLines(out, 5))
	}
	return string(out), nil
}

func lastLines(b []byte, n int) string {
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}
