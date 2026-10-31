package query

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"bifract/pkg/parser"
)

func resolvePTG(t *testing.T, f *fakeLineage, q string) (*parser.QueryOptions, []string) {
	t.Helper()
	pipeline, err := parser.ParseQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	opts := pgrOpts()
	warnings, err := ResolveProcessTree(context.Background(), f.handler().db, pipeline, &opts)
	if err != nil {
		t.Fatal(err)
	}
	return &opts, warnings
}

// ptg() walks its tree with pgr's walk before translation: one row per node, a polling loop
// collapsed into a counted node, ancestors included, and each descendant attached to its
// parent node with ptg()'s seed-relative depth and path.
func TestResolveProcessTree(t *testing.T) {
	f := newFakeLineage(">old", "old>up", "up>seed")
	f.outOfRange["old"] = true // created before the window: no row of its own
	var cmds []string
	for i := 0; i < 50; i++ {
		c := fmt.Sprintf("cmd%02d", i)
		f.addProc("seed", c)
		cmds = append(cmds, c)
	}
	f.same("cmd.exe", "cmd /c tasklist", cmds...)
	f.addProc("cmd37", "evil")

	opts, warnings := resolvePTG(t, f, `ptg(start="seed") | pgraph()`)
	if warnings != nil || opts.ProcessTree == nil {
		t.Fatalf("warnings %v, tree %v", warnings, opts.ProcessTree)
	}
	var got []string
	for _, r := range opts.ProcessTree.Rows {
		got = append(got, fmt.Sprintf("%s<%s d%d n%d [%s]", r.ProcessGUID, r.ParentGUID, r.Depth, r.GroupSize, r.Path))
	}
	want := []string{
		"seed<up d0 n1 [seed]",
		"up<old d1 n1 [up > seed]",
		"cmd00<seed d1 n50 [seed > cmd00]",
		"evil<cmd00 d2 n1 [seed > cmd00 > evil]", // under the group node, not its own parent cmd37
	}
	if strings.Join(got, ", ") != strings.Join(want, ", ") {
		t.Errorf("rows\n got: %v\nwant: %v", got, want)
	}
	if r := opts.ProcessTree.Rows[2]; r.GroupFirst == "" || r.GroupLast == "" || r.LogID != "log-cmd00" {
		t.Errorf("group row %+v", r)
	}
	if opts.ProcLineageTable == "" {
		t.Error("the lineage table must be set for the walk")
	}
}

// The tree is only walked when the pipeline has a ptg(); an unknown seed is an empty tree.
func TestResolveProcessTreeNoOpAndMissingSeed(t *testing.T) {
	f := newFakeLineage(">root")
	opts, _ := resolvePTG(t, f, `level=error`)
	if opts.ProcessTree != nil || len(f.kinds()) != 0 {
		t.Errorf("no ptg(): tree %v, queries %v", opts.ProcessTree, f.kinds())
	}
	opts, _ = resolvePTG(t, f, `ptg(start="ghost")`)
	if opts.ProcessTree == nil || len(opts.ProcessTree.Rows) != 0 {
		t.Errorf("missing seed must resolve to an empty tree, got %v", opts.ProcessTree)
	}
}

// A walk cut by the node cap says so.
func TestResolveProcessTreeWarnsWhenTruncated(t *testing.T) {
	f := newFakeLineage(">root")
	for i := 0; i < parser.MaxProvenanceNodes+5; i++ {
		f.addProc("root", fmt.Sprintf("c%05d", i))
	}
	_, warnings := resolvePTG(t, f, `ptg(start="root", direction=forward)`)
	if len(warnings) != 1 || !strings.HasPrefix(warnings[0], "ptg: process tree truncated") {
		t.Errorf("warnings %v", warnings)
	}
}
