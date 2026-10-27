package dictionaries

import (
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

func prefilterOf(t *testing.T, w *world, d *Dictionary) string {
	t.Helper()
	s, err := w.m.ListDictionaryMappings(ctx, fractalA, "")
	if err != nil {
		t.Fatal(err)
	}
	return s.Prefilter[chDictName(d.ID)]
}

// A pattern list gets a prefilter once ClickHouse has compiled its rows for
// Hyperscan, loses it to a row Hyperscan refuses, and regains it when that row
// goes.
func TestPatternPrefilterFollowsTheRows(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "rmm", Kind: KindPattern, Columns: cols("re", "tool")})
	// No row may reach the table while the screen still vouches for the old set.
	w.ch.FailExecFunc(func(stmt string) error {
		if reInsert.MatchString(stmt) {
			if rec, _ := w.dict(d.ID); rec.prefilter {
				t.Errorf("rows inserted under a standing prefilter: %s", stmt)
			}
		}
		return w.execCH(stmt)
	})
	if got := prefilterOf(t, w, d); got != "" {
		t.Errorf("an unprobed list has a prefilter: %s", got)
	}

	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("re", `(?i)x\.exe$`, "tool", "X")}}); err != nil {
		t.Fatal(err)
	}
	want := "(SELECT groupArray(`re`) FROM `logs`.`" + chTableName(d.ID) + "` FINAL WHERE notEmpty(`re`))"
	if got := prefilterOf(t, w, d); got != want {
		t.Errorf("prefilter = %q, want %q", got, want)
	}

	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("re", "(?U)a+b", "tool", "Y")}}); err != nil {
		t.Fatal(err)
	}
	if got := prefilterOf(t, w, d); got != "" {
		t.Errorf("a list Hyperscan refuses kept its prefilter: %s", got)
	}

	if err := w.m.DeleteRow(ctx, d.ID, "(?U)a+b"); err != nil {
		t.Fatal(err)
	}
	if got := prefilterOf(t, w, d); got != want {
		t.Errorf("after the refused row went, prefilter = %q", got)
	}
}

// A row that lands just after a probe read the table is not covered by it, so
// the probe's verdict must not stick.
func TestPrefilterProbeLosesToAConcurrentWrite(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "rmm", Kind: KindPattern, Columns: cols("re")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("re", "a")}}); err != nil {
		t.Fatal(err)
	}
	w.ch.Respond(func(q string, a []any) (fakedb.CHResult, bool) {
		res, ok := w.answerCH(q, a)
		if strings.HasPrefix(q, "SELECT multiMatchAny(") {
			if err := w.m.batchInsertRows(ctx, d, []DictionaryRow{{Fields: fields("re", "(?U)a+b")}}, []uint64{9}); err != nil {
				t.Error(err)
			}
		}
		return res, ok
	})
	if err := w.m.ReloadDictionary(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if got := prefilterOf(t, w, d); got != "" {
		t.Errorf("a probe overtaken by a write set the prefilter: %s", got)
	}
}

// Only a REGEXP_TREE is worth screening, so no other kind is ever probed.
func TestValueListIsNeverProbed(t *testing.T) {
	w := newWorld(t)
	w.ch.Respond(func(q string, a []any) (fakedb.CHResult, bool) {
		if strings.HasPrefix(q, "SELECT multiMatchAny(") {
			t.Errorf("probed a value list: %s", q)
		}
		return w.answerCH(q, a)
	})
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("email", "a")}}); err != nil {
		t.Fatal(err)
	}
	if got := prefilterOf(t, w, d); got != "" {
		t.Errorf("value list prefilter: %s", got)
	}
}

// On a cluster the rows live on one shard, so the prefilter reads them through
// the Distributed companion, as the dictionary's own source does. The probe reads
// every replica, since a query may land on any of them (the fake asserts it).
func TestPrefilterReadsThroughTheCompanionOnACluster(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "rmm", Kind: KindPattern, Columns: cols("re")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("re", "a")}}); err != nil {
		t.Fatal(err)
	}
	if got := prefilterOf(t, w, d); !strings.Contains(got, "`"+chDistTableName(d.ID)+"` FINAL") {
		t.Errorf("prefilter = %q", got)
	}
}

// A probe that runs between the withdrawal and the insert vouches for the old
// rows; the withdrawal after the insert has to undo that, whether or not the
// insert succeeded.
func TestInsertWithdrawsAProbeThatRanDuringIt(t *testing.T) {
	for _, fail := range []bool{false, true} {
		w := newWorld(t)
		d := mustCreate(t, w, CreateOptions{Name: "rmm", Kind: KindPattern, Columns: cols("re")})
		probed := false
		w.ch.FailExecFunc(func(stmt string) error {
			if reInsert.MatchString(stmt) && !probed {
				probed = true
				w.m.refreshPrefilter(ctx, d)
				if rec, _ := w.dict(d.ID); !rec.prefilter {
					t.Fatal("the concurrent probe did not set the prefilter")
				}
				if fail {
					return fakedb.Exception(241, "memory limit exceeded")
				}
			}
			return w.execCH(stmt)
		})
		err := w.m.batchInsertRows(ctx, d, []DictionaryRow{{Fields: fields("re", "(?U)a+b")}}, []uint64{1})
		if (err != nil) != fail {
			t.Fatalf("fail=%v: %v", fail, err)
		}
		if rec, _ := w.dict(d.ID); rec.prefilter {
			t.Errorf("fail=%v: the prefilter outlived the insert", fail)
		}
	}
}

// Compiling a large list takes tens of seconds, so the probe runs after the
// edit returns, and its verdict lands whenever it finishes.
func TestPrefilterProbeRunsInTheBackground(t *testing.T) {
	w := newWorld(t)
	var pending []func()
	w.m.background = func(f func()) { pending = append(pending, f) }
	d := mustCreate(t, w, CreateOptions{Name: "rmm", Kind: KindPattern, Columns: cols("re")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("re", "a")}}); err != nil {
		t.Fatal(err)
	}
	if len(pending) == 0 || prefilterOf(t, w, d) != "" {
		t.Fatalf("the edit waited for the probe (%d pending)", len(pending))
	}
	for _, f := range pending {
		f()
	}
	if prefilterOf(t, w, d) == "" {
		t.Error("the background probe's verdict was not recorded")
	}
}
