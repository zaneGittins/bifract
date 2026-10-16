package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

func fieldsType(typ string) fakedb.CHResult { return fakedb.Strs("type", typ) }

// Reconcile is additive: the MODIFY COLUMN carries every existing hint plus the new
// ones, so a hint added through the UI is never stripped by a reconcile that does
// not list it. And it only issues the MODIFY when something is actually new.
func TestReconcileSchemaFieldsIsAdditive(t *testing.T) {
	c, f := newFakeClient()
	f.On("name = 'fields'", fieldsType("JSON(max_dynamic_paths=1024, src_ip String, `user name` String)"))

	res, err := c.ReconcileSchemaFields(context.Background(), []SchemaFieldSpec{
		{FieldName: "src_ip", IndexType: "bloom_filter"},
		{FieldName: "image", IndexType: "set"},
		{FieldName: "note", IndexType: "none"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.IndexErrors) != 0 {
		t.Errorf("index errors = %v", res.IndexErrors)
	}

	modify := f.ExecutedMatching("ALTER TABLE logs MODIFY COLUMN fields")
	if len(modify) != 1 {
		t.Fatalf("MODIFY on logs issued %d times, want 1", len(modify))
	}
	for _, want := range []string{"`src_ip` String", "`user name` String", "`image` String", "`note` String", jsonLazyTypeHintSettings} {
		if !strings.Contains(modify[0], want) {
			t.Errorf("MODIFY is missing %q:\n%s", want, modify[0])
		}
	}

	idx := f.ExecutedMatching("ADD INDEX")
	if len(idx) != 2 {
		t.Fatalf("indexes = %v, want one per indexed field", idx)
	}
	if !strings.Contains(idx[0], "idx_src_ip fields.`src_ip` TYPE bloom_filter(0.001)") ||
		!strings.Contains(idx[1], "idx_image fields.`image` TYPE set(256)") {
		t.Errorf("indexes = %v", idx)
	}
	// Single node: logs_hot is mirrored, the distributed tables do not exist.
	if len(f.ExecutedMatching("ALTER TABLE logs_hot MODIFY COLUMN")) != 1 {
		t.Error("logs_hot must mirror the hint list")
	}
	if len(f.ExecutedMatching("_distributed")) != 0 {
		t.Error("single node has no distributed tables to mirror")
	}
}

// With nothing new the reconcile must not touch logs (a no-op MODIFY still costs a
// DDL round), but it still mirrors, so a secondary table that drifted converges.
func TestReconcileSchemaFieldsNothingNew(t *testing.T) {
	c, f := newFakeClient()
	c.topo = Topology{DistributedTables: true, DDLCluster: "c1"}
	f.On("name = 'fields'", fieldsType("JSON(max_dynamic_paths=1024, src_ip String)"))

	if _, err := c.ReconcileSchemaFields(context.Background(), []SchemaFieldSpec{{FieldName: "src_ip", IndexType: "none"}}); err != nil {
		t.Fatal(err)
	}
	if got := f.ExecutedMatching("ALTER TABLE logs MODIFY"); len(got) != 0 {
		t.Errorf("unexpected MODIFY on logs: %v", got)
	}
	for _, table := range []string{"logs_hot", "logs_distributed", "logs_hot_distributed"} {
		got := f.ExecutedMatching("ALTER TABLE " + table + " ON CLUSTER 'c1' MODIFY COLUMN fields")
		if len(got) != 1 {
			t.Errorf("%s mirrored %d times, want 1", table, len(got))
		}
	}
}

// A failed index costs only that field's pruning: the type hint already applied, so
// the reconcile succeeds and reports the field.
func TestReconcileSchemaFieldsRecordsIndexFailures(t *testing.T) {
	c, f := newFakeClient()
	f.On("name = 'fields'", fieldsType("JSON(max_dynamic_paths=1024)"))
	f.FailExec("idx_bad", errors.New("code 44"))

	res, err := c.ReconcileSchemaFields(context.Background(), []SchemaFieldSpec{
		{FieldName: "bad", IndexType: "bloom_filter"},
		{FieldName: "good", IndexType: "bloom_filter"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.IndexErrors["bad"], "code 44") || res.IndexErrors["good"] != "" {
		t.Errorf("index errors = %v", res.IndexErrors)
	}
}

func TestReconcileSchemaFieldsFailures(t *testing.T) {
	c, f := newFakeClient()
	f.On("name = 'fields'", fakedb.CHResult{Err: errors.New("denied")})
	if _, err := c.ReconcileSchemaFields(context.Background(), nil); err == nil {
		t.Error("an unreadable column type must fail: reconciling blind would drop hints")
	}

	c, f = newFakeClient()
	f.On("name = 'fields'", fakedb.CHResult{Cols: []fakedb.Col{{Column: "type", Type: "String"}}, IterErr: errors.New("reset")})
	if _, err := c.ReconcileSchemaFields(context.Background(), nil); err == nil {
		t.Error("an iteration error must fail the reconcile")
	}

	c, f = newFakeClient()
	f.On("name = 'fields'", fieldsType("JSON(max_dynamic_paths=1024)"))
	f.FailExec("ALTER TABLE logs MODIFY", errors.New("code 115"))
	if _, err := c.ReconcileSchemaFields(context.Background(), []SchemaFieldSpec{{FieldName: "x"}}); err == nil {
		t.Error("a failed MODIFY on logs must fail the reconcile")
	}
	if len(f.ExecutedMatching("ADD INDEX")) != 0 {
		t.Error("no index may be added for a hint that did not apply")
	}
}

// The reconcile changes the hint list, so the detail view's cached set is dropped.
func TestReconcileSchemaFieldsInvalidatesHintCache(t *testing.T) {
	c, f := newFakeClient()
	f.On("name = 'fields'", fieldsType("JSON(max_dynamic_paths=1024)"))
	c.typeHintFieldSet(context.Background())
	c.ReconcileSchemaFields(context.Background(), nil)
	c.typeHintMu.RLock()
	defer c.typeHintMu.RUnlock()
	if c.typeHintFields != nil {
		t.Error("type hint cache survived a reconcile")
	}
}

// The reset is authoritative: the new type is exactly the requested set, and the
// indexes of discarded hints are dropped too, not only those of the kept ones.
func TestTruncateAndReschemaIsAuthoritative(t *testing.T) {
	c, f := newFakeClient()
	f.On("name = 'fields'", fieldsType("JSON(max_dynamic_paths=1024, old_hint String, kept String)"))

	err := c.TruncateAndReschema(context.Background(), []SchemaFieldSpec{
		{FieldName: "kept", IndexType: "bloom_filter"},
		{FieldName: "fresh", IndexType: "none"},
	}, []string{"0000-aaaa"})
	if err != nil {
		t.Fatal(err)
	}

	execs := f.Executed()
	firstDrop, firstTruncate := -1, -1
	for i, s := range execs {
		if firstTruncate < 0 && strings.HasPrefix(s, "TRUNCATE") {
			firstTruncate = i
		}
		if firstDrop < 0 && strings.Contains(s, "DROP INDEX") {
			firstDrop = i
		}
	}
	if firstTruncate < 0 || firstDrop < firstTruncate {
		t.Errorf("data must be cleared before the schema is rebuilt: %v", execs)
	}
	if len(f.ExecutedMatching("TRUNCATE TABLE IF EXISTS `model_0000_aaaa`")) != 1 {
		t.Error("model data tables must be truncated with the logs")
	}

	drops := f.ExecutedMatching("DROP INDEX IF EXISTS")
	for _, want := range []string{"idx_fresh", "idx_kept", "idx_old_hint"} {
		found := false
		for _, d := range drops {
			found = found || strings.HasSuffix(d, want)
		}
		if !found {
			t.Errorf("index %s not dropped: %v", want, drops)
		}
	}

	modify := f.ExecutedMatching("ALTER TABLE logs MODIFY COLUMN fields")
	if len(modify) != 1 || strings.Contains(modify[0], "old_hint") ||
		!strings.Contains(modify[0], "`kept` String") || !strings.Contains(modify[0], "`fresh` String") {
		t.Errorf("reset type = %v, want exactly the requested hints", modify)
	}
	if got := f.ExecutedMatching("ADD INDEX"); len(got) != 1 || !strings.Contains(got[0], "idx_kept") {
		t.Errorf("indexes = %v", got)
	}
}

func TestTruncateAndReschemaFailures(t *testing.T) {
	c, f := newFakeClient()
	f.On("name = 'fields'", fakedb.CHResult{Err: errors.New("denied")})
	if err := c.TruncateAndReschema(context.Background(), nil, nil); err == nil {
		t.Error("must refuse before truncating when the current hints are unreadable")
	}
	if len(f.Executed()) != 0 {
		t.Errorf("executed %v", f.Executed())
	}

	c, f = newFakeClient()
	f.FailExec("TRUNCATE", errors.New("busy"))
	if err := c.TruncateAndReschema(context.Background(), nil, nil); err == nil {
		t.Error("a failed truncate must fail the reset")
	}
	if len(f.ExecutedMatching("MODIFY COLUMN")) != 0 {
		t.Error("the schema must not be narrowed over data that was not cleared")
	}
}

func TestDropSchemaFieldIndexUsesSharedName(t *testing.T) {
	c, f := newFakeClient()
	c.topo.DDLCluster = "c1"
	if err := c.DropSchemaFieldIndex(context.Background(), "proc.cmd-line"); err != nil {
		t.Fatal(err)
	}
	want := "ALTER TABLE logs ON CLUSTER 'c1' DROP INDEX IF EXISTS " + schemaFieldIndexName("proc.cmd-line")
	if got := f.Executed()[0]; got != want {
		t.Errorf("stmt = %q, want %q", got, want)
	}
	if schemaFieldIndexName("proc.cmd-line") != "idx_proc_cmd_line" {
		t.Errorf("index name = %q", schemaFieldIndexName("proc.cmd-line"))
	}
}

func TestParseJSONTypeHints(t *testing.T) {
	got := parseJSONTypeHints("JSON(max_dynamic_paths=1024, `src ip` String, user String, user String, ts DateTime64(3, 'UTC'), SKIP x, SKIP REGEXP 'a.*', a.b UInt8)")
	want := []string{"src ip", "user", "ts", "a.b"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("hints = %q, want %q", got, want)
	}
	if got := parseJSONTypeHints(""); len(got) != 0 {
		t.Errorf("empty type = %q", got)
	}
}
