package dictionaries

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
)

func fields(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func seqsOf(rows []tblRow) []uint64 {
	out := make([]uint64, len(rows))
	for i, r := range rows {
		out[i] = r.seq
	}
	return out
}

// New rows append after the highest ordinal in the table, in the order given;
// editing a row keeps its position; renaming its key keeps the position and
// removes the old key. Every write reloads each of the list's dictionaries and
// refreshes the stored row count.
func TestUpsertRowsKeepsEditorOrder(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		w := newWorld(t)
		if cluster {
			w = newClusterWorld(t)
		}
		d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host", "owner")})
		if _, err := w.m.SetColumnKey(ctx, d.ID, "owner"); err != nil {
			t.Fatal(err)
		}
		// Ordinals already in the table come from an earlier session.
		w.mu.Lock()
		w.tables[d.CHTableName].rows = append(w.tables[d.CHTableName].rows,
			row(7, "host", "web1", "owner", "ops"), row(9, "host", "db1", "owner", "dba"))
		w.mu.Unlock()

		err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{
			{Fields: fields("host", "zz-new", "owner", "x")},
			{Key: "web1", Fields: fields("host", "web1", "owner", "web-team")},
			{Fields: fields("host", "aa-new", "owner", "y")},
		})
		if err != nil {
			t.Fatal(err)
		}
		live := w.liveRows(d.ID)
		if got := keysOf(live, "host"); !slices.Equal(got, []string{"web1", "db1", "zz-new", "aa-new"}) {
			t.Errorf("cluster=%v order %v", cluster, got)
		}
		if got := seqsOf(live); !slices.Equal(got, []uint64{7, 9, 10, 11}) {
			t.Errorf("cluster=%v ordinals %v", cluster, got)
		}
		if live[0].vals["owner"] != "web-team" {
			t.Errorf("edit lost: %v", live[0].vals)
		}

		// Renaming db1 keeps its place and leaves no copy under the old key.
		if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Key: "db1", Fields: fields("host", "db2", "owner", "dba")}}); err != nil {
			t.Fatal(err)
		}
		live = w.liveRows(d.ID)
		if got := keysOf(live, "host"); !slices.Equal(got, []string{"web1", "db2", "zz-new", "aa-new"}) {
			t.Errorf("cluster=%v after rename %v (ordinals %v)", cluster, got, seqsOf(live))
		}

		if !slices.Contains(w.reloads, d.CHDictName) || !slices.Contains(w.reloads, chColDictName(d.ID, "owner")) {
			t.Errorf("reloads %v", w.reloads)
		}
		if rec, _ := w.dict(d.ID); rec.rowCount != 4 {
			t.Errorf("stored row count %d", rec.rowCount)
		}
	}
}

// Every rule a row must meet is checked before anything is written.
func TestUpsertRowsRefusals(t *testing.T) {
	w := newWorld(t)
	nets := mustCreate(t, w, CreateOptions{Name: "nets", Kind: KindNetwork, Columns: cols("cidr")})
	execs := len(w.ch.Executed())
	for _, rows := range [][]DictionaryRow{
		{{Fields: fields("cidr", "")}},
		{{Fields: fields("cidr", "10.0.0.1")}},
	} {
		if err := w.m.UpsertRows(ctx, nets.ID, rows); !IsInvalidInput(err) {
			t.Errorf("%v: %v", rows, err)
		}
	}
	if len(w.ch.Executed()) != execs {
		t.Error("a refused row reached ClickHouse")
	}
	if err := w.m.UpsertRows(ctx, nets.ID, nil); err != nil {
		t.Errorf("no rows: %v", err)
	}

	sourced := w.seed(dictRec{name: "feed", fractalID: fractalA, keyCol: "k", cols: []byte(`[{"name":"k"}]`),
		source: []byte(`{"url":"https://example.com/a.csv","key_field":"k"}`)})
	if err := w.m.UpsertRows(ctx, sourced.id, []DictionaryRow{{Fields: fields("k", "v")}}); !IsInvalidInput(err) {
		t.Errorf("hand edit of a sourced list: %v", err)
	}
	if err := w.m.DeleteRow(ctx, sourced.id, "v"); !IsInvalidInput(err) {
		t.Errorf("row delete on a sourced list: %v", err)
	}
	if _, err := w.m.ImportCSV(ctx, sourced.id, strings.NewReader("k\nv\n"), ImportOptions{}); !IsInvalidInput(err) {
		t.Errorf("import into a sourced list: %v", err)
	}
	if _, err := w.m.AddColumn(ctx, sourced.id, "x"); !IsInvalidInput(err) {
		t.Errorf("add column on a sourced list: %v", err)
	}
	if _, err := w.m.RemoveColumn(ctx, sourced.id, "x"); !IsInvalidInput(err) {
		t.Errorf("remove column on a sourced list: %v", err)
	}
}

// A write that fails is reported and leaves the table as it was.
func TestUpsertRowsFailure(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host")})
	w.ch.FailExec("INSERT INTO", errors.New("too many parts"))
	err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "a")}})
	if err == nil || !strings.Contains(err.Error(), "too many parts") {
		t.Errorf("err = %v", err)
	}
	if len(w.liveRows(d.ID)) != 0 {
		t.Error("rows landed")
	}
}

// Deleting a row mutates the local table (ON CLUSTER and synchronous on a
// cluster, both checked by the fake), and a key that is not there is a 400.
func TestDeleteRow(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "a'b")}, {Fields: fields("host", "c")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.m.DeleteRow(ctx, d.ID, "missing"); !IsInvalidInput(err) {
		t.Errorf("missing key: %v", err)
	}
	if err := w.m.DeleteRow(ctx, d.ID, "a'b"); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"c"}) {
		t.Errorf("rows %v", got)
	}
	if rec, _ := w.dict(d.ID); rec.rowCount != 1 {
		t.Errorf("row count %d", rec.rowCount)
	}
}

// The editor pages through rows in ordinal order; a search matches any column
// ignoring case and the total counts what matches, not the page.
func TestGetRowsPagingAndSearch(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host", "owner")})
	var rows []DictionaryRow
	for _, h := range []string{"web1", "web2", "db1", "WEB3", "cache"} {
		rows = append(rows, DictionaryRow{Fields: fields("host", h, "owner", "team-"+h)})
	}
	if err := w.m.UpsertRows(ctx, d.ID, rows); err != nil {
		t.Fatal(err)
	}

	page, total, err := w.m.GetRows(ctx, d.ID, "", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(page) != 2 || page[0].Key != "web2" || page[1].Key != "db1" {
		t.Errorf("page %+v total %d", page, total)
	}
	if _, ok := page[0].Fields[seqColumn]; ok {
		t.Error("the ordinal leaked into the editor")
	}

	page, total, err = w.m.GetRows(ctx, d.ID, "web", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, r := range page {
		keys = append(keys, r.Key)
	}
	if total != 3 || !slices.Equal(keys, []string{"web1", "web2", "WEB3"}) {
		t.Errorf("search: %v total %d", keys, total)
	}
}

// An import adds the columns the file names, numbers its rows after the ones
// already loaded, and (unless deferred) reloads and recounts once.
func TestImportCSV(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "existing")}}); err != nil {
		t.Fatal(err)
	}
	reloads := len(w.reloads)

	n, err := w.m.ImportCSV(ctx, d.ID, strings.NewReader("host,owner\nb,ops\n,skipped\na,dev\n"), ImportOptions{DeferReload: true})
	if err != nil || n != 2 {
		t.Fatalf("import: %d %v", n, err)
	}
	if !slices.Equal(colNames(w.columnsOf(d.ID)), []string{"host", "owner"}) {
		t.Errorf("columns %v", w.columnsOf(d.ID))
	}
	live := w.liveRows(d.ID)
	if got := keysOf(live, "host"); !slices.Equal(got, []string{"existing", "b", "a"}) {
		t.Errorf("order %v", got)
	}
	if live[1].vals["owner"] != "ops" {
		t.Errorf("imported row %v", live[1].vals)
	}
	if len(w.reloads) != reloads {
		t.Error("a deferred import reloaded")
	}
	if rec, _ := w.dict(d.ID); rec.rowCount != 1 {
		t.Errorf("a deferred import recounted: %d", rec.rowCount)
	}

	// ReloadDictionary is how a deferred series finishes.
	if err := w.m.ReloadDictionary(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if rec, _ := w.dict(d.ID); rec.rowCount != 3 || len(w.reloads) == reloads {
		t.Errorf("finalize: count %d reloads %d", rec.rowCount, len(w.reloads)-reloads)
	}

	if _, err := w.m.ImportCSV(ctx, d.ID, strings.NewReader("host\nc\n"), ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	if rec, _ := w.dict(d.ID); rec.rowCount != 4 {
		t.Errorf("an immediate import did not recount: %d", rec.rowCount)
	}
}

// A file that breaks the list's key rule stops at the batch that carries it;
// earlier batches stay, and the count says how many.
func TestImportCSVKeyRule(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "nets", Kind: KindNetwork, Columns: cols("cidr")})
	n, err := w.m.ImportCSV(ctx, d.ID, strings.NewReader("cidr\n10.0.0.0/8\nnot-a-range\n"), ImportOptions{})
	if !IsInvalidInput(err) || n != 0 {
		t.Errorf("import: %d %v", n, err)
	}
	if len(w.liveRows(d.ID)) != 0 {
		t.Error("the refused batch landed")
	}

	if _, err := w.m.ImportCSV(ctx, d.ID, strings.NewReader("cidr,bad name\n"), ImportOptions{}); !IsInvalidInput(err) {
		t.Error("an invalid header became a column")
	}

	wide := "h0"
	for i := 1; i <= maxImportColumns; i++ {
		wide += ",h" + string(rune('a'+i%26)) + strings.Repeat("x", i/26)
	}
	if _, err := w.m.ImportCSV(ctx, d.ID, strings.NewReader(wide+"\n"), ImportOptions{}); !IsInvalidInput(err) {
		t.Errorf("a file past the column cap: %v", err)
	}
}

// An export is the editor's view as CSV: header in column order, rows in
// ordinal order, superseded copies collapsed.
func TestExportCSV(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host", "owner")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{
		{Fields: fields("host", "b", "owner", "1")},
		{Fields: fields("host", "a", "owner", "2, with comma")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Key: "b", Fields: fields("host", "b", "owner", "edited")}}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := w.m.ExportCSV(ctx, d.ID, &buf); err != nil {
		t.Fatal(err)
	}
	if want := "host,owner\nb,edited\na,\"2, with comma\"\n"; buf.String() != want {
		t.Errorf("export:\n%s\nwant:\n%s", buf.String(), want)
	}

	empty := mustCreate(t, w, CreateOptions{Name: "empty"})
	buf.Reset()
	if err := w.m.ExportCSV(ctx, empty.ID, &buf); err != nil || buf.Len() != 0 {
		t.Errorf("empty list exported %q, %v", buf.String(), err)
	}
}

// GetKeys returns the distinct non-empty keys, up to the limit.
func TestGetKeys(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "needles", Columns: cols("hash")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("hash", "t1")}, {Fields: fields("hash", "t2")}, {Fields: fields("hash", "t3")}}); err != nil {
		t.Fatal(err)
	}
	keys, err := w.m.GetKeys(ctx, d.ID, 2)
	if err != nil || len(keys) != 2 {
		t.Errorf("keys %v %v", keys, err)
	}
	keys, _ = w.m.GetKeys(ctx, d.ID, 0)
	if len(keys) != 3 {
		t.Errorf("unlimited keys %v", keys)
	}
	empty := mustCreate(t, w, CreateOptions{Name: "empty"})
	if _, err := w.m.GetKeys(ctx, empty.ID, 0); err == nil {
		t.Error("a list with no key column returned keys")
	}
}
