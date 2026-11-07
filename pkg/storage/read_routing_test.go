package storage_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

var scopeF1 = storage.FractalScope{FractalIDs: []string{"f1"}}

// recordSentIDs captures the query id of every read the fake serves.
func recordSentIDs(ch *fakedb.CH) *[]string {
	var ids []string
	ch.CtxArgs = func(ctx context.Context) []any {
		ids = append(ids, storage.SentQueryIDOf(ctx))
		return nil
	}
	return &ids
}

func assertSentScoped(t *testing.T, ids []string, want int) {
	t.Helper()
	if len(ids) != want {
		t.Fatalf("%d reads, want %d", len(ids), want)
	}
	for _, id := range ids {
		if !strings.HasSuffix(id, "~bfscope~f1,") {
			t.Errorf("read sent as %q", id)
		}
	}
}

// The raw read helpers are the ones a new caller reaches for, so they must honour a
// class and scope exactly as the map-based helpers do.
func TestRawReadsGoThroughReadConn(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	ids := recordSentIDs(ch)
	ctx := storage.UncappedReadContext(storage.ScopeContext(context.Background(), scopeF1))

	rows, err := client.QueryRows(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	var n uint64
	ch.On("SELECT count()", fakedb.Count(3))
	if err := client.QueryRow(ctx, "SELECT count()").Scan(&n); err != nil || n != 3 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
	assertSentScoped(t, *ids, 2)
}

// A read readConn refuses must fail, including through QueryRow, which has no error
// return of its own.
func TestRawReadsRefuseAnUnscopedSearch(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	ctx := storage.WorkloadContext(context.Background(), storage.QuerySearchWorkload)

	if _, err := client.QueryRows(ctx, "SELECT 1"); err == nil {
		t.Error("QueryRows ran an unscoped search")
	}
	row := client.QueryRow(ctx, "SELECT 1")
	if row.Err() == nil {
		t.Error("QueryRow reported no error")
	}
	var n uint64
	if err := row.Scan(&n); err == nil {
		t.Error("QueryRow scanned an unscoped search")
	}
	if q := ch.Queried(); len(q) != 0 {
		t.Errorf("refused reads reached ClickHouse: %q", q)
	}
}

func TestHydrateReadsGoThroughReadConn(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	ids := recordSentIDs(ch)
	ctx := storage.UncappedReadContext(storage.ScopeContext(context.Background(), scopeF1))

	if _, err := client.HydrateLogFields(ctx, "logs", []storage.LogKey{{LogID: "l1"}}, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	assertSentScoped(t, *ids, 1)
}

func TestDisplayRowReadsGoThroughReadConn(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	ids := recordSentIDs(ch)
	ctx := storage.UncappedReadContext(storage.ScopeContext(context.Background(), scopeF1))

	if _, err := client.GetLogDisplayRowsByIDs(ctx, "f1", []string{"l1"}, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	assertSentScoped(t, *ids, 1)
}
