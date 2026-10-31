package query

import (
	"context"
	"testing"

	"bifract/pkg/pgrcal"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// Severity calibration tracks the scores analysts actually see, which with
// diffusion on are the propagated survivors, not the raw scan.
func TestDiffusionFeedsCalibration(t *testing.T) {
	pgc, pg := storagetest.Postgres()
	rec := pgrcal.NewRecorder(pgc.DB())

	f := newFakeLineage(">root", "root>a")
	f.scan = func(string, storage.ExternalTable) ([]map[string]interface{}, error) {
		// The second leaf is below the threshold before and after propagation.
		return []map[string]interface{}{spawnRow("root", "a"), leafRow("a", "net:203.0.113.9", 1), leafRow("a", "dns:example.test", 0)}, nil
	}
	h := f.handler()
	h.SetPgrRecorder(rec)
	if _, _, err := h.provenanceScoreSQL(context.Background(), pgrParams(true, false), pgrOpts()); err != nil {
		t.Fatal(err)
	}

	// Stopping the recorder flushes what it observed.
	ctx, cancel := context.WithCancel(context.Background())
	rec.Start(ctx)
	cancel()
	waitFor(t, "the calibration flush", func() bool { _, _, commits, _ := pg.Stats(); return commits > 0 })
	var observed int64
	for _, c := range pg.Matching("INSERT INTO pgr_score_histogram") {
		observed += c.Args[1].(int64)
	}
	if observed != 2 {
		t.Errorf("calibration observed %d edges, want the 2 survivors", observed)
	}
}
