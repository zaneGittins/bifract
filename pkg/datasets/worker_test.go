package datasets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"bifract/internal/fakedb"
	"bifract/pkg/datasetfile"
	"bifract/pkg/egress"
	"bifract/pkg/sse"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

const testFractal = "22222222-2222-2222-2222-222222222222"

var testClient = egress.NewClient(egress.Options{AllowPrivate: true})

func compress(t *testing.T, text string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

const testMaxDepth = 10

type fakeQueue struct {
	mu      sync.Mutex
	refuse  int // refuse this many batches first; -1 refuses forever
	busy    int // report a full queue for this many Depth calls
	full    bool
	entries []storage.LogEntry
	events  *[]string
	// onEnqueue runs after each accepted batch.
	onEnqueue func()
}

func (q *fakeQueue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.full = q.busy > 0
	if q.full {
		q.busy--
		return testMaxDepth
	}
	return 0
}

func (q *fakeQueue) Enqueue(logs []storage.LogEntry) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.full {
		*q.events = append(*q.events, "enqueue while full")
	}
	if q.refuse != 0 {
		if q.refuse > 0 {
			q.refuse--
		}
		return false
	}
	q.entries = append(q.entries, logs...)
	*q.events = append(*q.events, "enqueue")
	if q.onEnqueue != nil {
		q.onEnqueue()
	}
	return true
}

// fakeLogs stores whatever the queue accepted, less lose.
type fakeLogs struct {
	queue  *fakeQueue
	lose   int
	events *[]string
}

func (l *fakeLogs) DeleteLogsByFractalID(_ context.Context, id string, unscoped bool) error {
	if id != testFractal || unscoped {
		return fmt.Errorf("dropped %q unscoped=%v", id, unscoped)
	}
	*l.events = append(*l.events, "drop")
	return nil
}

func (l *fakeLogs) CountFractalLogs(_ context.Context, id string) (uint64, error) {
	if id != testFractal {
		return 0, fmt.Errorf("counted %q", id)
	}
	l.queue.mu.Lock()
	defer l.queue.mu.Unlock()
	if n := len(*l.events); n == 0 || (*l.events)[n-1] != "count" {
		*l.events = append(*l.events, "count")
	}
	return uint64(max(len(l.queue.entries)-l.lose, 0)), nil
}

var (
	testStart = time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	testEnd   = time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
)

// fakeContent records what the worker does to a fractal's models, alerts and
// lookups in the same order as its logs.
type fakeContent struct {
	events   *[]string
	resetErr error
	warnings []string
	// building answers each model check in turn, then repeats the last.
	building [][]string
	// loads answers each lookup check in turn, then repeats the last.
	loads []lookupLoads
	// onScore runs as scoring starts.
	onScore func()
}

type lookupLoads struct {
	loading, failed []string
	err             error
}

func (c *fakeContent) BuildingModels(context.Context, string) ([]string, error) {
	*c.events = append(*c.events, "models built")
	if len(c.building) == 0 {
		return nil, nil
	}
	b := c.building[0]
	if len(c.building) > 1 {
		c.building = c.building[1:]
	}
	return b, nil
}

func (c *fakeContent) ResetFractal(_ context.Context, id string) error {
	if id != testFractal {
		return fmt.Errorf("reset %q", id)
	}
	*c.events = append(*c.events, "reset models")
	return c.resetErr
}

func (c *fakeContent) ClearFractalHistory(_ context.Context, id string) error {
	if id != testFractal {
		return fmt.Errorf("cleared %q", id)
	}
	*c.events = append(*c.events, "clear alerts")
	return nil
}

func (c *fakeContent) FractalSourceLoads(context.Context, string) ([]string, []string, error) {
	*c.events = append(*c.events, "lookups")
	if len(c.loads) == 0 {
		return nil, nil, nil
	}
	l := c.loads[0]
	if len(c.loads) > 1 {
		c.loads = c.loads[1:]
	}
	return l.loading, l.failed, l.err
}

func (c *fakeContent) ScoreFractal(_ context.Context, id string, start, end time.Time) []string {
	if id == testFractal && start.Equal(testStart) && end.Equal(testEnd) {
		*c.events = append(*c.events, "score")
	}
	if c.onScore != nil {
		c.onScore()
	}
	return c.warnings
}

type fakeHub struct {
	mu     sync.Mutex
	events []sse.Event
}

func (h *fakeHub) Broadcast(room string, e sse.Event, _ string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if room == Room {
		h.events = append(h.events, e)
	}
}

func exportedLines(n int) string {
	var b strings.Builder
	base := time.Date(2026, 10, 29, 14, 0, 0, 5e8, time.UTC)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"timestamp":"%s","log_id":"x%d","fractal_id":"other","fields":{"hostname":"WS0%d","event_id":"1"}}`+"\n",
			base.Add(time.Duration(i)*time.Minute).Format(time.RFC3339Nano), i, i)
	}
	return b.String()
}

type workerRig struct {
	w       *Worker
	pg      *fakedb.PG
	queue   *fakeQueue
	logs    *fakeLogs
	hub     *fakeHub
	content *fakeContent
	events  []string
}

// newRig serves file and scripts Postgres with one claimable import of it.
func newRig(t *testing.T, file []byte, sha string, logs int64) *workerRig {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(file) }))
	t.Cleanup(srv.Close)
	if sha == "" {
		sum := sha256.Sum256(file)
		sha = hex.EncodeToString(sum[:])
	}
	pgc, pg := storagetest.Postgres()
	started := time.Date(2026, 10, 30, 1, 2, 3, 0, time.UTC)
	now := started
	pg.OnOnce("UPDATE dataset_imports\n\t\tSET status = 'downloading'", fakedb.Row(int64(7)))
	pg.On("WHERE i.id = $1", fakedb.Row(int64(7), "bifract-labs-001", testFractal, "bifract-labs-001",
		srv.URL+"/d.jsonl.zst", sha, int64(len(file)), logs, StatusDownloading, int64(0), int64(0), "", "", "admin",
		now, started, nil, now, testStart, testEnd))
	pg.On("UPDATE dataset_imports SET status = $3", fakedb.Affected(1))
	r := &workerRig{pg: pg, hub: &fakeHub{}}
	r.queue = &fakeQueue{events: &r.events}
	r.logs = &fakeLogs{queue: r.queue, events: &r.events}
	r.content = &fakeContent{events: &r.events}
	r.w = NewWorker(NewStore(pgc), testClient, r.queue, r.logs,
		Content{Models: r.content, Alerts: r.content, Lookups: r.content}, r.hub, testMaxDepth, t.TempDir())
	return r
}

// finish returns the args of the recorded Finish: status, error, bytes, logs.
func (r *workerRig) finish(t *testing.T) []driver.Value {
	t.Helper()
	calls := r.pg.Matching("finished_at = NOW()")
	if len(calls) != 1 {
		t.Fatalf("%d Finish calls, want 1", len(calls))
	}
	return calls[0].Args[2:]
}

// emptied is what every attempt starts with: logs first, so nothing reading them
// can rebuild what is reset after.
const emptied = "drop,reset models,clear alerts"

func TestWorkerImports(t *testing.T) {
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	if !r.w.RunOnce(context.Background()) {
		t.Fatal("no import was claimed")
	}
	if got := r.finish(t); got[0] != StatusSucceeded || got[1] != "" || got[3] != int64(3) {
		t.Fatalf("finish = %v", got)
	}
	if strings.Join(r.events, ",") != "models built,"+emptied+",lookups,enqueue,count,score" {
		t.Errorf("events %v: the fractal must be emptied first, logs wait for lookups, and scoring for every log", r.events)
	}
	if len(r.queue.entries) != 3 {
		t.Fatalf("%d entries enqueued", len(r.queue.entries))
	}
	e := r.queue.entries[1]
	want := time.Date(2026, 10, 29, 14, 1, 0, 5e8, time.UTC)
	if e.FractalID != testFractal || !e.Timestamp.Equal(want) || e.Fields["hostname"] != "WS01" {
		t.Errorf("entry = %+v", e)
	}
	if _, has := e.Fields["log_id"]; has || strings.Contains(e.RawLog, "other") {
		t.Errorf("the exporting install's ids were kept: %s", e.RawLog)
	}
	if e.LogID != storage.GenerateLogID(want, e.RawLog) {
		t.Error("log_id is not derived from the event time and raw log")
	}
	last := r.hub.events[len(r.hub.events)-1].Data.(*Import)
	if last.Status != StatusSucceeded || last.LogsDone != 3 {
		t.Errorf("last broadcast = %+v", last)
	}
}

// A file that is not what the Exchange lists never reaches the ingest queue.
func TestWorkerRefusesMismatchedFile(t *testing.T) {
	cases := map[string]*workerRig{
		"hash":  newRig(t, compress(t, exportedLines(3)), strings.Repeat("0", 64), 3),
		"count": newRig(t, compress(t, exportedLines(3)), "", 5),
		"shape": newRig(t, compress(t, exportedLines(2)+"{\"hostname\":\"x\"}\n"), "", 3),
	}
	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			r.w.RunOnce(context.Background())
			got := r.finish(t)
			if got[0] != StatusFailed || got[1] == "" {
				t.Fatalf("finish = %v", got)
			}
			if name != "shape" && len(r.queue.entries) != 0 {
				t.Errorf("%d entries enqueued from a mismatched file", len(r.queue.entries))
			}
			// A failure empties the fractal again, after whatever was enqueued.
			if !strings.HasSuffix(strings.Join(r.events, ","), emptied) {
				t.Errorf("events %v: a failed import must empty the fractal", r.events)
			}
		})
	}
}

// Each attempt re-ingests every log, so a retry that kept model state would double
// its counts. An import that cannot empty its models must not go on.
func TestWorkerStopsWhenModelsCannotBeEmptied(t *testing.T) {
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.content.resetErr = errors.New("no space on ch-0.int")
	r.w.RunOnce(context.Background())
	got := r.finish(t)
	if got[0] != StatusFailed || strings.Contains(got[1].(string), "ch-0.int") {
		t.Fatalf("finish = %v", got)
	}
	if len(r.queue.entries) != 0 {
		t.Errorf("%d logs imported over stale model state", len(r.queue.entries))
	}
}

// A model records only what is stamped after it is built, so an import added right
// behind its models waits for them before touching anything.
func TestWorkerWaitsForModels(t *testing.T) {
	old := contentPoll
	contentPoll = time.Millisecond
	t.Cleanup(func() { contentPoll = old })
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.content.building = [][]string{{"beacons"}, {"beacons"}, nil}
	r.w.RunOnce(context.Background())
	if got := r.finish(t); got[0] != StatusSucceeded {
		t.Fatalf("finish = %v", got)
	}
	if got := strings.Join(r.events, ","); !strings.HasPrefix(got, "models built,models built,models built,drop") {
		t.Errorf("events %s", got)
	}
}

// A model stuck building must not pass as one that saw the logs.
func TestWorkerFailsOnAModelThatNeverBuilds(t *testing.T) {
	oldPoll, oldTimeout := contentPoll, contentTimeout
	contentPoll, contentTimeout = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { contentPoll, contentTimeout = oldPoll, oldTimeout })
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.content.building = [][]string{{"beacons"}}
	r.w.RunOnce(context.Background())
	got := r.finish(t)
	if got[0] != StatusFailed || !strings.Contains(got[1].(string), "beacons") {
		t.Fatalf("finish = %v", got)
	}
	if len(r.queue.entries) != 0 {
		t.Errorf("%d logs imported ahead of a model", len(r.queue.entries))
	}
}

// A model reads each log once, so logs wait for the lookups it matches them with.
func TestWorkerWaitsForLookups(t *testing.T) {
	old := contentPoll
	contentPoll = time.Millisecond
	t.Cleanup(func() { contentPoll = old })
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.content.loads = []lookupLoads{{loading: []string{"lolrmm"}}, {loading: []string{"lolrmm"}}, {}}
	r.w.RunOnce(context.Background())
	if got := r.finish(t); got[0] != StatusSucceeded {
		t.Fatalf("finish = %v", got)
	}
	if got := strings.Join(r.events, ","); !strings.Contains(got, "lookups,lookups,lookups,enqueue") {
		t.Errorf("events %s", got)
	}
}

func TestWorkerFailsOnALookup(t *testing.T) {
	oldPoll, oldTimeout := contentPoll, contentTimeout
	contentPoll, contentTimeout = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { contentPoll, contentTimeout = oldPoll, oldTimeout })
	for name, c := range map[string]struct {
		loads lookupLoads
		want  string
	}{
		"failed": {lookupLoads{failed: []string{"lolrmm: status 404"}}, "lolrmm: status 404"},
		"slow":   {lookupLoads{loading: []string{"lolrmm"}}, "still loading"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, compress(t, exportedLines(3)), "", 3)
			r.content.loads = []lookupLoads{c.loads}
			r.w.RunOnce(context.Background())
			got := r.finish(t)
			if got[0] != StatusFailed || !strings.Contains(got[1].(string), c.want) {
				t.Fatalf("finish = %v", got)
			}
			if len(r.queue.entries) != 0 {
				t.Errorf("%d logs imported without their lookup", len(r.queue.entries))
			}
		})
	}
}

// The logs are in, so a model left unscored warns rather than fails: running the
// import again would redo all of it for what Score range can do alone.
func TestWorkerWarnsOfUnscoredModels(t *testing.T) {
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.content.warnings = []string{"beacons was not scored.", "conns was not scored."}
	r.w.RunOnce(context.Background())
	got := r.finish(t)
	if got[0] != StatusSucceeded || got[4] != "beacons was not scored. conns was not scored." {
		t.Fatalf("finish = %v", got)
	}
	last := r.hub.events[len(r.hub.events)-1].Data.(*Import)
	if last.Warning == "" {
		t.Errorf("last broadcast = %+v", last)
	}
}

// Scoring keeps the claim alive like the steps before it, and a server stopping
// during it records no outcome, so the import is run again.
func TestWorkerReportsScoringAndKeepsTheClaim(t *testing.T) {
	old := reportInterval
	reportInterval = time.Millisecond
	t.Cleanup(func() { reportInterval = old })
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	ctx, cancel := context.WithCancel(context.Background())
	r.content.onScore = func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for _, c := range r.pg.Matching("SET status = $3, bytes_done") {
				if c.Args[2] == StatusScoring {
					cancel()
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
		t.Error("scoring was never reported")
		cancel()
	}
	r.w.RunOnce(ctx)
	if n := len(r.pg.Matching("finished_at = NOW()")); n != 0 {
		t.Errorf("a stopped import recorded an outcome (%d Finish calls)", n)
	}
}

// A held import waits for Start, and a stale scoring run is taken over.
func TestClaimQuery(t *testing.T) {
	pgc, pg := storagetest.Postgres()
	if imp, err := NewStore(pgc).Claim(context.Background(), time.Minute); err != nil || imp != nil {
		t.Fatalf("claim = %v, %v", imp, err)
	}
	q := pg.Matching("UPDATE dataset_imports")[0].Query
	if strings.Contains(q, "held") || !strings.Contains(q, "'scoring'") {
		t.Errorf("claim: %s", q)
	}
}

// Backpressure delays an import; it does not drop or fail it.
func TestWorkerWaitsOutBackpressure(t *testing.T) {
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.queue.refuse = 2
	r.w.RunOnce(context.Background())
	if got := r.finish(t); got[0] != StatusSucceeded || len(r.queue.entries) != 3 {
		t.Errorf("finish = %v, %d entries", got, len(r.queue.entries))
	}
}

// A stopping server records no outcome: the claim goes stale and the import is
// run again from the start by whichever server claims it next.
func TestWorkerStopLeavesClaim(t *testing.T) {
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.queue.refuse = -1
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	r.w.RunOnce(ctx)
	if n := len(r.pg.Matching("finished_at = NOW()")); n != 0 {
		t.Errorf("a stopped import recorded an outcome (%d Finish calls)", n)
	}
}

// Losing the claim (the fractal was deleted, or another server took over a stale
// claim) stops the run without writing anything more for it.
func TestWorkerStopsWhenClaimIsLost(t *testing.T) {
	old := reportInterval
	reportInterval = 20 * time.Millisecond
	t.Cleanup(func() { reportInterval = old })
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.queue.refuse = -1
	r.pg.Respond(func(query string, _ []driver.Value) (fakedb.PGResult, bool) {
		if strings.Contains(query, "SET status = $3, bytes_done") {
			return fakedb.Affected(0), true
		}
		return fakedb.PGResult{}, false
	})
	done := make(chan struct{})
	go func() { r.w.RunOnce(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the run kept going after losing its claim")
	}
	if n := len(r.pg.Matching("finished_at = NOW()")); n != 0 {
		t.Errorf("a lost import recorded an outcome (%d Finish calls)", n)
	}
	if strings.Join(r.events, ",") != "models built,"+emptied+",lookups" {
		t.Errorf("events %v: a lost claim must not empty the fractal again", r.events)
	}
}

// Queued is not stored: an import whose logs do not all land in ClickHouse fails,
// and the fractal is emptied.
func TestWorkerFailsWhenLogsAreNotStored(t *testing.T) {
	old := settleTimeout
	settleTimeout = 0
	t.Cleanup(func() { settleTimeout = old })
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.logs.lose = 1
	r.w.RunOnce(context.Background())
	if got := r.finish(t); got[0] != StatusFailed || got[1] != "only 2 of 3 logs were stored" {
		t.Fatalf("finish = %v", got)
	}
	if !strings.HasSuffix(strings.Join(r.events, ","), emptied) {
		t.Errorf("events %v: a failed import must empty the fractal", r.events)
	}
}

// A failure after some batches were queued waits for them to land before
// emptying the fractal, so none arrive after the drop.
func TestWorkerSettlesBeforeDropping(t *testing.T) {
	r := newRig(t, compress(t, exportedLines(batchRows)+"{\"hostname\":\"x\"}\n"), "", batchRows+1)
	r.w.RunOnce(context.Background())
	if got := r.finish(t); got[0] != StatusFailed {
		t.Fatalf("finish = %v", got)
	}
	if got := strings.Join(r.events, ","); got != "models built,"+emptied+",lookups,enqueue,count,"+emptied {
		t.Errorf("events %s", got)
	}
}

// Stopping takes effect between batches even while the queue keeps accepting.
func TestWorkerStopsWhileQueueAccepts(t *testing.T) {
	r := newRig(t, compress(t, exportedLines(3*batchRows)), "", 3*batchRows)
	ctx, cancel := context.WithCancel(context.Background())
	r.queue.onEnqueue = cancel
	r.w.RunOnce(ctx)
	if n := len(r.queue.entries); n != batchRows {
		t.Errorf("%d logs enqueued after the stop, want one batch of %d", n, batchRows)
	}
	if n := len(r.pg.Matching("finished_at = NOW()")); n != 0 {
		t.Errorf("a stopped import recorded an outcome (%d Finish calls)", n)
	}
}

// An import waits while the queue is deep, leaving the room to live ingest.
func TestWorkerLeavesQueueHeadroom(t *testing.T) {
	old := retryEnqueue
	retryEnqueue = time.Millisecond
	t.Cleanup(func() { retryEnqueue = old })
	r := newRig(t, compress(t, exportedLines(3)), "", 3)
	r.queue.busy = 3
	r.w.RunOnce(context.Background())
	if got := r.finish(t); got[0] != StatusSucceeded {
		t.Fatalf("finish = %v", got)
	}
	for _, e := range r.events {
		if e == "enqueue while full" {
			t.Fatalf("events %v: a batch was offered to a deep queue", r.events)
		}
	}
}

// Failures about the dataset are shown as they are; anything else is not, since
// it may carry internal detail.
func TestPublicError(t *testing.T) {
	for err, want := range map[error]string{
		failf("only 2 of 3 logs were stored"):                           "only 2 of 3 logs were stored",
		fmt.Errorf("%w: line 3 is not JSON", datasetfile.ErrFormat):     "not a dataset file: line 3 is not JSON",
		fmt.Errorf("%w: status 404", datasetfile.ErrDownload):           "download failed: status 404",
		fmt.Errorf("%w: wrong size", datasetfile.ErrChecksum):           "the downloaded file does not match the Exchange: wrong size",
		errors.New("code: 241, Memory limit exceeded on host ch-0.int"): "the import failed; the server log has the details",
	} {
		if got := publicError(err).Error(); got != want {
			t.Errorf("publicError(%v) = %q, want %q", err, got, want)
		}
	}
}
