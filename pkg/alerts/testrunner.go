package alerts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"bifract/pkg/parser"
	"bifract/pkg/ruleeval"
	"bifract/pkg/storage"
)

// Test sessions are cheap to keep and expensive to rebuild, so an editor holds one
// while it iterates and it is swept once the editor goes quiet.
const (
	sessionIdleTimeout = 15 * time.Minute
	sessionSweepEvery  = 2 * time.Minute
	maxOpenSessions    = 64
)

// TestOutcome is the result of one test.
type TestOutcome struct {
	Name        string `json:"name"`
	Expectation string `json:"expectation"`
	Passed      bool   `json:"passed"`
	Matched     bool   `json:"matched"`
	Rows        int    `json:"rows"`
	Reason      string `json:"reason,omitempty"`
	DurationMs  int64  `json:"duration_ms"`
}

// TestRunResult is a whole run.
type TestRunResult struct {
	SessionID string        `json:"session_id"`
	Outcomes  []TestOutcome `json:"outcomes"`
	Passed    int           `json:"passed"`
	Failed    int           `json:"failed"`
	Error     string        `json:"error,omitempty"`
	RanAt     string        `json:"ran_at,omitempty"`
}

// OK reports whether every test passed.
func (r *TestRunResult) OK() bool { return r.Failed == 0 && r.Error == "" }

// testSession is a loaded corpus: a scratch table holding one editor's events, kept
// between runs so iterating on a query costs one SELECT rather than a reload.
type testSession struct {
	scratch  *ruleeval.Scratch
	units    []ruleeval.Unit
	window   ruleeval.Window
	corpus   string // hash of the events currently loaded
	lastUsed time.Time
}

// TestRunner evaluates alert tests against the deployment's own ClickHouse.
//
// Events go to a private clone of the logs table, never to `logs`: no materialized
// view fires, nothing reaches the archive or cold tiering, and cleanup is a DROP.
// Each test gets a synthetic fractal ID registered nowhere, so one test's events are
// invisible to another's query and to every real fractal.
type TestRunner struct {
	ch *storage.ClickHouseClient

	mu       sync.Mutex
	sessions map[string]*testSession
	stop     chan struct{}

	// pinned is the single node every scratch table lives on. A scratch table is a
	// local table, so a load-balanced connection could create it on one host and then
	// query another, which reads as an empty table and passes every "should not
	// match" test for the wrong reason. Nil on a single-node deployment, where the
	// shared client is already pinned by definition.
	pinned     *storage.ClickHouseClient
	pinnedAddr string
}

// NewTestRunner starts a runner and its idle-session sweeper.
func NewTestRunner(ch *storage.ClickHouseClient) *TestRunner {
	r := &TestRunner{
		ch:       ch,
		sessions: make(map[string]*testSession),
		stop:     make(chan struct{}),
	}
	go r.sweep()
	return r
}

// Available reports whether tests can run at all.
func (r *TestRunner) Available() bool { return r != nil && r.ch != nil }

// Close releases every session's scratch table and the pinned connection.
func (r *TestRunner) Close() {
	if r == nil {
		return
	}
	close(r.stop)

	r.mu.Lock()
	sessions := r.sessions
	r.sessions = make(map[string]*testSession)
	pinned := r.pinned
	r.pinned, r.pinnedAddr = nil, ""
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range sessions {
		_ = s.scratch.Drop(ctx)
	}
	if pinned != nil {
		pinned.Close()
	}
}

// newScratch creates a scratch table on the node every test session shares.
//
// On a multi-node deployment the shared client load balances, so all three statements a
// run needs (create, insert, read back) must go through one pinned connection. Nodes
// are tried in order, so one unreachable host does not take tests down with it.
func (r *TestRunner) newScratch(ctx context.Context) (*ruleeval.Scratch, error) {
	addrs := r.ch.Addrs()
	if len(addrs) <= 1 {
		return ruleeval.NewScratch(ctx, r.ch)
	}

	r.mu.Lock()
	pinned, pinnedAddr := r.pinned, r.pinnedAddr
	r.mu.Unlock()

	if pinned != nil {
		scratch, err := ruleeval.NewScratch(ctx, pinned)
		if err == nil {
			return scratch, nil
		}
		// The pinned node went away. Drop it and pick another; sessions still holding
		// scratch tables there are already lost, and the sweeper will fail to drop
		// them harmlessly.
		log.Printf("[AlertTests] pinned ClickHouse node %s unusable, repinning: %v", pinnedAddr, err)
		r.unpin(pinned)
	}

	var lastErr error
	for _, addr := range addrs {
		if addr == pinnedAddr {
			continue
		}
		client, err := r.ch.PinnedNodeClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		scratch, err := ruleeval.NewScratch(ctx, client)
		if err != nil {
			client.Close()
			lastErr = err
			continue
		}

		r.mu.Lock()
		previous := r.pinned
		r.pinned, r.pinnedAddr = client, addr
		r.mu.Unlock()
		if previous != nil && previous != client {
			previous.Close()
		}
		return scratch, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no ClickHouse node available")
	}
	return nil, fmt.Errorf("preparing test storage: %w", lastErr)
}

// unpin forgets the current pinned node, if it is still the one given, and discards
// every session with it: their scratch tables lived on that node, so reporting test
// failures from them would be misleading. They rebuild on the next run.
func (r *TestRunner) unpin(client *storage.ClickHouseClient) {
	r.mu.Lock()
	drop := r.pinned == client
	if drop {
		r.pinned, r.pinnedAddr = nil, ""
		r.sessions = make(map[string]*testSession)
	}
	r.mu.Unlock()

	if drop {
		client.Close()
	}
}

// Release drops one session's scratch table, called when an editor closes.
func (r *TestRunner) Release(ctx context.Context, sessionID string) {
	if r == nil || sessionID == "" {
		return
	}
	r.mu.Lock()
	s := r.sessions[sessionID]
	delete(r.sessions, sessionID)
	r.mu.Unlock()

	if s != nil {
		if err := s.scratch.Drop(ctx); err != nil {
			log.Printf("[AlertTests] %v", err)
		}
	}
}

func (r *TestRunner) sweep() {
	ticker := time.NewTicker(sessionSweepEvery)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-sessionIdleTimeout)

			r.mu.Lock()
			var stale []*testSession
			for id, s := range r.sessions {
				if s.lastUsed.Before(cutoff) {
					stale = append(stale, s)
					delete(r.sessions, id)
				}
			}
			r.mu.Unlock()

			for _, s := range stale {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := s.scratch.Drop(ctx); err != nil {
					log.Printf("[AlertTests] sweeping idle session: %v", err)
				}
				cancel()
			}
		}
	}
}

// Run evaluates every test against the given BQL.
//
// sessionID identifies one editor. Its corpus is loaded once and reused until the
// events change, so editing a query and re-running costs one query per test.
func (r *TestRunner) Run(ctx context.Context, sessionID, bql string, tests []AlertTest) (*TestRunResult, error) {
	if !r.Available() {
		return nil, fmt.Errorf("test runs need a ClickHouse connection")
	}
	if err := ValidateTests(tests); err != nil {
		return nil, err
	}

	result := &TestRunResult{SessionID: sessionID, Outcomes: []TestOutcome{}}
	if len(tests) == 0 {
		return result, nil
	}

	pipeline, err := parser.ParseQuery(bql)
	if err != nil {
		return nil, fmt.Errorf("invalid query syntax: %w", err)
	}

	session, err := r.session(ctx, sessionID, tests)
	if err != nil {
		return nil, err
	}

	for i := range tests {
		started := time.Now()
		outcome := TestOutcome{Name: tests[i].Name, Expectation: tests[i].Expectation}

		rows, _, err := session.scratch.Evaluate(ctx, pipeline, session.units[i], session.window)
		if err != nil {
			outcome.Reason = err.Error()
		} else {
			outcome.Rows = rows
			outcome.Matched = rows > 0
			outcome.Passed, outcome.Reason = ruleeval.Verdict(
				ruleeval.Expectation(tests[i].Expectation), outcome.Matched, rows, nil)
		}

		outcome.DurationMs = time.Since(started).Milliseconds()
		if outcome.Passed {
			result.Passed++
		} else {
			result.Failed++
		}
		result.Outcomes = append(result.Outcomes, outcome)
	}

	return result, nil
}

// RunOnce evaluates tests without leaving a session behind, for the save path where
// there is no editor to keep one warm.
func (r *TestRunner) RunOnce(ctx context.Context, bql string, tests []AlertTest) (*TestRunResult, error) {
	sessionID := "once:" + uuid.NewString()
	defer func() {
		// Not the caller's context: a cancelled request would abandon the DROP, and
		// Release has already removed the session so the sweeper can no longer find it.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		r.Release(cleanup, sessionID)
	}()
	return r.Run(ctx, sessionID, bql, tests)
}

// session returns a loaded session for these events, rebuilding it when the corpus
// changed and creating one when the editor has none.
func (r *TestRunner) session(ctx context.Context, sessionID string, tests []AlertTest) (*testSession, error) {
	hash, err := corpusHash(tests)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	existing := r.sessions[sessionID]
	if existing != nil && existing.corpus == hash {
		existing.lastUsed = time.Now()
		r.mu.Unlock()
		return existing, nil
	}
	overCapacity := existing == nil && len(r.sessions) >= maxOpenSessions
	r.mu.Unlock()

	if overCapacity {
		return nil, fmt.Errorf("too many editors are running tests right now, try again shortly")
	}

	// Build the units before touching ClickHouse, so a malformed event fails fast.
	units := make([]ruleeval.Unit, 0, len(tests))
	for i := range tests {
		// A test's events are always presented together: for an event alert each
		// event still stands alone, and for a compound or scheduled alert the whole
		// test is the scenario that must correlate.
		unit, err := ruleeval.NewUnit(tests[i].Name, tests[i].Events, nil)
		if err != nil {
			return nil, fmt.Errorf("test %q: %w", tests[i].Name, err)
		}
		units = append(units, unit)
	}

	scratch, err := r.newScratch(ctx)
	if err != nil {
		return nil, err
	}

	loaded := &testSession{
		scratch:  scratch,
		units:    units,
		window:   ruleeval.NewWindow(time.Now()),
		corpus:   hash,
		lastUsed: time.Now(),
	}
	if err := scratch.Insert(ctx, units); err != nil {
		_ = scratch.Drop(ctx)
		return nil, err
	}
	if err := scratch.WaitVisible(ctx, units, loaded.window); err != nil {
		_ = scratch.Drop(ctx)
		return nil, err
	}

	r.mu.Lock()
	previous := r.sessions[sessionID]
	r.sessions[sessionID] = loaded
	r.mu.Unlock()

	if previous != nil {
		if err := previous.scratch.Drop(ctx); err != nil {
			log.Printf("[AlertTests] replacing session corpus: %v", err)
		}
	}
	return loaded, nil
}

// corpusHash identifies the events currently loaded, so anything that does not change
// them reuses the scratch table and only an event edit rebuilds it.
//
// Names are excluded: renaming a test changes nothing about what is inserted, and
// hashing it would make a rename pay for a full reload. Order and length are included,
// since Run pairs each test with its unit positionally.
func corpusHash(tests []AlertTest) (string, error) {
	h := sha256.New()
	for i := range tests {
		raw, err := json.Marshal(tests[i].Events)
		if err != nil {
			return "", fmt.Errorf("test %q: %w", tests[i].Name, err)
		}
		fmt.Fprintf(h, "%d\x00", len(raw))
		h.Write(raw)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
