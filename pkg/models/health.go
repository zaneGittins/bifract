package models

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// Health states, worst first. The listing shows exactly one per model.
const (
	HealthError       = "error"        // the model failed to build
	HealthNotUpdating = "not_updating" // state cycles keep failing
	HealthNotStarted  = "not_started"  // state maintenance never took the model over
	HealthBehind      = "behind"       // state lags the logs by more than a cycle explains
	HealthRebuilding  = "rebuilding"   // ClickHouse objects are being (re)created
	HealthBackfilling = "backfilling"  // history is being seeded
	HealthStale       = "stale"        // the source stopped producing events
	HealthLearning    = "learning"     // too little history for scores to mean much
	HealthHealthy     = "healthy"
	HealthChecking    = "checking" // the state summary has not been read yet
	HealthUnknown     = "unknown"  // the state summary could not be read
)

// findingsDays is how many days the listing's findings count and sparkline cover.
const findingsDays = 7

// firstSeenLearnDays is the history a first_seen model needs before its new
// entities are findings rather than the population it is still discovering: one
// week, so weekly routines (patching, weekend jobs) are already known.
const firstSeenLearnDays = 7

// ModelHealth is the listing's verdict on a model: one state, the facts behind
// it, and what the model found recently.
type ModelHealth struct {
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`

	// History is how much the model has learned and HistoryNeeded how much it
	// needs, in HistoryUnit ("day" or "hour"). Zero needed means no learning
	// period applies.
	History       int    `json:"history"`
	HistoryNeeded int    `json:"history_needed,omitempty"`
	HistoryUnit   string `json:"history_unit,omitempty"`

	// NewestData is the newest event time the model's state holds.
	NewestData *time.Time `json:"newest_data,omitempty"`

	// Findings counts what the model surfaced over the last findingsDays days, or
	// is nil for a type with nothing to find (tlsh). FindingsSeries, when set,
	// is the per-day breakdown, oldest first and ending today (UTC).
	Findings       *uint64  `json:"findings,omitempty"`
	FindingsSeries []uint64 `json:"findings_series,omitempty"`
	FindingsBasis  string   `json:"findings_basis,omitempty"`
}

// stateSummary is what one cheap read of a model's state table says.
type stateSummary struct {
	oldest, newest time.Time // zero when the state holds nothing
	findings       *uint64
	series         []uint64
}

// healthSummarySQL returns the single query that summarizes a model's state for
// the listing, anchored at today (a UTC date). table is the model's read table
// and stateTable a network model's rolling-state read table. Every column is
// computed from the model's own aggregate tables, never from raw logs.
func healthSummarySQL(mt ModelType, def ModelDefinition, table, stateTable, fractalID string, today time.Time) string {
	fid := "fractal_id = '" + storage.EscCHStr(fractalID) + "'"
	qt := "`" + table + "`"
	epoch := func(expr string) string { return "toUInt64(toUnixTimestamp(" + expr + "))" }
	// flagged counts the Findings tab's rows: the same rule over the same scored
	// rows, so the listing and the tab cannot disagree.
	rule := findingsRuleFor(mt, def)
	flagged := func(scored string) string {
		if rule.Where == "" {
			return ""
		}
		return fmt.Sprintf(",\n    toUInt64(ifNull((SELECT count() FROM (%s) WHERE %s), 0)) AS flagged", scored, rule.Where)
	}
	fidEsc := storage.EscCHStr(fractalID)
	switch mt {
	case ModelTypeFirstSeen:
		// An entity is a finding on the day the model first recorded it, which
		// is what its alert fires on; seeded history is recorded at the epoch.
		return fmt.Sprintf(`SELECT %s AS oldest, %s AS newest, %s AS series%s
FROM (
    SELECT min(first_seen) AS fs, max(last_seen) AS ls, toDate(min(%s)) AS d
    FROM %s WHERE %s
    GROUP BY entity_key
)`, epoch("min(fs)"), epoch("max(ls)"), findingsSeriesSQL("d", today),
			flagged(firstSeenAggSQL(qt+" FINAL", fidEsc, "", "entity_key", true)), FirstRecordedColumn, qt, fid)
	case ModelTypeRarity:
		// A finding is a (partition, value) pair whose earliest day is that day.
		return fmt.Sprintf(`SELECT %s AS oldest, %s AS newest, %s AS series%s
FROM (
    SELECT min(arrayMin(finalizeAggregation(days))) AS fd, max(arrayMax(finalizeAggregation(days))) AS ld
    FROM %s WHERE %s
    GROUP BY partition_val, value_val
)`, epoch("toDateTime(min(fd), 'UTC')"), epoch("toDateTime(max(ld), 'UTC')"), findingsSeriesSQL("fd", today),
			flagged(buildRarityScoredSQL(qt+" FINAL", fidEsc)), qt, fid)
	case ModelTypeVolumeBaseline:
		// A finding is an entity whose latest complete bucket scores above the
		// alert's z threshold right now; there is no per-day history of scores.
		scored := parser.VolumeScoredSQL(qt, fid, def.TimeBucket, def.MinSample, "", false)
		return fmt.Sprintf(`SELECT %s AS oldest, %s AS newest%s
FROM %s WHERE %s`, epoch("toDateTime(min(bucket), 'UTC')"), epoch("toDateTime(max(bucket), 'UTC')"),
			flagged(scored), qt, fid)
	case ModelTypeBeacon, ModelTypeLongConnection:
		return fmt.Sprintf(`SELECT %s AS oldest, %s AS newest%s
FROM %s WHERE %s`, epoch("toDateTime(min(day), 'UTC')"), epoch("max(last_ts)"),
			flagged(fmt.Sprintf("SELECT * FROM %s FINAL WHERE %s", qt, fid)), "`"+stateTable+"`", fid)
	case ModelTypeTLSH:
		return fmt.Sprintf(`SELECT %s AS oldest, %s AS newest FROM %s WHERE %s`,
			epoch("min(first_seen)"), epoch("max(last_seen)"), qt, fid)
	}
	return ""
}

// findingsSeriesSQL counts rows whose day column equals each of the last
// findingsDays days, oldest first, as one Array(UInt64).
func findingsSeriesSQL(dayCol string, today time.Time) string {
	t := today.UTC().Format("2006-01-02")
	parts := make([]string, findingsDays)
	for i := 0; i < findingsDays; i++ {
		parts[i] = fmt.Sprintf("toUInt64(countIf(%s = toDate('%s') - %d))", dayCol, t, findingsDays-1-i)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// rarityPercentThreshold is the share threshold the model's alert uses, with the
// editor's default when none is set.
func rarityPercentThreshold(def ModelDefinition) float64 {
	if def.Alert != nil && def.Alert.PercentThreshold > 0 {
		return def.Alert.PercentThreshold
	}
	return 10
}

// learningNeed is the history a model needs before its scores mean what they
// say, derived from the model's own semantics (see docs/features/models.md):
//   - rarity: a value seen on one day can only fall below the share threshold
//     once its partition has more than 100/threshold days;
//   - volume_baseline: the min history an entity needs to be scored;
//   - first_seen: firstSeenLearnDays;
//   - beacon/long_connection: one full rolling window.
//
// tlsh is an index and never learns, so it needs 0.
func learningNeed(mt ModelType, def ModelDefinition) (int, string) {
	switch mt {
	case ModelTypeRarity:
		// The group's min history, or longer when the share threshold needs it: a
		// value seen on one day can only fall under share S after more than 100/S days.
		need := int(math.Floor(100/rarityPercentThreshold(def))) + 1
		if def.MinSample > need {
			need = def.MinSample
		}
		return need, "day"
	case ModelTypeVolumeBaseline:
		if def.TimeBucket == "hour" {
			// Min history counts past samples of an hour's slot, and a weekend hour
			// recurs only twice a week, so every slot has them after this many days.
			return 7 * ((parser.VolumeMinBuckets(def.MinSample, def.TimeBucket) + 1) / 2), "day"
		}
		return parser.VolumeMinBuckets(def.MinSample, def.TimeBucket), "day"
	case ModelTypeFirstSeen:
		return firstSeenLearnDays, "day"
	case ModelTypeBeacon, ModelTypeLongConnection:
		return def.WindowDays(), "day"
	}
	return 0, ""
}

// staleAfter is how old the newest recorded event may be before the model is
// treated as cut off from its source. An hourly volume model scores every
// entity at 0 after one empty hour, so it is held tightest; everything else
// gets two days, which a quiet weekend on a narrow filter does not trip.
func staleAfter(mt ModelType, def ModelDefinition) time.Duration {
	if mt == ModelTypeVolumeBaseline && def.TimeBucket == "hour" {
		return 3 * time.Hour
	}
	return 48 * time.Hour
}

// historySpan is how much history lies between oldest and newest in days: the
// whole days before the newest bucket for a volume model, as n_buckets counts a
// daily model's history, and whole days inclusive otherwise.
func historySpan(oldest, newest time.Time, mt ModelType) int {
	if oldest.IsZero() || newest.IsZero() || newest.Before(oldest) {
		return 0
	}
	d := newest.Sub(oldest)
	if mt == ModelTypeVolumeBaseline {
		return int(d / (24 * time.Hour))
	}
	return int(d/(24*time.Hour)) + 1
}

// deriveHealth picks the model's one state, worst first. sum is nil when the
// state has not been read (checking) and sumErr set when reading it failed.
func deriveHealth(m *Model, sum *stateSummary, sumErr error, now time.Time) *ModelHealth {
	h := &ModelHealth{}
	need, unit := learningNeed(m.ModelType, m.Definition)
	h.HistoryNeeded, h.HistoryUnit = need, unit
	if sum != nil {
		if !sum.newest.IsZero() {
			t := sum.newest
			h.NewestData = &t
		}
		h.History = historySpan(sum.oldest, sum.newest, m.ModelType)
		h.Findings = sum.findings
		h.FindingsSeries = sum.series
		h.FindingsBasis = findingsBasis(m)
	}

	switch {
	case m.Status == "error":
		h.State, h.Detail = HealthError, firstNonEmpty(m.ErrorMessage, "The model failed to build.")
		return h
	case m.Status == "rebuilding":
		h.State, h.Detail = HealthRebuilding, "Creating the model's tables. It starts recording once they exist."
		return h
	case m.Status == "active" && m.ErrorMessage != "":
		h.State, h.Detail = HealthNotUpdating, "State updates are failing: "+m.ErrorMessage
		return h
	case m.Status == "active" && m.StateLagSeconds == nil:
		h.State, h.Detail = HealthNotStarted, "State maintenance has not taken this model over; check the server logs for a handover failure."
		return h
	case m.StateBehind:
		h.State, h.Detail = HealthBehind, "State is behind the logs: it is catching up or a cycle cannot keep pace."
		return h
	case m.BackfillStatus == "running":
		h.State, h.Detail = HealthBackfilling, fmt.Sprintf("Seeding %s of history.", m.BackfillWindow)
		return h
	}

	if sumErr != nil {
		h.State, h.Detail = HealthUnknown, "Could not read the model's state."
		return h
	}
	if sum == nil {
		h.State, h.Detail = HealthChecking, "Reading the model's state."
		return h
	}

	limit := staleAfter(m.ModelType, m.Definition)
	if sum.newest.IsZero() {
		// Nothing recorded: a young model is waiting for data, an old one is not
		// matching any logs.
		if now.Sub(m.CreatedAt) > limit {
			h.State, h.Detail = HealthStale, "No events recorded. Check that the source query matches current logs."
			return h
		}
		if need > 0 {
			h.State, h.Detail = HealthLearning, "Waiting for the first matching events."
			return h
		}
		h.State, h.Detail = HealthHealthy, "Waiting for the first matching events."
		return h
	}
	if age := now.Sub(sum.newest); age > limit {
		h.State = HealthStale
		h.Detail = fmt.Sprintf("Newest recorded event is %s old (limit %s). The source may have stopped sending.",
			humanDuration(age), humanDuration(limit))
		if m.ModelType == ModelTypeVolumeBaseline {
			h.Detail += " Every entity scores against an empty bucket until it resumes."
		}
		return h
	}
	if need > 0 && h.History < need {
		h.State = HealthLearning
		h.Detail = fmt.Sprintf("%d of %d %ss of history. %s", h.History, need, unit, learningReason(m))
		return h
	}
	h.State, h.Detail = HealthHealthy, "Recording and scoring normally."
	return h
}

func learningReason(m *Model) string {
	switch m.ModelType {
	case ModelTypeRarity:
		return fmt.Sprintf("A value seen once only drops below %g%% of days after %d days.",
			rarityPercentThreshold(m.Definition), int(math.Floor(100/rarityPercentThreshold(m.Definition)))+1)
	case ModelTypeVolumeBaseline:
		return "Entities are scored once they have min history."
	case ModelTypeFirstSeen:
		return "Until then, routine entities still read as new."
	case ModelTypeBeacon, ModelTypeLongConnection:
		return "Scores firm up once the rolling window is full."
	}
	return ""
}

// findingsBasis says what the count is (the Findings tab's rule) and, where the
// model keeps one, what the sparkline plots.
func findingsBasis(m *Model) string {
	rule := findingsRuleFor(m.ModelType, m.Definition).Text
	if rule == "" {
		return ""
	}
	switch m.ModelType {
	case ModelTypeFirstSeen:
		return "Would alert: " + rule + ". Trend: entities first recorded per day."
	case ModelTypeRarity:
		return "Would alert: " + rule + ". Trend: new value pairs per day."
	}
	return "Would alert: " + rule + "."
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func humanDuration(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	case d >= time.Hour:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	default:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
}

// ---- Summary cache ----

const (
	// healthSummaryTTL is how long a summary is served before it is re-read. The
	// state advances once a cycle, so a summary younger than two cycles is current.
	healthSummaryTTL = 2 * time.Minute
	// healthWait is how long a list request waits for summaries it lacks. Those
	// that take longer finish in the background and land on the next request.
	healthWait = 2 * time.Second
	// healthConcurrency bounds summary reads across all requests.
	healthConcurrency = 4
	// healthQueryBudgetSec and healthQueryMaxMemory bound one summary read.
	healthQueryBudgetSec = 20
	healthQueryMaxMemory = 512 << 20
)

type healthEntry struct {
	version string
	at      time.Time
	sum     *stateSummary
	err     error
}

// healthCache holds the last summary per model and dedupes in-flight reads.
type healthCache struct {
	mu       sync.Mutex
	entries  map[string]healthEntry
	inflight map[string]chan struct{}
	sem      chan struct{}
}

func newHealthCache() *healthCache {
	return &healthCache{
		entries:  make(map[string]healthEntry),
		inflight: make(map[string]chan struct{}),
		sem:      make(chan struct{}, healthConcurrency),
	}
}

// healthVersion changes whenever the model's state may have been reset or
// reseeded, so a summary of the previous state is never served for it.
func healthVersion(m *Model) string {
	return fmt.Sprintf("%d|%s|%s|%d", m.UpdatedAt.UnixNano(), m.Status, m.BackfillStatus, m.BackfillDone)
}

// errNoStateTable stands in for a summary that cannot be read because the model
// has no table, so its health reads unknown rather than checking forever.
var errNoStateTable = errors.New("model has no state table")

// needsSummary is false for a model whose tables may not exist yet.
func needsSummary(m *Model) bool {
	return m.Status == "active" && m.CHTableName != ""
}

// AttachHealth sets Health on every model. Summaries are cached per model and
// read with a bounded pool; a request waits up to healthWait for any it lacks
// and reports the rest as checking.
func (m *Manager) AttachHealth(ctx context.Context, list []*Model) {
	now := time.Now().UTC()
	var waits []chan struct{}
	for _, mo := range list {
		if !needsSummary(mo) {
			continue
		}
		if done := m.health.ensure(m, mo, now); done != nil {
			waits = append(waits, done)
		}
	}
	if len(waits) > 0 {
		timer := time.NewTimer(healthWait)
		defer timer.Stop()
	wait:
		for _, ch := range waits {
			select {
			case <-ch:
			case <-timer.C:
				break wait
			case <-ctx.Done():
				break wait
			}
		}
	}
	for _, mo := range list {
		var sum *stateSummary
		err := errNoStateTable
		if needsSummary(mo) {
			sum, err = m.health.get(mo)
		}
		mo.Health = deriveHealth(mo, sum, err, now)
	}
}

func (c *healthCache) get(mo *Model) (*stateSummary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[mo.ID]
	if !ok || e.version != healthVersion(mo) {
		return nil, nil
	}
	return e.sum, e.err
}

// ensure starts a summary read for mo unless a current one is cached, and
// returns the channel that closes when the read in flight finishes, or nil.
func (c *healthCache) ensure(m *Manager, mo *Model, now time.Time) chan struct{} {
	version := healthVersion(mo)
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[mo.ID]; ok && e.version == version && now.Sub(e.at) < healthSummaryTTL {
		return nil
	}
	if ch, ok := c.inflight[mo.ID]; ok {
		return ch
	}
	ch := make(chan struct{})
	c.inflight[mo.ID] = ch
	model := *mo
	go func() {
		defer close(ch)
		// Detached from the request: a slow read still fills the cache for the
		// next listing instead of being thrown away.
		rctx, cancel := context.WithTimeout(context.Background(), time.Duration(healthQueryBudgetSec+5)*time.Second)
		defer cancel()
		c.sem <- struct{}{}
		sum, err := m.readStateSummary(rctx, &model, now)
		<-c.sem
		if err != nil {
			log.Printf("[Models] health summary %s: %v", model.ID, err)
		}
		c.mu.Lock()
		c.entries[model.ID] = healthEntry{version: version, at: time.Now().UTC(), sum: sum, err: err}
		delete(c.inflight, model.ID)
		c.mu.Unlock()
	}()
	return ch
}

// forget drops a deleted model's summary.
func (c *healthCache) forget(id string) {
	c.mu.Lock()
	delete(c.entries, id)
	c.mu.Unlock()
}

func (m *Manager) readStateSummary(ctx context.Context, mo *Model, now time.Time) (*stateSummary, error) {
	stateTable := ""
	if mo.ModelType.IsScheduled() {
		stateTable = m.networkStateReadTable(mo.ID)
	}
	q := healthSummarySQL(mo.ModelType, mo.Definition, m.readTableName(mo), stateTable, mo.FractalID, now)
	if q == "" {
		return nil, fmt.Errorf("no summary for model type %s", mo.ModelType)
	}
	rows, err := m.ch.QueryLowPriorityBounded(storage.QueryBudgetContext(ctx, healthQueryBudgetSec), q, healthQueryMaxMemory)
	if err != nil {
		return nil, err
	}
	return parseStateSummary(mo.ModelType, rows), nil
}

// parseStateSummary reads the row healthSummarySQL returns.
func parseStateSummary(mt ModelType, rows []map[string]interface{}) *stateSummary {
	s := &stateSummary{}
	if len(rows) == 0 {
		return s
	}
	r := rows[0]
	toTime := func(v interface{}) time.Time {
		sec := numToUint64(v)
		// An empty table aggregates to the epoch.
		if sec < 86400 {
			return time.Time{}
		}
		return time.Unix(int64(sec), 0).UTC()
	}
	s.oldest, s.newest = toTime(r["oldest"]), toTime(r["newest"])
	if series, ok := r["series"].([]uint64); ok && len(series) == findingsDays {
		s.series = series
	}
	if v, ok := r["flagged"]; ok {
		n := numToUint64(v)
		s.findings = &n
	}
	return s
}
