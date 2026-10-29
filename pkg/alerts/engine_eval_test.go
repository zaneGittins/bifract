package alerts

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"

	"bifract/pkg/parser"
)

// The alert pipeline reads the throttle field and any {{field}} in the name from each
// result row, so both must survive auto-projection.
func TestCollectAlertExtraFields(t *testing.T) {
	cases := []struct {
		alert Alert
		want  string
	}{
		{Alert{Name: "plain"}, ""},
		{Alert{Name: "plain", ThrottleField: "host"}, "host"},
		{Alert{Name: "Login by {{user}} on {{host}}", ThrottleField: "host"}, "host,user,host"},
		{Alert{Name: "{{ spaced }} and {{not-a-word}}"}, ""},
	}
	for _, tc := range cases {
		if got := strings.Join(collectAlertExtraFields(&tc.alert), ","); got != tc.want {
			t.Errorf("%q/%q: got %q, want %q", tc.alert.Name, tc.alert.ThrottleField, got, tc.want)
		}
	}
}

func TestBuildQueryOptsScoping(t *testing.T) {
	from := time.Now().Add(-10 * time.Minute)
	to := from.Add(5 * time.Minute)
	e := &Engine{}

	alert := &Alert{ID: "a", FractalID: "f1", QueryString: `status=500`, MaxEventLagSeconds: 90, ThrottleField: "host"}
	opts, err := e.buildQueryOpts(context.Background(), alert, from, to, nil)
	if err != nil {
		t.Fatal(err)
	}
	if opts.FractalID != "f1" || len(opts.FractalIDs) != 0 {
		t.Errorf("a fractal alert reads its own fractal: %q %v", opts.FractalID, opts.FractalIDs)
	}
	// Alerts are cursor-driven off ingest_timestamp, never event time: a late log must
	// still be seen by the window it arrived in.
	if !opts.UseIngestTimestamp || !opts.StartTime.Equal(from) || !opts.EndTime.Equal(to) {
		t.Errorf("window: ingest=%v %v..%v", opts.UseIngestTimestamp, opts.StartTime, opts.EndTime)
	}
	if opts.MaxEventLagSeconds != 90 || opts.MaxRows != 10000 || opts.TableName != "logs" {
		t.Errorf("lag=%d rows=%d table=%q", opts.MaxEventLagSeconds, opts.MaxRows, opts.TableName)
	}
	if strings.Join(opts.AlertExtraFields, ",") != "host" {
		t.Errorf("extra fields: %v", opts.AlertExtraFields)
	}

	// A prism alert resolves its members, here from the cycle cache so no lookup runs.
	cache := &prismResolveCache{entries: map[string][]string{"p1": {"f1", "f2"}}}
	prism := &Alert{ID: "b", PrismID: "p1", QueryString: `status=500`}
	opts, err = e.buildQueryOpts(context.Background(), prism, from, to, cache)
	if err != nil {
		t.Fatal(err)
	}
	if opts.FractalID != "" || strings.Join(opts.FractalIDs, ",") != "f1,f2" {
		t.Errorf("a prism alert reads its members: %q %v", opts.FractalID, opts.FractalIDs)
	}
}

func TestResolvePrismFractalIDsUsesTheCycleCache(t *testing.T) {
	cache := &prismResolveCache{entries: map[string][]string{"p1": {"f9"}}}
	ids, err := (&Engine{}).resolvePrismFractalIDs(context.Background(), "p1", cache)
	if err != nil || len(ids) != 1 || ids[0] != "f9" {
		t.Errorf("got %v %v", ids, err)
	}
}

func TestIsUnrecoverableChError(t *testing.T) {
	for _, code := range []int32{6, 27, 34, 36, 42, 43, 46, 47, 53, 62, 63, 70, 72} {
		got, ok := isUnrecoverableChError(fmt.Errorf("wrapped: %w", chErr(code, "x")))
		if !ok || got != code {
			t.Errorf("code %d: got (%d, %v), want unrecoverable", code, got, ok)
		}
	}
	// Memory, timeouts and network trouble pass on their own; disabling the alert for
	// them would silence a correct detection.
	for _, code := range []int32{159, 241, 202, 209, 210} {
		if _, ok := isUnrecoverableChError(chErr(code, "x")); ok {
			t.Errorf("code %d is transient and must not disable an alert", code)
		}
	}
	if _, ok := isUnrecoverableChError(errors.New("connection refused")); ok {
		t.Error("a non-ClickHouse error is never a query bug")
	}
}

func TestModelNamesIn(t *testing.T) {
	cases := map[string]string{
		`status=500`: "",
		`* | model_lookup(model="beacon", key=[src_ip, dst_ip])`:                                                     "beacon",
		`* | model_lookup(model="a", key=[x]) | model_lookup(model="b", key=[y]) | model_lookup(model="a", key=[z])`: "a,b",
		`message="model_lookup(model=decoy)"`:                                                                        "",
		`* | NOT modelLookup(model="seen", key=[user])`:                                                              "seen",
		`* | modelLookup(model="c", key=[x]) OR NOT modelLookup(model="d", key=[y])`:                                 "c,d",
		`* | chain(user) { NOT modelLookup(model="e", key=[user]); x=1 }`:                                            "e",
		`* | case { NOT modelLookup(model="f", key=[user]) | y:=1; * }`:                                              "f",
		`* | join(user) { * | modelLookup(model="g", key=[user]) | groupby(user) }`:                                  "g",
		`* | groupby(`: "",
	}
	for q, want := range cases {
		if got := strings.Join(modelNamesIn(q), ","); got != want {
			t.Errorf("%s: got %q, want %q", q, got, want)
		}
	}
}

// A query that consults no model is never held back, and never asks for a watermark.
func TestModelWatermarkWithoutModels(t *testing.T) {
	to := time.Now()
	if got := (&Engine{}).modelWatermark(context.Background(), &Alert{QueryString: `status=500`}, to); !got.Equal(to) {
		t.Errorf("got %v, want %v", got, to)
	}
}

// Each evaluator returns early, before touching any store, when its window is not
// ready. These engines have no stores: running a query would panic.
func TestEvaluatorsWaitForACompleteWindow(t *testing.T) {
	e := &Engine{}
	ctx := context.Background()

	recent := &Alert{Name: "recent", FractalID: "f1", QueryString: `a=1`, LastEvaluatedAt: time.Now().Add(-10 * time.Second)}
	if err := e.evaluateAlertCursor(ctx, recent, nil); err != nil {
		t.Errorf("a cursor inside the minimum window waits: %v", err)
	}

	window := 600
	compound := &Alert{Name: "c", AlertType: "compound", WindowDuration: &window, LastEvaluatedAt: time.Now().Add(-time.Minute)}
	if err := e.evaluateCompoundAlert(ctx, compound, nil); err != nil {
		t.Errorf("an incomplete tumbling window waits: %v", err)
	}

	hourly, err := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	qw := 3600
	scheduled := &Alert{Name: "s", AlertType: "scheduled", CronSchedule: hourly, QueryWindowSeconds: &qw, LastEvaluatedAt: time.Now()}
	if err := e.evaluateScheduledAlert(ctx, scheduled, nil); err != nil {
		t.Errorf("a scheduled alert before its next run waits: %v", err)
	}
}

func TestEvaluatorsRefuseIncompleteDefinitions(t *testing.T) {
	e := &Engine{}
	ctx := context.Background()
	zero := 0

	if err := e.evaluateCompoundAlert(ctx, &Alert{Name: "c"}, nil); err == nil {
		t.Error("a compound alert with no window cannot be evaluated")
	}
	if err := e.evaluateCompoundAlert(ctx, &Alert{Name: "c", WindowDuration: &zero}, nil); err == nil {
		t.Error("a zero window cannot be evaluated")
	}
	if err := e.evaluateScheduledAlert(ctx, &Alert{Name: "s"}, nil); err == nil {
		t.Error("a scheduled alert whose cron did not parse cannot be evaluated")
	}
	hourly, _ := cron.ParseStandard("@hourly")
	if err := e.evaluateScheduledAlert(ctx, &Alert{Name: "s", CronSchedule: hourly, QueryWindowSeconds: &zero}, nil); err == nil {
		t.Error("a scheduled alert needs a positive query window")
	}
}

func TestProcessAlertResultsIgnoresAnEmptyResult(t *testing.T) {
	// No match records nothing: an execution row per idle evaluation would bloat the
	// audit table at scale. This engine has no store, so any write would panic.
	if err := (&Engine{}).processAlertResults(context.Background(), &Alert{ID: "a"}, nil, parser.QueryOptions{}, 5); err != nil {
		t.Errorf("got %v", err)
	}
}

// Only enabled alerts are evaluated from a warm cache, which serves the cycle without
// a database round trip.
func TestGetEnabledAlertsFromCache(t *testing.T) {
	e := &Engine{
		alertsCache: map[string]*Alert{
			"a": {ID: "a", Enabled: true},
			"b": {ID: "b", Enabled: false},
			"c": {ID: "c", Enabled: true},
		},
		cacheRefreshedAt: time.Now(),
	}
	got, err := e.getEnabledAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d alerts, want the 2 enabled ones", len(got))
	}
	for _, a := range got {
		if !a.Enabled {
			t.Errorf("disabled alert %s would be evaluated", a.ID)
		}
	}
	if e.CachedAlertCount() != 3 {
		t.Errorf("the cache holds every alert, enabled or not: %d", e.CachedAlertCount())
	}
}

// A refresh that cannot read the hot table must leave the skip signal stale, so the
// cycle evaluates everything: a missing signal costs a redundant query, never a missed
// detection.
func TestRefreshRecentIngestFailsOpen(t *testing.T) {
	cursor := time.Now().Add(-5 * time.Minute)
	e := &Engine{recentIngest: map[string]time.Time{}, recentIngestFresh: true}
	alert := &Alert{FractalID: "f1", LastEvaluatedAt: cursor}
	if !e.shouldSkipAlert(alert) {
		t.Fatal("precondition: a fresh map with no data for the fractal skips")
	}

	e.refreshRecentIngest(context.Background(), cursor)
	if e.shouldSkipAlert(alert) {
		t.Error("without a ClickHouse client the signal must go stale and stop skipping")
	}
}

func TestSystemAlertsFractalIsCachedOnceFound(t *testing.T) {
	e := &Engine{alertsFractalID: "sys-1"}
	if got := e.getAlertsFractalID(context.Background()); got != "sys-1" {
		t.Errorf("a resolved id is reused without a lookup, got %q", got)
	}
	if got := (&Engine{}).getAlertsFractalID(context.Background()); got != "" {
		t.Errorf("no database, no system fractal: %q", got)
	}
}

// A nil ClickHouse client must leave hydration off entirely. Stored in the interface
// field it would be a non-nil interface wrapping a nil pointer, and the first trigger
// would panic.
func TestNewEngineWithoutClickHouseDisablesHydration(t *testing.T) {
	e := NewEngineWithDicts(nil, nil, nil, "https://bifract.example.test/")
	if e.hydrator != nil {
		t.Error("hydrator must be a true nil without a ClickHouse client")
	}
	if e.webhookClient == nil || e.webhookClient.baseURL != "https://bifract.example.test" {
		t.Errorf("webhook links need the trimmed base URL: %+v", e.webhookClient)
	}
	if e.emailClient == nil || e.emailClient.baseURL != "https://bifract.example.test" {
		t.Errorf("email links need the trimmed base URL: %+v", e.emailClient)
	}
}

func TestEngineStopWaitsForTheLoop(t *testing.T) {
	e := &Engine{}
	e.Start()
	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	if e.IsRunning() {
		t.Error("no cycle should be running after Stop")
	}
}
