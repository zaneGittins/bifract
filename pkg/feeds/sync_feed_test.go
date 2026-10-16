package feeds

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/alerts"
)

const nativeRule = `
name: Failed Logon Burst
description: Native rule
queryString: 'event_id=4625'
labels:
  - attack.t1110
references:
  - https://example.com/ref
`

// sigmaRuleWith returns a translatable Sigma rule with the given title, level and status.
func sigmaRuleWith(title, level, status string) string {
	return fmt.Sprintf(`
title: %s
id: 22222222-0000-0000-0000-000000000000
status: %s
level: %s
author: Tester
falsepositives:
  - Admins
  - Installers
references:
  - https://attack.mitre.org/techniques/T1543/003/
logsource:
    product: windows
    category: process_creation
tags:
    - attack.t1543.003
detection:
    selection:
        Image|endswith: '\sc.exe'
    condition: selection
`, title, status, level)
}

const untranslatableRule = `
title: Refers To Nothing
level: high
status: stable
logsource:
    product: windows
tags:
    - attack.t1110
detection:
    selection:
        EventID: 4625
    condition: other_selection
`

func testFeed(url string) *Feed {
	return &Feed{
		ID: "feed-1", Name: "SigmaHQ", RepoURL: url, Branch: "main",
		FractalID: "frac-1", CreatedBy: "alice", SyncSchedule: ScheduleDaily,
	}
}

func keys[M ~map[string]V, V any](m M) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestSyncFeedImportsNewRules(t *testing.T) {
	tmp := isolateTempDir(t)
	sc := sigmaRuleWith("Service Creation", "high", "stable")
	repo := newGitRepo(t, map[string]string{
		"rules/windows/sc.yml":     sc,
		"rules/native.yml":         nativeRule,
		"rules/README.md":          "not a rule",
		"rules/.github/ci.yml":     "name: ci\nqueryString: x\n",
		"other/out-of-scope.yml":   nativeRule,
		".github/workflows/ci.yml": "on: push",
	})
	env := newSyncEnv(t)
	feed := testFeed(repo.URL)
	feed.Path = "rules"

	res, err := env.syncer.SyncFeed(context.Background(), feed)
	if err != nil {
		t.Fatalf("SyncFeed: %v", err)
	}
	if res.Added != 2 || res.Updated != 0 || res.Skipped != 0 || len(res.Errors) != 0 {
		t.Fatalf("result = %+v, want 2 added and no errors", res)
	}

	created := env.created()
	if got := keys(created); !reflect.DeepEqual(got, []string{"rules/native.yml", "rules/windows/sc.yml"}) {
		t.Fatalf("created alerts for %v; only YAML under the feed path should be imported", got)
	}

	s := created["rules/windows/sc.yml"]
	if s.Name != "Service Creation" || s.Type != "event" || s.Severity != string(alerts.SeverityHigh) {
		t.Errorf("sigma alert = %+v", s)
	}
	if !strings.Contains(s.Query, `Image=`) || !strings.Contains(s.Query, `sc.exe`) {
		t.Errorf("sigma query not translated: %q", s.Query)
	}
	for _, want := range []string{"Sigma ID: 22222222-0000-0000-0000-000000000000", "Author: Tester", "False positives: Admins, Installers"} {
		if !strings.Contains(s.Description, want) {
			t.Errorf("description %q missing %q", s.Description, want)
		}
	}
	if !slices.Contains(s.Labels, "attack.t1543.003") || !slices.Contains(s.Labels, "sigma:high") {
		t.Errorf("labels = %v, want ATT&CK tag and sigma level", s.Labels)
	}
	if !reflect.DeepEqual(s.References, []string{"https://attack.mitre.org/techniques/T1543/003/"}) {
		t.Errorf("references = %v", s.References)
	}
	if s.Hash != ruleHash(sc) {
		t.Errorf("hash = %q, want sha256(content+TranslatorVersion)", s.Hash)
	}
	if s.FeedID != "feed-1" || s.FractalID != "frac-1" || s.PrismID != nil || s.CreatedBy != "alice" {
		t.Errorf("ownership = feed %q fractal %v prism %v by %v", s.FeedID, s.FractalID, s.PrismID, s.CreatedBy)
	}

	n := created["rules/native.yml"]
	if n.Name != "Failed Logon Burst" || n.Query != "event_id=4625" || n.Type != "event" || n.Description != "Native rule" {
		t.Errorf("native alert = %+v", n)
	}
	if n.Severity != string(alerts.SeverityMedium) {
		t.Errorf("native severity = %q, want the medium default", n.Severity)
	}
	if !reflect.DeepEqual(n.Labels, []string{"attack.t1110"}) || !reflect.DeepEqual(n.References, []string{"https://example.com/ref"}) {
		t.Errorf("native labels/references = %v / %v", n.Labels, n.References)
	}

	keep, ran := env.keepPaths()
	if !ran || !reflect.DeepEqual(sorted(keep), []string{"rules/native.yml", "rules/windows/sc.yml"}) {
		t.Errorf("delete pass keep-list = %v (ran %v)", keep, ran)
	}

	cat := env.catalog()
	if got := keys(cat); !reflect.DeepEqual(got, []string{"rules/native.yml", "rules/windows/sc.yml"}) {
		t.Fatalf("catalog = %v", got)
	}
	if e := cat["rules/windows/sc.yml"]; !e.Imported || e.SkipReason != "" || e.Level != "high" || e.Status != "stable" || !slices.Contains(e.Tags, "attack.t1543.003") {
		t.Errorf("catalog entry = %+v", e)
	}
	if prune, ran := env.catalogPrunePaths(); !ran || len(prune) != 2 {
		t.Errorf("catalog prune keep-list = %v (ran %v)", prune, ran)
	}

	if len(env.pg.Matching("FROM alerts a")) == 0 {
		t.Error("the alert engine cache was not refreshed after the sync")
	}
	if left := dirEntries(t, tmp); len(left) != 0 {
		t.Errorf("sync left %v in the temp dir", left)
	}
}

// A second sync after upstream changes: unchanged rules are left alone, edited
// ones are rewritten in place (keeping the analyst's enable/disable choice),
// new ones are added and removed ones are deleted.
func TestSyncFeedAppliesUpstreamChanges(t *testing.T) {
	isolateTempDir(t)
	unchanged := sigmaRuleWith("Unchanged", "high", "stable")
	edited := sigmaRuleWith("Edited Upstream", "critical", "stable")
	repo := newGitRepo(t, map[string]string{
		"unchanged.yml": unchanged,
		"edited.yml":    edited,
		"new.yml":       nativeRule,
	})
	env := newSyncEnv(t)
	env.seed("unchanged.yml", unchanged)
	env.seedStale("edited.yml")
	env.seedStale("removed-upstream.yml")
	env.deleted = 1

	feed := testFeed(repo.URL)
	feed.FractalID, feed.PrismID = "", "prism-9" // the feed moved to a prism

	res, err := env.syncer.SyncFeed(context.Background(), feed)
	if err != nil {
		t.Fatal(err)
	}
	want := SyncResult{Added: 1, Updated: 1, Deleted: 1, Skipped: 1}
	if res.Added != want.Added || res.Updated != want.Updated || res.Deleted != want.Deleted || res.Skipped != want.Skipped || len(res.Errors) != 0 {
		t.Fatalf("result = %+v, want %+v", res, want)
	}

	if got := keys(env.created()); !reflect.DeepEqual(got, []string{"new.yml"}) {
		t.Errorf("created %v, want only new.yml", got)
	}

	updates := env.updated()
	if got := keys(updates); !reflect.DeepEqual(got, []string{"alert-edited.yml"}) {
		t.Fatalf("updated %v, want only the edited rule (unchanged hash must not rewrite)", got)
	}
	u := updates["alert-edited.yml"]
	if u.Name != "Edited Upstream" || u.Hash != ruleHash(edited) || u.UpdatedBy != "alice" {
		t.Errorf("update = %+v", u)
	}
	// The update re-asserts scope from the feed.
	if u.PrismID != "prism-9" || u.FractalID != nil {
		t.Errorf("update scope = fractal %v prism %v, want the feed's prism", u.FractalID, u.PrismID)
	}
	// An analyst who disabled a feed rule keeps it disabled across upstream edits.
	if setClause := u.SQL[:strings.Index(u.SQL, "WHERE")]; strings.Contains(setClause, "enabled") {
		t.Errorf("feed update rewrites enabled:\n%s", u.SQL)
	}

	keep, ran := env.keepPaths()
	if !ran || !reflect.DeepEqual(sorted(keep), []string{"edited.yml", "new.yml", "unchanged.yml"}) {
		t.Errorf("keep-list = %v (ran %v); the removed rule must be the only one dropped", keep, ran)
	}
}

func TestSyncFeedLevelAndStatusFilters(t *testing.T) {
	isolateTempDir(t)
	low := sigmaRuleWith("Low Noise", "low", "stable")
	repo := newGitRepo(t, map[string]string{
		"critical-stable.yml":   sigmaRuleWith("Keep Me", "critical", "stable"),
		"high-experimental.yml": sigmaRuleWith("Too Immature", "high", "experimental"),
		"low-stable.yml":        low,
		"native.yml":            nativeRule, // no level/status: always passes
	})
	env := newSyncEnv(t)
	// Imported under an older, looser threshold and unchanged since: tightening
	// the feed must still remove it.
	env.seed("low-stable.yml", low)
	env.deleted = 1

	feed := testFeed(repo.URL)
	feed.MinLevel, feed.MinStatus = "medium", "test"

	res, err := env.syncer.SyncFeed(context.Background(), feed)
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 2 || res.Deleted != 1 || len(res.Errors) != 0 {
		t.Fatalf("result = %+v", res)
	}
	if got := keys(env.created()); !reflect.DeepEqual(got, []string{"critical-stable.yml", "native.yml"}) {
		t.Errorf("created %v", got)
	}
	keep, _ := env.keepPaths()
	if !reflect.DeepEqual(sorted(keep), []string{"critical-stable.yml", "native.yml"}) {
		t.Errorf("keep-list = %v; filtered rules must be left out so their alerts are removed", keep)
	}

	cat := env.catalog()
	if e := cat["low-stable.yml"]; e.Imported || e.SkipReason != SkipMinLevel || !strings.Contains(e.SkipDetail, `"low"`) {
		t.Errorf("low rule catalog = %+v", e)
	}
	if e := cat["high-experimental.yml"]; e.Imported || e.SkipReason != SkipMinStatus || !strings.Contains(e.SkipDetail, `"experimental"`) {
		t.Errorf("experimental rule catalog = %+v", e)
	}
	// The catalog keeps every rule the repo offers, filtered or not.
	if prune, _ := env.catalogPrunePaths(); len(prune) != 4 {
		t.Errorf("catalog prune keep-list = %v, want all 4 paths", prune)
	}
}

// One bad file must not abort the sync, must be reported against its path, and
// must never cause its existing alert to be deleted.
func TestSyncFeedIsolatesBadFiles(t *testing.T) {
	isolateTempDir(t)
	repo := newGitRepo(t, map[string]string{
		"a-good.yml":           sigmaRuleWith("Good", "high", "stable"),
		"b-garbage.yml":        "::::",
		"c-untranslatable.yml": untranslatableRule,
		"d-bad-bql.yml":        "name: Bad BQL\nqueryString: 'count('\n",
		"e-aggregate.yml":      "name: Aggregate\nqueryString: 'event_id=1 | count()'\n",
		"f-no-query.yml":       "name: Missing Query\n",
		"g-dup.yml":            sigmaRuleWith("Dup", "high", "stable"),
	})
	env := newSyncEnv(t)
	// b-garbage.yml was a working rule before upstream broke it.
	env.seedStale("b-garbage.yml")
	// A transient lookup failure falls through to create, which the unique
	// (feed_id, feed_rule_path) index rejects.
	env.failPaths["g-dup.yml"] = errors.New("connection reset")
	env.insertErr["g-dup.yml"] = errors.New(`duplicate key value violates unique constraint "idx_alerts_feed_rule"`)

	res, err := env.syncer.SyncFeed(context.Background(), testFeed(repo.URL))
	if err != nil {
		t.Fatalf("SyncFeed aborted on a bad file: %v", err)
	}
	if res.Added != 1 {
		t.Errorf("added = %d, want the one good rule", res.Added)
	}

	wantErr := map[string]string{
		"b-garbage.yml":        "parse error",
		"c-untranslatable.yml": "translate Sigma rule",
		"d-bad-bql.yml":        "create error: invalid query syntax",
		"e-aggregate.yml":      "create error: event alerts cannot use aggregate functions",
		"f-no-query.yml":       "parse error",
		"g-dup.yml":            "create error",
	}
	if len(res.Errors) != len(wantErr) {
		t.Errorf("errors = %q, want one per bad file", res.Errors)
	}
	for path, frag := range wantErr {
		found := slices.ContainsFunc(res.Errors, func(e string) bool {
			return strings.HasPrefix(e, path+": ") && strings.Contains(e, frag)
		})
		if !found {
			t.Errorf("no error %q for %s in %q", frag, path, res.Errors)
		}
	}

	keep, ran := env.keepPaths()
	if !ran {
		t.Fatal("delete pass skipped although most of the repo was read")
	}
	if len(keep) != 7 {
		t.Errorf("keep-list = %v; every failed path must be retained", keep)
	}

	cat := env.catalog()
	for path, reason := range map[string]string{
		"b-garbage.yml":        SkipParseError,
		"c-untranslatable.yml": SkipTranslateError,
		"d-bad-bql.yml":        SkipCreateError,
		"f-no-query.yml":       SkipParseError,
		"g-dup.yml":            SkipCreateError,
	} {
		if e := cat[path]; e.Imported || e.SkipReason != reason || e.SkipDetail == "" {
			t.Errorf("catalog[%s] = %+v, want skip %q", path, e, reason)
		}
	}
	// A rule Bifract cannot translate still contributes its ATT&CK tags to the gap list.
	if e := cat["c-untranslatable.yml"]; e.Title != "Refers To Nothing" || !slices.Contains(e.Tags, "attack.t1110") {
		t.Errorf("untranslatable catalog entry lost its metadata: %+v", e)
	}
}

func TestSyncFeedCapsRetainedErrors(t *testing.T) {
	isolateTempDir(t)
	files := map[string]string{}
	for i := range maxSyncErrors + 10 {
		files[fmt.Sprintf("bad-%03d.yml", i)] = "::::"
	}
	repo := newGitRepo(t, files)
	env := newSyncEnv(t)

	res, err := env.syncer.SyncFeed(context.Background(), testFeed(repo.URL))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Errors) != maxSyncErrors+1 {
		t.Fatalf("len(errors) = %d, want %d plus a summary line", len(res.Errors), maxSyncErrors)
	}
	if last := res.Errors[len(res.Errors)-1]; last != "... and 10 more errors" {
		t.Errorf("summary line = %q", last)
	}
}

// A sync cut short (timeout, shutdown) saw only part of the repo, so it must not
// run the delete pass: everything it did not reach would be deleted.
func TestSyncFeedStoppedMidwaySkipsDeletePass(t *testing.T) {
	isolateTempDir(t)
	repo := newGitRepo(t, map[string]string{
		"a.yml": sigmaRuleWith("A", "high", "stable"),
		"b.yml": sigmaRuleWith("B", "high", "stable"),
		"c.yml": sigmaRuleWith("C", "high", "stable"),
	})
	env := newSyncEnv(t)
	env.seedStale("c.yml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env.onLookup = func(string) { cancel() }

	res, err := env.syncer.SyncFeed(ctx, testFeed(repo.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, ran := env.keepPaths(); ran {
		t.Fatal("delete pass ran after a partial sync")
	}
	if _, ran := env.catalogPrunePaths(); ran {
		t.Error("catalog prune ran after a partial sync")
	}
	if !slices.ContainsFunc(res.Errors, func(e string) bool { return strings.Contains(e, "sync stopped after 1/3 rules") }) {
		t.Errorf("errors = %q, want the stop reported", res.Errors)
	}
}

func TestSyncFeedFailsBeforeWritingAnything(t *testing.T) {
	repo := newGitRepo(t, map[string]string{"rules/a.yml": nativeRule})

	tests := []struct {
		name    string
		mutate  func(*Feed, *syncEnv)
		wantErr string
	}{
		{"refused URL scheme", func(f *Feed, _ *syncEnv) { f.RepoURL = "file:///etc" }, "clone: repo URL validation failed"},
		{"missing branch", func(f *Feed, _ *syncEnv) { f.Branch = "gone" }, "clone:"},
		// A feed path that no longer exists must fail the sync, not list zero
		// files and delete every alert the feed owns.
		{"missing feed path", func(f *Feed, _ *syncEnv) { f.Path = "renamed-upstream" }, "list files"},
		{"feed path escapes the clone", func(f *Feed, _ *syncEnv) { f.Path = "../../" }, "list files"},
		{"undecryptable token", func(_ *Feed, e *syncEnv) { e.token = "enc:AAAA" }, "decrypt token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := isolateTempDir(t)
			setEncryptionKey(t, "")
			env := newSyncEnv(t)
			env.seedStale("rules/a.yml")
			feed := testFeed(repo.URL)
			tt.mutate(feed, env)

			res, err := env.syncer.SyncFeed(context.Background(), feed)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("SyncFeed = %+v, %v; want error containing %q", res, err, tt.wantErr)
			}
			for _, write := range []string{"INSERT", "UPDATE", "DELETE"} {
				if calls := env.pg.Matching(write); len(calls) != 0 {
					t.Errorf("failed sync issued %s: %q", write, calls[0].Query)
				}
			}
			if left := dirEntries(t, tmp); len(left) != 0 {
				t.Errorf("failed sync left %v in the temp dir", left)
			}
		})
	}
}

func TestSyncFeedUsesStoredToken(t *testing.T) {
	isolateTempDir(t)
	setEncryptionKey(t, hex.EncodeToString(testKey))
	const token = "glpat-feed-token"
	enc, err := EncryptToken(token)
	if err != nil {
		t.Fatal(err)
	}
	repo := newGitRepo(t, map[string]string{"a.yml": nativeRule})
	repo.requireBasicAuth("x-token-auth", token)

	env := newSyncEnv(t)
	env.token = enc
	res, err := env.syncer.SyncFeed(context.Background(), testFeed(repo.URL))
	if err != nil {
		t.Fatalf("SyncFeed with an encrypted token: %v", err)
	}
	if res.Added != 1 {
		t.Errorf("result = %+v", res)
	}
	if c := env.pg.Matching("FROM alert_feeds WHERE id = $1"); len(c) != 1 || c[0].Args[0] != "feed-1" {
		t.Errorf("token lookup = %+v", c)
	}
}

func normalizerRow(id, mappings string) fakedb.PGResult {
	now := time.Now()
	return fakedb.Row(id, "Windows", "", []byte("[]"), []byte(mappings), []byte("[]"), []byte("[]"),
		false, "", now, now, int64(1))
}

// Sigma field names are rewritten through the feed's normalizer so the query
// matches the field names ingestion actually stores.
func TestSyncFeedMapsFieldsThroughNormalizer(t *testing.T) {
	const mapping = `[{"sources":["Image"],"target":"process_path"}]`
	repo := newGitRepo(t, map[string]string{"a.yml": sigmaRuleWith("Mapped", "high", "stable")})

	t.Run("explicit normalizer", func(t *testing.T) {
		isolateTempDir(t)
		env := newSyncEnv(t)
		env.withNormalizers()
		env.pg.On("FROM normalizers WHERE id = $1", normalizerRow("norm-1", mapping))
		feed := testFeed(repo.URL)
		feed.NormalizerID = "norm-1"

		if _, err := env.syncer.SyncFeed(context.Background(), feed); err != nil {
			t.Fatal(err)
		}
		q := env.created()["a.yml"].Query
		if !strings.Contains(q, "process_path=") || strings.Contains(q, "Image") {
			t.Errorf("query = %q, want Image mapped to process_path", q)
		}
		if c := env.pg.Matching("FROM normalizers WHERE id = $1"); len(c) != 1 || c[0].Args[0] != "norm-1" {
			t.Errorf("normalizer lookups = %+v", c)
		}
		if len(env.pg.Matching("is_default = true")) != 0 {
			t.Error("default normalizer consulted although the feed names one")
		}
	})

	t.Run("falls back to the default normalizer", func(t *testing.T) {
		isolateTempDir(t)
		env := newSyncEnv(t)
		env.withNormalizers()
		env.pg.On("SELECT id FROM normalizers WHERE is_default = true", fakedb.Row("norm-default"))
		env.pg.On("FROM normalizers WHERE id = $1", normalizerRow("norm-default", mapping))

		if _, err := env.syncer.SyncFeed(context.Background(), testFeed(repo.URL)); err != nil {
			t.Fatal(err)
		}
		if q := env.created()["a.yml"].Query; !strings.Contains(q, "process_path=") {
			t.Errorf("query = %q, want the default normalizer's mapping", q)
		}
		if c := env.pg.Matching("FROM normalizers WHERE id = $1"); len(c) != 1 || c[0].Args[0] != "norm-default" {
			t.Errorf("normalizer lookups = %+v", c)
		}
	})

	t.Run("no normalizer leaves fields as written", func(t *testing.T) {
		isolateTempDir(t)
		env := newSyncEnv(t)
		env.withNormalizers() // no default configured
		if _, err := env.syncer.SyncFeed(context.Background(), testFeed(repo.URL)); err != nil {
			t.Fatal(err)
		}
		if q := env.created()["a.yml"].Query; !strings.Contains(q, "Image=") {
			t.Errorf("query = %q, want the Sigma field name untouched", q)
		}
	})
}
