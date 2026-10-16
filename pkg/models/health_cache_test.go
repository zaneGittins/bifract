package models

import (
	"context"
	"testing"
	"time"
)

// A model whose tables may not exist is never summarized: its health comes from
// its own status, without a state read.
func TestAttachHealthSkipsModelsWithoutState(t *testing.T) {
	m := testManager()
	lag := int64(30)
	list := []*Model{
		{ID: "a", Status: "error", ErrorMessage: "boom", CHTableName: "model_a", ModelType: ModelTypeRarity},
		{ID: "b", Status: "rebuilding", CHTableName: "model_b", ModelType: ModelTypeRarity},
		{ID: "c", Status: "active", CHTableName: "", StateLagSeconds: &lag, ModelType: ModelTypeRarity},
	}
	m.AttachHealth(context.Background(), list)
	want := []string{HealthError, HealthRebuilding, HealthUnknown}
	for i, mo := range list {
		if mo.Health == nil || mo.Health.State != want[i] {
			t.Errorf("%s: health %+v, want %s", mo.ID, mo.Health, want[i])
		}
	}
	if len(m.health.inflight) != 0 {
		t.Error("a summary read was started for a model with no state table")
	}
}

// A current cached summary is served without another read; one still being read
// does not hold the listing past the request, and reads as checking.
func TestAttachHealthUsesTheCache(t *testing.T) {
	m := testManager()
	lag := int64(30)
	now := time.Now().UTC()
	cached := &Model{ID: "a", Status: "active", CHTableName: "model_a", ModelType: ModelTypeFirstSeen,
		StateLagSeconds: &lag, CreatedAt: now.Add(-60 * 24 * time.Hour)}
	m.health.entries["a"] = healthEntry{
		version: healthVersion(cached), at: now,
		sum: &stateSummary{oldest: now.Add(-30 * 24 * time.Hour), newest: now.Add(-time.Hour)},
	}
	pending := &Model{ID: "b", Status: "active", CHTableName: "model_b", ModelType: ModelTypeFirstSeen, StateLagSeconds: &lag}
	m.health.inflight["b"] = make(chan struct{}) // a read that has not finished

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	m.AttachHealth(ctx, []*Model{cached, pending})
	if time.Since(start) > time.Second {
		t.Error("a pending summary held the listing past its request")
	}
	if cached.Health.State != HealthHealthy {
		t.Errorf("cached: %+v", cached.Health)
	}
	if pending.Health.State != HealthChecking {
		t.Errorf("pending: %+v", pending.Health)
	}
}

// A summary is keyed to the state it describes: once the model is edited, reset or
// reseeded, the old summary is never served for it.
func TestHealthCacheVersioning(t *testing.T) {
	c := newHealthCache()
	mo := &Model{ID: "a", Status: "active", BackfillStatus: "running", BackfillDone: 3, UpdatedAt: time.Unix(100, 0)}
	sum := &stateSummary{}
	c.entries["a"] = healthEntry{version: healthVersion(mo), at: time.Now(), sum: sum}
	if got, _ := c.get(mo); got != sum {
		t.Fatal("current summary not served")
	}
	for name, edit := range map[string]func(*Model){
		"backfill progressed": func(m *Model) { m.BackfillDone = 4 },
		"backfill finished":   func(m *Model) { m.BackfillStatus = "completed" },
		"edited":              func(m *Model) { m.UpdatedAt = time.Unix(200, 0) },
		"rebuilt":             func(m *Model) { m.Status = "rebuilding" },
	} {
		changed := *mo
		edit(&changed)
		if got, _ := c.get(&changed); got != nil {
			t.Errorf("%s: served a summary of the previous state", name)
		}
	}

	// A fresh entry needs no read; an in-flight read is shared, not duplicated.
	if ch := c.ensure(nil, mo, time.Now()); ch != nil {
		t.Error("a fresh summary was re-read")
	}
	other := &Model{ID: "b"}
	inflight := make(chan struct{})
	c.inflight["b"] = inflight
	if ch := c.ensure(nil, other, time.Now()); ch != inflight {
		t.Error("a second read was started while one was in flight")
	}

	c.forget("a")
	if got, _ := c.get(mo); got != nil {
		t.Error("a deleted model's summary is still served")
	}
}

func TestHealthCopy(t *testing.T) {
	if got := humanDuration(90 * time.Minute); got != "1h" {
		t.Errorf("hours: %q", got)
	}
	if got := humanDuration(45 * time.Minute); got != "45m" {
		t.Errorf("minutes: %q", got)
	}
	if got := humanDuration(72 * time.Hour); got != "3d" {
		t.Errorf("days: %q", got)
	}
	if learningReason(&Model{ModelType: ModelTypeBeacon}) == "" || learningReason(&Model{ModelType: ModelTypeTLSH}) != "" {
		t.Error("learning reasons by type")
	}
	if firstNonEmpty("", "b") != "b" || firstNonEmpty("a", "b") != "a" {
		t.Error("firstNonEmpty")
	}
}
