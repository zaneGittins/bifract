//go:build integration

// The operator's pass: what is this instance, is it keeping up, how much is it
// holding, and how long should it hold it. These are the calls a monitoring
// system or a capacity review makes.
//
//	go test -tags integration ./test/integration/ -run TestOperations -v

package integration

import (
	"testing"
)

func TestOperations(t *testing.T) {
	c := New(t)

	// 1. What am I running against. Deployment shape and the capability probes
	//    decide what the rest of the API can do: a single node cannot answer
	//    the cluster questions a sharded one can.
	var topology struct {
		Deployment        string `json:"deployment"`
		DistributedTables bool   `json:"distributed_tables"`
		Version           string `json:"version"`
	}
	// These system endpoints answer their own shape rather than the standard
	// envelope, so read them directly.
	c.DoRaw(t, "GET", "/system/topology", nil, &topology)
	if topology.Deployment == "" {
		t.Error("topology did not report a deployment kind")
	}

	// 2. Is ingestion keeping up. Alerts are deferred before ingestion is, so
	//    alerts_deferred turning true is the early warning, not the outage.
	var pressure struct {
		AlertsDeferred bool `json:"alerts_deferred"`
	}
	c.DoRaw(t, "GET", "/system/pressure", nil, &pressure)

	// 3. How much is each fractal holding. This is what a capacity review reads.
	var listed struct {
		Fractals []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"fractals"`
	}
	c.Do(t, "GET", "/fractals", nil, &listed)
	if len(listed.Fractals) == 0 {
		t.Skip("the credential can reach no fractal")
	}

	target := listed.Fractals[0]
	var stats map[string]any
	c.InScope(target.ID).Do(t, "GET", "/fractals/"+target.ID+"/stats", nil, &stats)

	// 4. Retention is per fractal, so noisy data can be aged out sooner than the
	//    data worth keeping. Read the current value before changing it, and put
	//    it back: this runs against a real instance.
	var before struct {
		RetentionDays *int `json:"retention_days"`
	}
	c.Do(t, "GET", "/fractals/"+target.ID, nil, &before)

	scoped := c.InScope(target.ID)
	scoped.Do(t, "PUT", "/fractals/"+target.ID+"/retention", map[string]any{"retention_days": 30}, nil)

	var after struct {
		RetentionDays *int `json:"retention_days"`
	}
	c.Do(t, "GET", "/fractals/"+target.ID, nil, &after)
	if after.RetentionDays == nil || *after.RetentionDays != 30 {
		t.Errorf("retention did not take: %v", after.RetentionDays)
	}

	restore := map[string]any{"retention_days": nil}
	if before.RetentionDays != nil {
		restore["retention_days"] = *before.RetentionDays
	}
	scoped.Do(t, "PUT", "/fractals/"+target.ID+"/retention", restore, nil)
}
