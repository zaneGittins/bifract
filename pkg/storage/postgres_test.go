package storage

import "testing"

// Two workers sharing an advisory lock id silently stop each other: the one that
// loses returns without doing its work and without logging. Three of them did.
func TestAdvisoryLockIDsAreDistinct(t *testing.T) {
	ids := map[string]int64{
		"LockSchemaProvision":  LockSchemaProvision,
		"LockAlertEngine":      LockAlertEngine,
		"LockModelScorer":      LockModelScorer,
		"LockSchemaFieldSweep": LockSchemaFieldSweep,
		"LockQuotaRollover":    LockQuotaRollover,
		"LockModelState":       LockModelState,
	}
	seen := map[int64]string{}
	for name, id := range ids {
		if other, dup := seen[id]; dup {
			t.Errorf("%s and %s share advisory lock id %#x; whichever takes it first "+
				"silently stops the other", name, other, id)
			continue
		}
		seen[id] = name
	}
}
