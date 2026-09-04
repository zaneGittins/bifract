package dictionaries

import "testing"

func chRow(key, val string, seq, shard uint64) map[string]interface{} {
	return map[string]interface{}{
		"group_name": key,
		"note":       val,
		seqColumn:    seq,
		"_shard_num": shard,
	}
}

var consolidateCols = []DictionaryColumn{{Name: "group_name"}, {Name: "note"}}

func TestPlanConsolidation(t *testing.T) {
	t.Run("nothing off-shard is a no-op", func(t *testing.T) {
		plan := planConsolidation(consolidateCols, "group_name", []map[string]interface{}{
			chRow("execs", "a", 1, 1),
			chRow("r&d", "b", 2, 1),
		})
		if len(plan.rows) != 0 {
			t.Fatalf("expected no rewrite, got %v", plan.keys)
		}
	})

	t.Run("write-shard copy wins over an off-shard duplicate", func(t *testing.T) {
		plan := planConsolidation(consolidateCols, "group_name", []map[string]interface{}{
			chRow("execs", "stale", 7, 3),
			chRow("execs", "current", 1, 1),
		})
		if len(plan.rows) != 1 || plan.rows[0].Fields["note"] != "current" {
			t.Fatalf("expected the write-shard copy, got %+v", plan.rows)
		}
		// The row keeps its write-shard ordinal, so the editor does not reorder.
		if plan.seqs[0] != 1 {
			t.Fatalf("expected seq 1, got %d", plan.seqs[0])
		}
	})

	t.Run("write-shard copy wins regardless of scan order", func(t *testing.T) {
		plan := planConsolidation(consolidateCols, "group_name", []map[string]interface{}{
			chRow("execs", "current", 1, 1),
			chRow("execs", "stale", 7, 3),
		})
		if len(plan.rows) != 1 || plan.rows[0].Fields["note"] != "current" {
			t.Fatalf("expected the write-shard copy, got %+v", plan.rows)
		}
	})

	t.Run("off-shard only keeps the lowest shard", func(t *testing.T) {
		plan := planConsolidation(consolidateCols, "group_name", []map[string]interface{}{
			chRow("execs", "shard3", 5, 3),
			chRow("execs", "shard2", 5, 2),
		})
		if len(plan.rows) != 1 || plan.rows[0].Fields["note"] != "shard2" {
			t.Fatalf("expected the lowest-numbered shard, got %+v", plan.rows)
		}
	})

	t.Run("only affected keys are rewritten, in key order", func(t *testing.T) {
		plan := planConsolidation(consolidateCols, "group_name", []map[string]interface{}{
			chRow("a", "1", 1, 1),
			chRow("b", "2", 2, 2),
			chRow("c", "3", 3, 1),
			chRow("d", "4", 4, 2),
		})
		if len(plan.keys) != 2 || plan.keys[0] != "b" || plan.keys[1] != "d" {
			t.Fatalf("expected [b d], got %v", plan.keys)
		}
	})

	t.Run("blank keys are skipped", func(t *testing.T) {
		plan := planConsolidation(consolidateCols, "group_name", []map[string]interface{}{
			chRow("", "junk", 1, 2),
		})
		if len(plan.rows) != 0 {
			t.Fatalf("expected no rewrite, got %v", plan.keys)
		}
	})
}
