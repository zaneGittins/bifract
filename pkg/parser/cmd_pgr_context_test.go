package parser

import (
	"strings"
	"testing"
)

// The lookup reads only the processes behind the rows in range, by key, and joins on the GUID.
func TestPgrContextSQL(t *testing.T) {
	cand := "SELECT " + procKeyExpr(pgrRowRef) + " FROM logs WHERE " + pgrWindow
	sql := mustTranslate(t, `pgrContext(commandline, user)`, pgrOpts())
	for _, want := range []string{
		"LEFT JOIN (SELECT process_guid, argMax(commandline, (timestamp, commandline != '')) AS pgr_commandline, argMax(if(proc_user = '\\0', '', proc_user), (timestamp, if(proc_user = '\\0', '', proc_user) != '')) AS pgr_user FROM proc_lineage WHERE fractal_id = 'f' AND proc_key IN (" + cand + ") GROUP BY process_guid) AS _mlookup",
		"ON " + pgrRowRef + " = _mlookup.process_guid",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "argMax(image") {
		t.Errorf("read columns it was not asked for:\n%s", sql)
	}
}

func TestPgrContextAllColumns(t *testing.T) {
	sql := mustTranslate(t, `pgrContext()`, pgrOpts())
	for _, col := range pgrContextColumns {
		if !strings.Contains(sql, " AS "+col.name) {
			t.Errorf("pgrContext() lacks %s:\n%s", col.name, sql)
		}
	}
}

// The parent's command line is the parent's own lineage row, looked up by key.
func TestPgrContextParentCommandline(t *testing.T) {
	sql := mustTranslate(t, `pgrContext(parent_commandline)`, pgrOpts())
	for _, want := range []string{"AS pgr_parent_commandline", "SELECT parent_key FROM proc_lineage", "ON c.parent_key = p.proc_key"} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
}

// Its columns filter, group and sort like any other field.
func TestPgrContextColumnsAreFields(t *testing.T) {
	for q, want := range map[string]string{
		`pgrContext(commandline) | pgr_commandline=~"-enc"`:          "pgr_commandline",
		`pgrContext(commandline) | groupby(pgr_commandline)`:         "GROUP BY",
		`pgrContext(user) | sort(pgr_user)`:                          "ORDER BY",
		`pgrContext(commandline, require=true) | pgr_commandline=~x`: "INNER JOIN",
	} {
		sql := mustTranslate(t, q, pgrOpts())
		if !strings.Contains(sql, want) {
			t.Errorf("%s: missing %q in:\n%s", q, want, sql)
		}
	}
}

// Plain filters narrow the processes looked up, as they do for the pgrHas filters.
func TestPgrContextNarrowsCandidates(t *testing.T) {
	sql := mustTranslate(t, `image=x | pgrContext(commandline)`, pgrOpts())
	if !strings.Contains(sql, "proc_key IN (SELECT "+procKeyExpr(pgrRowRef)+" FROM logs WHERE "+pgrWindow+" AND ") {
		t.Errorf("candidates not narrowed:\n%s", sql)
	}
}

func TestPgrContextDistributed(t *testing.T) {
	o := pgrOpts()
	o.TableName, o.ProcLineageTable = "logs_distributed", "proc_lineage_distributed"
	sql := mustTranslate(t, `pgrContext(commandline)`, o)
	for _, want := range []string{"GLOBAL LEFT JOIN (SELECT process_guid", "FROM proc_lineage_distributed", "proc_key GLOBAL IN (SELECT"} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
}

func TestPgrContextRefusals(t *testing.T) {
	arch := pgrOpts()
	arch.SourceMode = SourceIceberg
	for q, want := range map[string]string{
		`pgrContext(signer)`:                                         "pgrContext() has no column signer",
		`pgrContext(commandline, require=maybe)`:                     "require= must be true or false",
		`pgrContext() | pgrContext()`:                                "cannot be used more than once",
		`pgrContext() | modelLookup(model="m", key=[user])`:          "cannot be combined",
		`pgrContext(commandline) | groupby(user) | pgrContext(user)`: "cannot be used more than once",
		`process_guid := "x" | pgrContext()`:                         "cannot be assigned",
	} {
		if _, err := translateNoFatal(q, pgrOpts()); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", q, err, want)
		}
	}
	if _, err := translateNoFatal(`pgrContext()`, arch); err == nil {
		t.Error("archive search must refuse pgrContext()")
	}
}

// Every column is a String, so a row without lineage reads ” and compares like a missing field.
func TestPgrContextColumnsAreStrings(t *testing.T) {
	sql := mustTranslate(t, `pgrContext(start_time) | pgr_start_time=""`, pgrOpts())
	if !strings.Contains(sql, "argMax(toString(timestamp), (timestamp, toString(timestamp) != '')) AS pgr_start_time") {
		t.Errorf("start time is not a string:\n%s", sql)
	}
	// A backfilled row and one written before a column existed tie on time; the one with a value wins.
	if sql := mustTranslate(t, `pgrContext(hash)`, pgrOpts()); !strings.Contains(sql, "argMax(hash, (timestamp, hash != '')) AS pgr_hash") {
		t.Errorf("ties do not prefer a value:\n%s", sql)
	}
}
