package parser

import (
	"strings"
	"testing"
	"time"
)

// TestDynamicSafety_NoBareHintedRefInUnsafeContext is a regression guardrail for
// the JSON-Dynamic bug class: a type-hinted field is stored as a concrete String
// only in parts ingested AFTER the hint was added; older parts keep it as a
// Dynamic subcolumn. A BARE `fields.x` ref (no ::String) is rejected by
// ClickHouse in GROUP BY / ORDER BY / DISTINCT / LIMIT BY / IN / JOIN ON /
// multiSearch / isIPv4String (error 44 or 43) on such a path.
//
// jsonFieldRef now casts ALL subcolumn refs to ::String uniformly (verified to
// preserve bloom/set skip indexes via no-op cast elision on CH 26.6+), so no bare
// ref should exist anywhere. This test defends that invariant: if a future change
// hand-builds a bare `fields.x` string (bypassing jsonFieldRef) and drops it into
// an unsafe context, this fails. The battery exercises every field-referencing
// command with type-hinted fields.
func TestDynamicSafety_NoBareHintedRefInUnsafeContext(t *testing.T) {
	opts := QueryOptions{
		StartTime:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:      time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:      1000,
		FractalID:    "f1",
		GeoIPEnabled: true,
	}

	// All type-hinted fields (bare-ref emitters). process_guid/src_ip/etc.
	queries := []string{
		`* | groupby(process_guid)`,
		`* | groupby(image, user)`,
		`* | table(process_guid)`,
		`* | table(image, user, src_ip)`,
		`* | sort(process_guid)`,
		`* | sort(image, desc)`,
		`* | dedup(process_guid)`,
		`* | dedup(src_ip, dst_ip)`,
		`* | groupby(process_guid, distinct=true)`,
		`* | count(process_guid, unique=true)`,
		`* | frequency(process_guid)`,
		`* | top(image)`,
		`* | groupby(image) | count()`,
		`* | groupby(src_ip) | avg(dst_port)`,
		// Multi-value equality -> IN (my Bug-1 fix; bare IN errors 43 on Dynamic).
		`process_guid="a","b","c" | groupby(dst_ip)`,
		`image!="a","b" | groupby(user)`,
		// in() command -> IN.
		`* | in(process_guid, values=[a,b])`,
		`* | in(image, values=[x,y])`,
		// contains-any -> multiSearchAny (errors on Dynamic bare).
		`image=~powershell,cmd`,
		`src_ip=~10.0,192.168`,
		`event_id=~1 | groupby(image)`,
		// starts/ends-any combined with grouping.
		`image=^mimi | groupby(image)`,
		`image=$exe | sort(image)`,
		// chain (arrayJoin/GROUP BY on the field).
		`* | chain(process_guid) { image="a" ; image="b" }`,
		`* | chain(src_ip, dst_ip) { image="a" ; image="b" }`,
		// assignment alias then grouped.
		`p := process_guid | groupby(p)`,
		// IP functions.
		`* | cidr(src_ip, "10.0.0.0/8")`,
		`src_ip=~10 | cidr(dst_ip, "192.168.0.0/16")`,
		`* | lookupIP(field=src_ip, include=[country, asn])`,
		// scalar equality feeding a groupby (equality stays bare, group casts).
		`process_guid="X" | groupby(process_guid)`,
		`image="cmd.exe" | table(image)`,
	}

	for _, q := range queries {
		q := q
		t.Run(q, func(t *testing.T) {
			pipeline, err := ParseQuery(q)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatalf("translate error: %v", err)
			}
			for _, v := range unsafeBareHintedRefs(result.SQL) {
				t.Errorf("bare hinted ref in Dynamic-unsafe context: %s\nSQL: %s", v, result.SQL)
			}
		})
	}
}

// unsafeBareHintedRefs scans generated SQL for bare `fields.<hinted>` references
// (no ::String) sitting in a context that rejects a Dynamic subcolumn. It maps
// directly to the empirically-confirmed error forms (see the memory note
// project_json_dynamic_groupby_cast): IN, multiSearchAny, isIPv4/6String, the
// recursive-traversal JOIN key, ORDER BY / GROUP BY / LIMIT BY keys.
func unsafeBareHintedRefs(sql string) []string {
	var out []string
	for field := range jsonDefaultTypeHintedFields {
		bare := "fields.`" + field + "`"
		for i := 0; i < len(sql); {
			j := strings.Index(sql[i:], bare)
			if j < 0 {
				break
			}
			at := i + j
			after := sql[at+len(bare):]
			i = at + len(bare)
			if strings.HasPrefix(after, "::String") {
				continue // cast form is safe
			}
			before := sql[:at]

			// Following-context checks (the ref is the left operand).
			switch {
			case strings.HasPrefix(after, " IN ("), strings.HasPrefix(after, " NOT IN ("):
				out = append(out, "bare IN: "+field)
			case strings.HasPrefix(after, " = t._node_id"):
				out = append(out, "bare traversal JOIN key: "+field)
			case strings.HasPrefix(after, " ASC"), strings.HasPrefix(after, " DESC"):
				out = append(out, "bare ORDER BY key: "+field)
			}

			// Preceding-context checks (the ref is a function argument).
			switch {
			case strings.HasSuffix(before, "multiSearchAnyCaseInsensitive("):
				out = append(out, "bare multiSearchAny arg: "+field)
			case strings.HasSuffix(before, "isIPv4String("), strings.HasSuffix(before, "isIPv6String("):
				out = append(out, "bare IP-function arg: "+field)
			case strings.HasSuffix(before, "GROUP BY "), strings.HasSuffix(before, "LIMIT 1 BY "):
				out = append(out, "bare GROUP/LIMIT BY key: "+field)
			}
		}
	}
	return out
}
