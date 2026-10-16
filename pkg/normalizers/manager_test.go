package normalizers

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

var (
	created = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	updated = time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
)

// normalizerRow is one normalizers row as the SELECT/RETURNING column list orders it:
// id, name, description, transforms, field_mappings, value_mappings,
// timestamp_fields, is_default, created_by, created_at, updated_at, version.
// The JSONB columns arrive as text, which is how lib/pq hands them back.
func normalizerRow(id, name string, isDefault bool, version int64, transforms, mappings, valueMaps, tsFields string) []driver.Value {
	return []driver.Value{id, name, "desc of " + name, transforms, mappings, valueMaps, tsFields,
		isDefault, "alice", created, updated, version}
}

func sysmonRow() []driver.Value {
	return normalizerRow("n-1", "sysmon", false, 3,
		`["flatten_leaf","snake_case"]`,
		`[{"sources":["computer","host"],"target":"computer_name"}]`,
		`[{"from_field":"event_id","to_field":"category","map":{"1":"process_creation"},"default":"other"}]`,
		`[{"field":"UtcTime","format":"2006-01-02 15:04:05.000"}]`)
}

func newManager() (*Manager, *fakedb.PG) {
	pg, fake := storagetest.Postgres()
	return NewManager(pg), fake
}

func TestManagerGetDecodesJSONBColumns(t *testing.T) {
	m, fake := newManager()
	fake.On("FROM normalizers WHERE id = $1", fakedb.Row(sysmonRow()...))

	n, err := m.Get(context.Background(), "n-1")
	if err != nil {
		t.Fatal(err)
	}
	want := &Normalizer{
		ID: "n-1", Name: "sysmon", Description: "desc of sysmon",
		Transforms:      []Transform{TransformFlattenLeaf, TransformSnakeCase},
		FieldMappings:   []FieldMapping{{Sources: []string{"computer", "host"}, Target: "computer_name"}},
		ValueMappings:   []ValueMapping{{FromField: "event_id", ToField: "category", Map: map[string]string{"1": "process_creation"}, Default: "other"}},
		TimestampFields: []TimestampField{{Field: "UtcTime", Format: "2006-01-02 15:04:05.000"}},
		CreatedBy:       "alice", CreatedAt: created, UpdatedAt: updated, Version: 3,
	}
	if !reflect.DeepEqual(n, want) {
		t.Fatalf("Get decoded\n %+v\nwant\n %+v", n, want)
	}
	if c := fake.Matching("WHERE id = $1"); len(c) != 1 || c[0].Args[0] != "n-1" {
		t.Fatalf("Get must bind the id, got %+v", c)
	}
}

func TestManagerGetErrors(t *testing.T) {
	m, _ := newManager() // no rule: zero rows
	if _, err := m.Get(context.Background(), "gone"); err == nil || err.Error() != "normalizer not found" {
		t.Fatalf("missing row: got %v, want \"normalizer not found\"", err)
	}

	m, fake := newManager()
	fake.On("FROM normalizers WHERE id", fakedb.PGResult{Err: errors.New("conn reset")})
	_, err := m.Get(context.Background(), "n-1")
	if err == nil || !strings.Contains(err.Error(), "conn reset") || strings.Contains(err.Error(), "not found") {
		t.Fatalf("a database failure must not be reported as not found: %v", err)
	}
}

func TestManagerListOrdersDefaultFirstAndDecodes(t *testing.T) {
	m, fake := newManager()
	fake.On("FROM normalizers ORDER BY", fakedb.RowsOf(
		normalizerRow("n-0", "default", true, 1, `["lowercase"]`, `[]`, `[]`, `[]`),
		sysmonRow(),
	))
	list, err := m.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != "n-0" || !list[0].IsDefault || list[1].Name != "sysmon" {
		t.Fatalf("List = %+v", list)
	}
	if !reflect.DeepEqual(list[0].Transforms, []Transform{TransformLowercase}) {
		t.Errorf("transforms = %v", list[0].Transforms)
	}
	if q := fake.Calls()[0].Query; !strings.Contains(q, "ORDER BY is_default DESC, name") {
		t.Errorf("default normalizer must sort first: %s", q)
	}

	m, fake = newManager()
	fake.On("FROM normalizers", fakedb.PGResult{Err: errors.New("boom")})
	if _, err := m.List(context.Background()); err == nil {
		t.Fatal("List must surface a query error")
	}
}

func TestManagerGetDefault(t *testing.T) {
	m, fake := newManager()
	fake.On("WHERE is_default = true", fakedb.Row(sysmonRow()...))
	n, err := m.GetDefault(context.Background())
	if err != nil || n.ID != "n-1" {
		t.Fatalf("GetDefault = %+v, %v", n, err)
	}

	m, _ = newManager()
	if _, err := m.GetDefault(context.Background()); err == nil || !strings.Contains(err.Error(), "no default") {
		t.Fatalf("no default row: got %v", err)
	}

	m, fake = newManager()
	fake.On("WHERE is_default = true", fakedb.PGResult{Err: errors.New("timeout")})
	if _, err := m.GetDefault(context.Background()); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("query error: got %v", err)
	}
}

// GetDefaultID is best effort: the feed syncer treats "" as "no normalizer".
func TestManagerGetDefaultID(t *testing.T) {
	m, fake := newManager()
	fake.On("SELECT id FROM normalizers WHERE is_default", fakedb.Row("n-0"))
	if got := m.GetDefaultID(context.Background()); got != "n-0" {
		t.Fatalf("GetDefaultID = %q", got)
	}
	m, fake = newManager()
	fake.On("SELECT id FROM normalizers", fakedb.PGResult{Err: errors.New("down")})
	if got := m.GetDefaultID(context.Background()); got != "" {
		t.Fatalf("GetDefaultID on error = %q, want empty", got)
	}
}

func TestManagerCreateEncodesRequest(t *testing.T) {
	m, fake := newManager()
	fake.On("INSERT INTO normalizers", fakedb.Row(sysmonRow()...))

	req := CreateRequest{
		Name:          "sysmon",
		Description:   "d",
		Transforms:    []Transform{TransformFlattenLeaf, TransformSnakeCase},
		FieldMappings: []FieldMapping{{Sources: []string{"Computer"}, Target: "computer_name"}},
		// ValueMappings and TimestampFields left nil: they must be stored as [] not null,
		// so the hot path's CompileFromRaw and older readers see an empty list.
	}
	n, err := m.Create(context.Background(), req, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if n.ID != "n-1" {
		t.Errorf("Create must return the RETURNING row, got %+v", n)
	}
	calls := fake.Matching("INSERT INTO normalizers")
	if len(calls) != 1 {
		t.Fatalf("expected one INSERT, got %d", len(calls))
	}
	want := []driver.Value{"sysmon", "d", `["flatten_leaf","snake_case"]`,
		`[{"sources":["Computer"],"target":"computer_name"}]`, "[]", "[]", "alice"}
	if !reflect.DeepEqual(calls[0].Args, want) {
		t.Fatalf("INSERT args\n %q\nwant\n %q", calls[0].Args, want)
	}
}

func TestManagerCreateRejectsInvalidBeforeSQL(t *testing.T) {
	m, fake := newManager()
	_, err := m.Create(context.Background(), CreateRequest{Name: "x", Transforms: []Transform{"reverse"}}, "alice")
	if err == nil || !strings.Contains(err.Error(), "invalid transform") {
		t.Fatalf("got %v", err)
	}
	if n := len(fake.Calls()); n != 0 {
		t.Fatalf("an invalid request must not reach the database, ran %d statements", n)
	}

	m, fake = newManager()
	fake.On("INSERT INTO normalizers", fakedb.PGResult{Err: errors.New("duplicate key")})
	if _, err := m.Create(context.Background(), CreateRequest{Name: "x"}, "alice"); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("insert failure: got %v", err)
	}
}

func TestManagerUpdate(t *testing.T) {
	m, fake := newManager()
	fake.On("UPDATE normalizers", fakedb.Row(sysmonRow()...))
	req := UpdateRequest{
		Name:          "sysmon",
		ValueMappings: []ValueMapping{{FromField: "a", ToField: "b", Default: "x"}},
	}
	if _, err := m.Update(context.Background(), "n-1", req); err != nil {
		t.Fatal(err)
	}
	c := fake.Matching("UPDATE normalizers")[0]
	if !strings.Contains(c.Query, "version = version + 1") {
		t.Errorf("an update must bump the version stamped on ingested logs: %s", c.Query)
	}
	if c.Args[4] != `[{"from_field":"a","to_field":"b","map":null,"default":"x"}]` || c.Args[5] != "[]" || c.Args[6] != "n-1" {
		t.Errorf("UPDATE args = %q", c.Args)
	}

	m, _ = newManager()
	if _, err := m.Update(context.Background(), "gone", UpdateRequest{Name: "x"}); err == nil || err.Error() != "normalizer not found" {
		t.Fatalf("missing row: got %v", err)
	}

	m, fake = newManager()
	if _, err := m.Update(context.Background(), "n-1", UpdateRequest{Name: "  "}); err == nil {
		t.Fatal("a blank name must be rejected")
	}
	if len(fake.Calls()) != 0 {
		t.Fatal("an invalid update must not reach the database")
	}

	m, fake = newManager()
	fake.On("UPDATE normalizers", fakedb.PGResult{Err: errors.New("deadlock")})
	if _, err := m.Update(context.Background(), "n-1", UpdateRequest{Name: "x"}); err == nil || !strings.Contains(err.Error(), "deadlock") {
		t.Fatalf("update failure: got %v", err)
	}
}

func TestManagerDelete(t *testing.T) {
	t.Run("deletes a non-default normalizer", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT is_default", fakedb.Row(false))
		fake.On("DELETE FROM normalizers", fakedb.Affected(1))
		if err := m.Delete(context.Background(), "n-1"); err != nil {
			t.Fatal(err)
		}
		if c := fake.Matching("DELETE FROM normalizers"); len(c) != 1 || c[0].Args[0] != "n-1" {
			t.Fatalf("DELETE calls = %+v", c)
		}
	})
	t.Run("refuses the default without deleting", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT is_default", fakedb.Row(true))
		err := m.Delete(context.Background(), "n-0")
		if err == nil || !strings.Contains(err.Error(), "default") {
			t.Fatalf("got %v", err)
		}
		if len(fake.Matching("DELETE")) != 0 {
			t.Fatal("the default normalizer must never reach DELETE")
		}
	})
	t.Run("missing", func(t *testing.T) {
		m, _ := newManager()
		if err := m.Delete(context.Background(), "gone"); err == nil || err.Error() != "normalizer not found" {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("vanished between check and delete", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT is_default", fakedb.Row(false))
		fake.On("DELETE FROM normalizers", fakedb.Affected(0))
		if err := m.Delete(context.Background(), "n-1"); err == nil || err.Error() != "normalizer not found" {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("check fails", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT is_default", fakedb.PGResult{Err: errors.New("down")})
		if err := m.Delete(context.Background(), "n-1"); err == nil || !strings.Contains(err.Error(), "down") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("delete fails", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT is_default", fakedb.Row(false))
		fake.On("DELETE FROM normalizers", fakedb.PGResult{Err: errors.New("fk violation")})
		if err := m.Delete(context.Background(), "n-1"); err == nil || !strings.Contains(err.Error(), "fk violation") {
			t.Fatalf("got %v", err)
		}
	})
}

// SetDefault must swap the flag atomically: unset the old default and set the new
// one in one transaction, so there is never a moment with zero or two defaults.
func TestManagerSetDefaultIsOneTransaction(t *testing.T) {
	m, fake := newManager()
	fake.On("SELECT EXISTS", fakedb.Row(true))
	if err := m.SetDefault(context.Background(), "n-1"); err != nil {
		t.Fatal(err)
	}
	updates := fake.Matching("UPDATE normalizers SET is_default")
	if len(updates) != 2 {
		t.Fatalf("expected unset + set, got %+v", updates)
	}
	if !strings.Contains(updates[0].Query, "is_default = false WHERE is_default = true") ||
		!strings.Contains(updates[1].Query, "is_default = true WHERE id = $1") || updates[1].Args[0] != "n-1" {
		t.Fatalf("wrong statements or order: %+v", updates)
	}
	for _, u := range updates {
		if !u.InTx {
			t.Fatalf("statement ran outside the transaction: %s", u.Query)
		}
	}
	if _, _, commits, _ := fake.Stats(); commits != 1 {
		t.Fatalf("commits = %d, want 1", commits)
	}
}

func TestManagerSetDefaultFailures(t *testing.T) {
	t.Run("unknown id never opens a transaction", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT EXISTS", fakedb.Row(false))
		if err := m.SetDefault(context.Background(), "gone"); err == nil || err.Error() != "normalizer not found" {
			t.Fatalf("got %v", err)
		}
		if len(fake.Matching("UPDATE")) != 0 {
			t.Fatal("no UPDATE may run for an unknown id")
		}
	})
	t.Run("second update fails and rolls back", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT EXISTS", fakedb.Row(true))
		fake.On("is_default = true WHERE id", fakedb.PGResult{Err: errors.New("lock timeout")})
		if err := m.SetDefault(context.Background(), "n-1"); err == nil || !strings.Contains(err.Error(), "set default") {
			t.Fatalf("got %v", err)
		}
		if _, _, commits, rollbacks := fake.Stats(); commits != 0 || rollbacks != 1 {
			t.Fatalf("commits=%d rollbacks=%d: the unset must be rolled back", commits, rollbacks)
		}
	})
	t.Run("first update fails", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT EXISTS", fakedb.Row(true))
		fake.On("is_default = false", fakedb.PGResult{Err: errors.New("x")})
		if err := m.SetDefault(context.Background(), "n-1"); err == nil || !strings.Contains(err.Error(), "unset default") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("begin fails", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT EXISTS", fakedb.Row(true))
		fake.FailBegin = errors.New("no conns")
		if err := m.SetDefault(context.Background(), "n-1"); err == nil || !strings.Contains(err.Error(), "begin") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("commit fails", func(t *testing.T) {
		m, fake := newManager()
		fake.On("SELECT EXISTS", fakedb.Row(true))
		fake.FailCommit = errors.New("serialization failure")
		if err := m.SetDefault(context.Background(), "n-1"); err == nil || !strings.Contains(err.Error(), "serialization") {
			t.Fatalf("got %v", err)
		}
	})
}

func TestValidateRequest(t *testing.T) {
	ok := []FieldMapping{{Sources: []string{"a"}, Target: "b"}}
	cases := []struct {
		name    string
		reqName string
		tr      []Transform
		fm      []FieldMapping
		vm      []ValueMapping
		wantErr string
	}{
		{"valid", "n", []Transform{TransformFlattenFull, TransformDedot}, ok,
			[]ValueMapping{{FromField: "a", ToField: "c", Map: map[string]string{"1": "x"}}}, ""},
		{"default-only value map is valid", "n", nil, nil,
			[]ValueMapping{{FromField: "a", ToField: "c", Default: "x"}}, ""},
		{"blank name", " \t", nil, nil, nil, "name is required"},
		{"unknown transform", "n", []Transform{"Title_Case"}, nil, nil, "invalid transform: Title_Case"},
		{"mapping without sources", "n", nil, []FieldMapping{{Target: "b"}}, nil, "at least one source"},
		{"mapping without target", "n", nil, []FieldMapping{{Sources: []string{"a"}, Target: " "}}, nil, "target is required"},
		{"nested path without flatten", "n", nil, []FieldMapping{{Sources: []string{"$.a.b"}, Target: "x"}}, nil, "needs a flatten"},
		{"value map without source", "n", nil, nil, []ValueMapping{{ToField: "c", Default: "x"}}, "requires a source field"},
		{"value map without target", "n", nil, nil, []ValueMapping{{FromField: "a", Default: "x"}}, "requires a target field"},
		{"value map onto itself", "n", nil, nil, []ValueMapping{{FromField: "a ", ToField: " a", Default: "x"}}, "must differ"},
		{"value map that can never write", "n", nil, nil, []ValueMapping{{FromField: "a", ToField: "c"}}, "at least one value mapping or a default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRequest(tc.reqName, tc.tr, tc.fm, tc.vm)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}

// Every transform the API schema advertises must be accepted by validation, and
// vice versa, or the editor offers a choice the server then rejects.
func TestTransformEnumMatchesValidTransforms(t *testing.T) {
	values := Transform("").EnumValues()
	if len(values) != len(ValidTransforms) {
		t.Fatalf("EnumValues lists %d transforms, ValidTransforms %d", len(values), len(ValidTransforms))
	}
	for _, v := range values {
		if !ValidTransforms[Transform(v)] {
			t.Errorf("EnumValues advertises %q, which validation rejects", v)
		}
	}
}

func TestCompileByID(t *testing.T) {
	m, fake := newManager()
	if m.CompileByID(context.Background(), "") != nil {
		t.Fatal("an empty id means no normalizer")
	}
	if len(fake.Calls()) != 0 {
		t.Fatal("an empty id must not query")
	}
	if m.CompileByID(context.Background(), "gone") != nil {
		t.Fatal("a missing normalizer compiles to nil")
	}

	fake.On("WHERE id = $1", fakedb.Row(sysmonRow()...))
	c := m.CompileByID(context.Background(), "n-1")
	if c == nil {
		t.Fatal("expected a compiled normalizer")
	}
	if c.Stamp() != "sysmon@3" {
		t.Errorf("Stamp = %q", c.Stamp())
	}
	if got := c.ApplyFieldName("Computer"); got != "computer_name" {
		t.Errorf("ApplyFieldName(Computer) = %q", got)
	}
}

// CompileFromRaw is the ingest hot path's compile from the token's joined JSONB
// columns: it must produce the same normalizer the editor saved.
func TestCompileFromRaw(t *testing.T) {
	if CompileFromRaw("x", 1, nil, nil, []byte(`[]`), nil) != nil {
		t.Fatal("a token with no normalizer (both NULL) must compile to nil")
	}

	row := sysmonRow()
	c := CompileFromRaw("sysmon", 7,
		[]byte(row[3].(string)), []byte(row[4].(string)), []byte(row[5].(string)), []byte(row[6].(string)))
	if c.Stamp() != "sysmon@7" {
		t.Errorf("Stamp = %q", c.Stamp())
	}
	out := c.ApplyTransforms(map[string]string{"Computer": "WS01", "EventID": "1"})
	want := map[string]string{"computer_name": "WS01", "event_id": "1", "category": "process_creation"}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("compiled normalizer output %v, want %v", out, want)
	}
	if len(c.TimestampFields) != 1 || c.TimestampFields[0].Field != "UtcTime" {
		t.Errorf("timestamp fields = %+v", c.TimestampFields)
	}

	// Mappings only (transforms NULL) is still a normalizer.
	if CompileFromRaw("m", 1, nil, []byte(`[{"sources":["a"],"target":"b"}]`), nil, nil) == nil {
		t.Fatal("mappings without transforms must still compile")
	}
}

func TestStamp(t *testing.T) {
	var nilNorm *CompiledNormalizer
	if nilNorm.Stamp() != "" {
		t.Error("a nil normalizer stamps nothing")
	}
	if (&CompiledNormalizer{Version: 2}).Stamp() != "" {
		t.Error("an unnamed normalizer stamps nothing")
	}
	if got := (&Normalizer{Name: "edr", Version: 12}).Compile().Stamp(); got != "edr@12" {
		t.Errorf("Stamp = %q", got)
	}
}

func TestGetTokenUsage(t *testing.T) {
	m, fake := newManager()
	tokens, err := m.GetTokenUsage(context.Background(), "n-1")
	if err != nil {
		t.Fatal(err)
	}
	if tokens == nil || len(tokens) != 0 {
		t.Fatalf("no tokens must be an empty list, not nil: %#v", tokens)
	}

	fake.On("FROM ingest_tokens", fakedb.RowsOf(
		[]driver.Value{"t-1", "edr", "f-1", "Prod"},
		[]driver.Value{"t-2", "syslog", "f-2", "f-2"},
	))
	tokens, err = m.GetTokenUsage(context.Background(), "n-1")
	if err != nil {
		t.Fatal(err)
	}
	want := []TokenUsageInfo{
		{TokenID: "t-1", TokenName: "edr", FractalID: "f-1", FractalName: "Prod"},
		{TokenID: "t-2", TokenName: "syslog", FractalID: "f-2", FractalName: "f-2"},
	}
	if !reflect.DeepEqual(tokens, want) {
		t.Fatalf("tokens = %+v", tokens)
	}
	if c := fake.Matching("FROM ingest_tokens"); c[len(c)-1].Args[0] != "n-1" {
		t.Fatalf("normalizer id not bound: %+v", c)
	}

	m, fake = newManager()
	fake.On("FROM ingest_tokens", fakedb.PGResult{Err: errors.New("x")})
	if _, err := m.GetTokenUsage(context.Background(), "n-1"); err == nil {
		t.Fatal("query error must surface")
	}
}

func TestDuplicatePicksUniqueNameAndCopiesDefinition(t *testing.T) {
	m, fake := newManager()
	fake.On("WHERE id = $1", fakedb.Row(sysmonRow()...))
	// "sysmon (Copy)" and "sysmon (Copy) (2)" are taken; "(3)" is free.
	fake.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
		if !strings.Contains(q, "WHERE name = $1") {
			return fakedb.PGResult{}, false
		}
		taken := args[0] == "sysmon (Copy)" || args[0] == "sysmon (Copy) (2)"
		return fakedb.Row(taken), true
	})
	fake.On("INSERT INTO normalizers", fakedb.Row(sysmonRow()...))

	if _, err := m.Duplicate(context.Background(), "n-1", "bob"); err != nil {
		t.Fatal(err)
	}
	ins := fake.Matching("INSERT INTO normalizers")
	if len(ins) != 1 {
		t.Fatalf("expected one INSERT, got %d", len(ins))
	}
	args := ins[0].Args
	if args[0] != "sysmon (Copy) (3)" {
		t.Errorf("duplicate name = %q, want %q", args[0], "sysmon (Copy) (3)")
	}
	if args[6] != "bob" {
		t.Errorf("created_by = %q, want the duplicating user", args[6])
	}
	row := sysmonRow()
	for i, col := range []int{3, 4, 5, 6} {
		if !jsonEqual(t, args[2+i].(string), row[col].(string)) {
			t.Errorf("column %d not copied: got %s want %s", col, args[2+i], row[col])
		}
	}
}

func TestDuplicateFailures(t *testing.T) {
	m, _ := newManager()
	if _, err := m.Duplicate(context.Background(), "gone", "bob"); err == nil || !strings.Contains(err.Error(), "source normalizer not found") {
		t.Fatalf("got %v", err)
	}

	m, fake := newManager()
	fake.On("WHERE id = $1", fakedb.Row(sysmonRow()...))
	fake.On("WHERE name = $1", fakedb.PGResult{Err: errors.New("down")})
	if _, err := m.Duplicate(context.Background(), "n-1", "bob"); err == nil || !strings.Contains(err.Error(), "uniqueness") {
		t.Fatalf("got %v", err)
	}
	if len(fake.Matching("INSERT")) != 0 {
		t.Fatal("must not insert when the name check fails")
	}
}

func jsonEqual(t *testing.T, a, b string) bool {
	t.Helper()
	var va, vb interface{}
	if err := json.Unmarshal([]byte(a), &va); err != nil {
		t.Fatalf("bad JSON %q: %v", a, err)
	}
	if err := json.Unmarshal([]byte(b), &vb); err != nil {
		t.Fatalf("bad JSON %q: %v", b, err)
	}
	return reflect.DeepEqual(va, vb)
}
