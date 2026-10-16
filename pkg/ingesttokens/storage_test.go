package ingesttokens

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func TestGenerateToken(t *testing.T) {
	format := regexp.MustCompile(`^bifract_ingest_([0-9a-f]{32})$`)
	seen := map[string]bool{}
	prefixes := map[string]bool{}
	for i := 0; i < 200; i++ {
		full, prefix, err := GenerateToken()
		if err != nil {
			t.Fatal(err)
		}
		m := format.FindStringSubmatch(full)
		// 16 random bytes: 128 bits of entropy.
		if m == nil {
			t.Fatalf("token %q does not match bifract_ingest_<32 hex>", full)
		}
		// The displayed prefix is a strict prefix of the secret, never all of it.
		if prefix != m[1][:12] {
			t.Fatalf("prefix %q, want first 12 chars of %q", prefix, m[1])
		}
		if seen[full] {
			t.Fatalf("duplicate token after %d draws", i)
		}
		seen[full] = true
		prefixes[prefix] = true
	}
	// token_prefix is UNIQUE in the schema; 48-bit prefixes must not collide here.
	if len(prefixes) != 200 {
		t.Errorf("%d distinct prefixes from 200 tokens", len(prefixes))
	}
}

func TestHashToken(t *testing.T) {
	sum := sha256.Sum256([]byte("bifract_ingest_abc"))
	if got := HashToken("bifract_ingest_abc"); got != hex.EncodeToString(sum[:]) {
		t.Fatalf("HashToken = %q, want SHA-256 hex", got)
	}
}

func TestCreateToken(t *testing.T) {
	s := func(db *tokenDB) *Storage { return NewStorage(db.client) }

	t.Run("lookup key is the hash of the returned token", func(t *testing.T) {
		db := newTokenDB()
		tok, full, err := s(db).CreateToken(context.Background(), CreateTokenRequest{Name: "edge"}, fractalA, "alice")
		if err != nil {
			t.Fatal(err)
		}
		args := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args
		if args[3] != HashToken(full) {
			t.Errorf("token_hash = %v, want hash of returned token", args[3])
		}
		if args[2] != tok.TokenPrefix || !strings.HasPrefix(full, "bifract_ingest_"+tok.TokenPrefix) {
			t.Errorf("prefix %v does not lead token %q", args[2], full)
		}
		if args[5] != fractalA {
			t.Errorf("fractal = %v", args[5])
		}
		if args[9] != "alice" {
			t.Errorf("created_by = %v", args[9])
		}
		// token_value keeps the plaintext so the tenant-admin-only UI can show it
		// again. This is a deliberate exception to hash-only storage; see the
		// ingest token page's reveal button.
		if args[4] != full || tok.TokenValue != full {
			t.Errorf("token_value = %v", args[4])
		}
	})

	t.Run("parser type", func(t *testing.T) {
		for _, tt := range []struct{ in, want string }{{"", "json"}, {"json", "json"}, {"kv", "kv"}, {"syslog", "syslog"}} {
			db := newTokenDB()
			if _, _, err := s(db).CreateToken(context.Background(), CreateTokenRequest{Name: "x", ParserType: tt.in}, fractalA, "a"); err != nil {
				t.Fatal(err)
			}
			if got := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args[6]; got != tt.want {
				t.Errorf("parser %q bound as %v, want %q", tt.in, got, tt.want)
			}
		}
		for _, bad := range []string{"xml", "JSON", "csv"} {
			db := newTokenDB()
			if _, _, err := s(db).CreateToken(context.Background(), CreateTokenRequest{Name: "x", ParserType: bad}, fractalA, "a"); err == nil {
				t.Errorf("parser %q accepted", bad)
			}
			if len(db.writes()) != 0 {
				t.Errorf("parser %q wrote a token", bad)
			}
		}
	})

	t.Run("normalizer selection", func(t *testing.T) {
		for _, tt := range []struct {
			name        string
			req         CreateTokenRequest
			defaultNorm string
			want        any
			wantLookup  bool
		}{
			{name: "explicit", req: CreateTokenRequest{NormalizerID: ptr("norm-1")}, defaultNorm: "norm-default", want: "norm-1"},
			{name: "default", req: CreateTokenRequest{}, defaultNorm: "norm-default", want: "norm-default", wantLookup: true},
			{name: "no default exists", req: CreateTokenRequest{}, want: nil, wantLookup: true},
			// Clearing wins even over an explicit id, and skips the default.
			{name: "cleared", req: CreateTokenRequest{ClearNormalizer: true, NormalizerID: ptr("norm-1")}, defaultNorm: "norm-default", want: nil},
		} {
			t.Run(tt.name, func(t *testing.T) {
				db := newTokenDB()
				db.defaultNormalizer = tt.defaultNorm
				tt.req.Name = "x"
				tok, _, err := s(db).CreateToken(context.Background(), tt.req, fractalA, "a")
				if err != nil {
					t.Fatal(err)
				}
				if got := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args[7]; got != tt.want {
					t.Errorf("normalizer_id = %#v, want %#v", got, tt.want)
				}
				if looked := len(db.pg.Matching("FROM normalizers WHERE is_default")) > 0; looked != tt.wantLookup {
					t.Errorf("default lookup = %v, want %v", looked, tt.wantLookup)
				}
				if tt.want != nil && (tok.NormalizerID == nil || *tok.NormalizerID != tt.want || tok.NormalizerName == "") {
					t.Errorf("returned normalizer = %v %q", tok.NormalizerID, tok.NormalizerName)
				}
			})
		}
	})

	t.Run("timestamp fields", func(t *testing.T) {
		db := newTokenDB()
		tok, _, err := s(db).CreateToken(context.Background(), CreateTokenRequest{Name: "x"}, fractalA, "a")
		if err != nil {
			t.Fatal(err)
		}
		if got := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args[8]; got != "[]" {
			t.Errorf("nil fields bound as %v, want []", got)
		}
		if tok.TimestampFields == nil || len(tok.TimestampFields) != 0 {
			t.Errorf("returned fields = %#v", tok.TimestampFields)
		}

		db = newTokenDB()
		fields := []TsField{{Field: "ts", Format: "unix"}}
		tok, _, _ = s(db).CreateToken(context.Background(), CreateTokenRequest{Name: "x", TimestampFields: fields}, fractalA, "a")
		if got := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args[8]; got != `[{"field":"ts","format":"unix"}]` {
			t.Errorf("fields bound as %v", got)
		}
		if len(tok.TimestampFields) != 1 || tok.TimestampFields[0] != fields[0] {
			t.Errorf("returned fields = %#v", tok.TimestampFields)
		}
	})

	t.Run("no attributable user binds NULL", func(t *testing.T) {
		db := newTokenDB()
		if _, _, err := s(db).CreateToken(context.Background(), CreateTokenRequest{Name: "x"}, fractalA, ""); err != nil {
			t.Fatal(err)
		}
		if got := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args[9]; got != nil {
			t.Errorf("created_by = %#v, want NULL", got)
		}
	})

	t.Run("insert failure", func(t *testing.T) {
		db := newTokenDB()
		db.failTokens["INSERT"] = errors.New("unique violation")
		if _, full, err := s(db).CreateToken(context.Background(), CreateTokenRequest{Name: "x"}, fractalA, "a"); err == nil || full != "" {
			t.Fatalf("CreateToken = (%q, %v), want error and no token", full, err)
		}
	})
}

func TestCreateDefaultToken(t *testing.T) {
	db := newTokenDB()
	db.defaultNormalizer = "norm-default"
	tok, full, err := NewStorage(db.client).CreateDefaultToken(context.Background(), fractalA, "alpha", "bob")
	if err != nil {
		t.Fatal(err)
	}
	args := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args
	if args[3] != HashToken(full) || args[6] != "norm-default" || args[7] != "bob" {
		t.Errorf("args = %v", args)
	}
	if !tok.IsDefault || tok.Name != "Default" || tok.ParserType != "json" {
		t.Errorf("token = %+v", tok)
	}

	// created_by references users(username): a fractal with no recorded creator
	// must bind NULL, not "", or the insert violates the foreign key.
	db = newTokenDB()
	if _, _, err := NewStorage(db.client).CreateDefaultToken(context.Background(), fractalA, "alpha", ""); err != nil {
		t.Fatal(err)
	}
	if got := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args[7]; got != nil {
		t.Errorf("created_by = %#v, want NULL", got)
	}

	db = newTokenDB()
	db.failTokens["INSERT"] = errors.New("boom")
	if _, _, err := NewStorage(db.client).CreateDefaultToken(context.Background(), fractalA, "alpha", "bob"); err == nil {
		t.Error("insert failure swallowed")
	}
}

func TestEnsureDefaultTokens(t *testing.T) {
	db := newTokenDB()
	db.fractals = []fractalRow{
		{id: fractalA, name: "has-token", createdBy: "alice"},
		{id: fractalB, name: "bare", createdBy: "bob"},
		{id: "33333333-3333-3333-3333-333333333333", name: "orphan"},
	}
	db.add(tokenRow{id: "existing", fractalID: fractalA, active: false})
	s := NewStorage(db.client)
	if err := s.EnsureDefaultTokens(context.Background(), newTestHandler(db, nil).fractalManager); err != nil {
		t.Fatal(err)
	}
	inserts := db.pg.Matching("INSERT INTO ingest_tokens")
	var got []string
	for _, c := range inserts {
		got = append(got, fmt.Sprintf("%v by %v", c.Args[5], c.Args[7]))
	}
	// A fractal that has any token, even an inactive one, is left alone. One
	// with no recorded creator falls back to "admin".
	want := []string{fractalB + " by bob", "33333333-3333-3333-3333-333333333333 by admin"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("created %v, want %v", got, want)
	}

	// Running again is a no-op: every fractal now has a token.
	if err := s.EnsureDefaultTokens(context.Background(), newTestHandler(db, nil).fractalManager); err != nil {
		t.Fatal(err)
	}
	if n := len(db.pg.Matching("INSERT INTO ingest_tokens")); n != 2 {
		t.Fatalf("second run inserted again: %d inserts", n)
	}

	// A failed count for one fractal skips it rather than creating a duplicate default.
	db = newTokenDB()
	db.failTokens["SELECT COUNT(*)"] = errors.New("timeout")
	if err := NewStorage(db.client).EnsureDefaultTokens(context.Background(), newTestHandler(db, nil).fractalManager); err != nil {
		t.Fatal(err)
	}
	if n := len(db.writes()); n != 0 {
		t.Fatalf("created %d tokens without knowing the count", n)
	}

	db = newTokenDB()
	db.failList = errors.New("down")
	if err := NewStorage(db.client).EnsureDefaultTokens(context.Background(), newTestHandler(db, nil).fractalManager); err == nil {
		t.Fatal("fractal listing failure swallowed")
	}
}

func TestValidateToken(t *testing.T) {
	db := newTokenDB()
	db.add(tokenRow{id: "plain", hash: HashToken("bifract_ingest_plain"), fractalID: fractalA, parser: "kv", tsFields: `[{"field":"t","format":"rfc3339"}]`, active: true})
	db.add(tokenRow{id: "normed", hash: HashToken("bifract_ingest_normed"), fractalID: fractalB, normalizerID: "norm-1", tsFields: "[]", active: true})
	db.add(tokenRow{id: "off", hash: HashToken("bifract_ingest_off"), fractalID: fractalA, active: false})
	s := NewStorage(db.client)

	v, err := s.ValidateToken(context.Background(), "bifract_ingest_plain")
	if err != nil {
		t.Fatal(err)
	}
	// The token decides the fractal its logs land in.
	if v.TokenID != "plain" || v.FractalID != fractalA || v.ParserType != "kv" || v.Normalizer != nil {
		t.Errorf("validated = %+v", v)
	}
	if len(v.TimestampFields) != 1 || v.TimestampFields[0].Field != "t" {
		t.Errorf("timestamp fields = %+v", v.TimestampFields)
	}
	c := db.pg.Matching("t.token_hash = $1")[0]
	if c.Args[0] != HashToken("bifract_ingest_plain") || !strings.Contains(c.Query, "t.is_active = true") {
		t.Errorf("lookup = %q %v, want by hash and active only", c.Query, c.Args)
	}

	v, err = s.ValidateToken(context.Background(), "bifract_ingest_normed")
	if err != nil {
		t.Fatal(err)
	}
	if v.FractalID != fractalB || v.Normalizer == nil || v.Normalizer.Name != "ecs" || v.Normalizer.Version != 3 {
		t.Errorf("validated = %+v normalizer %+v", v, v.Normalizer)
	}

	for _, raw := range []string{"bifract_ingest_off", "bifract_ingest_unknown", ""} {
		if v, err := s.ValidateToken(context.Background(), raw); !errors.Is(err, ErrTokenRejected) || v != nil {
			t.Errorf("%q: ValidateToken = (%v, %v), want ErrTokenRejected", raw, v, err)
		}
	}

	// A database failure is not a rejection: the sender should retry, not drop.
	db.failTokens["token_hash"] = errors.New("connection reset")
	if _, err := s.ValidateToken(context.Background(), "bifract_ingest_plain"); err == nil || errors.Is(err, ErrTokenRejected) {
		t.Errorf("err = %v, want a retryable error", err)
	}
}

func TestDeleteToken(t *testing.T) {
	tests := []struct {
		name    string
		rows    []tokenRow
		del     string
		fractal string
		wantErr string
	}{
		{name: "default token is permanent", rows: []tokenRow{{id: "d", fractalID: fractalA, active: true, isDefault: true}, {id: "o", fractalID: fractalA, active: true}}, del: "d", fractal: fractalA, wantErr: "default"},
		{name: "last active token", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "i", fractalID: fractalA}}, del: "a", fractal: fractalA, wantErr: "last active"},
		// Another fractal's active token does not count towards this one's.
		{name: "last active here, active elsewhere", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "b", fractalID: fractalB, active: true}}, del: "a", fractal: fractalA, wantErr: "last active"},
		{name: "one of two active", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "b", fractalID: fractalA, active: true}}, del: "a", fractal: fractalA},
		{name: "inactive token", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "i", fractalID: fractalA}}, del: "i", fractal: fractalA},
		{name: "unknown", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}}, del: "zz", fractal: fractalA, wantErr: "not found"},
		{name: "another fractal's token", rows: []tokenRow{{id: "b1", fractalID: fractalB, active: true}, {id: "b2", fractalID: fractalB, active: true}}, del: "b1", fractal: fractalA, wantErr: "not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTokenDB()
			for _, r := range tt.rows {
				db.add(r)
			}
			err := NewStorage(db.client).DeleteToken(context.Background(), tt.del, tt.fractal)
			_, still := db.get(tt.del)
			if tt.wantErr == "" {
				if err != nil || still {
					t.Fatalf("err = %v, still present = %v", err, still)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
			if n := len(db.pg.Matching("DELETE FROM ingest_tokens")); n != 0 {
				t.Fatalf("refused delete still issued %d DELETE", n)
			}
		})
	}

	t.Run("count failure refuses", func(t *testing.T) {
		db := newTokenDB()
		db.add(tokenRow{id: "a", fractalID: fractalA, active: true})
		db.add(tokenRow{id: "b", fractalID: fractalA, active: true})
		db.failTokens["COUNT(*)"] = errors.New("timeout")
		if err := NewStorage(db.client).DeleteToken(context.Background(), "a", fractalA); err == nil {
			t.Fatal("deleted without knowing whether it was the last active token")
		}
		if _, ok := db.get("a"); !ok {
			t.Fatal("token deleted")
		}
	})
}

func TestToggleToken(t *testing.T) {
	tests := []struct {
		name       string
		rows       []tokenRow
		id         string
		fractal    string
		failCount  bool
		wantActive bool
		wantErr    string
	}{
		{name: "deactivate one of two", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "b", fractalID: fractalA, active: true}}, id: "a", fractal: fractalA, wantActive: false},
		{name: "last active stays on", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "i", fractalID: fractalA}}, id: "a", fractal: fractalA, wantErr: "last active"},
		{name: "reactivate", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "i", fractalID: fractalA}}, id: "i", fractal: fractalA, wantActive: true},
		// Without a count the toggle cannot tell whether it would cut off ingest.
		{name: "count failure refuses deactivation", rows: []tokenRow{{id: "a", fractalID: fractalA, active: true}, {id: "b", fractalID: fractalA, active: true}}, id: "a", fractal: fractalA, failCount: true, wantErr: "last active"},
		{name: "another fractal's token", rows: []tokenRow{{id: "b", fractalID: fractalB, active: true}, {id: "c", fractalID: fractalB, active: true}}, id: "b", fractal: fractalA, wantErr: "not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTokenDB()
			for _, r := range tt.rows {
				db.add(r)
			}
			if tt.failCount {
				db.failTokens["COUNT(*)"] = errors.New("timeout")
			}
			before, _ := db.get(tt.id)
			tok, err := NewStorage(db.client).ToggleToken(context.Background(), tt.id, tt.fractal)
			after, _ := db.get(tt.id)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				if after != before {
					t.Fatal("refused toggle changed the token")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tok.IsActive != tt.wantActive || after.active != tt.wantActive {
				t.Fatalf("active = %v (stored %v), want %v", tok.IsActive, after.active, tt.wantActive)
			}
		})
	}
}

func TestUpdateToken(t *testing.T) {
	seed := func() *tokenDB {
		db := newTokenDB()
		db.add(tokenRow{id: "a", fractalID: fractalA, active: true})
		return db
	}

	t.Run("binds in order and stays in scope", func(t *testing.T) {
		db := seed()
		req := UpdateTokenRequest{
			Name:            ptr("n"),
			Description:     ptr("d"),
			ParserType:      ptr("syslog"),
			NormalizerID:    ptr("norm-1"),
			TimestampFields: []TsField{{Field: "ts", Format: "unix"}},
		}
		if _, err := NewStorage(db.client).UpdateToken(context.Background(), "a", fractalA, req); err != nil {
			t.Fatal(err)
		}
		c := db.pg.Matching("UPDATE ingest_tokens SET")[0]
		want := "UPDATE ingest_tokens SET name = $1, description = $2, parser_type = $3, normalizer_id = $4, timestamp_fields = $5 WHERE id = $6 AND fractal_id = $7"
		if c.Query != want {
			t.Errorf("query =\n%s\nwant\n%s", c.Query, want)
		}
		if fmt.Sprint(c.Args) != fmt.Sprint([]any{"n", "d", "syslog", "norm-1", `[{"field":"ts","format":"unix"}]`, "a", fractalA}) {
			t.Errorf("args = %v", c.Args)
		}
	})

	t.Run("clearing the normalizer binds no id", func(t *testing.T) {
		db := seed()
		req := UpdateTokenRequest{ClearNormalizer: true, NormalizerID: ptr("norm-1")}
		if _, err := NewStorage(db.client).UpdateToken(context.Background(), "a", fractalA, req); err != nil {
			t.Fatal(err)
		}
		c := db.pg.Matching("UPDATE ingest_tokens SET")[0]
		if c.Query != "UPDATE ingest_tokens SET normalizer_id = NULL WHERE id = $1 AND fractal_id = $2" {
			t.Errorf("query = %s", c.Query)
		}
	})

	for _, tt := range []struct {
		name string
		req  UpdateTokenRequest
	}{
		{"nothing to update", UpdateTokenRequest{}},
		{"bad parser", UpdateTokenRequest{ParserType: ptr("xml")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := seed()
			if _, err := NewStorage(db.client).UpdateToken(context.Background(), "a", fractalA, tt.req); err == nil {
				t.Fatal("want error")
			}
			if len(db.writes()) != 0 {
				t.Fatal("refused update wrote")
			}
		})
	}

	t.Run("another fractal's token reads back as not found", func(t *testing.T) {
		db := seed()
		if _, err := NewStorage(db.client).UpdateToken(context.Background(), "a", fractalB, UpdateTokenRequest{Name: ptr("x")}); err == nil {
			t.Fatal("update through another fractal reported success")
		}
	})
}

func TestListAndGetTokens(t *testing.T) {
	db := newTokenDB()
	db.add(tokenRow{id: "a1", fractalID: fractalA, active: true, normalizerID: "norm-1"})
	db.add(tokenRow{id: "a2", fractalID: fractalA})
	db.add(tokenRow{id: "b1", fractalID: fractalB, active: true})
	s := NewStorage(db.client)

	list, err := s.ListTokens(context.Background(), fractalA)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != "a1" || list[1].ID != "a2" {
		t.Fatalf("list = %+v", list)
	}
	if list[0].NormalizerID == nil || *list[0].NormalizerID != "norm-1" || list[0].NormalizerName != "ecs" || list[1].NormalizerID != nil {
		t.Errorf("normalizers = %v/%q, %v", list[0].NormalizerID, list[0].NormalizerName, list[1].NormalizerID)
	}

	if _, err := s.GetToken(context.Background(), "b1", fractalA); err == nil {
		t.Error("read another fractal's token")
	}
	got, err := s.GetToken(context.Background(), "a1", fractalA)
	if err != nil || got.ID != "a1" {
		t.Errorf("GetToken = %+v, %v", got, err)
	}

	db.failTokens["SELECT t.id"] = errors.New("down")
	if _, err := s.ListTokens(context.Background(), fractalA); err == nil {
		t.Error("list failure swallowed")
	}
	if _, err := s.GetToken(context.Background(), "a1", fractalA); err == nil || strings.Contains(err.Error(), "not found") {
		t.Errorf("get failure reported as %v", err)
	}
}

func TestUpdateUsageStats(t *testing.T) {
	db := newTokenDB()
	if err := NewStorage(db.client).UpdateUsageStats(context.Background(), "a", 42); err != nil {
		t.Fatal(err)
	}
	c := db.pg.Matching("SET last_used_at")
	if len(c) != 1 || fmt.Sprint(c[0].Args) != "[a 42]" {
		t.Fatalf("calls = %+v", c)
	}
}
