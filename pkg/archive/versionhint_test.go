package archive

import "testing"

func TestVersionHintTarget(t *testing.T) {
	tests := []struct {
		name     string
		loc      string
		wantPath string
		wantHint string
		wantOK   bool
	}{
		{
			name:     "s3 uri",
			loc:      "s3://bucket/bifract.db/f_abc/metadata/00002-9224de36-03ae-49fa-af4c-e019f4608a5f.metadata.json",
			wantPath: "s3://bucket/bifract.db/f_abc/metadata/version-hint.text",
			wantHint: "00002-9224de36-03ae-49fa-af4c-e019f4608a5f",
			wantOK:   true,
		},
		{
			name:     "scheme double slash preserved",
			loc:      "abfss://c@a.dfs.core.windows.net/w/bifract.db/f_x/metadata/00013-uuid.metadata.json",
			wantPath: "abfss://c@a.dfs.core.windows.net/w/bifract.db/f_x/metadata/version-hint.text",
			wantHint: "00013-uuid",
			wantOK:   true,
		},
		{
			name:     "five digit version",
			loc:      "file:///var/lib/bifract/archive/bifract.db/f_x/metadata/01837-ae1211fc-c7c4-43fe-9420-e688bd14755c.metadata.json",
			wantPath: "file:///var/lib/bifract/archive/bifract.db/f_x/metadata/version-hint.text",
			wantHint: "01837-ae1211fc-c7c4-43fe-9420-e688bd14755c",
			wantOK:   true,
		},
		{
			name:     "local path version zero",
			loc:      "/var/lib/warehouse/bifract.db/f_x/metadata/00000-uuid.metadata.json",
			wantPath: "/var/lib/warehouse/bifract.db/f_x/metadata/version-hint.text",
			wantHint: "00000-uuid",
			wantOK:   true,
		},
		{name: "no separator", loc: "00002-uuid.metadata.json"},
		{name: "wrong suffix", loc: "s3://b/t/metadata/00002-uuid.json"},
		{name: "no version component", loc: "s3://b/t/metadata/metadata.json"},
		{name: "non-numeric version", loc: "s3://b/t/metadata/vNext-uuid.metadata.json"},
		{name: "empty version", loc: "s3://b/t/metadata/-uuid.metadata.json"},
		{name: "suffix only", loc: "s3://b/t/metadata/.metadata.json"},
		{name: "empty", loc: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotHint, gotOK := versionHintTarget(tt.loc)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if gotPath != tt.wantPath {
				t.Errorf("path = %q, want %q", gotPath, tt.wantPath)
			}
			if gotHint != tt.wantHint {
				t.Errorf("hint = %q, want %q", gotHint, tt.wantHint)
			}
		})
	}
}

// The hint must reconstruct the exact metadata file name a reader resolves it
// into, since that substitution is the whole mechanism.
func TestVersionHintRoundTrip(t *testing.T) {
	loc := "s3://b/bifract.db/f_x/metadata/00006-c3f9b266-75ec-4f57-b378-46d577430124.metadata.json"
	_, hint, ok := versionHintTarget(loc)
	if !ok {
		t.Fatal("expected a hint")
	}
	if got := "s3://b/bifract.db/f_x/metadata/" + hint + ".metadata.json"; got != loc {
		t.Errorf("round trip = %q, want %q", got, loc)
	}
}
