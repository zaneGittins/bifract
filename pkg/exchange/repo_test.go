package exchange

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"bifract/internal/ruletest"
	"bifract/pkg/alerts"
	"bifract/pkg/dashboards"
	"bifract/pkg/models"
	"bifract/pkg/normalizers"
)

// The exchange shipped in this repository is what every install reads by default,
// so it is checked here rather than by whoever installs a broken item.
const repoExchange = "../../exchange"

func loadRepoIndex(t *testing.T) *Index {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoExchange, "index.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	idx, err := ParseIndex(data)
	if err != nil {
		t.Fatalf("exchange/index.yaml: %v", err)
	}
	return idx
}

func readItem(it Item) ([]byte, error) {
	return os.ReadFile(filepath.Join(repoExchange, filepath.FromSlash(it.Path)))
}

// Each file loads as its import would, and is named in the index as it will be
// installed, since the UI shows an item as installed by that name.
func TestRepoIndexItemsResolve(t *testing.T) {
	idx := loadRepoIndex(t)
	for _, it := range idx.Items {
		if it.Kind == KindLookup || it.Kind == KindBundle {
			continue
		}
		content, err := readItem(it)
		if err != nil {
			t.Errorf("%s: %v", it.ID, err)
			continue
		}
		name, want := "", it.Name
		switch it.Kind {
		case KindNormalizer:
			var n normalizers.NormalizerExport
			err = yaml.Unmarshal(content, &n)
			name = n.Name
		case KindPolicy:
			_, err = alerts.ParsePolicyDocument(string(content))
			name = it.Name
		case KindDetection:
			name, err = repoDetectionName(idx, it)
		case KindModel:
			// A model is installed under its id, which BQL names in model=.
			name, err = models.ValidateImport(content)
			want = it.ID
		case KindSearch:
			var s *SavedSearch
			if s, err = ParseSearch(content); err == nil {
				name = s.Name
			}
		case KindDashboard:
			name, err = dashboards.ValidateImport(content)
		}
		if err != nil {
			t.Errorf("%s: %v", it.ID, err)
		} else if name != want {
			t.Errorf("%s: the index names it %q but it installs as %q", it.ID, want, name)
		}
	}
}

// repoDetectionName loads a detection as an import would, through the normalizer
// it requires, and returns its name.
func repoDetectionName(idx *Index, it Item) (string, error) {
	var norm *normalizers.CompiledNormalizer
	if n := idx.Normalizer(it); n != nil {
		var err error
		if norm, err = ruletest.LoadNormalizer(filepath.Join(repoExchange, n.Path)); err != nil {
			return "", err
		}
	}
	rule, err := ruletest.LoadRule(filepath.Join(repoExchange, filepath.FromSlash(it.Path)), norm)
	if err != nil {
		return "", err
	}
	return rule.Name, nil
}

var (
	dictRef  = regexp.MustCompile(`\bdict\s*=\s*"?([A-Za-z_][A-Za-z0-9_]*)`)
	modelRef = regexp.MustCompile(`\bmodel\s*=\s*"?([A-Za-z_][A-Za-z0-9_]*)`)
	tlshCall = regexp.MustCompile(`(?i)\btlsh\s*\(`)
)

// Every lookup and model a file names must be one it requires, directly or through
// another requirement, so adding the item alone leaves nothing missing.
func TestRepoReferencesAreRequired(t *testing.T) {
	idx := loadRepoIndex(t)
	for _, it := range idx.Items {
		if it.Kind == KindLookup || it.Kind == KindBundle {
			continue
		}
		content, err := readItem(it)
		if err != nil {
			t.Fatal(err)
		}
		provided := map[string]string{}
		for _, req := range idx.InstallOrder(it.ID) {
			if req.ID != it.ID {
				provided[req.ID] = req.Kind
			}
		}
		for _, m := range dictRef.FindAllStringSubmatch(string(content), -1) {
			if provided[m[1]] != KindLookup {
				t.Errorf("%s: uses the lookup %q but does not require it", it.ID, m[1])
			}
		}
		for _, m := range modelRef.FindAllStringSubmatch(string(content), -1) {
			if provided[m[1]] != KindModel {
				t.Errorf("%s: uses the model %q but does not require it", it.ID, m[1])
			}
		}
		// A model's own text may name tlsh(); only queries that call it need the index.
		if it.Kind != KindModel && tlshCall.Match(content) && !requiresTLSHIndex(t, idx, provided) {
			t.Errorf("%s: calls tlsh() but requires no TLSH index model", it.ID)
		}
	}
}

func requiresTLSHIndex(t *testing.T, idx *Index, provided map[string]string) bool {
	for id, kind := range provided {
		if kind != KindModel {
			continue
		}
		content, err := readItem(*idx.Find(id))
		if err != nil {
			t.Fatal(err)
		}
		var m struct {
			ModelType string `yaml:"model_type"`
		}
		if yaml.Unmarshal(content, &m) == nil && m.ModelType == "tlsh" {
			return true
		}
	}
	return false
}

// Every installable file in the exchange is listed, so nothing added to a folder
// is silently missing from the UI.
func TestRepoIndexListsEveryFile(t *testing.T) {
	idx := loadRepoIndex(t)
	listed := map[string]bool{}
	for _, it := range idx.Items {
		listed[it.Path] = true
	}
	err := filepath.WalkDir(repoExchange, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(repoExchange, p)
		rel = filepath.ToSlash(rel)
		if installable(rel) && !listed[rel] {
			t.Errorf("%s is not listed in exchange/index.yaml", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func installable(rel string) bool {
	top, _, _ := strings.Cut(rel, "/")
	ext := path.Ext(rel)
	switch top {
	case "detections", "normalizers", "policies", "models", "searches", "dashboards":
		return ext == ".yaml" || ext == ".yml"
	}
	return false
}

var safeName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// The exchange is reviewed by reading it, so it holds only plain YAML and Markdown:
// lowercase names, no links, and printable ASCII with newlines and tabs. That
// rules out control characters, CRLF, and the zero-width and bidirectional
// characters that can make text read differently from what it is.
func TestRepoFilesAreSafe(t *testing.T) {
	err := filepath.WalkDir(repoExchange, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(repoExchange, p)
		if rel == "." {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			t.Errorf("%s is a link; the exchange holds only plain files", rel)
			return nil
		}
		if !safeName.MatchString(d.Name()) && d.Name() != "README.md" {
			t.Errorf("%s: names use lowercase letters, digits, '.', '-' and '_'", rel)
		}
		if d.IsDir() {
			return nil
		}
		if ext := path.Ext(d.Name()); ext != ".yaml" && ext != ".yml" && ext != ".md" {
			t.Errorf("%s: only .yaml, .yml and .md files belong in the exchange", rel)
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if len(data) > maxFileBytes {
			t.Errorf("%s is larger than %d bytes", rel, maxFileBytes)
		}
		if msg := unsafeText(data); msg != "" {
			t.Errorf("%s: %s", rel, msg)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// unsafeText describes the first character outside printable ASCII, newline and
// tab, or returns "".
func unsafeText(data []byte) string {
	line, col := 1, 1
	for _, r := range string(data) {
		switch {
		case r == '\n':
			line, col = line+1, 0
		case r == '\t' || (r >= 0x20 && r <= 0x7e):
		default:
			return fmt.Sprintf("line %d, column %d: character %U is not printable ASCII", line, col, r)
		}
		col++
	}
	return ""
}

func TestUnsafeText(t *testing.T) {
	if msg := unsafeText([]byte("name: ok\n\tquery: '*'\n")); msg != "" {
		t.Errorf("plain text flagged: %s", msg)
	}
	for _, bad := range []string{"a\r\nb", "x​y", "safe ‮evil", "café", "bell\a"} {
		if unsafeText([]byte(bad)) == "" {
			t.Errorf("%q was not flagged", bad)
		}
	}
}
