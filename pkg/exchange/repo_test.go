package exchange

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"bifract/internal/ruletest"
	"bifract/pkg/alerts"
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

func TestRepoIndexItemsResolve(t *testing.T) {
	idx := loadRepoIndex(t)
	for _, it := range idx.Items {
		if it.Kind == KindLookup {
			continue
		}
		content, err := os.ReadFile(filepath.Join(repoExchange, filepath.FromSlash(it.Path)))
		if err != nil {
			t.Errorf("%s: %v", it.ID, err)
			continue
		}
		switch it.Kind {
		case KindNormalizer:
			var n normalizers.NormalizerExport
			if err := yaml.Unmarshal(content, &n); err != nil {
				t.Errorf("%s: %v", it.ID, err)
			}
			// A detection finds its normalizer on an install by this name.
			if n.Name != it.Name {
				t.Errorf("%s: index name %q differs from the normalizer's name %q", it.ID, it.Name, n.Name)
			}
		case KindPolicy:
			if _, err := alerts.ParsePolicyDocument(string(content)); err != nil {
				t.Errorf("%s: %v", it.ID, err)
			}
		case KindDetection:
			checkRepoDetection(t, idx, it)
		}
	}
}

// checkRepoDetection loads a detection as an import would, through the normalizer
// it names, and checks the index names it as the rule does: the UI shows an item
// as installed by that name.
func checkRepoDetection(t *testing.T, idx *Index, it Item) {
	t.Helper()
	var norm *normalizers.CompiledNormalizer
	if it.Normalizer != "" {
		var err error
		if norm, err = ruletest.LoadNormalizer(filepath.Join(repoExchange, idx.Find(it.Normalizer).Path)); err != nil {
			t.Errorf("%s: %v", it.ID, err)
			return
		}
	}
	rule, err := ruletest.LoadRule(filepath.Join(repoExchange, filepath.FromSlash(it.Path)), norm)
	if err != nil {
		t.Errorf("%s: %v", it.ID, err)
		return
	}
	if rule.Name != it.Name {
		t.Errorf("%s: index name %q differs from the rule's name %q", it.ID, it.Name, rule.Name)
	}
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
	case "detections":
		return path.Base(rel) == "rule.yml"
	case "normalizers", "policies":
		return ext == ".yaml" || ext == ".yml"
	}
	return false
}
