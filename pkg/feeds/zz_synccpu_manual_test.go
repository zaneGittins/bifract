//go:build synccpu

package feeds

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bifract/pkg/sigma"
)

// Measures the CPU cost of the per-rule work SyncFeed does, over a real repo.
// SIGMA_REPO=/path/to/sigma go test -tags synccpu -run TestSyncCPU ./pkg/feeds/ -v
func TestSyncCPU(t *testing.T) {
	root := os.Getenv("SIGMA_REPO")
	if root == "" {
		t.Skip("SIGMA_REPO unset")
	}
	var files []string
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(p))
		if ext == ".yml" || ext == ".yaml" {
			files = append(files, p)
		}
		return nil
	})
	s := &Syncer{}
	start := time.Now()
	var meta, translated, metaErr, transErr int
	for _, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		h := sha256.New()
		h.Write(content)
		h.Write([]byte(sigma.TranslatorVersion))
		_ = fmt.Sprintf("%x", h.Sum(nil))

		if _, err := parseRuleMetadata(string(content)); err != nil {
			metaErr++
			continue
		}
		meta++
		if _, _, _, _, _, _, _, _, err := s.parseRule(string(content), nil); err != nil {
			transErr++
			continue
		}
		translated++
	}
	t.Logf("files=%d meta_ok=%d meta_err=%d translated=%d translate_err=%d elapsed=%s",
		len(files), meta, metaErr, translated, transErr, time.Since(start))
}
