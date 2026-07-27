package setup

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// freshK8sConfig is what --install-k8s builds for a given size profile, minus
// the interactive prompts.
func freshK8sConfig(p SizeProfile, dir string) *K8sConfig {
	cfg := &K8sConfig{
		SizeProfile: p,
		CHShards:    p.CHShards,
		OutputDir:   dir,
	}
	cfg.ImageTag = "v0.0.3"
	cfg.Domain = "bifract.example.com"
	cfg.PostgresPassword = "pgpw"
	cfg.ClickHousePassword = "chpw"
	cfg.IngestClickHousePassword = "ingestpw"
	cfg.IngestPostgresPassword = "ingestpgpw"
	cfg.PasswordPepper = "pepper"
	cfg.AdminPasswordHash = "hash"
	cfg.LiteLLMMasterKey = "llmkey"
	cfg.FeedEncryptionKey = strings.Repeat("a", 64)
	cfg.BackupEncryptionKey = strings.Repeat("b", 64)
	return cfg
}

// walkYAML calls fn for every rendered manifest file under dir.
func walkYAML(t *testing.T, dir string, fn func(path, content string)) {
	t.Helper()
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		fn(path, string(b))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
}

// A fresh --install-k8s has to produce a tree that applies cleanly on every size
// profile. The pre-existing YAML test renders templates with hardcoded fixture
// data, so it cannot catch a profile field that is wired wrong or left unset;
// this drives the real install path instead.
func TestFreshInstallK8sAllProfiles(t *testing.T) {
	for _, p := range sizeProfiles {
		t.Run(p.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := writeK8sManifests(freshK8sConfig(p, dir)); err != nil {
				t.Fatalf("writeK8sManifests: %v", err)
			}

			walkYAML(t, dir, func(path, content string) {
				rel, _ := filepath.Rel(dir, path)
				if strings.Contains(content, "<no value>") {
					t.Errorf("%s: unresolved template field (<no value>)", rel)
				}
				for i, doc := range strings.Split(content, "\n---") {
					if strings.TrimSpace(doc) == "" {
						continue
					}
					var parsed map[string]any
					if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
						t.Errorf("%s doc %d: invalid YAML: %v", rel, i, err)
					}
				}
			})
		})
	}
}

// Every secretKeyRef the manifests use must name a key the rendered secret
// actually defines. Severity differs and so does the assertion: a missing
// REQUIRED key blocks the pod from starting and fails here, while a missing
// OPTIONAL one only means the setting is unreachable through bifract-setup and
// is reported. ARCHIVE_MAX_PENDING_BYTES sat in the second state for exactly
// this reason, which is why the softer case is surfaced rather than ignored.
func TestFreshInstallSecretRefsResolve(t *testing.T) {
	dir := t.TempDir()
	large, _ := lookupSizeProfile("Large")
	if err := writeK8sManifests(freshK8sConfig(large, dir)); err != nil {
		t.Fatalf("writeK8sManifests: %v", err)
	}

	secretDoc := readFile(t, dir, "bifract", "secrets.yaml")
	defined := map[string]map[string]bool{}
	for _, doc := range strings.Split(secretDoc, "\n---") {
		var parsed struct {
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			StringData map[string]string `yaml:"stringData"`
			Data       map[string]string `yaml:"data"`
		}
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil || parsed.Metadata.Name == "" {
			continue
		}
		keys := map[string]bool{}
		for k := range parsed.StringData {
			keys[k] = true
		}
		for k := range parsed.Data {
			keys[k] = true
		}
		defined[parsed.Metadata.Name] = keys
	}
	if len(defined["bifract-secrets"]) == 0 {
		t.Fatal("rendered secrets.yaml defines no keys under bifract-secrets")
	}

	// Walk every manifest looking for secretKeyRef blocks.
	walkYAML(t, dir, func(path, content string) {
		rel, _ := filepath.Rel(dir, path)
		lines := strings.Split(content, "\n")
		for i, line := range lines {
			if !strings.Contains(line, "secretKeyRef:") {
				continue
			}
			var name, key string
			var optional bool
			for _, follow := range lines[i+1 : min(i+5, len(lines))] {
				f := strings.TrimSpace(follow)
				switch {
				case strings.HasPrefix(f, "name:"):
					name = strings.TrimSpace(strings.TrimPrefix(f, "name:"))
				case strings.HasPrefix(f, "key:"):
					key = strings.TrimSpace(strings.TrimPrefix(f, "key:"))
				case strings.HasPrefix(f, "optional:"):
					optional = strings.TrimSpace(strings.TrimPrefix(f, "optional:")) == "true"
				}
			}
			if name == "" || key == "" {
				continue
			}
			keys, ok := defined[name]
			if !ok {
				continue // secret rendered elsewhere (e.g. operator-managed)
			}
			if keys[key] {
				continue
			}
			if optional {
				t.Logf("%s: optional secretKeyRef %s/%s has no key in the rendered secret; "+
					"the setting is unreachable through bifract-setup and falls back to the code default", rel, name, key)
				continue
			}
			t.Errorf("%s: REQUIRED secretKeyRef %s/%s is not defined by the rendered secret; the pod will not start",
				rel, name, key)
		}
	})
}

// kustomize build is what `kubectl apply -k` runs first, and it fails on a
// resources: entry pointing at a file the install did not write. No cluster
// needed, so this runs wherever kubectl exists.
func TestFreshInstallKustomizeBuilds(t *testing.T) {
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("kubectl not available")
	}
	for _, p := range sizeProfiles {
		t.Run(p.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := writeK8sManifests(freshK8sConfig(p, dir)); err != nil {
				t.Fatalf("writeK8sManifests: %v", err)
			}
			out, err := exec.Command(kubectl, "kustomize", dir).CombinedOutput()
			if err != nil {
				t.Fatalf("kubectl kustomize failed: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), "kind: StatefulSet") {
				t.Error("kustomize output has no StatefulSet; the build produced an unexpectedly thin tree")
			}
		})
	}
}

// A fresh --install must produce a compose file Docker itself accepts. `docker
// compose config` resolves interpolation and validates the schema, which is the
// closest check to `up` that needs no images pulled.
func TestFreshInstallDockerComposeConfig(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker not available")
	}
	rendered, err := RenderDockerCompose(&SetupConfig{
		InstallDir:               t.TempDir(),
		Domain:                   "bifract.example.com",
		ImageTag:                 "v0.0.3",
		PostgresPassword:         "pgpw",
		IngestPostgresPassword:   "ingestpgpw",
		ClickHousePassword:       "chpw",
		IngestClickHousePassword: "ingestchpw",
		LiteLLMMasterKey:         "llmkey",
		AdminPasswordHash:        "hash",
		PasswordPepper:           "pepper",
		FeedEncryptionKey:        strings.Repeat("a", 64),
		BackupEncryptionKey:      strings.Repeat("b", 64),
	})
	if err != nil {
		t.Fatalf("render docker-compose: %v", err)
	}
	if strings.Contains(rendered, "<no value>") {
		t.Fatal("rendered compose contains an unresolved template field")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(path, []byte(rendered), 0644); err != nil {
		t.Fatalf("write compose: %v", err)
	}
	cmd := exec.Command(docker, "compose", "-f", path, "config")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose config rejected the rendered file: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "BIFRACT_ARCHIVE_MAX_PENDING_BYTES") {
		t.Error("compose config output does not carry BIFRACT_ARCHIVE_MAX_PENDING_BYTES")
	}
}
