package setup

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestCaddyfilesAdapt runs every Caddyfile this repo ships, static and rendered,
// through Caddy's own config adapter. A syntax error or unknown directive stops
// Caddy from starting, which takes the instance offline including the mTLS
// listener, and nothing else in the build catches it: TestK8sManifestsAreValidYAML
// parses the ConfigMap as YAML, so the Caddyfile inside it is an opaque string.
//
// `adapt` rather than `validate` because validate provisions the config, which
// reads the TLS certificate files named by self-signed and custom SSL modes.
// Those do not exist at test time. Adapting still catches the whole
// syntax-and-directive class this guards.
func TestCaddyfilesAdapt(t *testing.T) {
	caddyBin, err := exec.LookPath("caddy")
	if err != nil {
		t.Skip("caddy not on PATH; install it to run this guard (CI does)")
	}

	dir := t.TempDir()

	// The adapter resolves trusted_ca_cert_file at parse time, so the mTLS
	// variants need a real CA at the path they name. Everything else, including
	// the tls certificate files, adapts exactly as shipped.
	caPEM, _, err := GenerateClientCAPEM()
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	caPath := filepath.Join(dir, "client-ca.pem")
	if err := os.WriteFile(caPath, []byte(caPEM), 0o600); err != nil {
		t.Fatal(err)
	}

	for name, body := range caddyfileVariants(t) {
		body = strings.ReplaceAll(body, "/etc/caddy/client-ca/ca.pem", caPath)
		t.Run(name, func(t *testing.T) {
			if strings.Contains(body, "<no value>") {
				t.Fatal("contains <no value> (unresolved template field)")
			}
			path := filepath.Join(dir, name+".Caddyfile")
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command(caddyBin, "adapt", "--adapter", "caddyfile", "--config", path).CombinedOutput()
			if err != nil {
				t.Errorf("caddy adapt failed: %v\n%s", err, out)
			}
		})
	}
}

// caddyfileVariants returns every Caddyfile the project can produce, keyed by a
// name safe for use as a filename.
func caddyfileVariants(t *testing.T) map[string]string {
	t.Helper()
	variants := map[string]string{}

	// The checked-in compose configs.
	for name, path := range map[string]string{
		"compose":     "../../caddy/Caddyfile",
		"compose-dev": "../../caddy/Caddyfile.dev",
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		variants[name] = string(body)
	}

	// Every combination the compose installer can write. The blocks are
	// conditional on both axes, so one rendering does not cover the others.
	for _, ssl := range []SSLMode{SSLSelfSigned, SSLLetsEncrypt, SSLCustom} {
		for _, ip := range []IPAccessMode{IPAccessAll, IPAccessRestrictApp, IPAccessRestrictAll, IPAccessMTLSApp} {
			cfg := &SetupConfig{
				Domain:     "example.com",
				SSLMode:    ssl,
				SSLEmail:   "admin@example.com",
				IPAccess:   ip,
				AllowedIPs: []string{"203.0.113.0/24", "198.51.100.7"},
			}
			body, err := RenderCaddyfile(cfg)
			if err != nil {
				t.Fatalf("render %s/%s: %v", ssl, ip, err)
			}
			variants["compose-"+string(ssl)+"-"+string(ip)] = body
		}
	}

	// The k8s ConfigMap embeds its Caddyfile as a YAML string. The IP matchers are
	// spliced in as pre-rendered text, so the real builders have to produce them or
	// the variant would not exercise what ships.
	restricted := &K8sConfig{SetupConfig: SetupConfig{
		IPAccess:   IPAccessRestrictAll,
		AllowedIPs: []string{"203.0.113.0/24", "198.51.100.7"},
	}}
	for name, data := range map[string]k8sTemplateData{
		"k8s":            {Domain: "example.com"},
		"k8s-mtls":       {Domain: "example.com", MTLSEnabled: true},
		"k8s-restricted": {Domain: "example.com", IPBlock: buildIPBlock(restricted), IPBlockIngest: buildIPBlockIngest(restricted)},
	} {
		out, err := renderK8sTemplate("templates/k8s/caddy-configmap.yaml.tmpl", data)
		if err != nil {
			t.Fatalf("render %s: %v", name, err)
		}
		var cm struct {
			Data struct {
				Caddyfile string `yaml:"Caddyfile"`
			} `yaml:"data"`
		}
		if err := yaml.Unmarshal([]byte(out), &cm); err != nil {
			t.Fatalf("%s is not valid YAML: %v", name, err)
		}
		if strings.TrimSpace(cm.Data.Caddyfile) == "" {
			t.Fatalf("%s rendered an empty Caddyfile", name)
		}
		variants[name] = cm.Data.Caddyfile
	}

	return variants
}
