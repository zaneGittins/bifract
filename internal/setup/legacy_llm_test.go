package setup

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"bifract/pkg/llm"
)

const legacyLiteLLMConfig = `model_list:
  - model_name: bifract-chat
    litellm_params:
      model: openai/gpt-4.1-mini
      api_key: os.environ/LITELLM_API_KEY
`

const legacyLiteLLMConfigMap = `apiVersion: v1
kind: ConfigMap
metadata:
  name: litellm-config
data:
  config.yaml: |
    model_list:
      - model_name: bifract-chat
        litellm_params:
          model: gemini/gemini-2.5-flash
`

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

// An install that chose a model in LiteLLM keeps it, and its key, after the upgrade.
func TestMigrateLiteLLMCarriesKeyAndModel(t *testing.T) {
	env := map[string]string{"LITELLM_API_KEY": "sk-live", "LITELLM_MASTER_KEY": "sk-master"}
	if warn := migrateLiteLLM(env, "BIFRACT_", writeTemp(t, "litellm-config.yaml", legacyLiteLLMConfig)); warn != "" {
		t.Fatalf("unexpected warning: %s", warn)
	}
	want := map[string]string{
		"BIFRACT_LLM_API_KEY":  "sk-live",
		"BIFRACT_LLM_PROVIDER": "openai",
		"BIFRACT_LLM_MODEL":    "gpt-4.1-mini",
	}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("got %v, want %v", env, want)
	}
}

func TestMigrateLiteLLMReadsK8sConfigMap(t *testing.T) {
	secrets := map[string]string{"LITELLM_API_KEY": "g-key"}
	migrateLiteLLM(secrets, "", writeTemp(t, "configmap.yaml", legacyLiteLLMConfigMap))
	if secrets["LLM_PROVIDER"] != "gemini" || secrets["LLM_MODEL"] != "gemini-2.5-flash" || secrets["LLM_API_KEY"] != "g-key" {
		t.Fatalf("got %v", secrets)
	}
}

// Settings already made for the built-in client win over the leftover LiteLLM ones.
func TestMigrateLiteLLMKeepsNewSettings(t *testing.T) {
	env := map[string]string{"LITELLM_API_KEY": "old", "BIFRACT_LLM_API_KEY": "new", "BIFRACT_LLM_PROVIDER": "anthropic"}
	migrateLiteLLM(env, "BIFRACT_", writeTemp(t, "litellm-config.yaml", legacyLiteLLMConfig))
	want := map[string]string{"BIFRACT_LLM_API_KEY": "new", "BIFRACT_LLM_PROVIDER": "anthropic"}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("got %v, want %v", env, want)
	}
}

func TestMigrateLiteLLMWarnsOnUnsupportedProvider(t *testing.T) {
	env := map[string]string{"LITELLM_API_KEY": "k"}
	cfg := "model_list:\n  - model_name: bifract-chat\n    litellm_params:\n      model: bedrock/claude\n"
	if warn := migrateLiteLLM(env, "BIFRACT_", writeTemp(t, "c.yaml", cfg)); warn == "" {
		t.Fatal("expected a warning for a provider the built-in client cannot reach")
	}
	if _, ok := env["BIFRACT_LLM_PROVIDER"]; ok {
		t.Fatalf("unsupported provider was carried over: %v", env)
	}
}

func TestMigrateLiteLLMWithoutConfigFile(t *testing.T) {
	env := map[string]string{}
	if warn := migrateLiteLLM(env, "BIFRACT_", filepath.Join(t.TempDir(), "missing.yaml")); warn != "" || len(env) != 0 {
		t.Fatalf("warn=%q env=%v", warn, env)
	}
}

// The stock route is left to the default, so a later default reaches the install.
func TestMigrateLiteLLMDoesNotPinTheStockModel(t *testing.T) {
	env := map[string]string{"LITELLM_API_KEY": "sk-live"}
	stock := "model_list:\n  - model_name: bifract-chat\n    litellm_params:\n      model: anthropic/" + llm.DefaultAnthropicModel + "\n"
	migrateLiteLLM(env, "BIFRACT_", writeTemp(t, "c.yaml", stock))
	if want := map[string]string{"BIFRACT_LLM_API_KEY": "sk-live"}; !reflect.DeepEqual(env, want) {
		t.Fatalf("got %v, want %v", env, want)
	}
}

// An install that never set a key never used AI, so there is nothing to carry.
func TestMigrateLiteLLMWithoutKeyWritesNothing(t *testing.T) {
	env := map[string]string{"LITELLM_MASTER_KEY": "sk-master"}
	migrateLiteLLM(env, "BIFRACT_", writeTemp(t, "c.yaml", legacyLiteLLMConfig))
	if len(env) != 0 {
		t.Fatalf("got %v", env)
	}
}

// A gateway route keeps its endpoint, and a key written into the config itself
// is used when the environment had none.
func TestMigrateLiteLLMCarriesGatewayAndLiteralKey(t *testing.T) {
	cfg := "model_list:\n  - model_name: bifract-chat\n    litellm_params:\n      model: openai/llama3\n      api_base: http://vllm:8000/v1\n      api_key: sk-inline\n"
	env := map[string]string{}
	migrateLiteLLM(env, "BIFRACT_", writeTemp(t, "c.yaml", cfg))
	want := map[string]string{
		"BIFRACT_LLM_API_KEY":  "sk-inline",
		"BIFRACT_LLM_PROVIDER": "openai",
		"BIFRACT_LLM_MODEL":    "llama3",
		"BIFRACT_LLM_BASE_URL": "http://vllm:8000/v1",
	}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("got %v, want %v", env, want)
	}
}

// kubectl apply leaves LITELLM_* in the live Secret. Before the migration they
// are needed; after it they must not come back and refill a cleared key.
func TestMergeLiveSecretsDropsLiteLLMAfterMigration(t *testing.T) {
	live := map[string]string{"LITELLM_API_KEY": "stale", "OIDC_CLIENT_ID": "sso", "POSTGRES_PASSWORD": "cluster"}

	before := map[string]string{"POSTGRES_PASSWORD": "disk"}
	mergeLiveSecrets(before, live)
	if before["LITELLM_API_KEY"] != "stale" || before["POSTGRES_PASSWORD"] != "disk" {
		t.Errorf("before migration: %v", before)
	}

	after := map[string]string{"POSTGRES_PASSWORD": "disk", "LLM_API_KEY": ""}
	if n := mergeLiveSecrets(after, live); n != 1 {
		t.Errorf("merged %d, want 1 (OIDC_CLIENT_ID)", n)
	}
	if _, ok := after["LITELLM_API_KEY"]; ok {
		t.Errorf("stale LiteLLM key merged after migration: %v", after)
	}
	if migrateLiteLLM(after, "", ""); after["LLM_API_KEY"] != "" {
		t.Errorf("cleared key was refilled: %q", after["LLM_API_KEY"])
	}
}

// Reconfigure keeps the old image, which still calls LiteLLM, so it must not
// render files without it.
func TestReconfigureRefusesLiteLLMInstall(t *testing.T) {
	t.Run("docker", func(t *testing.T) {
		cfg := installedConfig(t)
		writeComposeInstall(t, cfg, "v0.9.0", nil)
		os.WriteFile(filepath.Join(cfg.InstallDir, legacyLiteLLMConfigFile), []byte(legacyLiteLLMConfig), 0644)
		compose := readFile(t, cfg.InstallDir, "docker-compose.yml")
		var err error
		captureStdout(t, func() { err = RunReconfigure(cfg.InstallDir) })
		if !errors.Is(err, errLegacyLiteLLM) {
			t.Fatalf("err = %v", err)
		}
		if readFile(t, cfg.InstallDir, "docker-compose.yml") != compose {
			t.Error("compose rewritten despite the refusal")
		}
	})
	t.Run("k8s", func(t *testing.T) {
		withoutExternalCommands(t)
		dir := t.TempDir()
		writeK8sInstall(t, freshK8sConfig(sizeProfiles[0], dir))
		os.MkdirAll(filepath.Join(dir, legacyLiteLLMManifestDir), 0755)
		var err error
		captureStdout(t, func() { err = RunReconfigureK8s(dir, K8sReconfigureOpts{}) })
		if !errors.Is(err, errLegacyLiteLLM) {
			t.Fatalf("err = %v", err)
		}
	})
}
