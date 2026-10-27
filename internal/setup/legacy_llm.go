package setup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"bifract/pkg/llm"

	"gopkg.in/yaml.v3"
)

// legacyRoute is where a LiteLLM config sent bifract-chat.
type legacyRoute struct {
	model   string // e.g. "anthropic/claude-haiku-4-5-20251001"
	apiBase string
	apiKey  string // only a literal key; os.environ/ references are not resolvable here
}

// readLegacyRoute reads the bare LiteLLM config file (Docker) or the ConfigMap
// that embedded it (Kubernetes). A missing or unreadable file is an empty route.
func readLegacyRoute(path string) legacyRoute {
	raw, err := os.ReadFile(path)
	if err != nil {
		return legacyRoute{}
	}
	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if yaml.Unmarshal(raw, &cm) == nil && cm.Data["config.yaml"] != "" {
		raw = []byte(cm.Data["config.yaml"])
	}
	var cfg struct {
		ModelList []struct {
			ModelName string `yaml:"model_name"`
			Params    struct {
				Model   string `yaml:"model"`
				APIBase string `yaml:"api_base"`
				APIKey  string `yaml:"api_key"`
			} `yaml:"litellm_params"`
		} `yaml:"model_list"`
	}
	if yaml.Unmarshal(raw, &cfg) != nil {
		return legacyRoute{}
	}
	for _, m := range cfg.ModelList {
		if m.ModelName != "bifract-chat" {
			continue
		}
		r := legacyRoute{model: strings.TrimSpace(m.Params.Model), apiBase: strings.TrimSpace(m.Params.APIBase)}
		if k := strings.TrimSpace(m.Params.APIKey); !strings.HasPrefix(k, "os.environ/") {
			r.apiKey = k
		}
		return r
	}
	return legacyRoute{}
}

// migrateLiteLLM moves an install's AI settings from LiteLLM to the built-in
// client and drops the LiteLLM keys. prefix is "BIFRACT_" for .env and empty for
// Kubernetes secrets. Settings already made for the built-in client are kept,
// and an install that never had a key gets nothing, so it follows the defaults.
// It returns a warning when the old route cannot be carried over.
func migrateLiteLLM(vals map[string]string, prefix, litellmConfigPath string) string {
	defer delete(vals, "LITELLM_API_KEY")
	defer delete(vals, "LITELLM_MASTER_KEY")

	route := readLegacyRoute(litellmConfigPath)
	key := vals[prefix+"LLM_API_KEY"]
	for _, k := range []string{vals["LITELLM_API_KEY"], route.apiKey} {
		if key == "" {
			key = k
		}
	}
	if key == "" {
		return ""
	}
	vals[prefix+"LLM_API_KEY"] = key
	if vals[prefix+"LLM_PROVIDER"] != "" || vals[prefix+"LLM_MODEL"] != "" || vals[prefix+"LLM_BASE_URL"] != "" || route.model == "" {
		return ""
	}
	provider, name, _ := strings.Cut(route.model, "/")
	switch {
	case name == "" || (provider != llm.ProviderAnthropic && provider != llm.ProviderOpenAI && provider != llm.ProviderGemini):
		return fmt.Sprintf("LiteLLM model %q is not supported by the built-in AI client; set %sLLM_PROVIDER and %sLLM_MODEL (anthropic, openai or gemini). The old config is in .backups", route.model, prefix, prefix)
	case provider == llm.ProviderAnthropic && name == llm.DefaultAnthropicModel && route.apiBase == "":
		// The stock route: leave it to the default so later default changes apply.
		return ""
	}
	vals[prefix+"LLM_PROVIDER"] = provider
	vals[prefix+"LLM_MODEL"] = name
	if route.apiBase != "" {
		vals[prefix+"LLM_BASE_URL"] = route.apiBase
	}
	return ""
}

// legacyLiteLLMConfigFile is what Docker installs mounted into the LiteLLM container.
const legacyLiteLLMConfigFile = "litellm-config.yaml"

// removeLegacyLiteLLMConfig drops the LiteLLM config once its settings live in
// .env, so nothing suggests editing a file no service reads.
func removeLegacyLiteLLMConfig(dir string) {
	if err := os.Remove(filepath.Join(dir, legacyLiteLLMConfigFile)); err == nil {
		printDone("Removed LiteLLM; AI now calls the provider directly")
	}
}

// legacyLiteLLMManifestDir held the LiteLLM Deployment and ConfigMap in K8s installs.
const legacyLiteLLMManifestDir = "litellm"

// legacyLiteLLMCleanup removes what `kubectl apply -k` leaves behind: it never
// prunes objects dropped from the kustomization, nor keys dropped from a Secret.
var legacyLiteLLMCleanup = []string{
	"kubectl delete deployment/litellm service/litellm configmap/litellm-config networkpolicy/allow-litellm-from-bifract -n bifract --ignore-not-found",
	`kubectl patch secret bifract-secrets -n bifract --type=merge -p '{"data":{"LITELLM_API_KEY":null,"LITELLM_MASTER_KEY":null}}'`,
}

// removeLegacyLiteLLMManifests drops the LiteLLM manifests from the tree and
// reports whether there were any, so the caller can print legacyLiteLLMCleanup.
func removeLegacyLiteLLMManifests(dir string) bool {
	path := filepath.Join(dir, legacyLiteLLMManifestDir)
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return os.RemoveAll(path) == nil
}

func printLegacyLiteLLMCleanup() {
	fmt.Println()
	fmt.Println(WarningStyle.Render("  LiteLLM was removed; Bifract now calls the AI provider directly."))
	fmt.Println(DimStyle.Render("  After applying, delete the old LiteLLM objects and secret keys:"))
	for _, cmd := range legacyLiteLLMCleanup {
		fmt.Println(DimStyle.Render("    " + cmd))
	}
}

// errLegacyLiteLLM stops a reconfigure on an install that still runs LiteLLM: the
// regenerated files drop it while the old image still calls it.
var errLegacyLiteLLM = errors.New("this install still runs LiteLLM; run the upgrade first, which moves its AI settings to the built-in client")
