package setup

import (
	"bufio"
	"io"
	"os"
	"sort"
	"strings"
)

func ReadEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseEnv(f)
}

func parseEnv(r io.Reader) (map[string]string, error) {
	env := make(map[string]string)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			env[parts[0]] = parts[1]
		}
	}
	return env, scanner.Err()
}

// withCarriedEnv appends the previous .env's keys that rendered does not set,
// so operator settings survive --upgrade and --reconfigure.
func withCarriedEnv(rendered string, previous map[string]string) string {
	set, _ := parseEnv(strings.NewReader(rendered))
	var keys []string
	for k := range previous {
		if _, ok := set[k]; !ok && !isCHTargetKey(k) {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return rendered
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(rendered)
	b.WriteString("\n# Operator settings carried over from the previous .env\n")
	for _, k := range keys {
		b.WriteString(k + "=" + previous[k] + "\n")
	}
	return b.String()
}
