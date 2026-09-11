package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Manifests are decoded loosely: spec.selector is map[string]string on a Service
// but {matchLabels: ...} on a Deployment, so a single typed struct cannot hold both.
type rawDoc map[string]any

func (d rawDoc) str(path ...string) string {
	v, _ := d.at(path...).(string)
	return v
}

// asMap normalises a decoded node. yaml.v3 decodes nested mappings into the
// named target type, so sub-maps arrive as rawDoc rather than map[string]any.
func asMap(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case rawDoc:
		return map[string]any(m), true
	}
	return nil, false
}

func (d rawDoc) at(path ...string) any {
	var cur any = d
	for _, p := range path {
		m, ok := asMap(cur)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}

// strMap returns the node at path as string pairs, or nil if it is not a flat map.
func (d rawDoc) strMap(path ...string) map[string]string {
	m, ok := asMap(d.at(path...))
	if !ok {
		return nil
	}
	out := map[string]string{}
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil
		}
		out[k] = s
	}
	return out
}

// service is a Service and the ports it forwards to.
type service struct {
	name     string
	selector map[string]string
	ports    []int
}

// workload is a pod-producing manifest and the container ports it declares.
type workload struct {
	name   string
	labels map[string]string
	ports  map[int]bool
}

func renderedTopology(t *testing.T) ([]workload, []service) {
	t.Helper()
	dir := t.TempDir()
	if err := writeK8sManifests(freshK8sConfig(sizeProfiles[0], dir)); err != nil {
		t.Fatalf("render: %v", err)
	}

	var pods []workload
	var svcs []service
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		f, openErr := os.Open(path)
		if openErr != nil {
			return openErr
		}
		defer f.Close()

		dec := yaml.NewDecoder(f)
		for {
			var d rawDoc
			if decErr := dec.Decode(&d); decErr != nil {
				break
			}
			switch d.str("kind") {
			case "Service":
				sel := d.strMap("spec", "selector")
				if len(sel) == 0 {
					continue
				}
				s := service{name: d.str("metadata", "name"), selector: sel}
				ports, _ := d.at("spec", "ports").([]any)
				for _, p := range ports {
					pm, ok := asMap(p)
					if !ok {
						continue
					}
					if n, ok := pm["port"].(int); ok {
						s.ports = append(s.ports, n)
					}
				}
				svcs = append(svcs, s)

			case "Deployment", "StatefulSet":
				lbl := d.strMap("spec", "template", "metadata", "labels")
				if len(lbl) == 0 {
					continue
				}
				w := workload{name: d.str("metadata", "name"), labels: lbl, ports: map[int]bool{}}
				containers, _ := d.at("spec", "template", "spec", "containers").([]any)
				for _, c := range containers {
					cm, ok := asMap(c)
					if !ok {
						continue
					}
					cports, _ := cm["ports"].([]any)
					for _, p := range cports {
						pm, ok := asMap(p)
						if !ok {
							continue
						}
						if n, ok := pm["containerPort"].(int); ok {
							w.ports[n] = true
						}
					}
				}
				pods = append(pods, w)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(pods) < 2 || len(svcs) < 2 {
		t.Fatalf("topology looks wrong: %d workloads, %d services", len(pods), len(svcs))
	}
	return pods, svcs
}

func selectorMatches(sel, labels map[string]string) bool {
	if len(sel) == 0 {
		return false
	}
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// A Service must not select a pod that cannot serve the port it forwards to.
// bifract-archive-maintain carries app: bifract so the NetworkPolicies keyed on
// that label cover it, but it listens on nothing. A Service selecting app: bifract
// alone therefore put a dead endpoint behind the app, and kube-proxy refused
// roughly half of all new connections: intermittent 502s on every route, static
// assets included, with a healthy app pod that never restarted.
func TestNoServiceSelectsAPodThatCannotServeIt(t *testing.T) {
	pods, svcs := renderedTopology(t)

	for _, svc := range svcs {
		for _, port := range svc.ports {
			matched := 0
			for _, w := range pods {
				if !selectorMatches(svc.selector, w.labels) {
					continue
				}
				matched++
				if !w.ports[port] {
					t.Errorf("Service %q (port %d) selects workload %q, which declares no container port %d:"+
						" it becomes a dead endpoint that refuses connections",
						svc.name, port, w.name, port)
				}
			}
			if matched == 0 {
				t.Errorf("Service %q (port %d) selects no workload at all", svc.name, port)
			}
		}
	}
}
