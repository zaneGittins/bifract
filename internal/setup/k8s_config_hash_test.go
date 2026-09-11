package setup

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

// configMounts pairs each Deployment with the ConfigMap it mounts by subPath.
var configMounts = []struct{ name, deploy, cm string }{
	{"caddy", "caddy/deployment.yaml", "caddy/configmap.yaml"},
	{"litellm", "litellm/deployment.yaml", "litellm/configmap.yaml"},
}

func hashOf(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }

// Both files are mounted with subPath, which kubelet never updates in place, and
// kubectl apply does not roll a Deployment whose spec is unchanged. The pod
// template must therefore carry the ConfigMap's hash, or a config fix lands in
// the cluster while the running process keeps its boot-time copy.
func TestConfigHashMatchesRenderedConfigMap(t *testing.T) {
	dir := renderK8s(t, true)

	for _, c := range configMounts {
		deploy := readManifest(t, dir, c.deploy)
		want := fmt.Sprintf("bifract.io/config-hash: %q", hashOf(readManifest(t, dir, c.cm)))
		if !strings.Contains(deploy, want) {
			t.Errorf("%s does not carry its ConfigMap hash\nwant line: %s", c.name, want)
		}
		// The annotation belongs on the pod template: one on the Deployment
		// object itself does not roll the pods.
		i := strings.Index(deploy, "  template:")
		if i < 0 || !strings.Contains(deploy[i:], "bifract.io/config-hash") {
			t.Errorf("%s config-hash is not in the pod template", c.name)
		}
	}
}

// Identical input must give an identical hash, or every upgrade rolls both pods
// for no reason.
func TestConfigHashIsDeterministic(t *testing.T) {
	a, b := renderK8s(t, true), renderK8s(t, true)
	for _, c := range configMounts {
		if h1, h2 := hashOf(readManifest(t, a, c.cm)), hashOf(readManifest(t, b, c.cm)); h1 != h2 {
			t.Errorf("%s hash is nondeterministic: %s vs %s", c.name, h1, h2)
		}
	}
}

// A Caddyfile change must move the hash, otherwise the annotation is decoration.
// Toggling mTLS is the change that matters most: a pod left on a stale Caddyfile
// would keep the old client-auth stance.
func TestConfigHashTracksMTLSToggle(t *testing.T) {
	on, off := renderK8s(t, true), renderK8s(t, false)

	hOn := hashOf(readManifest(t, on, "caddy/configmap.yaml"))
	hOff := hashOf(readManifest(t, off, "caddy/configmap.yaml"))
	if hOn == hOff {
		t.Fatal("test is vacuous: toggling mTLS did not change the Caddyfile")
	}
	if !strings.Contains(readManifest(t, on, "caddy/deployment.yaml"), hOn) {
		t.Error("mTLS render does not carry its own Caddyfile hash")
	}
	if !strings.Contains(readManifest(t, off, "caddy/deployment.yaml"), hOff) {
		t.Error("non-mTLS render does not carry its own Caddyfile hash")
	}
}
