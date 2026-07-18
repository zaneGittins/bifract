package setup

import "testing"

// Regression: the log-shipper sidecar runs on a generic alpine image, so it must
// be located by its container name, not its image reference.
func TestExtractResourcesLogShipperByName(t *testing.T) {
	content := `
      containers:
        - name: caddy
          image: caddy:2-alpine
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: "500m"
              memory: 256Mi
        - name: log-shipper
          image: alpine:3
          resources:
            requests:
              cpu: 15m
              memory: 48Mi
            limits:
              cpu: 250m
              memory: 96Mi
      volumes:
        - name: log-shipper-script
          configMap:
            name: caddy-log-shipper
`
	caddy := extractResources(content, "caddy")
	if caddy.CPURequest != "100m" || caddy.MemLimit != "256Mi" {
		t.Fatalf("caddy resources wrong: %+v", caddy)
	}
	shipper := extractResources(content, "log-shipper")
	if resourceProfileEmpty(shipper) {
		t.Fatalf("log-shipper resources not found (regression)")
	}
	if shipper.CPURequest != "15m" || shipper.MemRequest != "48Mi" || shipper.CPULimit != "250m" || shipper.MemLimit != "96Mi" {
		t.Fatalf("log-shipper resources wrong: %+v", shipper)
	}
}
