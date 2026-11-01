package setup

import (
	"net"
	"reflect"
	"strings"
	"testing"
)

func TestComposeHostPorts(t *testing.T) {
	compose := `services:
  caddy:
    ports:
      - "80:80"
      - "127.0.0.1:9000:9000"
      - "5000"
      - "514:514/udp"
      - target: 8443
        published: "8443"
  app:
    image: x
`
	got, err := composeHostPorts(compose)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{":80", "127.0.0.1:9000", ":8443"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// The preflight reads the ports from the compose file the installer writes, so it
// cannot drift from what docker will try to bind.
func TestComposeHostPortsFromTemplate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Domain = "logs.example.com"
	rendered, err := RenderDockerCompose(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := composeHostPorts(rendered)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{":80", ":443", ":8443"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCheckPortsFree(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	busy := ln.Addr().String()

	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	freeAddr := free.Addr().String()
	free.Close()

	if err := checkPortsFree([]string{freeAddr}); err != nil {
		t.Errorf("free port reported busy: %v", err)
	}
	err = checkPortsFree([]string{freeAddr, busy})
	_, port, _ := net.SplitHostPort(busy)
	if err == nil || !strings.Contains(err.Error(), port) {
		t.Errorf("err = %v, want it to name port %s", err, port)
	}
}
