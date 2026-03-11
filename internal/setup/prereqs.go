package setup

import (
	"fmt"
	"os/exec"
	"strings"
)

type PrereqResult struct {
	DockerOK  bool
	ComposeOK bool
	DockerVer string
	ComposeVer string
	Errors    []string
}

func CheckPrereqs() PrereqResult {
	r := PrereqResult{}

	if out, err := exec.Command("docker", "--version").Output(); err == nil {
		r.DockerOK = true
		r.DockerVer = strings.TrimSpace(string(out))
	} else {
		r.Errors = append(r.Errors, "Docker not found. Install: https://docs.docker.com/get-docker/")
	}

	if out, err := exec.Command("docker", "compose", "version").Output(); err == nil {
		r.ComposeOK = true
		r.ComposeVer = strings.TrimSpace(string(out))
	} else {
		r.Errors = append(r.Errors, "Docker Compose not found. Install: https://docs.docker.com/compose/install/")
	}

	return r
}

func (r PrereqResult) OK() bool {
	return r.DockerOK && r.ComposeOK
}

func (r PrereqResult) Summary() string {
	var lines []string
	if r.DockerOK {
		lines = append(lines, fmt.Sprintf("  Docker:  %s", r.DockerVer))
	}
	if r.ComposeOK {
		lines = append(lines, fmt.Sprintf("  Compose: %s", r.ComposeVer))
	}
	for _, e := range r.Errors {
		lines = append(lines, fmt.Sprintf("  [!] %s", e))
	}
	return strings.Join(lines, "\n")
}
