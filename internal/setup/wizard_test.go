package setup

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

var (
	keyEnter = tea.KeyMsg{Type: tea.KeyEnter}
	keyEsc   = tea.KeyMsg{Type: tea.KeyEsc}
	keyUp    = tea.KeyMsg{Type: tea.KeyUp}
	keyDown  = tea.KeyMsg{Type: tea.KeyDown}
	keyTab   = tea.KeyMsg{Type: tea.KeyTab}
	keyCtrlC = tea.KeyMsg{Type: tea.KeyCtrlC}
)

func typed(s string) tea.KeyMsg { return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)} }

// keystrokes types s one key at a time, so single-letter shortcuts are hit.
func keystrokes(s string) []tea.Msg {
	var out []tea.Msg
	for _, r := range s {
		out = append(out, typed(string(r)))
	}
	return out
}

// isQuit reports whether cmd is tea.Quit. Compared by identity rather than by
// running it: other commands (the cursor blink) block on a timer.
func isQuit(cmd tea.Cmd) bool {
	return cmd != nil && reflect.ValueOf(cmd).Pointer() == reflect.ValueOf(tea.Quit).Pointer()
}

func wizardPress(t *testing.T, m WizardModel, keys ...tea.Msg) (WizardModel, tea.Cmd) {
	t.Helper()
	var cmd tea.Cmd
	for _, k := range keys {
		var next tea.Model
		next, cmd = m.Update(k)
		m = next.(WizardModel)
	}
	return m, cmd
}

// A docker install through every step, with the validation each step owns.
func TestWizardBundledFlow(t *testing.T) {
	newFakeExec(t) // the prerequisites step runs docker --version
	m := NewWizardModel(DefaultConfig())

	m, _ = wizardPress(t, m, tea.WindowSizeMsg{Width: 120, Height: 40}, keyEnter)
	if m.step != StepPrereqs || !m.prereq.OK() || m.width != 120 {
		t.Fatalf("after welcome: step %v, prereqs %+v", m.step, m.prereq)
	}
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepInstallDir {
		t.Fatalf("step = %v, want install dir", m.step)
	}
	m.installDirInput.SetValue("")
	m, _ = wizardPress(t, m, keyEnter)
	if m.config.InstallDir != "/opt/bifract" || m.step != StepClickHouse {
		t.Fatalf("empty install dir: %q, step %v", m.config.InstallDir, m.step)
	}
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepDomain || !m.config.CH.Bundled() {
		t.Fatalf("bundled ClickHouse: step %v, target %+v", m.step, m.config.CH)
	}

	m.domainInput.SetValue("https://logs.example.com")
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepDomain || m.inputErr == "" {
		t.Fatalf("a URL was accepted as the domain (step %v)", m.step)
	}
	m.domainInput.SetValue("")
	// "q" is text here, not quit.
	m, cmd := wizardPress(t, m, keystrokes("aq.example.com")...)
	if isQuit(cmd) || m.domainInput.Value() != "aq.example.com" {
		t.Fatalf("typing q in the domain field: quit=%v value=%q", isQuit(cmd), m.domainInput.Value())
	}
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepSSL || m.inputErr != "" || m.config.Domain != "aq.example.com" || !m.config.SecureCookies {
		t.Fatalf("valid domain: step %v err %q cfg %+v", m.step, m.inputErr, m.config)
	}

	m, _ = wizardPress(t, m, keyEnter) // self-signed
	if m.step != StepIPAccess || m.config.SSLMode != SSLSelfSigned {
		t.Fatalf("self-signed: step %v mode %q", m.step, m.config.SSLMode)
	}
	m, _ = wizardPress(t, m, keyEnter) // restrict app
	if m.step != StepAllowedIPs || m.config.IPAccess != IPAccessRestrictApp {
		t.Fatalf("restrict-app: step %v", m.step)
	}
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepAllowedIPs {
		t.Fatal("an empty allow list was accepted")
	}
	m.allowedIPsInput.SetValue("10.0.0.0/8, nope")
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepAllowedIPs || !strings.Contains(m.ipValidationErr, "nope") {
		t.Fatalf("invalid IP: step %v err %q", m.step, m.ipValidationErr)
	}
	m.allowedIPsInput.SetValue("10.0.0.0/8, 192.0.2.1")
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepConfirm || m.ipValidationErr != "" {
		t.Fatalf("valid IPs: step %v err %q", m.step, m.ipValidationErr)
	}

	m, cmd = wizardPress(t, m, keyEnter)
	if m.step != StepDone || !isQuit(cmd) {
		t.Fatalf("confirm: step %v quit %v", m.step, isQuit(cmd))
	}
	if err := m.config.Validate(); err != nil {
		t.Errorf("the wizard produced a config its own gate rejects: %v", err)
	}
}

func TestWizardPrereqsBlock(t *testing.T) {
	newFakeExec(t, fakeRule{Cmd: "docker", Exit: 1})
	m, _ := wizardPress(t, NewWizardModel(DefaultConfig()), keyEnter, keyEnter)
	if m.step != StepPrereqs {
		t.Errorf("continued past missing Docker to %v", m.step)
	}
	if !strings.Contains(m.prereq.Summary(), "Docker not found") {
		t.Errorf("summary = %q", m.prereq.Summary())
	}
}

func TestWizardTLSModes(t *testing.T) {
	at := func(step WizardStep) WizardModel {
		m := NewWizardModel(DefaultConfig())
		m.step = step
		return m
	}

	t.Run("letsencrypt needs a valid email", func(t *testing.T) {
		m, _ := wizardPress(t, at(StepSSL), keyDown, keyEnter)
		if m.step != StepSSLEmail || m.config.SSLMode != SSLLetsEncrypt {
			t.Fatalf("step %v mode %q", m.step, m.config.SSLMode)
		}
		m.emailInput.SetValue("Ops <ops@example.com>")
		m, _ = wizardPress(t, m, keyEnter)
		if m.step != StepSSLEmail || m.inputErr == "" {
			t.Fatal("invalid email accepted")
		}
		m.emailInput.SetValue(" ops@example.com ")
		m, _ = wizardPress(t, m, keyEnter)
		if m.step != StepIPAccess || m.config.SSLEmail != "ops@example.com" {
			t.Fatalf("step %v email %q", m.step, m.config.SSLEmail)
		}
	})

	t.Run("custom needs readable files", func(t *testing.T) {
		dir := t.TempDir()
		cert, key := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
		os.WriteFile(cert, []byte("c"), 0600)
		os.WriteFile(key, []byte("k"), 0600)

		m, _ := wizardPress(t, at(StepSSL), keyDown, keyDown, keyDown, keyEnter)
		if m.step != StepSSLCert || m.config.SSLMode != SSLCustom || !m.certPathInput.Focused() {
			t.Fatalf("step %v mode %q", m.step, m.config.SSLMode)
		}
		m.certPathInput.SetValue(cert)
		m, _ = wizardPress(t, m, keyEnter) // moves to the key field
		if !m.keyPathInput.Focused() || m.step != StepSSLCert {
			t.Fatal("enter on the certificate field did not move to the key field")
		}
		m.keyPathInput.SetValue(filepath.Join(dir, "missing.pem"))
		m, _ = wizardPress(t, m, keyEnter)
		if m.step != StepSSLCert || m.inputErr == "" {
			t.Fatal("a missing key file was accepted")
		}
		m, _ = wizardPress(t, m, keyTab)
		if !m.certPathInput.Focused() {
			t.Fatal("tab did not cycle back to the certificate field")
		}
		m, _ = wizardPress(t, m, keyTab)
		m.keyPathInput.SetValue(key)
		m, _ = wizardPress(t, m, keyEnter)
		if m.step != StepIPAccess || m.config.CertPath != cert || m.config.KeyPath != key {
			t.Fatalf("step %v cfg %+v", m.step, m.config)
		}
	})
}

func TestWizardIPAccessChoices(t *testing.T) {
	tests := []struct {
		downs int
		mode  IPAccessMode
		next  WizardStep
	}{
		{0, IPAccessRestrictApp, StepAllowedIPs},
		{1, IPAccessRestrictAll, StepAllowedIPs},
		{2, IPAccessMTLSApp, StepConfirm},
		{3, IPAccessAll, StepConfirm},
		{9, IPAccessAll, StepConfirm}, // the cursor stops at the last choice
	}
	for _, tt := range tests {
		m := NewWizardModel(DefaultConfig())
		m.step = StepIPAccess
		m.config.AllowedIPs = []string{"10.0.0.1"} // left over from an earlier pass
		keys := []tea.Msg{keyUp}
		for i := 0; i < tt.downs; i++ {
			keys = append(keys, keyDown)
		}
		m, _ = wizardPress(t, m, append(keys, keyEnter)...)
		if m.config.IPAccess != tt.mode || m.step != tt.next {
			t.Errorf("%d downs: mode %q step %v, want %q %v", tt.downs, m.config.IPAccess, m.step, tt.mode, tt.next)
		}
		// A mode with no allow list must not carry one into validation.
		if tt.next == StepConfirm && len(m.config.AllowedIPs) != 0 {
			t.Errorf("%q kept a stale allow list %v", tt.mode, m.config.AllowedIPs)
		}
	}
}

func TestWizardPrevStep(t *testing.T) {
	external := ClickHouseTarget{Backend: CHBackendExternal}
	tests := []struct {
		step WizardStep
		cfg  func(*SetupConfig)
		want WizardStep
	}{
		{StepPrereqs, nil, StepWelcome},
		{StepInstallDir, nil, StepPrereqs},
		{StepClickHouse, nil, StepInstallDir},
		{StepClickHouseHost, nil, StepClickHouse},
		{StepClickHouseAuth, nil, StepClickHouseHost},
		{StepDomain, nil, StepClickHouse},
		{StepDomain, func(c *SetupConfig) { c.CH = external }, StepClickHouseAuth},
		{StepSSL, nil, StepDomain},
		{StepSSLEmail, nil, StepSSL},
		{StepSSLCert, nil, StepSSL},
		{StepIPAccess, nil, StepSSL},
		{StepIPAccess, func(c *SetupConfig) { c.SSLMode = SSLLetsEncrypt }, StepSSLEmail},
		{StepIPAccess, func(c *SetupConfig) { c.SSLMode = SSLCustom }, StepSSLCert},
		{StepAllowedIPs, nil, StepIPAccess},
		{StepConfirm, nil, StepIPAccess},
		{StepConfirm, func(c *SetupConfig) { c.IPAccess = IPAccessRestrictAll }, StepAllowedIPs},
	}
	for _, tt := range tests {
		m := NewWizardModel(DefaultConfig())
		m.step = tt.step
		if tt.cfg != nil {
			tt.cfg(m.config)
		}
		m, _ = wizardPress(t, m, keyEsc)
		if m.step != tt.want {
			t.Errorf("esc from %v = %v, want %v", tt.step, m.step, tt.want)
		}
	}
	// Esc on the first step goes nowhere.
	m, _ := wizardPress(t, NewWizardModel(DefaultConfig()), keyEsc)
	if m.step != StepWelcome {
		t.Errorf("esc on welcome moved to %v", m.step)
	}
}

func TestWizardQuitKeys(t *testing.T) {
	for _, step := range []WizardStep{StepWelcome, StepConfirm} {
		m := NewWizardModel(DefaultConfig())
		m.step = step
		if _, cmd := wizardPress(t, m, typed("q")); !isQuit(cmd) {
			t.Errorf("q on %v did not quit", step)
		}
	}
	m := NewWizardModel(DefaultConfig())
	m.step = StepInstallDir
	if _, cmd := wizardPress(t, m, keyCtrlC); !isQuit(cmd) {
		t.Error("ctrl+c did not quit")
	}
	// A rejected final config keeps the wizard open with the reason shown.
	m = NewWizardModel(DefaultConfig())
	m.step = StepConfirm
	m.config.IPAccess = IPAccessRestrictApp
	m, cmd := wizardPress(t, m, keyEnter)
	if m.step != StepConfirm || isQuit(cmd) || m.inputErr == "" {
		t.Errorf("invalid config confirmed: step %v err %q", m.step, m.inputErr)
	}
}

// listen starts a loopback listener that accepts and drops connections, standing
// in for an external ClickHouse.
func listen(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

func TestWizardExternalClickHouse(t *testing.T) {
	addr := listen(t)
	m := NewWizardModel(DefaultConfig())
	m.step = StepClickHouse

	m, _ = wizardPress(t, m, keyDown, keyDown, keyUp, keyEnter)
	if m.step != StepClickHouseHost || m.config.CH.Bundled() || m.config.CH.Secure {
		t.Fatalf("external: step %v target %+v", m.step, m.config.CH)
	}
	m.chHostInput.SetValue("ch.internal:99999")
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepClickHouseHost || m.chValidationErr == "" {
		t.Fatal("an out-of-range port was accepted")
	}
	m.chHostInput.SetValue(addr)
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepClickHouseAuth || m.chValidationErr != "" {
		t.Fatalf("reachable host: step %v err %q", m.step, m.chValidationErr)
	}

	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepClickHouseAuth || m.chValidationErr == "" {
		t.Fatal("an external ClickHouse was accepted without a password")
	}
	m, _ = wizardPress(t, m, append([]tea.Msg{keyTab}, keystrokes("s3cret-q")...)...)
	if !m.chPassInput.Focused() || m.chPassInput.Value() != "s3cret-q" {
		t.Fatalf("tab did not move to the password field (value %q)", m.chPassInput.Value())
	}
	m, _ = wizardPress(t, m, keyEnter)
	if m.step != StepDomain || m.config.CH.User != "default" || m.config.ClickHousePassword != "s3cret-q" {
		t.Fatalf("auth: step %v target %+v", m.step, m.config.CH)
	}

	t.Run("cloud", func(t *testing.T) {
		m := NewWizardModel(DefaultConfig())
		m.step = StepClickHouse
		m, _ = wizardPress(t, m, keyDown, keyDown, keyEnter)
		if m.config.CH.Deployment != "cloud" || !m.config.CH.Secure || !strings.Contains(m.chHostInput.Placeholder, "clickhouse.cloud") {
			t.Errorf("cloud target %+v", m.config.CH)
		}
	})
}

func k8sPress(t *testing.T, m k8sWizardModel, keys ...tea.Msg) (k8sWizardModel, tea.Cmd) {
	t.Helper()
	var cmd tea.Cmd
	for _, k := range keys {
		var next tea.Model
		next, cmd = m.Update(k)
		m = next.(k8sWizardModel)
	}
	return m, cmd
}

func TestK8sWizardBundledFlow(t *testing.T) {
	m := newK8sWizardModel()
	m, _ = k8sPress(t, m, tea.WindowSizeMsg{Width: 100, Height: 30}, keyEnter)
	if m.step != k8sStepDomain {
		t.Fatalf("step = %v", m.step)
	}

	// Regression: "q" used to cancel the wizard from any step, so a domain,
	// password or path containing the letter could not be typed.
	m, cmd := k8sPress(t, m, keystrokes("aq.example.com")...)
	if isQuit(cmd) || m.err != nil || m.domainInput.Value() != "aq.example.com" {
		t.Fatalf("typing q: quit=%v err=%v value=%q", isQuit(cmd), m.err, m.domainInput.Value())
	}
	m, _ = k8sPress(t, m, typed("/"), keyEnter)
	if m.step != k8sStepDomain || m.inputErr == "" {
		t.Fatal("a domain with a path was accepted")
	}
	m.domainInput.SetValue("aq.example.com")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepSSL || m.config.Domain != "aq.example.com" || m.inputErr != "" {
		t.Fatalf("step %v domain %q", m.step, m.config.Domain)
	}

	m, _ = k8sPress(t, m, keyDown, keyDown, keyEnter)
	if m.config.SSLMode != SSLCustom || m.step != k8sStepIPAccess {
		t.Fatalf("ssl: %q step %v", m.config.SSLMode, m.step)
	}

	m, _ = k8sPress(t, m, keyDown, keyEnter)
	if m.config.IPAccess != IPAccessRestrictApp || m.step != k8sStepAllowedIPs {
		t.Fatalf("access: %q step %v", m.config.IPAccess, m.step)
	}
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepAllowedIPs {
		t.Fatal("an empty allow list was accepted")
	}
	m.allowedIPsInput.SetValue("10.0.0.0/33")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepAllowedIPs || m.ipValidationErr == "" {
		t.Fatal("an invalid CIDR was accepted")
	}
	m.allowedIPsInput.SetValue("10.0.0.0/8")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepSizeProfile || m.ipValidationErr != "" {
		t.Fatalf("step %v", m.step)
	}

	m, _ = k8sPress(t, m, keyDown, keyDown, keyDown, keyUp, keyEnter)
	small, _ := lookupSizeProfile("Small")
	if m.config.SizeProfile.Name != "Small" || m.config.CHShards != small.CHShards || m.step != k8sStepClickHouse {
		t.Fatalf("profile %q shards %d step %v", m.config.SizeProfile.Name, m.config.CHShards, m.step)
	}

	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepCHShards || !m.config.CH.Bundled() {
		t.Fatalf("bundled: step %v", m.step)
	}
	m.shardsInput.SetValue("0")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepCHShards || m.inputErr == "" {
		t.Fatal("zero shards accepted")
	}
	m.shardsInput.SetValue("2")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepCHStorage || m.config.CHShards != 2 {
		t.Fatalf("shards: step %v n %d", m.step, m.config.CHShards)
	}
	m.storageInput.SetValue("5")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepCHStorage || m.inputErr == "" {
		t.Fatal("storage below the minimum accepted")
	}
	m.storageInput.SetValue("250")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepOutputDir || m.config.CHStorageGB != 250 {
		t.Fatalf("storage: step %v gb %d", m.step, m.config.CHStorageGB)
	}

	// An existing install's secrets would be replaced by a fresh render.
	existing := t.TempDir()
	os.MkdirAll(filepath.Join(existing, "bifract"), 0755)
	os.WriteFile(filepath.Join(existing, "bifract", "secrets.yaml"), nil, 0600)
	m.outputDirInput.SetValue(existing)
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepOutputDir || !strings.Contains(m.inputErr, "existing manifests") {
		t.Fatalf("rendering over an install was allowed: step %v err %q", m.step, m.inputErr)
	}
	out := filepath.Join(t.TempDir(), "q-out")
	m.outputDirInput.SetValue(out)
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepConfirm || m.config.OutputDir != out {
		t.Fatalf("output: step %v dir %q", m.step, m.config.OutputDir)
	}

	m, cmd = k8sPress(t, m, keyEnter)
	if m.step != k8sStepDone || !isQuit(cmd) || m.err != nil {
		t.Fatalf("confirm: step %v quit %v err %v", m.step, isQuit(cmd), m.err)
	}
	if err := m.config.Validate(); err != nil {
		t.Errorf("the wizard produced a config its own gate rejects: %v", err)
	}
}

func TestK8sWizardAccessShortcuts(t *testing.T) {
	m := newK8sWizardModel()
	m.step = k8sStepIPAccess
	m, _ = k8sPress(t, m, keyEnter)
	if m.config.IPAccess != IPAccessAll || m.step != k8sStepSizeProfile {
		t.Errorf("all: %q step %v", m.config.IPAccess, m.step)
	}

	m = newK8sWizardModel()
	m.step = k8sStepIPAccess
	m, _ = k8sPress(t, m, keyDown, keyDown, keyDown, keyDown, keyEnter)
	if m.config.IPAccess != IPAccessMTLSApp || !m.config.MTLSEnabled || m.step != k8sStepSizeProfile {
		t.Errorf("mtls: %q enabled %v step %v", m.config.IPAccess, m.config.MTLSEnabled, m.step)
	}
}

func TestK8sWizardExternalSkipsSizing(t *testing.T) {
	addr := listen(t)
	m := newK8sWizardModel()
	m.step = k8sStepClickHouse
	m, _ = k8sPress(t, m, keyDown, keyEnter)
	if m.step != k8sStepClickHouseHost || m.config.CH.Bundled() {
		t.Fatalf("step %v target %+v", m.step, m.config.CH)
	}
	m.chHostInput.SetValue("[::1]:9000")
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepClickHouseHost || !strings.Contains(m.chValidationErr, "IPv6") {
		t.Fatalf("IPv6 host: step %v err %q", m.step, m.chValidationErr)
	}
	m.chHostInput.SetValue(addr)
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepClickHouseAuth {
		t.Fatalf("reachable host: step %v err %q", m.step, m.chValidationErr)
	}
	m, _ = k8sPress(t, m, keyEnter)
	if m.chValidationErr == "" {
		t.Fatal("accepted without a password")
	}
	m, _ = k8sPress(t, m, append(append(append(keystrokes("admin"), keyTab), keystrokes("pq")...), keyTab)...)
	if m.chUserInput.Value() != "admin" || m.chPassInput.Value() != "pq" || !m.chUserInput.Focused() {
		t.Fatalf("user %q pass %q", m.chUserInput.Value(), m.chPassInput.Value())
	}
	m, _ = k8sPress(t, m, keyEnter)
	if m.step != k8sStepOutputDir || m.config.CH.User != "admin" || m.config.ClickHousePassword != "pq" {
		t.Fatalf("external auth: step %v target %+v", m.step, m.config.CH)
	}

	m = newK8sWizardModel()
	m.step = k8sStepClickHouse
	m, _ = k8sPress(t, m, keyDown, keyDown, keyDown, keyEnter)
	if m.config.CH.Deployment != "cloud" || !m.config.CH.Secure {
		t.Errorf("cloud target %+v", m.config.CH)
	}
}

func TestK8sWizardQuit(t *testing.T) {
	for _, step := range []k8sStep{k8sStepWelcome, k8sStepSSL, k8sStepIPAccess, k8sStepSizeProfile, k8sStepClickHouse, k8sStepConfirm} {
		m := newK8sWizardModel()
		m.step = step
		m, cmd := k8sPress(t, m, typed("q"))
		if !isQuit(cmd) || m.err == nil {
			t.Errorf("q on step %v: quit %v err %v", step, isQuit(cmd), m.err)
		}
	}
	for _, step := range []k8sStep{k8sStepDomain, k8sStepAllowedIPs, k8sStepClickHouseHost, k8sStepClickHouseAuth, k8sStepCHShards, k8sStepCHStorage, k8sStepOutputDir} {
		m := newK8sWizardModel()
		m.step = step
		if _, cmd := k8sPress(t, m, typed("q")); isQuit(cmd) {
			t.Errorf("q quit from text step %v", step)
		}
		if _, cmd := k8sPress(t, m, keyCtrlC); !isQuit(cmd) {
			t.Errorf("ctrl+c did not quit from step %v", step)
		}
	}
	// A rejected final config keeps the wizard open with the reason shown.
	m := newK8sWizardModel()
	m.step = k8sStepConfirm
	m.config.IPAccess = IPAccessRestrictAll
	m, cmd := k8sPress(t, m, keyEnter)
	if m.step != k8sStepConfirm || isQuit(cmd) || m.inputErr == "" {
		t.Errorf("invalid config confirmed: step %v err %q", m.step, m.inputErr)
	}
}
