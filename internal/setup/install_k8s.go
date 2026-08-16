package setup

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"bifract/pkg/archive"
	"bifract/pkg/storage"
)

// ResourceProfile defines CPU and memory requests/limits for a component.
type ResourceProfile struct {
	CPURequest string
	CPULimit   string
	MemRequest string
	MemLimit   string
}

// SizeProfile defines resource allocations for all components at a given scale.
// ClickHouse runs a single replica per shard; durability and disaster recovery
// are handled by the Apache Iceberg archive, not ClickHouse replication.
type SizeProfile struct {
	Name        string
	Description string
	CHShards    int
	ClickHouse  ResourceProfile
	CHKeeper    ResourceProfile
	Bifract     ResourceProfile
	// ArchiveMaintain sizes the archive-maintain Deployment. It is deliberately
	// NOT the Bifract profile: compaction decodes whole Parquet row groups into
	// Arrow, and zstd-compressed log text expands several-fold on the way, so its
	// memory shape has nothing in common with the app's. Borrowing Bifract gave
	// the maintainer 1Gi on Dev, well under one compaction worker's working set,
	// which is an OOMKill on the first pass that finds real work. Memory here is
	// also what MaintainScanConcurrency divides to pick its parallelism, so these
	// limits set compaction throughput as well as its ceiling.
	ArchiveMaintain ResourceProfile
	// Archiver sizes the archiver drain-loop sidecar in the ingest pod. Like
	// ArchiveMaintain it is deliberately NOT the Bifract profile: the archiver's
	// memory is dominated by ArchiveMaxPendingBytes, the log entries it holds in
	// heap waiting to be rolled into Parquet, which has nothing to do with what an
	// HTTP server needs. Borrowing Bifract gave a Large install 8Gi by coincidence
	// and a Dev install 1Gi, neither related to the roll thresholds below.
	Archiver        ResourceProfile
	Postgres        ResourceProfile
	Caddy           ResourceProfile
	CaddyShipper    ResourceProfile
	LiteLLM         ResourceProfile
	IngestQueueSize int

	// IngestWorkers is how many inserts run against ClickHouse concurrently per ingest pod.
	// ClickHouse inserts are CPU-bound building skip indexes, not disk-bound (measured on a
	// 3-shard Large cluster: 684ms CPU vs 12ms disk per insert, OSIOWaitTime 0), so ingest
	// throughput scales with concurrency until the shard CPUs saturate. These track the CPU
	// actually provisioned per shard: too low and ClickHouse sits idle while ingest plateaus
	// below target, which looks like a platform limit but is only a concurrency limit.
	// Large (32) is measured: raising it from 8 moved 6,800 -> 8,966 events/sec, no other change.
	IngestWorkers int

	// ArchiveMaintainByteBudget caps how many bytes one compaction pass rewrites
	// (BIFRACT_ARCHIVE_MAINTAIN_BYTE_BUDGET). It is pass-wide, split across all
	// fractal tables, so with the default 1h cadence the system-wide compaction
	// ceiling is this value x 24 per day. That ceiling has to exceed the daily
	// Parquet the archiver writes or compaction falls permanently behind, leaving
	// small files that slow every archive query (recall) and restore. Values below
	// carry roughly 3x headroom over the top of each profile's ingest band,
	// assuming ~10x zstd compression from raw log bytes to Parquet. Unused budget
	// costs nothing (it is a cap, and leftover work carries to the next pass), so
	// the headroom is cheap insurance against a lower-than-assumed ratio.
	ArchiveMaintainByteBudget int64

	// ArchiveRollBytes is the PER-FRACTAL threshold at which the archiver commits
	// a fractal's buffer to Parquet (BIFRACT_ARCHIVE_ROLL_BYTES), and
	// ArchiveMaxPendingBytes caps the archiver's TOTAL in-memory buffer across all
	// fractals (BIFRACT_ARCHIVE_MAX_PENDING_BYTES).
	//
	// Both are UNCOMPRESSED in-heap bytes (see archive.approxSize), while
	// compaction judges files by their COMPRESSED on-disk size. That unit mismatch
	// is the whole reason these are profile-scaled rather than one constant: a
	// threshold that looks generous in heap can still write Parquet below
	// iceberg-go's optimal-file floor (75% of a 512MB target = 384MB), and every
	// file below that floor is a compaction candidate the moment it lands. A Large
	// install left at the old flat 256MB default wrote ~65-130MB files, so 100% of
	// its archive was permanently backlog and compaction could never converge.
	//
	// Sized on a ~3x uncompressed-to-Parquet ratio, which is the one number here
	// estimated rather than measured -- it varies with log shape, so verify
	// against real output before assuming it. ArchiveMaxPendingBytes is held at 4x
	// ArchiveRollBytes so several busy fractals can buffer concurrently without
	// the cap forcing an early, undersized commit, and the Archiver profile's
	// memory limit covers ArchiveMaxPendingBytes + 2x ArchiveRollBytes (the
	// buffer, plus the Arrow copy and Parquet encode of the fractal being flushed)
	// with ~30% GC headroom.
	ArchiveRollBytes       int64
	ArchiveMaxPendingBytes int64

	// SpoolPVCSizeGB is the per-ingest-pod durable archive-spool PVC size. The
	// spool is fsync-before-ack and pod-local; on a StatefulSet each pod keeps its
	// own PVC so a rolling update or eviction never drops un-archived batches. It
	// is sized for OUTAGE HEADROOM, not throughput: how long object storage can be
	// unreachable before the spool fills and ingest backpressures (fail-closed, so
	// 429s and client retries, never data loss). The spool cap
	// (BIFRACT_ARCHIVE_SPOOL_MAX_BYTES) defaults to 80% of this; the PVC is sized a
	// little above the cap so a full spool hits backpressure rather than ENOSPC.
	// Roughly 1-2h of per-pod ingest at the top of each band; the spool is
	// uncompressed and carries raw_log, so shrink these if raw_log leaves the archive.
	SpoolPVCSizeGB int
}

// defaultUserSecretBytes writes v into secrets[key] only when the operator has
// not set one and v is a real value. Both guards matter: an explicit choice must
// survive regeneration, and the zero-valued "custom" profile built from parsed
// manifests must not blank a key that the deployment is already relying on.
func defaultUserSecretBytes(secrets map[string]string, key string, v int64) {
	if v <= 0 || secrets[key] != "" {
		return
	}
	secrets[key] = strconv.FormatInt(v, 10)
}

// sizeProfiles are anchored on ~500 GB/day = 3 shards at 32 vCPU / 64GB per node.
// Each shard is a single ClickHouse replica; the node size shown is the ClickHouse
// shard node, sized so the ClickHouse container gets the bulk of it.
var sizeProfiles = []SizeProfile{
	{
		Name:                      "Dev",
		Description:               "Development/testing, ~1-10 GB/day (1 shard, 4 vCPU / 8GB per node)",
		CHShards:                  1,
		ClickHouse:                ResourceProfile{"2", "3", "4Gi", "5Gi"},
		CHKeeper:                  ResourceProfile{"250m", "500m", "256Mi", "512Mi"},
		Bifract:                   ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		ArchiveMaintain:           ResourceProfile{"250m", "2", "2Gi", "4Gi"},
		Archiver:                  ResourceProfile{"250m", "1", "512Mi", "1Gi"},
		Postgres:                  ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		Caddy:                     ResourceProfile{"100m", "500m", "128Mi", "256Mi"},
		CaddyShipper:              ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:                   ResourceProfile{"100m", "500m", "512Mi", "1Gi"},
		IngestQueueSize:           100,
		IngestWorkers:             4,
		ArchiveMaintainByteBudget: 1 << 30,
		ArchiveRollBytes:          128 << 20,
		ArchiveMaxPendingBytes:    512 << 20,
		SpoolPVCSizeGB:            10,
	},
	{
		Name:                      "X-Small",
		Description:               "Staging/light production, ~10-50 GB/day (1 shard, 8 vCPU / 16GB per node)",
		CHShards:                  1,
		ClickHouse:                ResourceProfile{"6", "8", "8Gi", "12Gi"},
		CHKeeper:                  ResourceProfile{"250m", "1", "512Mi", "1Gi"},
		Bifract:                   ResourceProfile{"500m", "2", "512Mi", "2Gi"},
		ArchiveMaintain:           ResourceProfile{"250m", "2", "2Gi", "4Gi"},
		Archiver:                  ResourceProfile{"250m", "1", "768Mi", "2Gi"},
		Postgres:                  ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		Caddy:                     ResourceProfile{"100m", "500m", "128Mi", "256Mi"},
		CaddyShipper:              ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:                   ResourceProfile{"100m", "500m", "512Mi", "1Gi"},
		IngestQueueSize:           200,
		IngestWorkers:             8,
		ArchiveMaintainByteBudget: 2 << 30,
		ArchiveRollBytes:          256 << 20,
		ArchiveMaxPendingBytes:    1 << 30,
		SpoolPVCSizeGB:            10,
	},
	{
		Name:                      "Small",
		Description:               "Light production, ~50-200 GB/day (1 shard, 16 vCPU / 32GB per node)",
		CHShards:                  1,
		ClickHouse:                ResourceProfile{"10", "12", "12Gi", "24Gi"},
		CHKeeper:                  ResourceProfile{"250m", "1", "512Mi", "1Gi"},
		Bifract:                   ResourceProfile{"1", "2", "1Gi", "2Gi"},
		ArchiveMaintain:           ResourceProfile{"500m", "2", "3Gi", "5Gi"},
		Archiver:                  ResourceProfile{"500m", "2", "2Gi", "6Gi"},
		Postgres:                  ResourceProfile{"500m", "2", "1Gi", "2Gi"},
		Caddy:                     ResourceProfile{"200m", "1", "256Mi", "512Mi"},
		CaddyShipper:              ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:                   ResourceProfile{"250m", "1", "512Mi", "1Gi"},
		IngestQueueSize:           300,
		IngestWorkers:             12,
		ArchiveMaintainByteBudget: 4 << 30,
		ArchiveRollBytes:          896 << 20,
		ArchiveMaxPendingBytes:    3584 << 20,
		SpoolPVCSizeGB:            20,
	},
	{
		Name:                      "Medium",
		Description:               "Production workloads, ~200-500 GB/day (2 shards, 24 vCPU / 48GB per node)",
		CHShards:                  2,
		ClickHouse:                ResourceProfile{"12", "20", "20Gi", "40Gi"},
		CHKeeper:                  ResourceProfile{"500m", "2", "1Gi", "2Gi"},
		Bifract:                   ResourceProfile{"1", "4", "1Gi", "4Gi"},
		ArchiveMaintain:           ResourceProfile{"500m", "3", "4Gi", "6Gi"},
		Archiver:                  ResourceProfile{"500m", "2", "3Gi", "8Gi"},
		Postgres:                  ResourceProfile{"500m", "2", "1Gi", "4Gi"},
		Caddy:                     ResourceProfile{"250m", "1", "256Mi", "1Gi"},
		CaddyShipper:              ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:                   ResourceProfile{"250m", "1", "512Mi", "1Gi"},
		IngestQueueSize:           500,
		IngestWorkers:             24,
		ArchiveMaintainByteBudget: 8 << 30,
		ArchiveRollBytes:          896 << 20,
		ArchiveMaxPendingBytes:    4 << 30,
		SpoolPVCSizeGB:            32,
	},
	{
		Name:                      "Large",
		Description:               "High-volume production, ~500 GB-2 TB/day (3 shards, 32 vCPU / 64GB per node)",
		CHShards:                  3,
		ClickHouse:                ResourceProfile{"16", "28", "28Gi", "56Gi"},
		CHKeeper:                  ResourceProfile{"500m", "2", "1Gi", "2Gi"},
		Bifract:                   ResourceProfile{"2", "4", "2Gi", "8Gi"},
		ArchiveMaintain:           ResourceProfile{"1", "4", "6Gi", "8Gi"},
		Archiver:                  ResourceProfile{"1", "4", "3Gi", "8Gi"},
		Postgres:                  ResourceProfile{"1", "4", "2Gi", "8Gi"},
		Caddy:                     ResourceProfile{"500m", "2", "512Mi", "1Gi"},
		CaddyShipper:              ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:                   ResourceProfile{"500m", "1", "1Gi", "1Gi"},
		IngestQueueSize:           1000,
		IngestWorkers:             32,
		ArchiveMaintainByteBudget: 32 << 30,
		ArchiveRollBytes:          1 << 30,
		ArchiveMaxPendingBytes:    4 << 30,
		SpoolPVCSizeGB:            64,
	},
	{
		Name:                      "X-Large",
		Description:               "Very high-volume production, ~2-10 TB/day (6 shards, 32 vCPU / 64GB per node)",
		CHShards:                  6,
		ClickHouse:                ResourceProfile{"16", "28", "28Gi", "56Gi"},
		CHKeeper:                  ResourceProfile{"1", "2", "2Gi", "4Gi"},
		Bifract:                   ResourceProfile{"4", "8", "4Gi", "16Gi"},
		ArchiveMaintain:           ResourceProfile{"1", "6", "8Gi", "12Gi"},
		Archiver:                  ResourceProfile{"1", "4", "5Gi", "16Gi"},
		Postgres:                  ResourceProfile{"2", "4", "4Gi", "16Gi"},
		Caddy:                     ResourceProfile{"1", "4", "1Gi", "2Gi"},
		CaddyShipper:              ResourceProfile{"10m", "200m", "32Mi", "128Mi"},
		LiteLLM:                   ResourceProfile{"500m", "2", "1Gi", "2Gi"},
		IngestQueueSize:           2000,
		IngestWorkers:             48,
		ArchiveMaintainByteBudget: 96 << 30,
		ArchiveRollBytes:          2 << 30,
		ArchiveMaxPendingBytes:    8 << 30,
		SpoolPVCSizeGB:            128,
	},
}

// K8sConfig extends SetupConfig with Kubernetes-specific settings.
type K8sConfig struct {
	SetupConfig
	SizeProfile  SizeProfile
	CHShards     int
	CHStorageGB  int
	StorageClass string
	OutputDir    string
	MTLSEnabled  bool
	MTLSCACert   string // PEM-encoded CA cert for client verification
	MTLSCAKey    string // PEM-encoded CA key for signing client certs

	// UserSecrets holds optional user-configured secrets that are not
	// managed by the setup wizard (e.g. LITELLM_API_KEY, OIDC settings).
	// Preserved during upgrades, empty on fresh installs.
	UserSecrets map[string]string

	// ImagePullSecrets preserves manually-added pull secret names across upgrades.
	ImagePullSecrets []string

	// MaxmindPVCAccessMode and MaxmindPVCStorageClass preserve user-customized
	// PVC settings (e.g. ReadWriteMany + azurefile-csi for Azure) across upgrades.
	// Empty values fall back to defaults (ReadWriteOnce, no storageClassName).
	MaxmindPVCAccessMode   string
	MaxmindPVCStorageClass string

	// Dashboard executor tuning. Zero means "use default"; preserved across upgrades.
	DashboardTick       int
	DashboardMinRefresh int
	DashboardWorkers    int

	// IngestReplicas is the ingest tier's replica count. Zero means "use default"
	// (1); preserved across upgrades so an operator-set value survives regeneration.
	IngestReplicas int

	// ArchiveMaintainByteBudget and ArchiveMaintainCommitRetries tune the
	// archive-maintain compaction pass (see archive.DefaultMaintainOptions).
	// Zero means "use the binary default"; preserved across upgrades.
	ArchiveMaintainByteBudget    int64
	ArchiveMaintainCommitRetries int
}

// Dashboard executor defaults (mirror the server's getEnvInt fallbacks).
const (
	defaultDashboardTick       = 5
	defaultDashboardMinRefresh = 10
	defaultDashboardWorkers    = 4
)

// defaultArchiveJobConcurrency mirrors archive.ConfigFromEnv's default: the cap
// is on concurrent archive scans hitting ClickHouse, so it is a policy number
// rather than a sizing one and stays constant across profiles.
const defaultArchiveJobConcurrency = 2

// K8s wizard steps
type k8sStep int

const (
	k8sStepWelcome k8sStep = iota
	k8sStepDomain
	k8sStepSSL
	k8sStepIPAccess
	k8sStepAllowedIPs
	k8sStepSizeProfile
	k8sStepClickHouse
	k8sStepClickHouseHost
	k8sStepClickHouseAuth
	k8sStepCHShards
	k8sStepCHStorage
	k8sStepOutputDir
	k8sStepConfirm
	k8sStepDone
)

// Steps that show in the k8s progress bar (excludes conditional sub-steps)
var k8sStepLabels = []struct {
	step  k8sStep
	label string
}{
	{k8sStepWelcome, "Welcome"},
	{k8sStepDomain, "Domain"},
	{k8sStepSSL, "SSL"},
	{k8sStepIPAccess, "IP Access"},
	{k8sStepSizeProfile, "Resources"},
	{k8sStepClickHouse, "ClickHouse"},
	{k8sStepCHShards, "Cluster"},
	{k8sStepOutputDir, "Output"},
	{k8sStepConfirm, "Confirm"},
}

type k8sWizardModel struct {
	step   k8sStep
	config *K8sConfig
	err    error

	domainInput     textinput.Model
	allowedIPsInput textinput.Model
	shardsInput     textinput.Model
	storageInput    textinput.Model
	outputDirInput  textinput.Model

	sslChoices      []string
	sslCursor       int
	ipChoices       []string
	ipCursor        int
	ipValidationErr string
	sizeCursor      int
	chChoices       []string
	chCursor        int
	chHostInput     textinput.Model
	chUserInput     textinput.Model
	chPassInput     textinput.Model
	chValidationErr string

	width  int
	height int
}

func newK8sWizardModel() k8sWizardModel {
	domain := textinput.New()
	domain.Placeholder = "bifract.example.com"
	domain.Focus()
	domain.Width = 40
	domain.PromptStyle = PromptStyle
	domain.TextStyle = lipgloss.NewStyle().Foreground(White)

	allowedIPs := textinput.New()
	allowedIPs.Placeholder = "10.0.0.0/8, 192.168.1.0/24"
	allowedIPs.Width = 50
	allowedIPs.PromptStyle = PromptStyle
	allowedIPs.TextStyle = lipgloss.NewStyle().Foreground(White)

	shards := textinput.New()
	shards.Placeholder = "1"
	shards.SetValue("1")
	shards.Width = 10
	shards.PromptStyle = PromptStyle
	shards.TextStyle = lipgloss.NewStyle().Foreground(White)

	storage := textinput.New()
	storage.Placeholder = "100"
	storage.SetValue("100")
	storage.Width = 10
	storage.PromptStyle = PromptStyle
	storage.TextStyle = lipgloss.NewStyle().Foreground(White)

	outputDir := textinput.New()
	outputDir.Placeholder = "./bifract-k8s"
	outputDir.SetValue("./bifract-k8s")
	outputDir.Width = 40
	outputDir.PromptStyle = PromptStyle
	outputDir.TextStyle = lipgloss.NewStyle().Foreground(White)

	return k8sWizardModel{
		step: k8sStepWelcome,
		config: &K8sConfig{
			SetupConfig: SetupConfig{ImageTag: Version},
			SizeProfile: sizeProfiles[0],
			CHShards:    1,
			CHStorageGB: 100,
			OutputDir:   "./bifract-k8s",
		},
		domainInput:     domain,
		allowedIPsInput: allowedIPs,
		shardsInput:     shards,
		storageInput:    storage,
		outputDirInput:  outputDir,
		sslChoices:      []string{"Let's Encrypt (automatic)", "Custom certificate"},
		chChoices:       []string{"Bundled (recommended)", "External ClickHouse", "ClickHouse Cloud"},
		chCursor:        0,
		chHostInput:     k8sCHHostInput(),
		chUserInput:     k8sCHUserInput(),
		chPassInput:     k8sCHPassInput(),
		sslCursor:       0,
		ipChoices:       []string{"Allow all traffic", "Restrict UI only (allow ingest)", "Restrict all traffic", "mTLS (mutual TLS for UI)"},
		ipCursor:        0,
	}
}

func (m k8sWizardModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m k8sWizardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.err = fmt.Errorf("cancelled")
			return m, tea.Quit
		case "enter":
			return m.handleEnter()
		case "tab", "shift+tab":
			if m.step == k8sStepClickHouseAuth {
				if m.chUserInput.Focused() {
					m.chUserInput.Blur()
					m.chPassInput.Focus()
				} else {
					m.chPassInput.Blur()
					m.chUserInput.Focus()
				}
				return m, textinput.Blink
			}
		case "up", "k":
			m.handleUp()
		case "down", "j":
			m.handleDown()
		}
	}

	var cmd tea.Cmd
	switch m.step {
	case k8sStepDomain:
		m.domainInput, cmd = m.domainInput.Update(msg)
	case k8sStepClickHouseHost:
		m.chHostInput, cmd = m.chHostInput.Update(msg)
	case k8sStepClickHouseAuth:
		if m.chUserInput.Focused() {
			m.chUserInput, cmd = m.chUserInput.Update(msg)
		} else {
			m.chPassInput, cmd = m.chPassInput.Update(msg)
		}
	case k8sStepAllowedIPs:
		m.allowedIPsInput, cmd = m.allowedIPsInput.Update(msg)
	case k8sStepCHShards:
		m.shardsInput, cmd = m.shardsInput.Update(msg)
	case k8sStepCHStorage:
		m.storageInput, cmd = m.storageInput.Update(msg)
	case k8sStepOutputDir:
		m.outputDirInput, cmd = m.outputDirInput.Update(msg)
	}
	return m, cmd
}

func (m *k8sWizardModel) handleUp() {
	switch m.step {
	case k8sStepSSL:
		if m.sslCursor > 0 {
			m.sslCursor--
		}
	case k8sStepIPAccess:
		if m.ipCursor > 0 {
			m.ipCursor--
		}
	case k8sStepSizeProfile:
		if m.sizeCursor > 0 {
			m.sizeCursor--
		}
	case k8sStepClickHouse:
		if m.chCursor > 0 {
			m.chCursor--
		}
	}
}

func (m *k8sWizardModel) handleDown() {
	switch m.step {
	case k8sStepSSL:
		if m.sslCursor < len(m.sslChoices)-1 {
			m.sslCursor++
		}
	case k8sStepIPAccess:
		if m.ipCursor < len(m.ipChoices)-1 {
			m.ipCursor++
		}
	case k8sStepSizeProfile:
		if m.sizeCursor < len(sizeProfiles)-1 {
			m.sizeCursor++
		}
	case k8sStepClickHouse:
		if m.chCursor < len(m.chChoices)-1 {
			m.chCursor++
		}
	}
}

func (m k8sWizardModel) handleEnter() (tea.Model, tea.Cmd) {
	switch m.step {
	case k8sStepWelcome:
		m.step = k8sStepDomain
		m.domainInput.Focus()
		return m, textinput.Blink

	case k8sStepDomain:
		domain := strings.TrimSpace(m.domainInput.Value())
		if domain == "" {
			return m, nil
		}
		m.config.Domain = domain
		m.step = k8sStepSSL
		return m, nil

	case k8sStepSSL:
		switch m.sslCursor {
		case 0:
			m.config.SSLMode = SSLLetsEncrypt
		case 1:
			m.config.SSLMode = SSLCustom
		}
		m.step = k8sStepIPAccess
		return m, nil

	case k8sStepIPAccess:
		switch m.ipCursor {
		case 0:
			m.config.IPAccess = IPAccessAll
			m.step = k8sStepSizeProfile
			return m, nil
		case 1:
			m.config.IPAccess = IPAccessRestrictApp
		case 2:
			m.config.IPAccess = IPAccessRestrictAll
		case 3:
			m.config.IPAccess = IPAccessMTLSApp
			m.config.MTLSEnabled = true
			m.step = k8sStepSizeProfile
			return m, nil
		}
		m.step = k8sStepAllowedIPs
		m.allowedIPsInput.Focus()
		return m, textinput.Blink

	case k8sStepAllowedIPs:
		ips := strings.TrimSpace(m.allowedIPsInput.Value())
		if ips == "" {
			return m, nil
		}
		m.config.ParseAllowedIPs(ips)
		if err := m.config.ValidateAllowedIPs(); err != nil {
			m.ipValidationErr = err.Error()
			return m, nil
		}
		m.ipValidationErr = ""
		m.step = k8sStepSizeProfile
		return m, nil

	case k8sStepSizeProfile:
		profile := sizeProfiles[m.sizeCursor]
		m.config.SizeProfile = profile
		m.config.CHShards = profile.CHShards
		m.shardsInput.SetValue(fmt.Sprintf("%d", profile.CHShards))
		m.step = k8sStepClickHouse
		return m, nil

	case k8sStepClickHouse:
		switch m.chCursor {
		case 0:
			m.config.CH = ClickHouseTarget{}
			m.config.CH.Normalize()
			// Shard and storage sizing only describe a ClickHouse we render.
			m.step = k8sStepCHShards
			m.shardsInput.Focus()
			return m, textinput.Blink
		case 1:
			m.config.CH = ClickHouseTarget{Backend: CHBackendExternal}
			m.chHostInput.Placeholder = "clickhouse.example.internal:9000"
		case 2:
			m.config.CH = ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cloud", Secure: true}
			m.chHostInput.Placeholder = "your-service.clickhouse.cloud"
		}
		m.chValidationErr = ""
		m.step = k8sStepClickHouseHost
		m.chHostInput.Focus()
		return m, textinput.Blink

	case k8sStepClickHouseHost:
		val := strings.TrimSpace(m.chHostInput.Value())
		if val == "" {
			m.chValidationErr = "A host is required."
			return m, nil
		}
		host, port := val, 0
		if h, p, err := net.SplitHostPort(val); err == nil {
			host = h
			if n, convErr := strconv.Atoi(p); convErr == nil {
				port = n
			}
		}
		m.config.CH.Host = host
		m.config.CH.Port = port
		m.config.CH.Normalize()
		if err := m.config.CH.Validate(); err != nil {
			m.chValidationErr = err.Error()
			return m, nil
		}
		if err := CheckClickHouseReachable(m.config.CH); err != nil {
			m.chValidationErr = err.Error()
			return m, nil
		}
		m.chValidationErr = ""
		m.step = k8sStepClickHouseAuth
		m.chUserInput.Focus()
		return m, textinput.Blink

	case k8sStepClickHouseAuth:
		user := strings.TrimSpace(m.chUserInput.Value())
		if user == "" {
			user = "default"
		}
		if m.chPassInput.Value() == "" {
			m.chValidationErr = "A password is required for an external ClickHouse."
			return m, nil
		}
		m.config.CH.User = user
		m.config.ClickHousePassword = m.chPassInput.Value()
		m.chValidationErr = ""
		// External ClickHouse: skip shard and storage sizing entirely, they
		// describe a workload this installer no longer renders.
		m.step = k8sStepOutputDir
		m.outputDirInput.Focus()
		return m, textinput.Blink

	case k8sStepCHShards:
		val := strings.TrimSpace(m.shardsInput.Value())
		if val == "" {
			val = "1"
		}
		n := 1
		fmt.Sscanf(val, "%d", &n)
		if n < 1 {
			n = 1
		}
		m.config.CHShards = n
		m.step = k8sStepCHStorage
		m.storageInput.Focus()
		return m, textinput.Blink

	case k8sStepCHStorage:
		val := strings.TrimSpace(m.storageInput.Value())
		if val == "" {
			val = "100"
		}
		n := 100
		fmt.Sscanf(val, "%d", &n)
		if n < 10 {
			n = 10
		}
		m.config.CHStorageGB = n
		m.step = k8sStepOutputDir
		m.outputDirInput.Focus()
		return m, textinput.Blink

	case k8sStepOutputDir:
		dir := strings.TrimSpace(m.outputDirInput.Value())
		if dir == "" {
			dir = "./bifract-k8s"
		}
		m.config.OutputDir = dir
		m.step = k8sStepConfirm
		return m, nil

	case k8sStepConfirm:
		m.step = k8sStepDone
		return m, tea.Quit
	}

	return m, nil
}

func (m k8sWizardModel) renderProgress() string {
	var parts []string
	for i, sl := range k8sStepLabels {
		var style lipgloss.Style
		var marker string
		// Map sub-steps to their parent for active highlighting
		current := m.step
		if current == k8sStepAllowedIPs {
			current = k8sStepIPAccess
		} else if current == k8sStepCHStorage {
			current = k8sStepCHShards
		}
		if current == sl.step {
			marker = ">"
			style = StepActiveStyle
		} else if current > sl.step {
			marker = "*"
			style = StepDoneStyle
		} else {
			marker = "."
			style = StepPendingStyle
		}
		label := fmt.Sprintf(" %s %s", marker, sl.label)
		parts = append(parts, style.Render(label))
		if i < len(k8sStepLabels)-1 {
			parts = append(parts, StepPendingStyle.Render(" --"))
		}
	}
	return strings.Join(parts, "")
}

func (m k8sWizardModel) View() string {
	var content string
	var hint string

	switch m.step {
	case k8sStepWelcome:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("Bifract Kubernetes Setup"))
		b.WriteString("\n\n")
		b.WriteString("This wizard generates Kubernetes manifests with secure defaults.\n")
		b.WriteString("You will need the official ClickHouse Operator and cert-manager installed.")
		content = b.String()
		hint = "Enter to continue  |  q to quit"

	case k8sStepDomain:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("Domain"))
		b.WriteString("\n\n")
		b.WriteString("Enter your domain name.\n\n")
		b.WriteString(LabelStyle.Render("  Domain"))
		b.WriteString("\n")
		b.WriteString("  " + m.domainInput.View())
		content = b.String()
		hint = "Enter to confirm  |  Esc to go back"

	case k8sStepSSL:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("SSL/TLS Configuration"))
		b.WriteString("\n\n")
		b.WriteString("How should Bifract handle HTTPS?\n\n")
		sslDescriptions := []string{
			"Caddy obtains a trusted certificate from Let's Encrypt. Requires a public domain.",
			"Provide your own certificate and key files.",
		}
		b.WriteString(RenderOptionList(m.sslChoices, sslDescriptions, m.sslCursor))
		content = b.String()
		hint = "Up/Down to select  |  Enter to confirm  |  Esc to go back"

	case k8sStepIPAccess:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("IP Access Control"))
		b.WriteString("\n\n")
		b.WriteString("Restrict IP access to Bifract.\n")
		b.WriteString(DimStyle.Render("Non-allowed IPs are rejected by Caddy before reaching the application."))
		b.WriteString("\n\n")
		ipDescriptions := []string{
			"No restrictions. All traffic is allowed through.",
			"Only allowed IPs can access the UI and API. Ingest endpoints remain open to all.",
			"Only allowed IPs can access anything, including ingest endpoints.",
			"Require client certificates for UI and API. Ingest endpoints remain open.",
		}
		b.WriteString(RenderOptionList(m.ipChoices, ipDescriptions, m.ipCursor))
		content = b.String()
		hint = "Up/Down to select  |  Enter to confirm  |  Esc to go back"

	case k8sStepAllowedIPs:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("Allowed IP Addresses"))
		b.WriteString("\n\n")
		b.WriteString("Enter the IPs or CIDR ranges that should be allowed, separated by commas.\n")
		b.WriteString(DimStyle.Render("Example: 10.0.0.0/8, 192.168.1.0/24, 203.0.113.5"))
		b.WriteString("\n\n")
		b.WriteString(LabelStyle.Render("  Allowed IPs"))
		b.WriteString("\n")
		b.WriteString("  " + m.allowedIPsInput.View())
		if m.ipValidationErr != "" {
			b.WriteString("\n\n")
			b.WriteString(ErrorStyle.Render("  " + m.ipValidationErr))
		}
		content = b.String()
		hint = "Enter to confirm  |  Esc to go back"

	case k8sStepClickHouse:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("ClickHouse Backend"))
		b.WriteString("\n\n")
		b.WriteString("Where should Bifract store log data?\n\n")
		b.WriteString(RenderOptionList(m.chChoices, []string{
			"Deploy ClickHouse into this cluster with the ClickHouse operator.",
			"Connect to a ClickHouse you already run. No ClickHouse workloads are deployed.",
			"Connect to a managed ClickHouse Cloud service over TLS.",
		}, m.chCursor))
		content = b.String()
		hint = "Up/Down to select  |  Enter to confirm  |  Esc to go back"

	case k8sStepClickHouseHost:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("ClickHouse Endpoint"))
		b.WriteString("\n\n")
		if m.config.CH.Deployment == "cloud" {
			b.WriteString("Enter your service hostname. TLS is required and port 9440 is assumed.\n\n")
		} else {
			b.WriteString("Enter the native-protocol endpoint. Port 9000 is assumed if omitted.\n\n")
		}
		b.WriteString(LabelStyle.Render("  Host"))
		b.WriteString("\n")
		b.WriteString("  " + m.chHostInput.View())
		if m.chValidationErr != "" {
			b.WriteString("\n\n")
			b.WriteString(ErrorStyle.Render("  " + m.chValidationErr))
		}
		content = b.String()
		hint = "Enter to confirm  |  Esc to go back"

	case k8sStepClickHouseAuth:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("ClickHouse Credentials"))
		b.WriteString("\n\n")
		b.WriteString("The user Bifract connects as. It needs privileges to create the schema,\n")
		b.WriteString("and to create the least-privilege ingest user.\n\n")
		b.WriteString(LabelStyle.Render("  Username"))
		b.WriteString("\n")
		b.WriteString("  " + m.chUserInput.View())
		b.WriteString("\n\n")
		b.WriteString(LabelStyle.Render("  Password"))
		b.WriteString("\n")
		b.WriteString("  " + m.chPassInput.View())
		if m.chValidationErr != "" {
			b.WriteString("\n\n")
			b.WriteString(ErrorStyle.Render("  " + m.chValidationErr))
		}
		content = b.String()
		hint = "Tab to switch fields  |  Enter to confirm"

	case k8sStepSizeProfile:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("Resource Profile"))
		b.WriteString("\n\n")
		b.WriteString("Select a resource profile for your cluster.\n\n")
		var profileNames, profileDescs []string
		for _, p := range sizeProfiles {
			profileNames = append(profileNames, p.Name)
			profileDescs = append(profileDescs, p.Description)
		}
		b.WriteString(RenderOptionList(profileNames, profileDescs, m.sizeCursor))
		b.WriteString("\n\n")
		b.WriteString(DimStyle.Render("Shard count can be adjusted in the next step."))
		content = b.String()
		hint = "Up/Down to select  |  Enter to confirm  |  Esc to go back"

	case k8sStepCHShards:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("ClickHouse Shards"))
		b.WriteString("\n\n")
		b.WriteString(LabelStyle.Render("  Shards"))
		b.WriteString("\n")
		b.WriteString("  " + m.shardsInput.View())
		b.WriteString("\n\n")
		b.WriteString(DimStyle.Render("Shards distribute data horizontally. Each shard is a single replica; Iceberg handles durability. 1 is fine for most workloads."))
		content = b.String()
		hint = "Enter to confirm  |  Esc to go back"

	case k8sStepCHStorage:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("ClickHouse Storage"))
		b.WriteString("\n\n")
		b.WriteString(LabelStyle.Render("  Storage per shard (GB)"))
		b.WriteString("\n")
		b.WriteString("  " + m.storageInput.View())
		content = b.String()
		hint = "Enter to confirm  |  Esc to go back"

	case k8sStepOutputDir:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("Output Directory"))
		b.WriteString("\n\n")
		b.WriteString(LabelStyle.Render("  Directory"))
		b.WriteString("\n")
		b.WriteString("  " + m.outputDirInput.View())
		content = b.String()
		hint = "Enter to confirm  |  Esc to go back"

	case k8sStepConfirm:
		var b strings.Builder
		b.WriteString(TitleStyle.Render("Ready to Generate"))
		b.WriteString("\n\n")
		row := func(label, value string) string {
			return fmt.Sprintf("  %s  %s\n", PromptStyle.Render(label), ValueStyle.Render(value))
		}
		b.WriteString(row("Domain:           ", m.config.Domain))
		b.WriteString(row("SSL:              ", string(m.config.SSLMode)))
		b.WriteString(row("IP Access:        ", string(m.config.IPAccess)))
		b.WriteString(row("Resource Profile: ", m.config.SizeProfile.Name))
		b.WriteString(row("CH Shards:        ", fmt.Sprintf("%d", m.config.CHShards)))
		b.WriteString(row("CH Storage:       ", fmt.Sprintf("%dGi per shard", m.config.CHStorageGB)))
		b.WriteString(row("Output:           ", m.config.OutputDir))
		content = b.String()
		hint = "Enter to generate  |  Esc to go back  |  q to quit"
	}

	if m.step == k8sStepDone {
		return "\n"
	}

	var out strings.Builder
	out.WriteString(TitleStyle.Render(bannerArt))
	out.WriteString("\n")
	out.WriteString(SubtitleStyle.Render("Log Management, Detection, and Collaboration"))
	out.WriteString("  ")
	out.WriteString(DimStyle.Render(Version))
	out.WriteString("\n")

	if m.step != k8sStepWelcome {
		out.WriteString("\n")
		out.WriteString(m.renderProgress())
		out.WriteString("\n")
	}

	out.WriteString(PanelStyle.Render(content))
	out.WriteString("\n")
	out.WriteString(HintStyle.Render("  " + hint))
	return out.String()
}

// RunInstallK8s runs the Kubernetes installation wizard and generates manifests.
func RunInstallK8s() error {
	defer abandonStep() // clean up any in-progress spinner on an early error return
	model := newK8sWizardModel()
	p := tea.NewProgram(model)
	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("wizard error: %w", err)
	}

	final := finalModel.(k8sWizardModel)
	if final.err != nil {
		return final.err
	}
	if final.step != k8sStepDone {
		return fmt.Errorf("wizard did not complete")
	}

	cfg := final.config

	// Generate secure credentials
	PrintBanner()
	fmt.Println(TitleStyle.Render("  Generating Kubernetes Manifests"))
	fmt.Println()

	resetSteps(3) // credentials, directories, manifests

	printStep("Generating secure credentials...")
	if err := cfg.GeneratePasswords(); err != nil {
		return fmt.Errorf("generate passwords: %w", err)
	}
	if cfg.MTLSEnabled {
		caCert, caKey, err := GenerateClientCAPEM()
		if err != nil {
			return fmt.Errorf("generate mTLS CA: %w", err)
		}
		cfg.MTLSCACert = caCert
		cfg.MTLSCAKey = caKey
	}
	printDone("Credentials generated")

	// Create output directory
	printStep("Creating output directory...")
	dirs := []string{
		cfg.OutputDir,
		filepath.Join(cfg.OutputDir, "clickhouse"),
		filepath.Join(cfg.OutputDir, "postgres"),
		filepath.Join(cfg.OutputDir, "bifract"),
		filepath.Join(cfg.OutputDir, "caddy"),
		filepath.Join(cfg.OutputDir, "litellm"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("create directory %s: %w", d, err)
		}
	}
	printDone("Directories created")

	// Generate manifests
	printStep("Writing manifests...")
	if err := writeK8sManifests(cfg); err != nil {
		return fmt.Errorf("write manifests: %w", err)
	}
	if cfg.MTLSEnabled {
		caDir := filepath.Join(cfg.OutputDir, "client-ca")
		if err := os.MkdirAll(caDir, 0700); err != nil {
			return fmt.Errorf("create client-ca dir: %w", err)
		}
		if err := os.WriteFile(filepath.Join(caDir, "ca.pem"), []byte(cfg.MTLSCACert), 0644); err != nil {
			return fmt.Errorf("write CA cert: %w", err)
		}
		if err := os.WriteFile(filepath.Join(caDir, "ca-key.pem"), []byte(cfg.MTLSCAKey), 0600); err != nil {
			return fmt.Errorf("write CA key: %w", err)
		}
	}
	printDone("Manifests written to " + cfg.OutputDir)

	// Final summary
	fmt.Println()
	fmt.Println(TitleStyle.Render("  Kubernetes Manifests Ready"))
	fmt.Println()

	summaryText := fmt.Sprintf(
		"%s  %s\n%s  %s\n%s  %s\n\n%s  %s",
		PromptStyle.Render("Domain:   "), ValueStyle.Render(cfg.Domain),
		PromptStyle.Render("Username: "), ValueStyle.Render("admin"),
		PromptStyle.Render("Password: "), lipgloss.NewStyle().Foreground(White).Bold(true).Render(cfg.AdminPassword),
		PromptStyle.Render("Manifests:"), DimStyle.Render(cfg.OutputDir),
	)

	summary := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Green).
		Padding(1, 3).
		Render(summaryText)
	fmt.Println(summary)
	fmt.Println()
	fmt.Println(WarningStyle.Render("  Save the admin password above. It will not be shown again."))
	if cfg.MTLSEnabled {
		fmt.Println()
		fmt.Println(WarningStyle.Render("  mTLS is enabled. CA files are in " + filepath.Join(cfg.OutputDir, "client-ca") + "/"))
		fmt.Println(DimStyle.Render("  Generate a client certificate with:"))
		fmt.Println(DimStyle.Render("    bifract --gen-client-cert --dir " + cfg.OutputDir + " --name \"user@example.com\" --password changeme"))
	}
	fmt.Println()
	fmt.Println(DimStyle.Render("  Deploy with:"))
	fmt.Println(DimStyle.Render("    1. Install cert-manager:"))
	fmt.Println(DimStyle.Render("       kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml"))
	fmt.Println(DimStyle.Render("    2. Install the ClickHouse Operator:"))
	fmt.Println(DimStyle.Render("       helm install clickhouse-operator -n clickhouse-operator-system --create-namespace \\"))
	fmt.Println(DimStyle.Render("         oci://ghcr.io/clickhouse/clickhouse-operator-helm"))
	fmt.Println(DimStyle.Render("    3. Apply the manifests:"))
	fmt.Println(DimStyle.Render("       kubectl apply -k " + cfg.OutputDir))
	fmt.Println()

	return nil
}

// k8sTemplateData holds all values needed by the K8s manifest templates.
type k8sTemplateData struct {
	ImageTag       string
	Domain         string
	CHShards       int
	CHStorageGB    int
	CHStorageStr   string
	CHPasswordHash string
	CHHostsList    string
	// CHBundled drives every manifest that only exists for a ClickHouse this
	// installer renders: the ClickHouseInstallation, its load balancer, its
	// NetworkPolicy and its kustomization entries.
	CHBundled                bool
	CHEnvApp                 []EnvVar
	CHEnvIngest              []EnvVar
	PostgresPassword         string
	IngestPostgresPassword   string
	ClickHousePassword       string
	IngestClickHousePassword string
	PasswordPepper           string
	AdminPasswordHash        string
	FeedEncryptionKey        string
	BackupEncryptionKey      string
	LiteLLMMasterKey         string
	IPBlock                  string
	IPBlockIngest            string
	MTLSEnabled              bool
	MTLSCACert               string
	MTLSCAKey                string

	// Resource profiles
	CH         ResourceProfile
	CHKeeper   ResourceProfile
	BifractRes ResourceProfile
	// ArchiverRes sizes the archiver drain-loop sidecar; ArchiveMaintainRes sizes
	// the compaction Deployment. Split because only the latter decodes Parquet.
	ArchiverRes        ResourceProfile
	ArchiveMaintainRes ResourceProfile
	PostgresRes        ResourceProfile
	CaddyRes           ResourceProfile
	CaddyShipper       ResourceProfile
	LiteLLMRes         ResourceProfile

	// User-configured secrets (preserved during upgrades, empty on fresh install)
	UserSecrets map[string]string

	// ImagePullSecrets preserves manually-added pull secret names across upgrades.
	ImagePullSecrets []string

	// MaxmindPVCAccessMode and MaxmindPVCStorageClass carry user-customized PVC
	// settings through upgrades. Empty = use template defaults.
	MaxmindPVCAccessMode   string
	MaxmindPVCStorageClass string

	// IngestQueueSize and IngestWorkers tune the bifract ingest queue.
	IngestQueueSize int

	// IngestWorkers is how many inserts run against ClickHouse concurrently per ingest pod.
	// ClickHouse inserts are CPU-bound building skip indexes, not disk-bound (measured on a
	// 3-shard Large cluster: 684ms CPU vs 12ms disk per insert, OSIOWaitTime 0), so ingest
	// throughput scales with concurrency until the shard CPUs saturate. These track the CPU
	// actually provisioned per shard: too low and ClickHouse sits idle while ingest plateaus
	// below target, which looks like a platform limit but is only a concurrency limit.
	// Large (32) is measured: raising it from 8 moved 6,800 -> 8,966 events/sec, no other change.
	IngestWorkers int
	// IngestReplicas is the replica count for the independently-scalable ingest tier.
	IngestReplicas int

	// SpoolPVCSize is the per-ingest-pod durable spool PVC size (e.g. "64Gi"), and
	// SpoolMaxBytes is the spool capacity watermark in bytes (~80% of the PVC), at
	// which ingest backpressures. They move together with the size profile.
	SpoolPVCSize  string
	SpoolMaxBytes int64

	// ArchiveMaintainByteBudget and ArchiveMaintainCommitRetries tune the
	// archive-maintain compaction pass (resolved to binary defaults when unset).
	ArchiveMaintainByteBudget    int64
	ArchiveMaintainCommitRetries int

	// ArchiveJobConcurrency caps concurrent recall (and, separately, restore) jobs
	// deployment-wide. It bounds archive scans against ClickHouse rather than
	// worker processes, so it does not scale with the size profile.
	ArchiveJobConcurrency int

	// Dashboard executor tuning (resolved to defaults when unset).
	DashboardTick       int
	DashboardMinRefresh int
	DashboardWorkers    int

	// ClickHouse memory tuning, derived from the CH memory limit. We set an
	// explicit max_server_memory_usage and disable the cgroup memory worker
	// (memory_worker_use_cgroup=0): under container memory limits, page cache
	// counts toward cgroup usage, and the observer clamps ClickHouse's effective
	// limit down as cache fills -- which starves merges/inserts and can stall the
	// cluster. An explicit cap at 80% of the pod limit (20% headroom for OS/cache)
	// avoids that.
	//
	// CHMaxBytesToMerge caps a single merge's input size. This is NOT scaled to
	// leave a fraction of the pod's memory budget -- the per-byte memory cost of
	// merging this schema (a wide `fields` JSON column plus a dozen-plus skip
	// indexes rebuilt every merge) is a property of the table, not of how much RAM
	// the pod happens to have. Measured on 2026-07-22 against real merge history
	// (system.part_log.peak_memory_usage): merges from ~2GB up to ~19.5GB never
	// peaked above ~5.4GB of actual memory, so 25% of CHMaxServerMemory (~8.8GiB on
	// a 44Gi pod) keeps meaningful headroom above every observed case while
	// recovering most of the query-performance cost of an aggressive cap (fewer,
	// smaller final parts per partition). The incident that motivated this (merges
	// crash-looping under MEMORY_LIMIT_EXCEEDED) was actually driven by concurrency
	// -- background task count spiking to background_pool_size x
	// background_merges_mutations_concurrency_ratio -- compounded by an unrelated
	// unbounded background query (see storage.QueryLowPriorityBounded), not by any
	// single merge's size.
	//
	// CHMergesMutationsMemoryLimit caps how much memory ALL concurrent background
	// merges/mutations may use at once. ClickHouse's own default for this
	// (merges_mutations_memory_usage_to_ram_ratio=0.5) is computed against
	// getMemoryAmount()/OSMemoryTotal, which under Kubernetes reflects the node's
	// full memory rather than the pod's cgroup limit -- so the default leaves the
	// throttle far too loose to protect the pod's real ceiling, and background
	// tasks keep starting until they collide with max_server_memory_usage and get
	// killed (MEMORY_LIMIT_EXCEEDED), retrying forever. Anchoring the same 0.5
	// ratio to the correctly-scoped CHMaxServerMemory instead fixes this.
	//
	// All three are 0 when MemLimit can't be parsed, in which case the template
	// omits the block and ClickHouse defaults apply.
	CHMaxServerMemory            int64
	CHMaxBytesToMerge            int64
	CHMergesMutationsMemoryLimit int64
}

// k8sManifestFile maps an embedded template to its output path.
type k8sManifestFile struct {
	template string // path within TemplateFS
	output   string // path relative to output dir
}

var k8sManifests = []k8sManifestFile{
	{"templates/k8s/namespace.yaml.tmpl", "namespace.yaml"},
	{"templates/k8s/kustomization.yaml.tmpl", "kustomization.yaml"},
	{"templates/k8s/postgres-statefulset.yaml.tmpl", "postgres/statefulset.yaml"},
	{"templates/k8s/bifract-deployment.yaml.tmpl", "bifract/deployment.yaml"},
	{"templates/k8s/bifract-ingest-deployment.yaml.tmpl", "bifract/ingest-deployment.yaml"},
	{"templates/k8s/bifract-configmap.yaml.tmpl", "bifract/configmap.yaml"},
	{"templates/k8s/bifract-secrets.yaml.tmpl", "bifract/secrets.yaml"},
	{"templates/k8s/bifract-archive-maintain-deployment.yaml.tmpl", "bifract/archive-maintain-deployment.yaml"},
	{"templates/k8s/caddy-deployment.yaml.tmpl", "caddy/deployment.yaml"},
	{"templates/k8s/caddy-configmap.yaml.tmpl", "caddy/configmap.yaml"},
	{"templates/k8s/caddy-log-shipper.yaml.tmpl", "caddy/log-shipper.yaml"},
	{"templates/k8s/litellm-deployment.yaml.tmpl", "litellm/deployment.yaml"},
	{"templates/k8s/litellm-configmap.yaml.tmpl", "litellm/configmap.yaml"},
	{"templates/k8s/network-policies.yaml.tmpl", "network-policies.yaml"},
}

// k8sConditionalManifests are rendered only for some configurations. They are a
// separate list rather than an inline branch so the YAML-validity test can
// iterate both sets and keep covering them.
var k8sConditionalManifests = []k8sManifestFile{
	{"templates/k8s/clickhouse-installation.yaml.tmpl", "clickhouse/clickhouse-installation.yaml"},
	{"templates/k8s/clickhouse-lb-service.yaml.tmpl", "clickhouse/lb-service.yaml"},
}

func writeK8sManifests(cfg *K8sConfig) error {
	if cfg.UserSecrets == nil {
		cfg.UserSecrets = make(map[string]string)
	}
	// Seed the archive roll thresholds from the size profile so a fresh install
	// gets values matched to its scale instead of the archiver's flat code
	// defaults, which are Dev-sized and leave a large install writing Parquet
	// below compaction's optimal-file floor forever. An operator-set value always
	// wins, and a zero profile value (the "custom" profile that upgrade/reconfigure
	// build from parsed manifests) writes nothing, so an existing install's
	// behaviour is never changed underneath it.
	defaultUserSecretBytes(cfg.UserSecrets, "ARCHIVE_ROLL_BYTES", cfg.SizeProfile.ArchiveRollBytes)
	defaultUserSecretBytes(cfg.UserSecrets, "ARCHIVE_MAX_PENDING_BYTES", cfg.SizeProfile.ArchiveMaxPendingBytes)
	// Derive ClickHouse memory settings from the CH pod memory limit. 80% of the
	// limit leaves ~20% headroom for the OS and (reclaimable) page cache. The merge
	// cap (25%) is schema-driven, not pod-size-driven -- see the CHMaxBytesToMerge
	// field doc comment for the 2026-07-22 measurement behind it. The
	// merges/mutations concurrency budget is ~50% of chMaxServerMemory -- mirrors
	// ClickHouse's own default ratio, but anchored to the pod-scoped value instead
	// of ClickHouse's node-wide getMemoryAmount().
	var chMaxServerMemory, chMaxBytesToMerge, chMergesMutationsMemoryLimit int64
	if chMemBytes := parseK8sMemToBytes(cfg.SizeProfile.ClickHouse.MemLimit); chMemBytes > 0 {
		chMaxServerMemory = chMemBytes * 8 / 10
		chMaxBytesToMerge = chMaxServerMemory / 4
		chMergesMutationsMemoryLimit = chMaxServerMemory / 2
	}
	// Guard against a zero-value profile (a custom/partial SizeProfile) yielding a
	// "0Gi" PVC, which the API server rejects: fall back to the Dev spool size.
	spoolPVCSizeGB := cfg.SizeProfile.SpoolPVCSizeGB
	if spoolPVCSizeGB <= 0 {
		spoolPVCSizeGB = 10
	}
	data := k8sTemplateData{
		ImageTag:                 cfg.ImageTag,
		ImagePullSecrets:         cfg.ImagePullSecrets,
		Domain:                   cfg.Domain,
		CHShards:                 cfg.CHShards,
		CHStorageGB:              cfg.CHStorageGB,
		CHStorageStr:             formatStorageSize(cfg.CHStorageGB),
		CHPasswordHash:           fmt.Sprintf("%x", sha256.Sum256([]byte(cfg.ClickHousePassword))),
		CHHostsList:              buildCHHostsList(cfg),
		CHBundled:                cfg.CH.Bundled(),
		CHEnvApp:                 k8sCHEnv(cfg, ""),
		CHEnvIngest:              k8sCHEnv(cfg, storage.IngestCHUser),
		PostgresPassword:         cfg.PostgresPassword,
		ClickHousePassword:       cfg.ClickHousePassword,
		IngestClickHousePassword: cfg.IngestClickHousePassword,
		IngestPostgresPassword:   cfg.IngestPostgresPassword,
		PasswordPepper:           cfg.PasswordPepper,
		AdminPasswordHash:        cfg.AdminPasswordHash,
		FeedEncryptionKey:        cfg.FeedEncryptionKey,
		BackupEncryptionKey:      cfg.BackupEncryptionKey,
		LiteLLMMasterKey:         cfg.LiteLLMMasterKey,
		UserSecrets:              cfg.UserSecrets,
		IPBlock:                  buildIPBlock(cfg),
		IPBlockIngest:            buildIPBlockIngest(cfg),
		MTLSEnabled:              cfg.MTLSEnabled,
		MTLSCACert:               indentPEM(cfg.MTLSCACert, "    "),
		MTLSCAKey:                indentPEM(cfg.MTLSCAKey, "    "),
		MaxmindPVCAccessMode:     cfg.MaxmindPVCAccessMode,
		MaxmindPVCStorageClass:   cfg.MaxmindPVCStorageClass,
		IngestQueueSize:          cfg.SizeProfile.IngestQueueSize,
		IngestWorkers:            cfg.SizeProfile.IngestWorkers,
		IngestReplicas:           fallbackInt(cfg.IngestReplicas, 1),
		SpoolPVCSize:             formatStorageSize(spoolPVCSizeGB),
		SpoolMaxBytes:            int64(spoolPVCSizeGB) * (1 << 30) * 80 / 100,
		DashboardTick:            fallbackInt(cfg.DashboardTick, defaultDashboardTick),
		DashboardMinRefresh:      fallbackInt(cfg.DashboardMinRefresh, defaultDashboardMinRefresh),
		DashboardWorkers:         fallbackInt(cfg.DashboardWorkers, defaultDashboardWorkers),
		// An explicitly tuned value wins; otherwise scale with the size profile so
		// compaction throughput keeps pace with the profile's ingest band (see
		// SizeProfile.ArchiveMaintainByteBudget). The binary default is the last
		// resort for a zero-value profile.
		ArchiveMaintainByteBudget: fallbackInt64(cfg.ArchiveMaintainByteBudget,
			fallbackInt64(cfg.SizeProfile.ArchiveMaintainByteBudget, archive.DefaultMaintainOptions().ByteBudget)),
		ArchiveMaintainCommitRetries: fallbackInt(cfg.ArchiveMaintainCommitRetries, archive.DefaultMaintainOptions().CommitRetries),
		ArchiveJobConcurrency:        defaultArchiveJobConcurrency,
		CHMaxServerMemory:            chMaxServerMemory,
		CHMaxBytesToMerge:            chMaxBytesToMerge,
		CHMergesMutationsMemoryLimit: chMergesMutationsMemoryLimit,
		CH:                           cfg.SizeProfile.ClickHouse,
		CHKeeper:                     cfg.SizeProfile.CHKeeper,
		BifractRes:                   cfg.SizeProfile.Bifract,
		ArchiverRes:                  cfg.SizeProfile.Archiver,
		ArchiveMaintainRes:           cfg.SizeProfile.ArchiveMaintain,
		PostgresRes:                  cfg.SizeProfile.Postgres,
		CaddyRes:                     cfg.SizeProfile.Caddy,
		CaddyShipper:                 cfg.SizeProfile.CaddyShipper,
		LiteLLMRes:                   cfg.SizeProfile.LiteLLM,
	}

	for _, m := range k8sManifests {
		content, err := renderK8sTemplate(m.template, data)
		if err != nil {
			return fmt.Errorf("render %s: %w", m.template, err)
		}
		outPath := filepath.Join(cfg.OutputDir, m.output)
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return fmt.Errorf("create dir for %s: %w", m.output, err)
		}
		if err := os.WriteFile(outPath, []byte(content), 0600); err != nil {
			return fmt.Errorf("write %s: %w", m.output, err)
		}
	}

	// ClickHouse manifests exist only for a ClickHouse this installer renders.
	// An external one brings its own load balancing, and there is no in-cluster
	// workload to declare or select.
	if cfg.CH.Bundled() {
		for _, m := range k8sConditionalManifests {
			// The LB only exists once there is more than one shard to spread across.
			if m.output == "clickhouse/lb-service.yaml" && cfg.CHShards <= 1 {
				continue
			}
			content, err := renderK8sTemplate(m.template, data)
			if err != nil {
				return fmt.Errorf("render %s: %w", m.template, err)
			}
			outPath := filepath.Join(cfg.OutputDir, m.output)
			if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
				return fmt.Errorf("create dir for %s: %w", m.output, err)
			}
			if err := os.WriteFile(outPath, []byte(content), 0600); err != nil {
				return fmt.Errorf("write %s: %w", m.output, err)
			}
		}
	}

	// Conditionally write mTLS CA secrets.
	if cfg.MTLSEnabled {
		// Caddy secret: CA cert only (for client verification)
		content, err := renderK8sTemplate("templates/k8s/caddy-mtls.yaml.tmpl", data)
		if err != nil {
			return fmt.Errorf("render mTLS template: %w", err)
		}
		outPath := filepath.Join(cfg.OutputDir, "caddy/mtls-ca.yaml")
		if err := os.WriteFile(outPath, []byte(content), 0600); err != nil {
			return fmt.Errorf("write mTLS CA secret: %w", err)
		}

		// Bifract secret: CA cert + key (for client cert generation)
		content, err = renderK8sTemplate("templates/k8s/bifract-mtls.yaml.tmpl", data)
		if err != nil {
			return fmt.Errorf("render bifract mTLS template: %w", err)
		}
		outPath = filepath.Join(cfg.OutputDir, "bifract/mtls-ca.yaml")
		if err := os.WriteFile(outPath, []byte(content), 0600); err != nil {
			return fmt.Errorf("write bifract mTLS secret: %w", err)
		}
	}

	return nil
}

func renderK8sTemplate(name string, data k8sTemplateData) (string, error) {
	content, err := TemplateFS.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("read template %s: %w", name, err)
	}
	tmpl, err := template.New(filepath.Base(name)).Parse(string(content))
	if err != nil {
		return "", fmt.Errorf("parse template %s: %w", name, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template %s: %w", name, err)
	}
	return buf.String(), nil
}

// buildCHHostsList generates the comma-separated ClickHouse host list for the
// Bifract deployment env var based on the official operator's naming convention.
// Pods are named: bifract-ch-clickhouse-{shard}-{replica}-0
// Each shard runs a single replica (replica index 0), so the list is one host per shard.
func formatStorageSize(gb int) string {
	if gb >= 1024 && gb%1024 == 0 {
		return fmt.Sprintf("%dTi", gb/1024)
	}
	return fmt.Sprintf("%dGi", gb)
}

// parseK8sMemToBytes parses a Kubernetes memory quantity ("44Gi", "512Mi",
// "2Ti", or a bare byte count) into bytes. Returns 0 when it can't be parsed,
// which callers treat as "unknown" (falling back to ClickHouse defaults).
func parseK8sMemToBytes(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "Ti"):
		mult, s = 1<<40, strings.TrimSuffix(s, "Ti")
	case strings.HasSuffix(s, "Gi"):
		mult, s = 1<<30, strings.TrimSuffix(s, "Gi")
	case strings.HasSuffix(s, "Mi"):
		mult, s = 1<<20, strings.TrimSuffix(s, "Mi")
	case strings.HasSuffix(s, "Ki"):
		mult, s = 1<<10, strings.TrimSuffix(s, "Ki")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n * mult
}

func buildCHHostsList(cfg *K8sConfig) string {
	if !cfg.CH.Bundled() {
		// An external cluster's addresses are the operator's to choose; the pod
		// naming convention below only describes one we render ourselves.
		return cfg.CH.Hosts
	}
	hosts := make([]string, 0, cfg.CHShards)
	for s := 0; s < cfg.CHShards; s++ {
		hosts = append(hosts, fmt.Sprintf("bifract-ch-clickhouse-%d-0-0.bifract-ch-clickhouse-headless", s))
	}
	return strings.Join(hosts, ",")
}

// indentPEM prepends each line of a PEM string with the given prefix.
func indentPEM(pem, prefix string) string {
	lines := strings.Split(strings.TrimRight(pem, "\n"), "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

// buildIPBlock generates the Caddy IP restriction block for the main site in the Caddyfile template.
func buildIPBlock(cfg *K8sConfig) string {
	if cfg.IPAccess != IPAccessRestrictApp && cfg.IPAccess != IPAccessRestrictAll {
		return ""
	}
	if len(cfg.AllowedIPs) == 0 {
		return ""
	}
	ipList := strings.Join(cfg.AllowedIPs, " ")
	return fmt.Sprintf("      @blocked not remote_ip %s\n      respond @blocked 403\n", ipList)
}

// buildIPBlockIngest generates the Caddy IP restriction block for the ingest port (8443).
// Only restrict-all mode restricts ingest; restrict-app leaves ingest open to all IPs.
func buildIPBlockIngest(cfg *K8sConfig) string {
	if cfg.IPAccess != IPAccessRestrictAll {
		return ""
	}
	if len(cfg.AllowedIPs) == 0 {
		return ""
	}
	ipList := strings.Join(cfg.AllowedIPs, " ")
	return fmt.Sprintf("      @blocked_ingest not remote_ip %s\n      respond @blocked_ingest 403\n", ipList)
}

// k8sCHEnv renders the ClickHouse environment for one k8s workload. The bundled
// addressing is the operator's convention: per-shard pod FQDNs behind the
// headless service, a cluster named "default" created by the ClickHouseCluster,
// and a load balancer service once there is more than one shard to spread across.
func k8sCHEnv(cfg *K8sConfig, userOverride string) []EnvVar {
	o := CHEnvOptions{UserOverride: userOverride}
	if cfg.CH.Bundled() {
		o.BundledHosts = buildCHHostsList(cfg)
		o.BundledCluster = "default"
		if cfg.CHShards > 1 {
			o.BundledWriteHost = k8sCHLoadBalancerService
		}
	}
	return cfg.CH.EnvVars(o)
}

// k8sCHLoadBalancerService must match the Service name in
// clickhouse-lb-service.yaml.tmpl.
const k8sCHLoadBalancerService = "bifract-ch-clickhouse-lb"

// k8sCHHostInput is the endpoint field for an external ClickHouse.
func k8sCHHostInput() textinput.Model {
	in := textinput.New()
	in.Placeholder = "clickhouse.example.internal:9000"
	in.Width = 48
	in.PromptStyle = PromptStyle
	in.TextStyle = lipgloss.NewStyle().Foreground(White)
	return in
}

// k8sCHUserInput and k8sCHPassInput collect the credential for an external
// ClickHouse. It cannot be generated: the server already exists and owns its
// own users.
func k8sCHUserInput() textinput.Model {
	in := textinput.New()
	in.Placeholder = "default"
	in.Width = 32
	in.PromptStyle = PromptStyle
	in.TextStyle = lipgloss.NewStyle().Foreground(White)
	return in
}

func k8sCHPassInput() textinput.Model {
	in := textinput.New()
	in.EchoMode = textinput.EchoPassword
	in.Width = 48
	in.PromptStyle = PromptStyle
	in.TextStyle = lipgloss.NewStyle().Foreground(White)
	return in
}
