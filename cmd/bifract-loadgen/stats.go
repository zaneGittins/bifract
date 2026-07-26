package main

import (
	"encoding/csv"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// bitset tracks which corpus indices a worker actually touched, so the run can report
// exact emitted cardinality rather than the pool size.
type bitset struct{ w []uint64 }

func newBitset(n int) *bitset { return &bitset{w: make([]uint64, (n+63)/64)} }

func (b *bitset) set(i uint64) {
	if idx := i >> 6; int(idx) < len(b.w) {
		b.w[idx] |= 1 << (i & 63)
	}
}

func (b *bitset) or(o *bitset) {
	for i := range b.w {
		if i < len(o.w) {
			b.w[i] |= o.w[i]
		}
	}
}

func (b *bitset) count() int {
	n := 0
	for _, x := range b.w {
		for ; x != 0; x &= x - 1 {
			n++
		}
	}
	return n
}

// cardinality is per-worker to avoid contention; merged only at snapshot time.
type cardinality struct {
	domains *bitset
	extIPs  *bitset
	images  *bitset
	hosts   *bitset
}

func newCardinality(c *corpus, hosts int) *cardinality {
	return &cardinality{
		domains: newBitset(len(c.domains)),
		extIPs:  newBitset(len(c.extIPs)),
		images:  newBitset(len(c.images)),
		hosts:   newBitset(hosts),
	}
}

type kindStat struct {
	count atomic.Int64
	bytes atomic.Int64
}

// stats is the whole client-side picture. Everything here is what the generator can see
// on its own; ClickHouse and Bifract server metrics are collected separately.
type stats struct {
	cfg       Config
	startWall time.Time

	offered   atomic.Int64 // events the schedule asked for
	delivered atomic.Int64 // events in 2xx batches
	rejected  atomic.Int64 // events in 429 batches
	lost      atomic.Int64 // events in 5xx or connection-error batches

	batchesOK   atomic.Int64
	batchesFail atomic.Int64
	bytesSent   atomic.Int64

	http2xx atomic.Int64
	http429 atomic.Int64
	http4xx atomic.Int64
	http5xx atomic.Int64
	connErr atomic.Int64

	retryAfterMax atomic.Int64
	lagTotalNanos atomic.Int64
	lagMaxNanos   atomic.Int64
	lagSamples    atomic.Int64

	kinds [kindCount]kindStat

	mu       sync.Mutex
	interval []float64 // latency ms, reset each sample
	all      []float64 // capped reservoir across the run
	allSeen  int64

	cards []*cardinality

	prevDelivered int64
	prevOffered   int64
	prevBytes     int64
	prevSample    time.Time

	cpu *cpuMeter
}

const latReservoir = 200000

func newStats(cfg Config) *stats {
	return &stats{cfg: cfg, startWall: time.Now(), prevSample: time.Now(), cpu: newCPUMeter()}
}

func (s *stats) addLatency(ms float64) {
	s.mu.Lock()
	s.interval = append(s.interval, ms)
	s.allSeen++
	if len(s.all) < latReservoir {
		s.all = append(s.all, ms)
	} else if j := int(fastrand() % uint64(s.allSeen)); j < latReservoir {
		s.all[j] = ms
	}
	s.mu.Unlock()
}

func (s *stats) recordLag(d time.Duration) {
	s.lagTotalNanos.Add(int64(d))
	s.lagSamples.Add(1)
	for {
		cur := s.lagMaxNanos.Load()
		if int64(d) <= cur || s.lagMaxNanos.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// Snapshot is one sampling interval, written to CSV and rendered by the TUI.
type Snapshot struct {
	Time           time.Time `json:"time"`
	ElapsedSec     float64   `json:"elapsed_sec"`
	OfferedEPS     float64   `json:"offered_eps"`
	DeliveredEPS   float64   `json:"delivered_eps"`
	MBPerSec       float64   `json:"mb_per_sec"`
	P50            float64   `json:"p50_ms"`
	P95            float64   `json:"p95_ms"`
	P99            float64   `json:"p99_ms"`
	Max            float64   `json:"max_ms"`
	HTTP2xx        int64     `json:"http_2xx"`
	HTTP429        int64     `json:"http_429"`
	HTTP4xx        int64     `json:"http_4xx"`
	HTTP5xx        int64     `json:"http_5xx"`
	ConnErrors     int64     `json:"conn_errors"`
	LagMeanMs      float64   `json:"lag_mean_ms"`
	LagMaxMs       float64   `json:"lag_max_ms"`
	CPUPercent     float64   `json:"cpu_percent"`
	AvgEventBytes  float64   `json:"avg_event_bytes"`
	TotalDelivered int64     `json:"total_delivered"`
	TotalOffered   int64     `json:"total_offered"`
	TotalRejected  int64     `json:"total_rejected"`
	TotalGB        float64   `json:"total_gb"`
	BatchesOK      int64     `json:"batches_ok"`
	BatchesFail    int64     `json:"batches_fail"`
	RetryAfterMaxS int64     `json:"retry_after_max_sec"`
}

func (s *stats) snapshot() Snapshot {
	now := time.Now()
	dt := now.Sub(s.prevSample).Seconds()
	if dt <= 0 {
		dt = 1
	}
	off, del, by := s.offered.Load(), s.delivered.Load(), s.bytesSent.Load()

	s.mu.Lock()
	iv := s.interval
	s.interval = nil
	s.mu.Unlock()
	sort.Float64s(iv)

	var events int64
	for i := range s.kinds {
		events += s.kinds[i].count.Load()
	}
	avgBytes := 0.0
	if events > 0 {
		avgBytes = float64(by) / float64(events)
	}
	lagMean := 0.0
	if n := s.lagSamples.Load(); n > 0 {
		lagMean = float64(s.lagTotalNanos.Load()) / float64(n) / 1e6
	}

	snap := Snapshot{
		Time:           now,
		ElapsedSec:     now.Sub(s.startWall).Seconds(),
		OfferedEPS:     float64(off-s.prevOffered) / dt,
		DeliveredEPS:   float64(del-s.prevDelivered) / dt,
		MBPerSec:       float64(by-s.prevBytes) / dt / (1 << 20),
		P50:            pct(iv, 0.50),
		P95:            pct(iv, 0.95),
		P99:            pct(iv, 0.99),
		Max:            pct(iv, 1.0),
		HTTP2xx:        s.http2xx.Load(),
		HTTP429:        s.http429.Load(),
		HTTP4xx:        s.http4xx.Load(),
		HTTP5xx:        s.http5xx.Load(),
		ConnErrors:     s.connErr.Load(),
		LagMeanMs:      lagMean,
		LagMaxMs:       float64(s.lagMaxNanos.Load()) / 1e6,
		CPUPercent:     s.cpu.sample(),
		AvgEventBytes:  avgBytes,
		TotalDelivered: del,
		TotalOffered:   off,
		TotalRejected:  s.rejected.Load(),
		TotalGB:        float64(by) / (1 << 30),
		BatchesOK:      s.batchesOK.Load(),
		BatchesFail:    s.batchesFail.Load(),
		RetryAfterMaxS: s.retryAfterMax.Load(),
	}
	s.prevSample, s.prevOffered, s.prevDelivered, s.prevBytes = now, off, del, by
	return snap
}

// liveMark lets the TUI compute an instantaneous rate every second without disturbing the
// sampling interval, which only the sampler goroutine may reset.
type liveMark struct {
	t         time.Time
	offered   int64
	delivered int64
	bytes     int64
}

func newLiveMark(s *stats) liveMark {
	return liveMark{t: time.Now(), offered: s.offered.Load(), delivered: s.delivered.Load(), bytes: s.bytesSent.Load()}
}

func (s *stats) live(m *liveMark) (offEPS, delEPS, mbs float64) {
	now := time.Now()
	dt := now.Sub(m.t).Seconds()
	if dt < 0.2 {
		return 0, 0, 0
	}
	off, del, by := s.offered.Load(), s.delivered.Load(), s.bytesSent.Load()
	offEPS = float64(off-m.offered) / dt
	delEPS = float64(del-m.delivered) / dt
	mbs = float64(by-m.bytes) / dt / (1 << 20)
	*m = liveMark{t: now, offered: off, delivered: del, bytes: by}
	return
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// KindSummary reports the realized event mix, which should match the configured weights.
type KindSummary struct {
	Name       string  `json:"name"`
	Count      int64   `json:"count"`
	Percent    float64 `json:"percent"`
	AvgBytes   float64 `json:"avg_bytes"`
	TotalBytes int64   `json:"total_bytes"`
}

// Summary is the run's final artifact. Written to disk every sample interval, not just at
// exit, so a kill or a crash still leaves a valid file.
type Summary struct {
	Config          Config        `json:"config"`
	StartedAt       time.Time     `json:"started_at"`
	EndedAt         time.Time     `json:"ended_at"`
	DurationSec     float64       `json:"duration_sec"`
	Complete        bool          `json:"complete"`
	EventsOffered   int64         `json:"events_offered"`
	EventsDelivered int64         `json:"events_delivered"`
	EventsRejected  int64         `json:"events_rejected_429"`
	EventsLost      int64         `json:"events_lost"`
	DeliveryRate    float64       `json:"delivery_rate"`
	MeanEPS         float64       `json:"mean_delivered_eps"`
	MeanMBPerSec    float64       `json:"mean_mb_per_sec"`
	ProjectedGBDay  float64       `json:"projected_gb_per_day"`
	TotalBytes      int64         `json:"total_bytes"`
	TotalGB         float64       `json:"total_gb"`
	AvgEventBytes   float64       `json:"avg_event_bytes"`
	BatchesOK       int64         `json:"batches_ok"`
	BatchesFail     int64         `json:"batches_fail"`
	HTTP2xx         int64         `json:"http_2xx"`
	HTTP429         int64         `json:"http_429"`
	HTTP4xx         int64         `json:"http_4xx"`
	HTTP5xx         int64         `json:"http_5xx"`
	ConnErrors      int64         `json:"conn_errors"`
	RetryAfterMaxS  int64         `json:"retry_after_max_sec"`
	LatencyP50      float64       `json:"latency_p50_ms"`
	LatencyP95      float64       `json:"latency_p95_ms"`
	LatencyP99      float64       `json:"latency_p99_ms"`
	LatencyMax      float64       `json:"latency_max_ms"`
	LagMeanMs       float64       `json:"schedule_lag_mean_ms"`
	LagMaxMs        float64       `json:"schedule_lag_max_ms"`
	PeakCPUPercent  float64       `json:"peak_cpu_percent"`
	MeanCPUPercent  float64       `json:"mean_cpu_percent"`
	GeneratorHealth string        `json:"generator_health"`
	Kinds           []KindSummary `json:"event_mix"`
	UniqueHosts     int           `json:"unique_hosts_emitted"`
	UniqueProcGUIDs int64         `json:"unique_process_guids_emitted"`
	UniqueDomains   int           `json:"unique_domains_emitted"`
	UniqueExtIPs    int           `json:"unique_external_ips_emitted"`
	UniqueImages    int           `json:"unique_images_emitted"`
}

func (s *stats) summarize(complete bool, peakCPU, meanCPU float64) Summary {
	now := time.Now()
	dur := now.Sub(s.startWall).Seconds()
	if dur <= 0 {
		dur = 1
	}
	del, by := s.delivered.Load(), s.bytesSent.Load()

	s.mu.Lock()
	all := append([]float64(nil), s.all...)
	s.mu.Unlock()
	sort.Float64s(all)

	var total int64
	for i := range s.kinds {
		total += s.kinds[i].count.Load()
	}
	kinds := make([]KindSummary, 0, kindCount)
	for i := range s.kinds {
		cnt := s.kinds[i].count.Load()
		b := s.kinds[i].bytes.Load()
		ks := KindSummary{Name: kindNames[i], Count: cnt, TotalBytes: b}
		if total > 0 {
			ks.Percent = float64(cnt) / float64(total) * 100
		}
		if cnt > 0 {
			ks.AvgBytes = float64(b) / float64(cnt)
		}
		kinds = append(kinds, ks)
	}

	merged := &cardinality{
		domains: newBitset(0), extIPs: newBitset(0), images: newBitset(0), hosts: newBitset(0),
	}
	if len(s.cards) > 0 {
		merged = &cardinality{
			domains: newBitset(len(s.cards[0].domains.w) * 64),
			extIPs:  newBitset(len(s.cards[0].extIPs.w) * 64),
			images:  newBitset(len(s.cards[0].images.w) * 64),
			hosts:   newBitset(len(s.cards[0].hosts.w) * 64),
		}
		for _, c := range s.cards {
			merged.domains.or(c.domains)
			merged.extIPs.or(c.extIPs)
			merged.images.or(c.images)
			merged.hosts.or(c.hosts)
		}
	}

	lagMean := 0.0
	if n := s.lagSamples.Load(); n > 0 {
		lagMean = float64(s.lagTotalNanos.Load()) / float64(n) / 1e6
	}
	avgBytes := 0.0
	if total > 0 {
		avgBytes = float64(by) / float64(total)
	}
	mbps := float64(by) / dur / (1 << 20)

	health := "ok"
	switch {
	case peakCPU >= 85:
		health = "SATURATED: generator CPU peaked above 85 percent, results are a generator limit"
	case peakCPU >= 70:
		health = "WARNING: generator CPU exceeded 70 percent, add generator capacity before trusting results"
	case float64(s.lagMaxNanos.Load())/1e6 > 5000:
		health = "WARNING: schedule lag exceeded 5s, offered rate did not hold"
	}

	offered := s.offered.Load()
	rate := 0.0
	if offered > 0 {
		rate = float64(del) / float64(offered)
	}

	return Summary{
		Config:          s.cfg,
		StartedAt:       s.startWall,
		EndedAt:         now,
		DurationSec:     dur,
		Complete:        complete,
		EventsOffered:   offered,
		EventsDelivered: del,
		EventsRejected:  s.rejected.Load(),
		EventsLost:      s.lost.Load(),
		DeliveryRate:    rate,
		MeanEPS:         float64(del) / dur,
		MeanMBPerSec:    mbps,
		ProjectedGBDay:  mbps * 86400 / 1024,
		TotalBytes:      by,
		TotalGB:         float64(by) / (1 << 30),
		AvgEventBytes:   avgBytes,
		BatchesOK:       s.batchesOK.Load(),
		BatchesFail:     s.batchesFail.Load(),
		HTTP2xx:         s.http2xx.Load(),
		HTTP429:         s.http429.Load(),
		HTTP4xx:         s.http4xx.Load(),
		HTTP5xx:         s.http5xx.Load(),
		ConnErrors:      s.connErr.Load(),
		RetryAfterMaxS:  s.retryAfterMax.Load(),
		LatencyP50:      pct(all, 0.50),
		LatencyP95:      pct(all, 0.95),
		LatencyP99:      pct(all, 0.99),
		LatencyMax:      pct(all, 1.0),
		LagMeanMs:       lagMean,
		LagMaxMs:        float64(s.lagMaxNanos.Load()) / 1e6,
		PeakCPUPercent:  peakCPU,
		MeanCPUPercent:  meanCPU,
		GeneratorHealth: health,
		Kinds:           kinds,
		UniqueHosts:     merged.hosts.count(),
		UniqueProcGUIDs: s.kinds[kindProcess].count.Load(),
		UniqueDomains:   merged.domains.count(),
		UniqueExtIPs:    merged.extIPs.count(),
		UniqueImages:    merged.images.count(),
	}
}

// recorder owns the on-disk artifacts: an append-only CSV of every sample and a summary
// JSON rewritten atomically each interval.
type recorder struct {
	dir     string
	csvFile *os.File
	csvW    *csv.Writer
}

func newRecorder(dir string) (*recorder, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join(dir, "samples.csv"))
	if err != nil {
		return nil, err
	}
	r := &recorder{dir: dir, csvFile: f, csvW: csv.NewWriter(f)}
	_ = r.csvW.Write([]string{
		"timestamp", "elapsed_sec", "offered_eps", "delivered_eps", "mb_per_sec",
		"p50_ms", "p95_ms", "p99_ms", "max_ms",
		"http_2xx", "http_429", "http_4xx", "http_5xx", "conn_errors",
		"lag_mean_ms", "lag_max_ms", "cpu_percent", "avg_event_bytes",
		"total_delivered", "total_rejected", "total_gb", "batches_ok", "batches_fail",
	})
	r.csvW.Flush()
	return r, f.Sync()
}

func (r *recorder) writeSample(s Snapshot) {
	f := func(v float64, prec int) string { return strconv.FormatFloat(v, 'f', prec, 64) }
	_ = r.csvW.Write([]string{
		s.Time.UTC().Format(time.RFC3339), f(s.ElapsedSec, 1), f(s.OfferedEPS, 1), f(s.DeliveredEPS, 1), f(s.MBPerSec, 4),
		f(s.P50, 2), f(s.P95, 2), f(s.P99, 2), f(s.Max, 2),
		strconv.FormatInt(s.HTTP2xx, 10), strconv.FormatInt(s.HTTP429, 10), strconv.FormatInt(s.HTTP4xx, 10),
		strconv.FormatInt(s.HTTP5xx, 10), strconv.FormatInt(s.ConnErrors, 10),
		f(s.LagMeanMs, 2), f(s.LagMaxMs, 2), f(s.CPUPercent, 1), f(s.AvgEventBytes, 1),
		strconv.FormatInt(s.TotalDelivered, 10), strconv.FormatInt(s.TotalRejected, 10), f(s.TotalGB, 4),
		strconv.FormatInt(s.BatchesOK, 10), strconv.FormatInt(s.BatchesFail, 10),
	})
	r.csvW.Flush()
	_ = r.csvFile.Sync()
}

// writeSummary replaces summary.json atomically so an interrupted write never truncates it.
func (r *recorder) writeSummary(sum Summary) error {
	b, err := json.MarshalIndent(sum, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(r.dir, ".summary.json.tmp")
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(r.dir, "summary.json"))
}

func (r *recorder) close() {
	r.csvW.Flush()
	_ = r.csvFile.Close()
}

var randState atomic.Uint64

func fastrand() uint64 {
	x := randState.Add(0x9e3779b97f4a7c15)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	return x
}
