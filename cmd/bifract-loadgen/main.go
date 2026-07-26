// bifract-loadgen generates security-shaped synthetic telemetry at a sustained rate and
// reports everything the client side can see about the run.
//
// It exists because a generic log generator produces low-cardinality data that ClickHouse
// dictionary-encodes into nothing, yielding a compression ratio and ingest rate that do not
// mean anything. This emits Sysmon-shaped events with realistic cardinality: Zipf-distributed
// domains and destination IPs, per-image (not per-event) hashes, and process GUIDs reused
// across each process's file, network, and DNS events so provenance edges are real.
//
//	bifract-loadgen -url https://bifract.example.com -token <ingest-token> -rate 4000 -duration 24h
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"golang.org/x/term"
)

// Config is serialized into summary.json. It deliberately holds no credentials.
type Config struct {
	URL         string  `json:"url"`
	Rate        float64 `json:"target_events_per_sec"`
	Batch       int     `json:"batch_size"`
	Workers     int     `json:"workers"`
	Hosts       int     `json:"hosts"`
	DomainPool  int     `json:"domain_pool"`
	ExtIPPool   int     `json:"external_ip_pool"`
	Seed        uint64  `json:"seed"`
	OutDir      string  `json:"out_dir"`
	Retries     int     `json:"retries"`
	Malice      float64 `json:"malice_rate"`
	DurationStr string  `json:"duration"`
	SampleStr   string  `json:"sample_interval"`
	TimeoutStr  string  `json:"http_timeout"`
	GoVersion   string  `json:"go_version"`
	NumCPU      int     `json:"num_cpu"`

	Duration time.Duration `json:"-"`
	Sample   time.Duration `json:"-"`
	Timeout  time.Duration `json:"-"`
}

func main() {
	var (
		url      = flag.String("url", "http://localhost:8080", "Bifract base URL")
		token    = flag.String("token", os.Getenv("BIFRACT_INGEST_TOKEN"), "ingest token (or BIFRACT_INGEST_TOKEN)")
		rate     = flag.Float64("rate", 4000, "target events per second, sustained")
		batch    = flag.Int("batch", 500, "events per HTTP request (keep under 5000)")
		workers  = flag.Int("workers", 0, "concurrent senders (0 = auto)")
		hosts    = flag.Int("hosts", 10000, "simulated host population")
		duration = flag.Duration("duration", 0, "run duration (0 = until interrupted)")
		domains  = flag.Int("domains", 200000, "domain pool size for the Zipf tail")
		extIPs   = flag.Int("ext-ips", 50000, "external destination IP pool size")
		seed     = flag.Uint64("seed", 1, "RNG seed, for reproducible runs")
		outDir   = flag.String("out", "loadgen-out", "directory for samples.csv and summary.json")
		sample   = flag.Duration("sample", 10*time.Second, "metrics sampling interval")
		timeout  = flag.Duration("timeout", 30*time.Second, "HTTP request timeout")
		retries  = flag.Int("retries", 0, "retries on 429/5xx (0 keeps offered rate honest)")
		insecure = flag.Bool("insecure", false, "skip TLS verification")
		plain    = flag.Bool("plain", false, "disable the TUI and log one line per sample")
		validate = flag.Bool("validate", false, "print sample events and exit without sending")
		malice   = flag.Float64("malice", 0.002, "fraction of process creations that are LOLBin activity (0 = none)")
	)
	flag.Parse()

	if *workers <= 0 {
		*workers = min(8, max(2, runtime.NumCPU()))
	}
	if *batch > 5000 {
		fmt.Fprintln(os.Stderr, "batch capped at 5000 (per-queue-slot limit; larger batches are split server side)")
		*batch = 5000
	}

	cfg := Config{
		URL: *url, Rate: *rate, Batch: *batch, Workers: *workers, Hosts: *hosts,
		DomainPool: *domains, ExtIPPool: *extIPs, Seed: *seed, OutDir: *outDir, Retries: *retries, Malice: *malice,
		DurationStr: durStr(*duration), SampleStr: sample.String(), TimeoutStr: timeout.String(),
		GoVersion: runtime.Version(), NumCPU: runtime.NumCPU(),
		Duration: *duration, Sample: *sample, Timeout: *timeout,
	}

	corp := buildCorpus(*domains, *extIPs, *seed)

	if *validate {
		runValidate(corp, cfg)
		return
	}
	if *token == "" {
		fmt.Fprintln(os.Stderr, "error: -token or BIFRACT_INGEST_TOKEN is required")
		os.Exit(1)
	}

	st := newStats(cfg)
	rec, err := newRecorder(*outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: output dir: %v\n", err)
		os.Exit(1)
	}
	defer rec.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *duration > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, *duration)
		defer stop()
	}
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		cancel()
	}()

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        *workers * 2,
			MaxIdleConnsPerHost: *workers * 2,
			MaxConnsPerHost:     *workers * 2,
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: *insecure},
			DisableCompression:  true,
		},
	}

	ws := make([]*worker, 0, *workers)
	perHost := *hosts / *workers
	for i := 0; i < *workers; i++ {
		s := newSampler(corp, *seed+uint64(i)*0x1000193)
		card := newCardinality(corp, *hosts)
		s.card = card
		start := i * perHost
		end := start + perHost
		if i == *workers-1 {
			end = *hosts
		}
		w := &worker{
			id: i, c: corp, s: s, card: card, st: st, client: client,
			cfg: cfg, token: *token,
		}
		for hi := start; hi < end; hi++ {
			w.hosts = append(w.hosts, newHost(corp, s, hi))
		}
		ws = append(ws, w)
		st.cards = append(st.cards, card)
	}

	var wg sync.WaitGroup
	perWorker := *rate / float64(*workers)
	for _, w := range ws {
		wg.Add(1)
		go func(w *worker) { defer wg.Done(); w.run(ctx, perWorker) }(w)
	}

	useTUI := !*plain && term.IsTerminal(int(os.Stdout.Fd()))
	var prog *tea.Program
	if useTUI {
		prog = tea.NewProgram(newUIModel(st, cfg, cancel), tea.WithAltScreen())
	}

	go sampleLoop(ctx, st, rec, prog, *sample, useTUI)

	done := make(chan Summary, 1)
	go func() {
		wg.Wait()
		final := st.snapshot()
		rec.writeSample(final)
		peak, mean := st.cpu.peakMean()
		sum := st.summarize(true, peak, mean)
		if err := rec.writeSummary(sum); err != nil {
			fmt.Fprintf(os.Stderr, "warning: summary write failed: %v\n", err)
		}
		done <- sum
	}()

	if useTUI {
		go func() {
			sum := <-done
			prog.Send(doneMsg{summary: sum})
		}()
		if _, err := prog.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "ui error: %v\n", err)
		}
		return
	}

	fmt.Printf("bifract-loadgen: %s -> %s, %.0f ev/s across %d workers, %d hosts\n",
		durStr(*duration), *url, *rate, *workers, *hosts)
	sum := <-done
	printPlainSummary(sum, cfg)
}

func sampleLoop(ctx context.Context, st *stats, rec *recorder, prog *tea.Program, every time.Duration, tui bool) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			snap := st.snapshot()
			rec.writeSample(snap)
			peak, mean := st.cpu.peakMean()
			// Summary is rewritten every interval so a kill still leaves a valid file.
			_ = rec.writeSummary(st.summarize(false, peak, mean))
			if tui && prog != nil {
				prog.Send(sampleMsg(snap))
			} else {
				fmt.Printf("[%s] %6.0f ev/s  %5.2f MB/s  p50 %6.1fms p95 %6.1fms p99 %6.1fms  429 %d  err %d  cpu %.2f%%\n",
					fmtDur(time.Duration(snap.ElapsedSec)*time.Second), snap.DeliveredEPS, snap.MBPerSec,
					snap.P50, snap.P95, snap.P99, snap.HTTP429, snap.HTTP5xx+snap.ConnErrors+snap.HTTP4xx, snap.CPUPercent)
			}
		}
	}
}

// worker owns a disjoint slice of the host population, so process trees stay coherent
// without any shared mutable state or locking between senders.
type worker struct {
	id     int
	c      *corpus
	s      *sampler
	hosts  []*host
	card   *cardinality
	st     *stats
	client *http.Client
	cfg    Config
	token  string
	buf    jbuf
}

func (w *worker) run(ctx context.Context, perWorkerRate float64) {
	if perWorkerRate <= 0 {
		return
	}
	interval := time.Duration(float64(w.cfg.Batch) / perWorkerRate * float64(time.Second))
	next := time.Now()
	for {
		if ctx.Err() != nil {
			return
		}
		next = next.Add(interval)
		w.buildBatch()
		w.send(ctx)

		if behind := time.Since(next); behind > 0 {
			w.st.recordLag(behind)
		} else {
			w.st.recordLag(0)
			t := time.NewTimer(-behind)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}

func (w *worker) buildBatch() {
	w.buf.b = w.buf.b[:0]
	w.buf.b = append(w.buf.b, '[')
	base := utcBase(time.Now())
	for i := 0; i < w.cfg.Batch; i++ {
		if i > 0 {
			w.buf.b = append(w.buf.b, ',')
		}
		h := w.hosts[w.s.rng.IntN(len(w.hosts))]
		w.card.hosts.set(uint64(h.idx))
		start := len(w.buf.b)
		kind := w.s.emit(&w.buf, w.c, h, base, w.s.rng.IntN(1000), w.cfg.Malice)
		w.st.kinds[kind].count.Add(1)
		w.st.kinds[kind].bytes.Add(int64(len(w.buf.b) - start))
	}
	w.buf.b = append(w.buf.b, ']')
}

func (w *worker) send(ctx context.Context) {
	n := int64(w.cfg.Batch)
	w.st.offered.Add(n)
	size := int64(len(w.buf.b))

	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL+"/api/v1/ingest", bytes.NewReader(w.buf.b))
		if err != nil {
			w.st.lost.Add(n)
			w.st.batchesFail.Add(1)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+w.token)

		start := time.Now()
		resp, err := w.client.Do(req)
		lat := float64(time.Since(start).Microseconds()) / 1000

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.st.connErr.Add(1)
			w.st.lost.Add(n)
			w.st.batchesFail.Add(1)
			return
		}
		w.st.addLatency(lat)
		code := resp.StatusCode
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, e := strconv.ParseInt(ra, 10, 64); e == nil {
				for {
					cur := w.st.retryAfterMax.Load()
					if secs <= cur || w.st.retryAfterMax.CompareAndSwap(cur, secs) {
						break
					}
				}
			}
		}
		resp.Body.Close()

		switch {
		case code >= 200 && code < 300:
			w.st.http2xx.Add(1)
			w.st.delivered.Add(n)
			w.st.bytesSent.Add(size)
			w.st.batchesOK.Add(1)
			return
		case code == http.StatusTooManyRequests:
			w.st.http429.Add(1)
			if attempt < w.cfg.Retries {
				continue
			}
			w.st.rejected.Add(n)
			w.st.batchesFail.Add(1)
			return
		case code >= 500:
			w.st.http5xx.Add(1)
			if attempt < w.cfg.Retries {
				continue
			}
			w.st.lost.Add(n)
			w.st.batchesFail.Add(1)
			return
		default:
			w.st.http4xx.Add(1)
			w.st.lost.Add(n)
			w.st.batchesFail.Add(1)
			return
		}
	}
}

// runValidate prints one of each event type and the realized size distribution, without
// sending anything. Use it to confirm the normalizer mapping before a paid run.
func runValidate(c *corpus, cfg Config) {
	s := newSampler(c, cfg.Seed)
	s.card = newCardinality(c, cfg.Hosts)
	h := newHost(c, s, 0)
	base := utcBase(time.Now())

	fmt.Println("Sample events, one per type:")
	for kind := 0; kind < kindCount; kind++ {
		var j jbuf
		for tries := 0; tries < 400; tries++ {
			j.b = j.b[:0]
			if s.emit(&j, c, h, base, 123, cfg.Malice) == kind {
				break
			}
		}
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, j.b, "  ", "  "); err != nil {
			fmt.Printf("\n  %s: INVALID JSON: %v\n  %s\n", kindNames[kind], err, j.b)
			continue
		}
		fmt.Printf("\n  %s (%d bytes)\n  %s\n", kindNames[kind], len(j.b), pretty.String())
	}

	// Size distribution over a realistic mix.
	const n = 20000
	var total int
	counts := [kindCount]int{}
	bytesBy := [kindCount]int{}
	for i := 0; i < n; i++ {
		var j jbuf
		k := s.emit(&j, c, h, base, i%1000, cfg.Malice)
		var probe map[string]any
		if err := json.Unmarshal(j.b, &probe); err != nil {
			fmt.Printf("\nINVALID JSON at sample %d: %v\n", i, err)
			os.Exit(1)
		}
		counts[k]++
		bytesBy[k] += len(j.b)
		total += len(j.b)
	}
	fmt.Printf("\nRealized mix over %d events (all parsed as valid JSON):\n", n)
	for k := 0; k < kindCount; k++ {
		avg := 0
		if counts[k] > 0 {
			avg = bytesBy[k] / counts[k]
		}
		fmt.Printf("  %-20s %5.1f%%  avg %5d B\n", kindNames[k], float64(counts[k])/float64(n)*100, avg)
	}
	avg := float64(total) / float64(n)
	fmt.Printf("\n  average event: %.0f B\n", avg)
	fmt.Printf("  at %.0f ev/s: %.2f MB/s, %.0f GB/day\n", cfg.Rate, avg*cfg.Rate/(1<<20), avg*cfg.Rate*86400/(1<<30))
	fmt.Printf("  for 500 GB/day you need %.0f ev/s at this size\n", 500*float64(1<<30)/86400/avg)
}

func printPlainSummary(s Summary, cfg Config) {
	fmt.Printf("\n=== bifract-loadgen summary ===\n")
	fmt.Printf("duration        %s\n", fmtDur(time.Duration(s.DurationSec)*time.Second))
	fmt.Printf("delivered       %s events (%.2f%% of offered)\n", comma(s.EventsDelivered), s.DeliveryRate*100)
	fmt.Printf("sustained       %s ev/s, %.2f MB/s, %.1f GB/day projected\n", comma(int64(s.MeanEPS)), s.MeanMBPerSec, s.ProjectedGBDay)
	fmt.Printf("volume          %.2f GB, avg event %.0f B\n", s.TotalGB, s.AvgEventBytes)
	fmt.Printf("latency         p50 %.1fms  p95 %.1fms  p99 %.1fms  max %.1fms\n", s.LatencyP50, s.LatencyP95, s.LatencyP99, s.LatencyMax)
	fmt.Printf("429s            %s\n", comma(s.HTTP429))
	fmt.Printf("errors          4xx %d  5xx %d  conn %d\n", s.HTTP4xx, s.HTTP5xx, s.ConnErrors)
	fmt.Printf("generator cpu   peak %.2f%%  mean %.2f%%\n", s.PeakCPUPercent, s.MeanCPUPercent)
	fmt.Printf("cardinality     %s hosts, %s proc guids, %s domains, %s ext ips\n",
		comma(int64(s.UniqueHosts)), comma(s.UniqueProcGUIDs), comma(int64(s.UniqueDomains)), comma(int64(s.UniqueExtIPs)))
	fmt.Printf("health          %s\n", s.GeneratorHealth)
	fmt.Printf("wrote           %s/summary.json, %s/samples.csv\n\n", cfg.OutDir, cfg.OutDir)
}

func durStr(d time.Duration) string {
	if d == 0 {
		return "until interrupted"
	}
	return d.String()
}
