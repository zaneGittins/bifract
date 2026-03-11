package ingestcli

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"
)

const (
	barWidth   = 30
	tickRate   = 150 * time.Millisecond
	filledChar = "█"
	emptyChar  = "░"
)

// tickMsg triggers a UI refresh.
type tickMsg time.Time

// doneMsg signals ingestion is complete.
type doneMsg struct {
	err error
}

// ProgressModel is the bubbletea model for the ingestion progress UI.
type ProgressModel struct {
	stats    *Stats
	done     bool
	err      error
	quitting bool
	width    int
}

func NewProgressModel(stats *Stats) ProgressModel {
	return ProgressModel{
		stats: stats,
		width: 80,
	}
}

func (m ProgressModel) Init() tea.Cmd {
	return tick()
}

func (m ProgressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.quitting = true
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width

	case tickMsg:
		if m.done {
			return m, nil
		}
		return m, tick()

	case doneMsg:
		m.done = true
		m.err = msg.err
		return m, tea.Quit
	}

	return m, nil
}

func (m ProgressModel) View() string {
	if m.quitting {
		return DimStyle.Render("Interrupted.") + "\n"
	}

	var b strings.Builder

	if !m.done {
		b.WriteString(m.renderLiveStatus())
	} else {
		b.WriteString(m.renderSummary())
	}

	return b.String()
}

func (m ProgressModel) renderLiveStatus() string {
	var b strings.Builder

	sent := m.stats.LogsSent.Load()
	total := m.stats.TotalLogs.Load()
	errors := m.stats.Errors.Load()
	retries := m.stats.Retries.Load()
	rate := m.stats.LogsPerSec()

	// Current file
	m.stats.mu.Lock()
	currentFile := m.stats.CurrentFile
	filesDone := m.stats.FilesDone
	filesTotal := m.stats.FilesTotal
	m.stats.mu.Unlock()

	// Header line
	b.WriteString(TitleStyle.Render("  bifractctl ingest"))
	b.WriteString(DimStyle.Render(fmt.Sprintf("  %d/%d files", filesDone, filesTotal)))
	if currentFile != "" {
		b.WriteString(DimStyle.Render(fmt.Sprintf("  %s", currentFile)))
	}
	b.WriteString("\n")

	// Progress bar
	var pct float64
	if total > 0 {
		pct = float64(sent+errors) / float64(total)
		if pct > 1 {
			pct = 1
		}
	}
	filled := int(pct * float64(barWidth))
	empty := barWidth - filled

	bar := BarFilledStyle.Render(strings.Repeat(filledChar, filled)) +
		BarEmptyStyle.Render(strings.Repeat(emptyChar, empty))

	b.WriteString(fmt.Sprintf("  %s %s\n", bar, DimStyle.Render(fmt.Sprintf("%.0f%%", pct*100))))

	// Stats line
	if m.stats.Pacer != nil {
		throttled := m.stats.Throttled.Load()
		currentLimit := m.stats.Pacer.CurrentLimit()
		b.WriteString(fmt.Sprintf("  %s sent  %s errors  %s retries  %s/s  %s  %s workers  %s throttled\n",
			ValueStyle.Render(formatNumber(sent)),
			renderErrors(errors),
			DimStyle.Render(fmt.Sprintf("%d", retries)),
			ValueStyle.Render(fmt.Sprintf("%.0f", rate)),
			DimStyle.Render(formatDuration(time.Since(m.stats.StartTime))),
			ValueStyle.Render(fmt.Sprintf("%d", currentLimit)),
			renderThrottled(throttled),
		))
	} else {
		b.WriteString(fmt.Sprintf("  %s sent  %s errors  %s retries  %s/s  %s\n",
			ValueStyle.Render(formatNumber(sent)),
			renderErrors(errors),
			DimStyle.Render(fmt.Sprintf("%d", retries)),
			ValueStyle.Render(fmt.Sprintf("%.0f", rate)),
			DimStyle.Render(formatDuration(time.Since(m.stats.StartTime))),
		))
	}

	return b.String()
}

func (m ProgressModel) renderSummary() string {
	var b strings.Builder

	sent := m.stats.LogsSent.Load()
	errors := m.stats.Errors.Load()
	retries := m.stats.Retries.Load()
	batches := m.stats.Batches.Load()
	elapsed := time.Since(m.stats.StartTime)
	bytesSent := m.stats.BytesSent.Load()

	b.WriteString("\n")

	if m.err != nil {
		b.WriteString(ErrorStyle.Render("  Error: ") + fmt.Sprintf("%v\n", m.err))
		b.WriteString("\n")
	}

	// Summary box
	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Dim).
		Padding(0, 2).
		MarginLeft(1)

	var summary strings.Builder
	summary.WriteString(TitleStyle.Render("Ingestion Complete") + "\n\n")

	summary.WriteString(fmt.Sprintf("  %s  %s\n",
		SuccessStyle.Render("*"),
		fmt.Sprintf("Logs sent:   %s", ValueStyle.Render(fmt.Sprintf("%d", sent)))))

	if errors > 0 {
		summary.WriteString(fmt.Sprintf("  %s  %s\n",
			ErrorStyle.Render("x"),
			fmt.Sprintf("Errors:      %s", ErrorStyle.Render(fmt.Sprintf("%d", errors)))))
	} else {
		summary.WriteString(fmt.Sprintf("  %s  %s\n",
			SuccessStyle.Render("*"),
			fmt.Sprintf("Errors:      %s", DimStyle.Render("0"))))
	}

	summary.WriteString(fmt.Sprintf("  %s  %s\n",
		DimStyle.Render("~"),
		fmt.Sprintf("Retries:     %s", DimStyle.Render(fmt.Sprintf("%d", retries)))))

	throttled := m.stats.Throttled.Load()
	summary.WriteString(fmt.Sprintf("  %s  %s\n",
		DimStyle.Render("~"),
		fmt.Sprintf("Throttled:   %s", renderThrottled(throttled))))

	summary.WriteString(fmt.Sprintf("  %s  %s\n",
		DimStyle.Render("~"),
		fmt.Sprintf("Batches:     %s", DimStyle.Render(fmt.Sprintf("%d", batches)))))

	summary.WriteString(fmt.Sprintf("  %s  %s\n",
		DimStyle.Render("~"),
		fmt.Sprintf("Data sent:   %s", DimStyle.Render(formatBytes(bytesSent)))))

	summary.WriteString(fmt.Sprintf("  %s  %s\n",
		DimStyle.Render("~"),
		fmt.Sprintf("Duration:    %s", DimStyle.Render(formatDuration(elapsed)))))

	if elapsed.Seconds() > 0 && sent > 0 {
		summary.WriteString(fmt.Sprintf("  %s  %s",
			DimStyle.Render("~"),
			fmt.Sprintf("Rate:        %s logs/sec", ValueStyle.Render(fmt.Sprintf("%.0f", float64(sent)/elapsed.Seconds())))))
	}

	b.WriteString(border.Render(summary.String()))
	b.WriteString("\n\n")

	return b.String()
}

func renderErrors(errors int64) string {
	if errors > 0 {
		return ErrorStyle.Render(fmt.Sprintf("%d", errors))
	}
	return DimStyle.Render("0")
}

func renderThrottled(count int64) string {
	if count > 0 {
		return WarningStyle.Render(fmt.Sprintf("%d", count))
	}
	return DimStyle.Render("0")
}

func tick() tea.Cmd {
	return tea.Tick(tickRate, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// RunWithProgress runs the full ingestion pipeline with a live TUI.
// Falls back to plain output when no TTY is available.
func RunWithProgress(cfg *Config) error {
	client := NewClient(cfg)

	// Test connection first
	if err := client.TestConnection(); err != nil {
		return err
	}

	stats := &Stats{
		StartTime:  time.Now(),
		FilesTotal: len(cfg.Files),
	}

	isTTY := term.IsTerminal(int(os.Stdout.Fd()))

	if isTTY {
		return runWithTUI(cfg, client, stats)
	}
	return runPlain(cfg, client, stats)
}

func runWithTUI(cfg *Config, client *Client, stats *Stats) error {
	model := NewProgressModel(stats)
	p := tea.NewProgram(model)

	go func() {
		err := runIngestion(cfg, client, stats)
		p.Send(doneMsg{err: err})
	}()

	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("progress UI: %w", err)
	}

	final := finalModel.(ProgressModel)
	fmt.Print(final.renderSummary())

	if final.err != nil {
		return final.err
	}
	if final.stats.Errors.Load() > 0 {
		return fmt.Errorf("%d logs failed to ingest", final.stats.Errors.Load())
	}
	return nil
}

func runPlain(cfg *Config, client *Client, stats *Stats) error {
	mode := "manual"
	if cfg.Adaptive {
		mode = "auto"
	}
	fmt.Printf("%s  %s  files=%d  workers=%d  batch=%d  mode=%s\n",
		TitleStyle.Render("bifractctl ingest"),
		DimStyle.Render(cfg.URL),
		len(cfg.Files), cfg.Workers, cfg.BatchSize, mode)

	err := runIngestion(cfg, client, stats)

	// Print summary
	model := NewProgressModel(stats)
	model.done = true
	model.err = err
	fmt.Print(model.renderSummary())

	if err != nil {
		return err
	}
	if stats.Errors.Load() > 0 {
		return fmt.Errorf("%d logs failed to ingest", stats.Errors.Load())
	}
	return nil
}

func runIngestion(cfg *Config, client *Client, stats *Stats) error {
	// Pre-count total logs (sampled estimate for large files).
	var total atomic.Int64
	var countWG sync.WaitGroup
	for _, file := range cfg.Files {
		countWG.Add(1)
		go func(f string) {
			defer countWG.Done()
			if n, err := CountLogs(f); err == nil {
				total.Add(n)
			}
		}(file)
	}
	countWG.Wait()
	stats.TotalLogs.Store(total.Load())

	batchCh := make(chan Batch, cfg.Workers*2)

	pacer := NewAdaptivePacer(cfg.Workers, cfg.Adaptive)
	stats.Pacer = pacer
	defer pacer.Stop()

	wg := RunWorkers(client, batchCh, stats, cfg.Workers, pacer)

	var firstErr error
	for _, file := range cfg.Files {
		if err := ReadFile(file, cfg.BatchSize, cfg.Limit, batchCh, stats); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", file, err)
			}
			stats.Errors.Add(1)
		}

		stats.mu.Lock()
		stats.FilesDone++
		stats.mu.Unlock()
	}

	close(batchCh)
	wg.Wait()
	pacer.Stop()

	return firstErr
}
