package main

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	cAccent = lipgloss.AdaptiveColor{Light: "#0F766E", Dark: "#2DD4BF"}
	cDim    = lipgloss.AdaptiveColor{Light: "#6B7280", Dark: "#6B7280"}
	cText   = lipgloss.AdaptiveColor{Light: "#111827", Dark: "#E5E7EB"}
	cWarn   = lipgloss.AdaptiveColor{Light: "#B45309", Dark: "#F5A524"}
	cErr    = lipgloss.AdaptiveColor{Light: "#B91C1C", Dark: "#F87171"}
	cOK     = lipgloss.AdaptiveColor{Light: "#15803D", Dark: "#4ADE80"}

	sTitle   = lipgloss.NewStyle().Foreground(cAccent).Bold(true)
	sLabel   = lipgloss.NewStyle().Foreground(cDim)
	sValue   = lipgloss.NewStyle().Foreground(cText).Bold(true)
	sBig     = lipgloss.NewStyle().Foreground(cAccent).Bold(true)
	sWarn    = lipgloss.NewStyle().Foreground(cWarn).Bold(true)
	sErr     = lipgloss.NewStyle().Foreground(cErr).Bold(true)
	sOK      = lipgloss.NewStyle().Foreground(cOK)
	sSection = lipgloss.NewStyle().Foreground(cDim).Bold(true)
)

type tickMsg time.Time
type sampleMsg Snapshot
type doneMsg struct{ summary Summary }

func tick() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}

type uiModel struct {
	st       *stats
	cfg      Config
	mark     liveMark
	offEPS   float64
	delEPS   float64
	mbs      float64
	last     Snapshot
	spark    []float64
	width    int
	quitting bool
	finished bool
	summary  Summary
	cancel   func()
}

func newUIModel(st *stats, cfg Config, cancel func()) *uiModel {
	return &uiModel{st: st, cfg: cfg, mark: newLiveMark(st), width: 100, cancel: cancel}
}

func (m *uiModel) Init() tea.Cmd { return tick() }

func (m *uiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			m.quitting = true
			m.cancel()
			return m, nil
		}
	case sampleMsg:
		m.last = Snapshot(msg)
		m.spark = append(m.spark, m.last.DeliveredEPS)
		if len(m.spark) > 72 {
			m.spark = m.spark[len(m.spark)-72:]
		}
	case doneMsg:
		m.finished = true
		m.summary = msg.summary
		return m, tea.Quit
	case tickMsg:
		m.offEPS, m.delEPS, m.mbs = m.st.live(&m.mark)
		return m, tick()
	}
	return m, nil
}

func (m *uiModel) View() string {
	if m.finished {
		return renderFinal(m.summary, m.cfg)
	}
	var b strings.Builder
	elapsed := time.Since(m.st.startWall)

	head := sTitle.Render("BIFRACT LOADGEN")
	right := sLabel.Render("elapsed ") + sValue.Render(fmtDur(elapsed))
	if m.cfg.Duration > 0 {
		right += sLabel.Render(" / ") + sValue.Render(fmtDur(m.cfg.Duration))
	}
	if m.quitting {
		right += sWarn.Render("  draining...")
	}
	b.WriteString(pad(head, right, m.width))
	b.WriteString("\n\n")

	b.WriteString(statCol("DELIVERED", sBig.Render(fmt.Sprintf("%s ev/s", comma(int64(m.delEPS)))), 22))
	b.WriteString(statCol("TARGET", sValue.Render(fmt.Sprintf("%s ev/s", comma(int64(m.cfg.Rate)))), 20))
	b.WriteString(statCol("THROUGHPUT", sValue.Render(fmt.Sprintf("%.2f MB/s", m.mbs)), 20))
	b.WriteString(statCol("PROJECTED", sValue.Render(fmt.Sprintf("%.0f GB/day", m.mbs*86400/1024)), 20))
	b.WriteString("\n\n")

	if len(m.spark) > 1 {
		b.WriteString(sLabel.Render("  throughput  "))
		b.WriteString(lipgloss.NewStyle().Foreground(cAccent).Render(sparkline(m.spark)))
		b.WriteString("\n\n")
	}

	b.WriteString(row("LATENCY", fmt.Sprintf("p50 %s   p95 %s   p99 %s   max %s",
		sValue.Render(ms(m.last.P50)), sValue.Render(ms(m.last.P95)),
		sValue.Render(ms(m.last.P99)), sValue.Render(ms(m.last.Max)))))

	httpLine := fmt.Sprintf("2xx %s", sValue.Render(comma(m.last.HTTP2xx)))
	httpLine += "   429 " + colorCount(m.last.HTTP429, sWarn)
	httpLine += "   4xx " + colorCount(m.last.HTTP4xx, sErr)
	httpLine += "   5xx " + colorCount(m.last.HTTP5xx, sErr)
	httpLine += "   conn " + colorCount(m.last.ConnErrors, sErr)
	b.WriteString(row("HTTP", httpLine))

	cpu := m.last.CPUPercent
	cpuStyle, note := sOK, "headroom ok"
	switch {
	case cpu >= 85:
		cpuStyle, note = sErr, "SATURATED, results invalid"
	case cpu >= 70:
		cpuStyle, note = sWarn, "above gate, add capacity"
	}
	b.WriteString(row("CPU", gauge(cpu, 24, cpuStyle)+"  "+cpuStyle.Render(pctStr(cpu))+"  "+sLabel.Render(note)))
	b.WriteString(row("LAG", fmt.Sprintf("mean %s   max %s", sValue.Render(ms(m.last.LagMeanMs)), sValue.Render(ms(m.last.LagMaxMs)))))
	b.WriteString(row("TOTAL", fmt.Sprintf("%s events   %s   %s rejected",
		sValue.Render(comma(m.last.TotalDelivered)), sValue.Render(fmt.Sprintf("%.2f GB", m.last.TotalGB)),
		sValue.Render(comma(m.last.TotalRejected)))))
	b.WriteString("\n")

	b.WriteString(sSection.Render("  EVENT MIX") + "\n")
	for i := range m.st.kinds {
		c := m.st.kinds[i].count.Load()
		by := m.st.kinds[i].bytes.Load()
		avg := 0.0
		if c > 0 {
			avg = float64(by) / float64(c)
		}
		var tot int64
		for k := range m.st.kinds {
			tot += m.st.kinds[k].count.Load()
		}
		p := 0.0
		if tot > 0 {
			p = float64(c) / float64(tot) * 100
		}
		b.WriteString(fmt.Sprintf("    %-20s %s  %s  %s\n",
			sLabel.Render(kindNames[i]),
			sValue.Render(fmt.Sprintf("%5.1f%%", p)),
			sLabel.Render(fmt.Sprintf("%12s", comma(c))),
			sLabel.Render(fmt.Sprintf("%6.0f B", avg))))
	}
	b.WriteString("\n")
	b.WriteString(sLabel.Render("  out ") + sValue.Render(m.cfg.OutDir) +
		sLabel.Render("    avg event ") + sValue.Render(fmt.Sprintf("%.0f B", m.last.AvgEventBytes)) +
		sLabel.Render("    q to stop") + "\n")
	return b.String()
}

func renderFinal(s Summary, cfg Config) string {
	var b strings.Builder
	b.WriteString("\n" + sTitle.Render("  BIFRACT LOADGEN, run complete") + "\n\n")
	line := func(k, v string) { b.WriteString(fmt.Sprintf("  %-22s %s\n", sLabel.Render(k), sValue.Render(v))) }
	line("duration", fmtDur(time.Duration(s.DurationSec)*time.Second))
	line("delivered", fmt.Sprintf("%s events (%.2f%% of offered)", comma(s.EventsDelivered), s.DeliveryRate*100))
	line("sustained", fmt.Sprintf("%s ev/s, %.2f MB/s", comma(int64(s.MeanEPS)), s.MeanMBPerSec))
	line("projected", fmt.Sprintf("%.1f GB/day", s.ProjectedGBDay))
	line("volume", fmt.Sprintf("%.2f GB, avg event %.0f B", s.TotalGB, s.AvgEventBytes))
	line("latency", fmt.Sprintf("p50 %s  p95 %s  p99 %s  max %s", ms(s.LatencyP50), ms(s.LatencyP95), ms(s.LatencyP99), ms(s.LatencyMax)))
	line("429s", comma(s.HTTP429))
	line("errors", fmt.Sprintf("4xx %d  5xx %d  conn %d", s.HTTP4xx, s.HTTP5xx, s.ConnErrors))
	line("generator cpu", fmt.Sprintf("peak %.2f%%  mean %.2f%%", s.PeakCPUPercent, s.MeanCPUPercent))
	line("cardinality", fmt.Sprintf("%s hosts, %s proc guids, %s domains, %s ext ips",
		comma(int64(s.UniqueHosts)), comma(s.UniqueProcGUIDs), comma(int64(s.UniqueDomains)), comma(int64(s.UniqueExtIPs))))

	st := sOK
	if strings.HasPrefix(s.GeneratorHealth, "WARNING") {
		st = sWarn
	} else if strings.HasPrefix(s.GeneratorHealth, "SATURATED") {
		st = sErr
	}
	b.WriteString("\n  " + st.Render(s.GeneratorHealth) + "\n\n")
	b.WriteString(sLabel.Render("  wrote ") + sValue.Render(cfg.OutDir+"/summary.json") +
		sLabel.Render(" and ") + sValue.Render(cfg.OutDir+"/samples.csv") + "\n\n")
	return b.String()
}

func row(label, val string) string {
	return fmt.Sprintf("  %s  %s\n", sSection.Render(fmt.Sprintf("%-9s", label)), val)
}

func statCol(label, val string, w int) string {
	return lipgloss.NewStyle().Width(w).Render("  " + sLabel.Render(label) + "\n  " + val)
}

func pad(left, right string, w int) string {
	lw := lipgloss.Width(left) + lipgloss.Width(right)
	if w-lw-2 < 1 {
		return "  " + left + "  " + right
	}
	return "  " + left + strings.Repeat(" ", w-lw-4) + right
}

var sparkRunes = []rune("▁▂▃▄▅▆▇█")

func sparkline(v []float64) string {
	maxv := 0.0
	for _, x := range v {
		if x > maxv {
			maxv = x
		}
	}
	if maxv <= 0 {
		return strings.Repeat("▁", len(v))
	}
	var b strings.Builder
	for _, x := range v {
		i := int(x / maxv * float64(len(sparkRunes)-1))
		if i < 0 {
			i = 0
		}
		if i >= len(sparkRunes) {
			i = len(sparkRunes) - 1
		}
		b.WriteRune(sparkRunes[i])
	}
	return b.String()
}

func gauge(pct float64, w int, st lipgloss.Style) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100 * float64(w))
	return st.Render(strings.Repeat("█", filled)) + sLabel.Render(strings.Repeat("░", w-filled))
}

func colorCount(n int64, st lipgloss.Style) string {
	if n == 0 {
		return sValue.Render("0")
	}
	return st.Render(comma(n))
}

// pctStr keeps sub-1% CPU readable. A Go generator sits far below the headroom gate, and
// rounding that to "0%" makes a working meter look broken.
func pctStr(v float64) string {
	if v < 10 {
		return fmt.Sprintf("%.2f%%", v)
	}
	return fmt.Sprintf("%.0f%%", v)
}

func ms(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%.2fs", v/1000)
	}
	return fmt.Sprintf("%.1fms", v)
}

func fmtDur(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
