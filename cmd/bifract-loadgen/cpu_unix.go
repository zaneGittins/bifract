//go:build unix

package main

import (
	"runtime"
	"sync"
	"syscall"
	"time"
)

// cpuMeter reports this process's CPU use as a percentage of all available cores. This is
// the benchmark's headroom gate: above roughly 70 percent the published throughput is a
// generator limit, not a Bifract limit.
type cpuMeter struct {
	mu       sync.Mutex
	lastWall time.Time
	lastCPU  time.Duration
	peak     float64
	sum      float64
	n        int
}

func newCPUMeter() *cpuMeter {
	return &cpuMeter{lastWall: time.Now(), lastCPU: procCPU()}
}

func procCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	u := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	s := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return u + s
}

func (m *cpuMeter) sample() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	now, cpu := time.Now(), procCPU()
	wall := now.Sub(m.lastWall)
	if wall <= 0 {
		return 0
	}
	pct := float64(cpu-m.lastCPU) / float64(wall) / float64(runtime.NumCPU()) * 100
	m.lastWall, m.lastCPU = now, cpu
	if pct < 0 {
		pct = 0
	}
	if pct > m.peak {
		m.peak = pct
	}
	m.sum += pct
	m.n++
	return pct
}

func (m *cpuMeter) peakMean() (float64, float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.n == 0 {
		return m.peak, 0
	}
	return m.peak, m.sum / float64(m.n)
}
