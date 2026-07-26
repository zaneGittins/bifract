//go:build !unix

package main

// cpuMeter is a no-op outside unix; the load generators run on Linux droplets.
type cpuMeter struct{}

func newCPUMeter() *cpuMeter                     { return &cpuMeter{} }
func (m *cpuMeter) sample() float64              { return 0 }
func (m *cpuMeter) peakMean() (float64, float64) { return 0, 0 }
