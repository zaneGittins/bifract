package exportcli

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/term"

	"bifract/internal/ingestcli"
)

type countingWriter struct {
	w io.Writer
	n atomic.Int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(int64(n))
	return n, err
}

// showProgress redraws a byte counter on stderr once a second while it is a
// terminal. The returned func stops it and clears the line.
func showProgress(c *countingWriter) func() {
	if !term.IsTerminal(int(os.Stderr.Fd())) {
		return func() {}
	}
	start := time.Now()
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				fmt.Fprint(os.Stderr, "\r\033[K")
				return
			case <-t.C:
				fmt.Fprintf(os.Stderr, "\r\033[K%s %s written, %s elapsed",
					ingestcli.DimStyle.Render("Exporting:"), ingestcli.FormatBytes(c.n.Load()), time.Since(start).Round(time.Second))
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}
