package setup

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/term"
)

// Step progress rendering.
//
// The setup flows (install, upgrade, and their k8s variants) run an ordered
// sequence of steps. Output aims to match best-in-class CLI installers:
//
//   - On a TTY, a long-running step shows an animated spinner with an elapsed
//     counter that updates in place, then collapses to a single persistent
//     result line ("ephemeral progress, persistent result"). Instant steps skip
//     the spinner entirely and print only their result line, so there is no
//     redundant "doing X..." / "did X" double line.
//   - On a non-TTY (piped output, CI logs), no control codes are emitted: each
//     step prints a plain "[~]" start line and a "[+]" result line, forming an
//     appendable transcript.
//   - Steps are numbered "[n/N]" when a total has been declared via resetSteps.
//
// Only one step is active at a time (the flows are strictly sequential), so the
// state below is deliberately package-level and single-threaded apart from the
// spinner goroutine, which is synchronized through the stop/stopped channels.

// isStdoutTTY reports whether stdout is an interactive terminal.
var isStdoutTTY = term.IsTerminal(int(os.Stdout.Fd()))

var spinnerFrames = []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")

const (
	spinnerInterval = 100 * time.Millisecond
	// Delay before the first spinner frame so sub-120ms steps never flicker a
	// spinner and instead render as a single result line.
	spinnerDelay = 120 * time.Millisecond
)

type stepState struct {
	n       int
	label   string
	start   time.Time
	stop    chan struct{} // closed to ask the spinner goroutine to exit (nil = no spinner)
	stopped chan struct{} // closed by the spinner goroutine once it has exited
	drew    bool          // spinner rendered at least one frame (safe to read after <-stopped)
}

var (
	stepTotal   int
	stepCounter int
	curStep     *stepState
)

// resetSteps begins a new numbered sequence of `total` steps. Pass the happy-path
// count; skipped optional steps simply end the sequence early, which reads fine.
func resetSteps(total int) {
	abandonStep()
	stepTotal = total
	stepCounter = 0
}

// stepTag renders the dimmed "[n/N] " prefix, or "" when no total is set. If the
// counter ever exceeds the declared total it degrades to "[n] " rather than
// showing an impossible ratio.
func stepTag(n int) string {
	if stepTotal <= 0 {
		return ""
	}
	if n > stepTotal {
		return DimStyle.Render(fmt.Sprintf("[%d] ", n))
	}
	return DimStyle.Render(fmt.Sprintf("[%d/%d] ", n, stepTotal))
}

// printStep begins a silent step. On a TTY a spinner animates after a short delay
// and collapses into the result line printed by the next printDone/printWarn.
func printStep(msg string) {
	abandonStep()
	stepCounter++
	s := &stepState{n: stepCounter, label: msg, start: time.Now()}
	curStep = s
	if isStdoutTTY {
		s.stop = make(chan struct{})
		s.stopped = make(chan struct{})
		go spin(s)
	} else {
		fmt.Printf("[%s] %s%s\n", DimStyle.Render("~"), stepTag(s.n), msg)
	}
}

// printStepStream begins a step whose operation streams its own output to stdout
// (e.g. docker pull). No spinner is used, since it would fight the streamed
// output; a persistent header line is printed and the result follows below.
func printStepStream(msg string) {
	abandonStep()
	stepCounter++
	curStep = &stepState{n: stepCounter, label: msg, start: time.Now()}
	fmt.Printf("[%s] %s%s\n", DimStyle.Render("~"), stepTag(curStep.n), msg)
}

func printDone(msg string) { endStep(SuccessStyle.Render("+"), msg, true) }
func printWarn(msg string) { endStep(WarningStyle.Render("!"), msg, false) }

// endStep finalizes the active step (or prints a standalone result line if none
// is active, e.g. a sub-result following the step's first result line).
func endStep(marker, msg string, withElapsed bool) {
	s := curStep
	if s == nil {
		fmt.Printf("[%s] %s\n", marker, msg)
		return
	}
	curStep = nil

	if isStdoutTTY && s.stop != nil {
		close(s.stop)
		<-s.stopped
		if s.drew {
			fmt.Print("\r\033[K") // erase the spinner line so the result replaces it
		}
	}

	suffix := ""
	if withElapsed {
		if e := time.Since(s.start); e >= time.Second {
			suffix = " " + DimStyle.Render("("+e.Round(100*time.Millisecond).String()+")")
		}
	}
	fmt.Printf("[%s] %s%s%s\n", marker, stepTag(s.n), msg, suffix)
}

// abandonStep stops a dangling spinner without printing a result. Guards against
// goroutine leaks if a step is never explicitly completed.
func abandonStep() {
	s := curStep
	if s == nil {
		return
	}
	curStep = nil
	if isStdoutTTY && s.stop != nil {
		close(s.stop)
		<-s.stopped
		if s.drew {
			fmt.Print("\r\033[K")
		}
	}
}

func spin(s *stepState) {
	defer close(s.stopped)

	select {
	case <-s.stop:
		return
	case <-time.After(spinnerDelay):
	}

	for i := 0; ; i++ {
		elapsed := time.Since(s.start).Round(time.Second)
		fmt.Printf("\r[%s] %s%s %s\033[K",
			HighlightStyle.Render(string(spinnerFrames[i%len(spinnerFrames)])),
			stepTag(s.n), s.label, DimStyle.Render(elapsed.String()))
		s.drew = true

		select {
		case <-s.stop:
			return
		case <-time.After(spinnerInterval):
		}
	}
}
