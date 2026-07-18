package setup

import (
	"bufio"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// captureStdout redirects os.Stdout for the duration of fn and returns what was
// written. fmt.Print* writes to the os.Stdout variable, so swapping it captures
// the progress output regardless of the real terminal.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		var b strings.Builder
		io.Copy(&b, bufio.NewReader(r))
		done <- b.String()
	}()

	fn()

	w.Close()
	os.Stdout = orig
	return <-done
}

func TestProgressNonTTYTranscript(t *testing.T) {
	defer func() { isStdoutTTY = false; resetSteps(0) }()
	isStdoutTTY = false // deterministic transcript path

	out := captureStdout(t, func() {
		resetSteps(3)
		printStep("Doing first thing")
		printDone("First thing done")
		printStep("Doing second thing")
		printWarn("Second thing degraded")
		printStep("Doing third thing")
		printDone("Third thing done")
	})

	// Numbering is present and correct.
	for _, want := range []string{"[1/3]", "[2/3]", "[3/3]", "First thing done", "Second thing degraded"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q\n---\n%s", want, out)
		}
	}
	// Non-TTY must not emit cursor control codes.
	if strings.Contains(out, "\r") || strings.Contains(out, "\033[K") {
		t.Fatalf("non-TTY output contains control codes:\n%q", out)
	}
}

func TestProgressStandaloneAndMultiResult(t *testing.T) {
	defer func() { isStdoutTTY = false; resetSteps(0) }()
	isStdoutTTY = false

	out := captureStdout(t, func() {
		resetSteps(2)
		printStep("Running migrations")
		printDone("Applied 2 Postgres migration(s)")   // ends the step
		printDone("Applied 5 ClickHouse migration(s)") // standalone sub-result
		printDone("Standalone note")                   // standalone, no active step
	})

	lines := strings.Split(strings.TrimSpace(out), "\n")
	// The step result carries a number; the trailing sub-results do not.
	var numbered, plain int
	for _, l := range lines {
		if strings.Contains(l, "Applied 2 Postgres") && strings.Contains(l, "[1/2]") {
			numbered++
		}
		if (strings.Contains(l, "ClickHouse migration") || strings.Contains(l, "Standalone note")) && !strings.Contains(l, "[1/2]") {
			plain++
		}
	}
	if numbered != 1 {
		t.Fatalf("expected the step line numbered [1/2], got:\n%s", out)
	}
	if plain != 2 {
		t.Fatalf("expected 2 un-numbered standalone results, got:\n%s", out)
	}
}

// Exercises the spinner goroutine + collapse under -race: a step long enough to
// render at least one frame, then completed. Verifies clean synchronization and
// that the erase sequence + final result are emitted.
func TestProgressTTYSpinnerRace(t *testing.T) {
	defer func() { isStdoutTTY = false; resetSteps(0) }()
	isStdoutTTY = true

	out := captureStdout(t, func() {
		resetSteps(1)
		printStep("Waiting for something slow")
		time.Sleep(1100 * time.Millisecond) // draw frames and cross the 1s elapsed threshold
		printDone("Something is ready")
	})

	if !strings.Contains(out, "\033[K") {
		t.Fatalf("expected spinner erase sequence in TTY output:\n%q", out)
	}
	if !strings.Contains(out, "Something is ready") {
		t.Fatalf("expected final result line:\n%q", out)
	}
	// A slow step should report elapsed time.
	if !strings.Contains(out, "s)") {
		t.Fatalf("expected elapsed suffix on slow step:\n%q", out)
	}
}

// abandonStep (invoked via `defer` on an early error return) must stop the
// spinner goroutine, erase its line, and print no result. Run under -race to
// confirm the goroutine is cleaned up rather than leaked or left mid-write.
func TestProgressAbandonOnError(t *testing.T) {
	defer func() { isStdoutTTY = false; resetSteps(0) }()
	isStdoutTTY = true

	out := captureStdout(t, func() {
		resetSteps(2)
		printStep("Doing something that will fail")
		time.Sleep(spinnerDelay + 3*spinnerInterval) // let the spinner draw
		abandonStep()                                // simulates `defer abandonStep()` firing on error
	})

	if !strings.Contains(out, "\033[K") {
		t.Fatalf("expected spinner erase on abandon:\n%q", out)
	}
	// No result marker should be printed for an abandoned step.
	if strings.Contains(out, "[+]") || strings.Contains(out, "[!]") {
		t.Fatalf("abandoned step must not print a result line:\n%q", out)
	}
}

// A printStep with no matching printDone must not silently vanish or wedge: the
// next printStep abandons it cleanly and completes normally.
func TestProgressOrphanStepDoesNotWedge(t *testing.T) {
	defer func() { isStdoutTTY = false; resetSteps(0) }()
	isStdoutTTY = true

	out := captureStdout(t, func() {
		resetSteps(2)
		printStep("Orphan step with no done")
		printStep("Second step")
		printDone("Second step done")
	})

	if !strings.Contains(out, "Second step done") {
		t.Fatalf("second step must complete after an orphan:\n%q", out)
	}
}
