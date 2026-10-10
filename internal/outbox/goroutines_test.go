package outbox

import (
	"bytes"
	"context"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Worker.Run is synchronous (handlers run inside it and it starts no goroutines of its
// own), so after it returns nothing it created may still be running. The existing shutdown
// tests prove Run returns and releases claims; this one asserts the goroutine count, so a
// future change that spawns background goroutines without joining them is noticed.
func TestRunLeavesNoGoroutinesBehind(t *testing.T) {
	e := newEnv(t)

	cycle := func() {
		t.Helper()
		w := NewWorker(e.pool, e.cfg("w"), func(context.Context, Event) error { return nil })
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.Run(runCtx) }()

		id := e.insertEvent("test.event", map[string]int{})
		deadline := time.Now().Add(10 * time.Second)
		for !e.row(id).Published {
			if time.Now().After(deadline) {
				cancel()
				t.Fatal("event was not published by the running worker")
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}

	cycle() // warm up: pool connections and lazily started runtime goroutines exist from here on
	before := runtime.NumGoroutine()
	for range 3 {
		cycle()
	}

	// Give goroutines that were already told to exit a moment to finish.
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
		t.Errorf("goroutines: %d before, %d after three start/stop cycles\n%s", before, after, buf.String())
	}
}
