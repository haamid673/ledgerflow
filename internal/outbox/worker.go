package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Worker struct {
	pool    *pgxpool.Pool
	cfg     Config
	handler Handler
	log     *slog.Logger
}

func NewWorker(pool *pgxpool.Pool, cfg Config, handler Handler) *Worker {
	cfg = cfg.withDefaults()
	return &Worker{pool: pool, cfg: cfg, handler: handler, log: slog.With("component", "outbox", "worker", cfg.WorkerID)}
}

func (w *Worker) WorkerID() string { return w.cfg.WorkerID }

func sortByID(events []Event) {
	slices.SortFunc(events, func(a, b Event) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
}

// settleTimeout bounds ack/fail/release writes, which use a context detached from the
// caller's so that they still happen during shutdown.
const settleTimeout = 5 * time.Second

// RunOnce claims one batch and processes it sequentially. It returns how many events
// were claimed. Events are not strictly ordered: failures retry later, and multiple
// workers run concurrently.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	events, err := w.Claim(ctx)
	if err != nil {
		return 0, err
	}
	for i, e := range events {
		if ctx.Err() != nil { // shutting down: give back what we haven't started
			for _, rest := range events[i:] {
				w.settleRelease(rest)
			}
			return len(events), nil
		}
		w.process(ctx, e)
	}
	return len(events), nil
}

func (w *Worker) process(ctx context.Context, e Event) {
	log := w.log.With("event_id", e.ID, "event_type", e.EventType, "attempt", e.Attempt)

	hctx, cancel := context.WithTimeout(ctx, w.cfg.Lease)
	err := w.callHandler(hctx, e)
	cancel()

	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer scancel()

	switch {
	case err == nil:
		if aerr := w.Ack(sctx, e); aerr != nil {
			// The handler ran but we can't record it: the event will be redelivered.
			log.Warn("outbox ack failed; event will be redelivered", "outcome", "ack_failed", "err", aerr)
			return
		}
		log.Info("outbox event published", "outcome", "published")
	case ctx.Err() != nil: // worker is shutting down; not the event's fault
		w.settleRelease(e)
	default:
		exhausted, ferr := w.Fail(sctx, e, err)
		switch {
		case ferr != nil:
			log.Warn("outbox fail-record failed; lease will expire", "outcome", "fail_record_failed", "handler_err", err, "err", ferr)
		case exhausted:
			log.Error("outbox event failed permanently", "outcome", "failed", "max_attempts", w.cfg.MaxAttempts, "err", err)
		default:
			log.Warn("outbox event will be retried", "outcome", "retry_scheduled", "retry_in", w.cfg.backoff(e.Attempt), "err", err)
		}
	}
}

func (w *Worker) settleRelease(e Event) {
	sctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	if err := w.release(sctx, e); err != nil {
		w.log.Warn("outbox release failed", "event_id", e.ID, "err", err)
		return
	}
	w.log.Info("outbox claim released", "event_id", e.ID, "outcome", "released")
}

func (w *Worker) callHandler(ctx context.Context, e Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return w.handler(ctx, e)
}

// Run polls until ctx is cancelled. It drains eagerly while work is available and
// otherwise sleeps for PollInterval (no busy loop). It returns nil on cancellation.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("outbox worker started", "batch", w.cfg.BatchSize, "lease", w.cfg.Lease,
		"max_attempts", w.cfg.MaxAttempts, "poll", w.cfg.PollInterval)
	defer w.log.Info("outbox worker stopped")
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		n, err := w.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			w.log.Error("outbox poll failed", "err", err)
		}
		if err == nil && n > 0 {
			continue
		}
		timer.Reset(w.cfg.PollInterval)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
	}
}
