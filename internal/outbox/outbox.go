// Package outbox implements a PostgreSQL-backed outbox worker.
//
// Delivery is at-least-once: an event is marked published only after its handler
// returns successfully, in a separate transaction. A crash between the two causes
// redelivery, so handlers must be idempotent (see Consumer).
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrLeaseLost means the claim being acknowledged is no longer the current claim
// (it expired and was reclaimed, or was already settled).
var ErrLeaseLost = errors.New("outbox: lease lost")

type Config struct {
	WorkerID     string        // unique per worker instance; used as the lease owner
	PollInterval time.Duration // idle wait between polls
	BatchSize    int           // events claimed per poll
	Lease        time.Duration // how long a claim is valid; also the per-event handler timeout
	MaxAttempts  int           // attempts (including crashed ones) before an event is marked failed
	BaseBackoff  time.Duration // delay after the first failure; doubles per attempt
	MaxBackoff   time.Duration
}

func (c Config) withDefaults() Config {
	if c.WorkerID == "" {
		c.WorkerID = "worker-" + uuid.NewString()[:8]
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = time.Minute
	}
	return c
}

// backoff returns the delay after the given (1-based) failed attempt.
func (c Config) backoff(attempt int) time.Duration {
	d := c.BaseBackoff
	for i := 1; i < attempt && d < c.MaxBackoff; i++ {
		d *= 2
	}
	if d > c.MaxBackoff {
		d = c.MaxBackoff
	}
	return d
}

// Event is a claimed outbox event. Attempt doubles as the fencing token for the claim.
type Event struct {
	ID            int64
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	Payload       json.RawMessage
	CreatedAt     time.Time
	Attempt       int
}

// Handler processes one event. It may be called more than once for the same event.
type Handler func(ctx context.Context, e Event) error

// Claim atomically leases up to BatchSize eligible events for this worker.
//
// The transaction is short: it only flips lease columns, then commits, so no row
// locks are held while handlers run. FOR UPDATE SKIP LOCKED lets concurrent workers
// claim disjoint events without blocking each other. Eligibility is based on
// next_attempt_at / locked_until (database time), never on an id cursor.
func (w *Worker) Claim(ctx context.Context) ([]Event, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// An expired lease on a row that already used its last attempt means the worker
	// crashed (or timed out) on the final try: park it as failed rather than strand it.
	if _, err := tx.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'failed', locked_by = NULL, locked_until = NULL,
		    last_error = 'lease expired on final attempt (worker crash or timeout)'
		WHERE status = 'pending' AND locked_until < now() AND attempts >= $1`, w.cfg.MaxAttempts); err != nil {
		return nil, fmt.Errorf("reap exhausted claims: %w", err)
	}

	rows, err := tx.Query(ctx, `
		WITH c AS (
			SELECT id FROM outbox_events
			WHERE status = 'pending'
			  AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			  AND attempts < $4
			ORDER BY next_attempt_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events o
		SET locked_by = $2,
		    locked_until = now() + make_interval(secs => $3::float8),
		    attempts = o.attempts + 1
		FROM c WHERE o.id = c.id
		RETURNING o.id, o.aggregate_type, o.aggregate_id, o.event_type, o.payload, o.created_at, o.attempts`,
		w.cfg.BatchSize, w.cfg.WorkerID, w.cfg.Lease.Seconds(), w.cfg.MaxAttempts)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Payload, &e.CreatedAt, &e.Attempt); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	sortByID(events)
	return events, nil
}

// Ack marks the event published. It succeeds only for the current claim: same owner
// AND same attempt number. A stale worker (even one with the same WorkerID) gets ErrLeaseLost.
func (w *Worker) Ack(ctx context.Context, e Event) error {
	tag, err := w.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'published', published_at = now(), locked_by = NULL, locked_until = NULL
		WHERE id = $1 AND locked_by = $2 AND attempts = $3 AND status = 'pending'`,
		e.ID, w.cfg.WorkerID, e.Attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail records a handler failure for the current claim. Below MaxAttempts the event goes
// back to pending with exponential backoff; at MaxAttempts it becomes 'failed' (kept, not
// discarded) and is never claimed again until RetryFailed.
func (w *Worker) Fail(ctx context.Context, e Event, cause error) (exhausted bool, err error) {
	exhausted = e.Attempt >= w.cfg.MaxAttempts
	msg := cause.Error()
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	tag, err := w.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = CASE WHEN $5::bool THEN 'failed' ELSE 'pending' END,
		    last_error = $4,
		    locked_by = NULL, locked_until = NULL,
		    next_attempt_at = CASE WHEN $5::bool THEN next_attempt_at
		                           ELSE now() + make_interval(secs => $6::float8) END
		WHERE id = $1 AND locked_by = $2 AND attempts = $3 AND status = 'pending'`,
		e.ID, w.cfg.WorkerID, e.Attempt, msg, exhausted, w.cfg.backoff(e.Attempt).Seconds())
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, ErrLeaseLost
	}
	return exhausted, nil
}

// release gives a claim back without penalty (graceful shutdown): the attempt is not counted.
func (w *Worker) release(ctx context.Context, e Event) error {
	tag, err := w.pool.Exec(ctx, `
		UPDATE outbox_events
		SET locked_by = NULL, locked_until = NULL, attempts = attempts - 1, next_attempt_at = now()
		WHERE id = $1 AND locked_by = $2 AND attempts = $3 AND status = 'pending'`,
		e.ID, w.cfg.WorkerID, e.Attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// FailedEvent is an event that exhausted its attempts.
type FailedEvent struct {
	ID        int64     `json:"id"`
	EventType string    `json:"event_type"`
	Aggregate uuid.UUID `json:"aggregate_id"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error"`
	CreatedAt time.Time `json:"created_at"`
}

// ListFailed returns failed events, oldest first.
func ListFailed(ctx context.Context, pool *pgxpool.Pool, limit int) ([]FailedEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := pool.Query(ctx, `
		SELECT id, event_type, aggregate_id, attempts, COALESCE(last_error, ''), created_at
		FROM outbox_events WHERE status = 'failed' ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FailedEvent{}
	for rows.Next() {
		var f FailedEvent
		if err := rows.Scan(&f.ID, &f.EventType, &f.Aggregate, &f.Attempts, &f.LastError, &f.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// ErrNotFailed is returned by RetryFailed when the event isn't in the failed state.
var ErrNotFailed = errors.New("outbox: event is not in failed state")

// RetryFailed deliberately re-queues a failed event with a fresh attempt budget.
// last_error is kept as history.
func RetryFailed(ctx context.Context, pool *pgxpool.Pool, id int64) error {
	tag, err := pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'pending', attempts = 0, next_attempt_at = now()
		WHERE id = $1 AND status = 'failed'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFailed
	}
	return nil
}
