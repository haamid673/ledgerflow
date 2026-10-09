package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/testutil"
	"ledgerflow/migrations"
)

var ctx = context.Background()

// Outbox tests run against their own database (see testutil.IsolatedPool) because the
// worker claims ANY pending row. Each test starts from an empty outbox.
type env struct {
	t    *testing.T
	pool *pgxpool.Pool
}

func newEnv(t *testing.T) *env {
	pool := testutil.IsolatedPool(t, "outbox")
	e := &env{t: t, pool: pool}
	e.reset()
	t.Cleanup(e.reset)
	return e
}

func (e *env) reset() {
	if _, err := e.pool.Exec(ctx, `DELETE FROM consumer_processed_events; DELETE FROM outbox_events`); err != nil {
		e.t.Fatalf("reset: %v", err)
	}
}

func (e *env) cfg(id string) Config {
	return Config{WorkerID: id, BatchSize: 10, Lease: 30 * time.Second, MaxAttempts: 3,
		BaseBackoff: 0, MaxBackoff: time.Minute, PollInterval: 10 * time.Millisecond}
}

func (e *env) insertEvent(eventType string, payload any) int64 {
	e.t.Helper()
	b, _ := json.Marshal(payload)
	var id int64
	err := e.pool.QueryRow(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ('test', $1, $2, $3) RETURNING id`, uuid.New(), eventType, b).Scan(&id)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

type row struct {
	Status      string
	Attempts    int
	LockedBy    *string
	LastError   *string
	Published   bool
	NextAttempt time.Time
}

func (e *env) row(id int64) row {
	e.t.Helper()
	var r row
	err := e.pool.QueryRow(ctx, `SELECT status, attempts, locked_by, last_error, published_at IS NOT NULL, next_attempt_at
		FROM outbox_events WHERE id = $1`, id).Scan(&r.Status, &r.Attempts, &r.LockedBy, &r.LastError, &r.Published, &r.NextAttempt)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// expire simulates the passage of lease time deterministically (no sleeping).
func (e *env) expire(id int64) {
	e.t.Helper()
	if _, err := e.pool.Exec(ctx, `UPDATE outbox_events SET locked_until = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) drain(w *Worker) {
	e.t.Helper()
	for {
		n, err := w.RunOnce(ctx)
		if err != nil {
			e.t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

// accounts creates n INR accounts with no opening balance (so no outbox events are produced).
func (e *env) accounts(n int) []uuid.UUID {
	e.t.Helper()
	l := ledger.New(e.pool)
	u, err := l.CreateUser(ctx, uuid.NewString()+"@t.local", "T")
	if err != nil {
		e.t.Fatal(err)
	}
	ids := make([]uuid.UUID, n)
	for i := range ids {
		a, err := l.CreateAccount(ctx, u.ID, 0)
		if err != nil {
			e.t.Fatal(err)
		}
		ids[i] = a.ID
	}
	return ids
}

type activity struct{ DebitCount, CreditCount, Debited, Credited int64 }

func (e *env) activity(acct uuid.UUID) activity {
	e.t.Helper()
	var a activity
	err := e.pool.QueryRow(ctx, `SELECT debit_count, credit_count, total_debited, total_credited FROM account_activity WHERE account_id=$1`, acct).
		Scan(&a.DebitCount, &a.CreditCount, &a.Debited, &a.Credited)
	if err != nil {
		return activity{} // no row yet
	}
	return a
}

func transferPayloadFor(src, dst uuid.UUID, amount int64) map[string]any {
	return map[string]any{"transaction_id": uuid.New(), "source_account_id": src, "destination_account_id": dst, "amount": amount, "currency": "INR"}
}

func TestBackoffIsBoundedExponential(t *testing.T) {
	c := Config{BaseBackoff: time.Second, MaxBackoff: 10 * time.Second}
	want := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	for i, w := range want {
		if got := c.backoff(i + 1); got != w*time.Second {
			t.Errorf("backoff(%d) = %v, want %v", i+1, got, w*time.Second)
		}
	}
	if got := c.backoff(1000); got != 10*time.Second {
		t.Errorf("backoff(1000) = %v", got)
	}
}

// Real transfers produce exactly one outbox row in the same transaction; rejected and
// rolled-back transfers produce none. The row starts pending and untouched.
func TestTransferWritesOutboxAtomically(t *testing.T) {
	e := newEnv(t)
	l := ledger.New(e.pool)
	alice, _ := l.CreateUser(ctx, uuid.NewString()+"@t.local", "A")
	bob, _ := l.CreateUser(ctx, uuid.NewString()+"@t.local", "B")
	a, _ := l.CreateAccount(ctx, alice.ID, 10_000)
	b, _ := l.CreateAccount(ctx, bob.ID, 0)
	e.reset() // drop the deposit event from funding

	res, _, err := l.Transfer(ctx, ledger.TransferInput{UserID: alice.ID, IdempotencyKey: "k", Source: a.ID, Dest: b.ID, Amount: 2_500, Currency: "INR"})
	if err != nil {
		t.Fatal(err)
	}
	// Replay and rejected transfers add nothing.
	if _, _, err := l.Transfer(ctx, ledger.TransferInput{UserID: alice.ID, IdempotencyKey: "k", Source: a.ID, Dest: b.ID, Amount: 2_500, Currency: "INR"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Transfer(ctx, ledger.TransferInput{UserID: alice.ID, IdempotencyKey: "k2", Source: a.ID, Dest: b.ID, Amount: 999_999, Currency: "INR"}); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v", err)
	}

	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("outbox rows = %d (err %v), want exactly 1", n, err)
	}
	var id int64
	var payload []byte
	if err := e.pool.QueryRow(ctx, `SELECT id, payload FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'transfer.completed'`, res.TransactionID).Scan(&id, &payload); err != nil {
		t.Fatal(err)
	}
	if r := e.row(id); r.Status != "pending" || r.Attempts != 0 || r.Published || r.LockedBy != nil {
		t.Errorf("new event state = %+v", r)
	}
	var p transferPayload
	if err := json.Unmarshal(payload, &p); err != nil || p.Amount != 2_500 || p.Source != a.ID || p.Destination != b.ID {
		t.Errorf("payload = %s (err %v)", payload, err)
	}
}

func TestSuccessfulProcessingEndToEnd(t *testing.T) {
	e := newEnv(t)
	l := ledger.New(e.pool)
	alice, _ := l.CreateUser(ctx, uuid.NewString()+"@t.local", "A")
	a, _ := l.CreateAccount(ctx, alice.ID, 10_000) // emits a deposit.completed event
	b, _ := l.CreateAccount(ctx, alice.ID, 0)
	if _, _, err := l.Transfer(ctx, ledger.TransferInput{UserID: alice.ID, IdempotencyKey: "k", Source: a.ID, Dest: b.ID, Amount: 3_000, Currency: "INR"}); err != nil {
		t.Fatal(err)
	}

	w := NewWorker(e.pool, e.cfg("w1"), NewConsumer(e.pool).Handle)
	e.drain(w)

	var pending, published int
	e.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status<>'published'), count(*) FILTER (WHERE status='published' AND published_at IS NOT NULL AND locked_by IS NULL) FROM outbox_events`).Scan(&pending, &published)
	if pending != 0 || published != 2 {
		t.Errorf("pending=%d published=%d, want 0/2", pending, published)
	}
	// a: +10000 deposit credit, -3000 transfer debit. b: +3000 credit.
	if got := e.activity(a.ID); got != (activity{DebitCount: 1, CreditCount: 1, Debited: 3_000, Credited: 10_000}) {
		t.Errorf("a activity = %+v", got)
	}
	if got := e.activity(b.ID); got != (activity{CreditCount: 1, Credited: 3_000}) {
		t.Errorf("b activity = %+v", got)
	}
}

func TestDuplicateDeliveryDoesNotRepeatEffect(t *testing.T) {
	e := newEnv(t)
	acc := e.accounts(2)
	c := NewConsumer(e.pool)
	id := e.insertEvent("transfer.completed", transferPayloadFor(acc[0], acc[1], 700))
	ev := Event{ID: id, EventType: "transfer.completed", Payload: mustPayload(t, e, id)}

	for i := 0; i < 3; i++ {
		if err := c.Handle(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.activity(acc[0]); got != (activity{DebitCount: 1, Debited: 700}) {
		t.Errorf("source activity after 3 deliveries = %+v", got)
	}
	if got := e.activity(acc[1]); got != (activity{CreditCount: 1, Credited: 700}) {
		t.Errorf("destination activity after 3 deliveries = %+v", got)
	}
}

// The consumer succeeds but the worker's ack never lands (crash between the two commits).
// The event is redelivered; the consumer dedupes it; the event still ends up published.
func TestLostAckRedeliversButEffectAppliedOnce(t *testing.T) {
	e := newEnv(t)
	acc := e.accounts(2)
	id := e.insertEvent("transfer.completed", transferPayloadFor(acc[0], acc[1], 900))
	c := NewConsumer(e.pool)

	crashing := NewWorker(e.pool, e.cfg("crashy"), c.Handle)
	events, err := crashing.Claim(ctx)
	if err != nil || len(events) != 1 {
		t.Fatalf("claim: %v %v", events, err)
	}
	if err := c.Handle(ctx, events[0]); err != nil { // consumer commits...
		t.Fatal(err)
	}
	// ...worker dies before Ack. Lease expires.
	e.expire(id)

	survivor := NewWorker(e.pool, e.cfg("survivor"), c.Handle)
	e.drain(survivor)

	if r := e.row(id); r.Status != "published" || r.Attempts != 2 {
		t.Errorf("row = %+v, want published after 2 deliveries", r)
	}
	if got := e.activity(acc[0]); got != (activity{DebitCount: 1, Debited: 900}) {
		t.Errorf("effect applied wrong number of times: %+v", got)
	}
}

// The dedupe row and the effect commit together: a failure after the dedupe insert must
// leave no dedupe row (otherwise a retry would be skipped and the effect lost).
func TestConsumerEffectAndDedupeAreAtomic(t *testing.T) {
	e := newEnv(t)
	acc := e.accounts(1)
	id := e.insertEvent("transfer.completed", transferPayloadFor(acc[0], uuid.New(), 100)) // destination violates the FK
	ev := Event{ID: id, EventType: "transfer.completed", Payload: mustPayload(t, e, id)}

	if err := NewConsumer(e.pool).Handle(ctx, ev); err == nil {
		t.Fatal("expected FK failure")
	}
	var n int
	e.pool.QueryRow(ctx, `SELECT count(*) FROM consumer_processed_events WHERE event_id = $1`, id).Scan(&n)
	if n != 0 {
		t.Errorf("dedupe row survived a failed effect")
	}
	if got := e.activity(acc[0]); got != (activity{}) {
		t.Errorf("partial effect survived: %+v", got)
	}
}

func mustPayload(t *testing.T, e *env, id int64) json.RawMessage {
	t.Helper()
	var p []byte
	if err := e.pool.QueryRow(ctx, `SELECT payload FROM outbox_events WHERE id=$1`, id).Scan(&p); err != nil {
		t.Fatal(err)
	}
	return p
}

// Several workers drain 200 events concurrently. Without failures, every event must be
// handled exactly once (SKIP LOCKED gives disjoint claims) and end up published.
func TestConcurrentWorkersHandleEachEventOnce(t *testing.T) {
	e := newEnv(t)
	const events, workers = 200, 5
	ids := make([]int64, events)
	for i := range ids {
		ids[i] = e.insertEvent("test.event", map[string]int{"n": i})
	}

	var mu sync.Mutex
	handled := map[int64]int{}
	handler := func(_ context.Context, ev Event) error {
		mu.Lock()
		handled[ev.ID]++
		mu.Unlock()
		return nil
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		cfg := e.cfg(fmt.Sprintf("w%d", i))
		cfg.BatchSize = 7
		w := NewWorker(e.pool, cfg, handler)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				n, err := w.RunOnce(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	for _, id := range ids {
		if handled[id] != 1 {
			t.Errorf("event %d handled %d times, want 1", id, handled[id])
		}
		if r := e.row(id); r.Status != "published" || r.Attempts != 1 {
			t.Errorf("event %d row = %+v", id, r)
		}
	}
}

func TestTransientFailureThenSuccessfulRetry(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	calls := 0
	w := NewWorker(e.pool, e.cfg("w"), func(context.Context, Event) error {
		calls++
		if calls <= 2 {
			return fmt.Errorf("consumer unavailable (call %d)", calls)
		}
		return nil
	})

	if n, _ := w.RunOnce(ctx); n != 1 {
		t.Fatalf("claimed %d", n)
	}
	if r := e.row(id); r.Status != "pending" || r.Attempts != 1 || r.LockedBy != nil || r.LastError == nil || !strings.Contains(*r.LastError, "call 1") {
		t.Errorf("after 1st failure: %+v", r)
	}
	w.RunOnce(ctx)
	w.RunOnce(ctx)
	if r := e.row(id); r.Status != "published" || r.Attempts != 3 || !r.Published {
		t.Errorf("after recovery: %+v", r)
	}
}

func TestBackoffDefersRetry(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	cfg := e.cfg("w")
	cfg.BaseBackoff, cfg.MaxBackoff = time.Hour, 2*time.Hour
	fail := true
	w := NewWorker(e.pool, cfg, func(context.Context, Event) error {
		if fail {
			return errors.New("down")
		}
		return nil
	})

	w.RunOnce(ctx)
	var ahead time.Duration
	e.pool.QueryRow(ctx, `SELECT next_attempt_at - now() FROM outbox_events WHERE id=$1`, id).Scan(&ahead)
	if ahead < 59*time.Minute {
		t.Fatalf("next attempt only %v ahead, want ~1h", ahead)
	}
	if n, _ := w.RunOnce(ctx); n != 0 {
		t.Fatalf("event claimed %d before its backoff elapsed", n)
	}
	// Time passes (simulated), the consumer recovers.
	fail = false
	e.pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() WHERE id=$1`, id)
	w.RunOnce(ctx)
	if r := e.row(id); r.Status != "published" || r.Attempts != 2 {
		t.Errorf("row = %+v", r)
	}
}

func TestPanickingHandlerIsTreatedAsFailure(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	w := NewWorker(e.pool, e.cfg("w"), func(context.Context, Event) error { panic("boom") })
	w.RunOnce(ctx)
	if r := e.row(id); r.Status != "pending" || r.LastError == nil || !strings.Contains(*r.LastError, "panic") {
		t.Errorf("row = %+v", r)
	}
}

func TestExpiredClaimIsRecovered(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	a := NewWorker(e.pool, e.cfg("a"), nil)
	b := NewWorker(e.pool, e.cfg("b"), func(context.Context, Event) error { return nil })

	if evs, err := a.Claim(ctx); err != nil || len(evs) != 1 {
		t.Fatalf("a claim: %v %v", evs, err)
	}
	// Worker a "crashes". While its lease is live nobody else can take the event.
	if evs, _ := b.Claim(ctx); len(evs) != 0 {
		t.Fatalf("b stole a live claim")
	}
	e.expire(id)
	evs, err := b.Claim(ctx)
	if err != nil || len(evs) != 1 || evs[0].Attempt != 2 {
		t.Fatalf("b claim after expiry: %+v %v", evs, err)
	}
	if err := b.Ack(ctx, evs[0]); err != nil {
		t.Fatal(err)
	}
	if r := e.row(id); r.Status != "published" || r.Attempts != 2 {
		t.Errorf("row = %+v", r)
	}
}

func TestStaleWorkerCannotSettleAnotherWorkersClaim(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	a := NewWorker(e.pool, e.cfg("a"), nil)
	b := NewWorker(e.pool, e.cfg("b"), nil)

	first, _ := a.Claim(ctx)
	e.expire(id)
	second, _ := b.Claim(ctx)
	if len(first) != 1 || len(second) != 1 {
		t.Fatal("setup")
	}

	if err := a.Ack(ctx, first[0]); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("stale Ack err = %v, want ErrLeaseLost", err)
	}
	if _, err := a.Fail(ctx, first[0], errors.New("x")); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("stale Fail err = %v, want ErrLeaseLost", err)
	}
	if r := e.row(id); r.Status != "pending" || r.LockedBy == nil || *r.LockedBy != "b" || r.Published {
		t.Errorf("stale worker disturbed b's claim: %+v", r)
	}
	if err := b.Ack(ctx, second[0]); err != nil {
		t.Errorf("b Ack: %v", err)
	}
	if err := b.Ack(ctx, second[0]); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("double Ack err = %v, want ErrLeaseLost", err)
	}
}

// Same worker id reclaiming after expiry: the attempt number fences the old claim.
func TestAttemptNumberFencesSameWorkerID(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	a := NewWorker(e.pool, e.cfg("a"), nil)
	first, _ := a.Claim(ctx)
	e.expire(id)
	second, _ := a.Claim(ctx)
	if len(first) != 1 || len(second) != 1 || second[0].Attempt != 2 {
		t.Fatal("setup")
	}
	if err := a.Ack(ctx, first[0]); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("old attempt Ack err = %v, want ErrLeaseLost", err)
	}
	if err := a.Ack(ctx, second[0]); err != nil {
		t.Errorf("current attempt Ack: %v", err)
	}
}

func TestRetryExhaustionIsInspectableAndRetryable(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	healthy := false
	w := NewWorker(e.pool, e.cfg("w"), func(context.Context, Event) error {
		if healthy {
			return nil
		}
		return errors.New("poison")
	})

	for i := 0; i < 3; i++ {
		w.RunOnce(ctx)
	}
	r := e.row(id)
	if r.Status != "failed" || r.Attempts != 3 || r.LastError == nil || !strings.Contains(*r.LastError, "poison") || r.Published || r.LockedBy != nil {
		t.Fatalf("after exhaustion: %+v", r)
	}
	if n, _ := w.RunOnce(ctx); n != 0 {
		t.Fatalf("failed event was claimed again")
	}
	failed, err := ListFailed(ctx, e.pool, 10)
	if err != nil || len(failed) != 1 || failed[0].ID != id || failed[0].Attempts != 3 || !strings.Contains(failed[0].LastError, "poison") {
		t.Fatalf("ListFailed = %+v (%v)", failed, err)
	}

	if err := RetryFailed(ctx, e.pool, id); err != nil {
		t.Fatal(err)
	}
	if err := RetryFailed(ctx, e.pool, id); !errors.Is(err, ErrNotFailed) {
		t.Errorf("second RetryFailed err = %v, want ErrNotFailed", err)
	}
	if r := e.row(id); r.Status != "pending" || r.Attempts != 0 {
		t.Fatalf("after RetryFailed: %+v", r)
	}
	healthy = true
	w.RunOnce(ctx)
	if r := e.row(id); r.Status != "published" || r.Attempts != 1 {
		t.Errorf("after deliberate retry: %+v", r)
	}
}

// A worker that dies on the LAST attempt leaves an expired lease with attempts == max.
// It must be parked as failed, not left pending forever.
func TestCrashOnFinalAttemptEndsAsFailed(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	if _, err := e.pool.Exec(ctx, `UPDATE outbox_events SET attempts = 3, locked_by = 'dead', locked_until = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(e.pool, e.cfg("w"), func(context.Context, Event) error { t.Error("handler must not run"); return nil })
	if n, _ := w.RunOnce(ctx); n != 0 {
		t.Fatalf("claimed %d", n)
	}
	r := e.row(id)
	if r.Status != "failed" || r.LockedBy != nil || r.LastError == nil || !strings.Contains(*r.LastError, "lease expired") {
		t.Errorf("row = %+v", r)
	}
}

func TestRunProcessesEventsAndStopsOnCancel(t *testing.T) {
	e := newEnv(t)
	w := NewWorker(e.pool, e.cfg("w"), func(context.Context, Event) error { return nil })
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	id := e.insertEvent("test.event", map[string]int{})
	deadline := time.Now().Add(10 * time.Second)
	for !e.row(id).Published {
		if time.Now().After(deadline) {
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

// Shutdown mid-handler must not burn an attempt or leave a lease behind.
func TestShutdownReleasesInFlightClaim(t *testing.T) {
	e := newEnv(t)
	id := e.insertEvent("test.event", map[string]int{})
	started := make(chan struct{})
	w := NewWorker(e.pool, e.cfg("w"), func(c context.Context, _ Event) error {
		close(started)
		<-c.Done()
		return c.Err()
	})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if r := e.row(id); r.Status != "pending" || r.Attempts != 0 || r.LockedBy != nil || r.Published {
		t.Errorf("row after shutdown = %+v", r)
	}
}

// Existing outbox rows (pending and published) survive migrating up through the worker
// migrations, and the migrations are reversible.
func TestMigrationPreservesExistingRowsAndIsReversible(t *testing.T) {
	dbURL := testutil.DatabaseURL(t, "outbox_migrate")
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		t.Fatal(err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+strings.SplitN(dbURL, "://", 2)[1])
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	// Bring the schema to version 2 (before the worker columns), whatever state it was in.
	if v, dirty, _ := m.Version(); dirty {
		t.Fatalf("dirty migration state at %d", v)
	}
	if err := m.Migrate(2); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate to 2: %v", err)
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var pendingID, publishedID int64
	if err := pool.QueryRow(ctx, `INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload) VALUES ('t', gen_random_uuid(), 'x', '{}') RETURNING id`).Scan(&pendingID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, published_at) VALUES ('t', gen_random_uuid(), 'x', '{}', now()) RETURNING id`).Scan(&publishedID); err != nil {
		t.Fatal(err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("up: %v", err)
	}
	check := func(id int64, status string, published bool) {
		t.Helper()
		var st string
		var pub bool
		var attempts int
		if err := pool.QueryRow(ctx, `SELECT status, published_at IS NOT NULL, attempts FROM outbox_events WHERE id=$1`, id).Scan(&st, &pub, &attempts); err != nil {
			t.Fatalf("row %d lost after migration: %v", id, err)
		}
		if st != status || pub != published || attempts != 0 {
			t.Errorf("row %d: status=%s published=%v attempts=%d", id, st, pub, attempts)
		}
	}
	check(pendingID, "pending", false)
	check(publishedID, "published", true)

	// Down to 2 and back up again: data survives and the schema is consistent.
	if err := m.Migrate(2); err != nil {
		t.Fatalf("down to 2: %v", err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	check(pendingID, "pending", false)
	check(publishedID, "published", true)
}
