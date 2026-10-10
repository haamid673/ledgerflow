package httpapi_test

// End-to-end test across components that are otherwise tested separately: HTTP transfers
// (with duplicate idempotency keys, a rejection and a key reuse, issued concurrently) ->
// ledger -> outbox worker -> demo consumer, with the reconciliation endpoint polled while
// writes are in flight and again at the end.
//
// It runs in its own database ("<TEST_DATABASE_URL database>_e2e") because the outbox worker
// claims every pending event. Only valid data is ever committed there.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"ledgerflow/internal/httpapi"
	"ledgerflow/internal/outbox"
	"ledgerflow/internal/reconcile"
	"ledgerflow/internal/testutil"
)

func TestEndToEndTransfersOutboxAndReconciliation(t *testing.T) {
	pool := testutil.IsolatedPool(t, "e2e")
	c := client{t, httpapi.New(pool, true)}

	// Real outbox worker + consumer, as cmd/api wires them. Stopped and joined before the
	// pool is closed (this cleanup is registered after the pool's, so it runs first).
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	w := outbox.NewWorker(pool, outbox.Config{
		WorkerID: "e2e", BatchSize: 10, Lease: 30 * time.Second, MaxAttempts: 5,
		BaseBackoff: 10 * time.Millisecond, MaxBackoff: time.Second, PollInterval: 10 * time.Millisecond,
	}, outbox.NewConsumer(pool).Handle)
	go func() { workerDone <- w.Run(workerCtx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		stopWorker()
		select {
		case err := <-workerDone:
			if err != nil {
				t.Errorf("worker Run returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop after cancel")
		}
	}
	t.Cleanup(stop)

	// Setup through the API: 4 users, one account each holding 100_000.
	const nAccounts, opening = 4, 100_000
	users, accts := make([]string, nAccounts), make([]string, nAccounts)
	for i := range users {
		rec := c.do("POST", "/dev/users", nil, fmt.Sprintf(`{"email":"%s@e2e.local","name":"E2E"}`, uuid.NewString()))
		var u struct{ ID string }
		c.decode(rec, &u)
		users[i] = u.ID
		rec = c.do("POST", "/dev/accounts", nil, fmt.Sprintf(`{"user_id":"%s","opening_balance":%d}`, u.ID, opening))
		if rec.Code != http.StatusCreated {
			t.Fatalf("create account: %d %s", rec.Code, rec.Body)
		}
		var a struct{ ID string }
		c.decode(rec, &a)
		accts[i] = a.ID
	}

	// Plan: 40 distinct transfers; the first 5 are each sent 3 times concurrently with the
	// same idempotency key. Amounts are small enough that none can be insufficient.
	type op struct {
		user, key, src, dst string
		amount              int64
		si, di              int
	}
	run := uuid.NewString()
	rng := rand.New(rand.NewSource(42))
	var distinct []op
	for i := 0; i < 40; i++ {
		s := rng.Intn(nAccounts)
		d := (s + 1 + rng.Intn(nAccounts-1)) % nAccounts // never the same account; opposing pairs occur
		distinct = append(distinct, op{users[s], fmt.Sprintf("e2e-%s-%d", run, i), accts[s], accts[d], int64(1 + rng.Intn(500)), s, d})
	}
	var sends []op
	sends = append(sends, distinct...)
	for _, o := range distinct[:5] {
		sends = append(sends, o, o)
	}

	// Reconciliation loop while writes and the worker are active.
	var reports, inconsistent atomic.Int64
	reconStop := make(chan struct{})
	var recon sync.WaitGroup
	recon.Add(1)
	go func() {
		defer recon.Done()
		for {
			rec := c.do("GET", "/dev/reconciliation", nil, "")
			var rep reconcile.Report
			if rec.Code != http.StatusOK {
				t.Errorf("reconciliation status %d: %s", rec.Code, rec.Body)
				return
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
				t.Errorf("decode report: %v", err)
				return
			}
			reports.Add(1)
			if !rep.Consistent {
				inconsistent.Add(1)
				t.Errorf("inconsistent report during writes: %v %+v", rep.Counts, rep.Discrepancies)
			}
			select {
			case <-reconStop:
				if reports.Load() >= 3 {
					return
				}
			default:
			}
		}
	}()

	type outcome struct {
		code     int
		replayed bool
		txn      string
	}
	var mu sync.Mutex
	outcomes := map[string][]outcome{}
	work := make(chan op)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range work {
				rec := c.do("POST", "/api/transfers",
					map[string]string{"X-User-ID": o.user, "Idempotency-Key": o.key},
					fmt.Sprintf(`{"source_account_id":"%s","destination_account_id":"%s","amount":%d,"currency":"INR"}`, o.src, o.dst, o.amount))
				var res struct {
					TransactionID string `json:"transaction_id"`
				}
				_ = json.Unmarshal(rec.Body.Bytes(), &res)
				mu.Lock()
				outcomes[o.key] = append(outcomes[o.key], outcome{rec.Code, rec.Header().Get("Idempotent-Replayed") == "true", res.TransactionID})
				mu.Unlock()
			}
		}()
	}
	for _, o := range sends {
		work <- o
	}
	close(work)
	wg.Wait()

	// Rejections after the fact: insufficient funds, and a key reused for a different request.
	over := c.do("POST", "/api/transfers", map[string]string{"X-User-ID": users[0], "Idempotency-Key": "e2e-over-" + run},
		fmt.Sprintf(`{"source_account_id":"%s","destination_account_id":"%s","amount":999999999,"currency":"INR"}`, accts[0], accts[1]))
	if over.Code != http.StatusUnprocessableEntity {
		t.Errorf("overspend status = %d, want 422: %s", over.Code, over.Body)
	}
	first := distinct[0]
	reuse := c.do("POST", "/api/transfers", map[string]string{"X-User-ID": first.user, "Idempotency-Key": first.key},
		fmt.Sprintf(`{"source_account_id":"%s","destination_account_id":"%s","amount":%d,"currency":"INR"}`, first.src, first.dst, first.amount+1))
	if reuse.Code != http.StatusUnprocessableEntity {
		t.Errorf("key reuse status = %d, want 422: %s", reuse.Code, reuse.Body)
	}

	// Idempotency: every key succeeded; duplicates agree on one transaction and exactly one
	// of them actually executed.
	expected := make([]int64, nAccounts)
	for i := range expected {
		expected[i] = opening
	}
	debits, credits := make([]int64, nAccounts), make([]int64, nAccounts)
	for _, o := range distinct {
		outs := outcomes[o.key]
		want := 1
		for _, d := range distinct[:5] {
			if d.key == o.key {
				want = 3
			}
		}
		if len(outs) != want {
			t.Fatalf("key %s: %d responses, want %d", o.key, len(outs), want)
		}
		executed := 0
		for _, out := range outs {
			if out.code != http.StatusCreated {
				t.Errorf("key %s: status %d", o.key, out.code)
			}
			if out.txn == "" || out.txn != outs[0].txn {
				t.Errorf("key %s: transaction ids differ: %+v", o.key, outs)
			}
			if !out.replayed {
				executed++
			}
		}
		if executed != 1 {
			t.Errorf("key %s executed %d times, want exactly 1", o.key, executed)
		}
		expected[o.si] -= o.amount
		expected[o.di] += o.amount
		debits[o.si]++
		credits[o.di]++
	}

	// Ledger state through the API and the database.
	var total int64
	for i, id := range accts {
		rec := c.do("GET", "/api/accounts/"+id, map[string]string{"X-User-ID": users[i]}, "")
		var a struct{ Balance int64 }
		c.decode(rec, &a)
		if a.Balance != expected[i] {
			t.Errorf("account %d balance = %d, want %d", i, a.Balance, expected[i])
		}
		total += a.Balance
	}
	if total != nAccounts*opening {
		t.Errorf("total money = %d, want %d", total, nAccounts*opening)
	}
	var txns, events int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM transactions WHERE type='transfer' AND source_account_id::text = ANY($1)`, accts).Scan(&txns); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE event_type='transfer.completed' AND payload->>'source_account_id' = ANY($1)`, accts).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if txns != len(distinct) || events != len(distinct) {
		t.Errorf("transactions=%d outbox events=%d, want %d each (no duplicates from retries)", txns, events, len(distinct))
	}

	// Worker drains everything it was given: nothing pending, failed or leased.
	deadline := time.Now().Add(20 * time.Second)
	for {
		var notPublished int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE status <> 'published'`).Scan(&notPublished); err != nil {
			t.Fatal(err)
		}
		if notPublished == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d outbox events still not published", notPublished)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The consumer's read model agrees with the ledger: net activity equals the balance,
	// and the counts match exactly (1 funding deposit + transfers), i.e. each event applied once.
	for i, id := range accts {
		var dc, cc, td, tc int64
		if err := pool.QueryRow(context.Background(),
			`SELECT debit_count, credit_count, total_debited, total_credited FROM account_activity WHERE account_id = $1`, id).
			Scan(&dc, &cc, &td, &tc); err != nil {
			t.Fatalf("account_activity for account %d: %v", i, err)
		}
		if tc-td != expected[i] {
			t.Errorf("account %d: credited-debited = %d, balance %d", i, tc-td, expected[i])
		}
		if dc != debits[i] || cc != credits[i]+1 {
			t.Errorf("account %d: debit_count=%d credit_count=%d, want %d/%d", i, dc, cc, debits[i], credits[i]+1)
		}
	}

	stop() // join the worker before the final report
	close(reconStop)
	recon.Wait()
	if reports.Load() < 3 || inconsistent.Load() != 0 {
		t.Errorf("reports=%d inconsistent=%d", reports.Load(), inconsistent.Load())
	}

	final := c.do("GET", "/dev/reconciliation", nil, "")
	var rep reconcile.Report
	c.decode(final, &rep)
	if final.Code != http.StatusOK || !rep.Consistent || len(rep.Discrepancies) != 0 {
		t.Errorf("final report: status=%d consistent=%v counts=%v %+v", final.Code, rep.Consistent, rep.Counts, rep.Discrepancies)
	}
	t.Logf("%d reconciliation reports taken while %d requests were in flight", reports.Load(), len(sends))
}
