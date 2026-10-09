package httpapi

// These tests need the handler to see data that must never be committed, so they build the
// API directly and replace the reconcile function (a.reconcile). The real checks still run:
// reconcile.Check executes inside an uncommitted REPEATABLE READ transaction holding the
// injected corruption, which is rolled back at cleanup. Nothing corrupt is ever committed.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/reconcile"
	"ledgerflow/internal/testutil"
)

const systemUserID = "00000000-0000-0000-0000-000000000001"

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil).WithContext(context.Background()))
	return rec
}

func apiWith(pool *pgxpool.Pool, fn func(context.Context) (*reconcile.Report, error)) http.Handler {
	return (&API{pool: pool, ledger: ledger.New(pool), reconcile: fn}).routes(true)
}

// rollbackOnly opens a transaction that is rolled back at cleanup. There is no Commit call
// anywhere in this file.
func rollbackOnly(t *testing.T, pool *pgxpool.Pool, opts pgx.TxOptions) pgx.Tx {
	t.Helper()
	tx, err := pool.BeginTx(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) }) // before the pool closes
	return tx
}

func TestReconciliationEndpointReportsDiscrepanciesAsData(t *testing.T) {
	pool := testutil.IsolatedPool(t, "reconcile_http")
	tx := rollbackOnly(t, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})

	// Stored balance 500 with no ledger entries behind it.
	var bad string
	if err := tx.QueryRow(context.Background(),
		`INSERT INTO accounts (user_id, currency, balance) VALUES ($1, 'INR', 500) RETURNING id`, systemUserID).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	h := apiWith(pool, func(ctx context.Context) (*reconcile.Report, error) {
		return reconcile.Check(ctx, tx, reconcile.Options{})
	})

	rec := get(h, "/dev/reconciliation")
	if rec.Code != http.StatusOK {
		t.Fatalf("a report with discrepancies must be 200, got %d: %s", rec.Code, rec.Body)
	}
	var rep reconcile.Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Consistent {
		t.Fatal("consistent = true despite an injected balance mismatch")
	}
	if rep.Counts[reconcile.BalanceMismatch] != 1 || len(rep.Discrepancies) != 1 || rep.Truncated {
		t.Fatalf("counts=%v discrepancies=%+v truncated=%v", rep.Counts, rep.Discrepancies, rep.Truncated)
	}
	d := rep.Discrepancies[0]
	if d.Category != reconcile.BalanceMismatch || d.AccountID.String() != bad ||
		d.Expected != "0" || d.Actual != "500" || d.Difference != "500" {
		t.Errorf("unexpected finding %+v", d)
	}

	// Wire format: stable field names and string-typed decimals.
	var raw struct {
		Consistent    bool `json:"consistent"`
		Discrepancies []struct {
			Category string `json:"category"`
			Expected string `json:"expected"`
			Actual   string `json:"actual"`
		} `json:"discrepancies"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &raw)
	if raw.Consistent || len(raw.Discrepancies) != 1 || raw.Discrepancies[0].Category != "BALANCE_MISMATCH" ||
		raw.Discrepancies[0].Actual != "500" {
		t.Errorf("unexpected wire format: %s", rec.Body)
	}
}

func TestReconciliationEndpointHidesFailureDetails(t *testing.T) {
	pool := testutil.IsolatedPool(t, "reconcile_http")
	const secret = "hunter2"

	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{
			name: "generic database error",
			err: fmt.Errorf("reconcile: BALANCE_MISMATCH: dial tcp postgres://ledgerflow:%s@db:5432/ledgerflow: "+
				"ERROR: relation \"accounts\" does not exist (SQL: SELECT secret FROM accounts)", secret),
			wantStatus: http.StatusInternalServerError, wantCode: "internal_error",
		},
		{
			name: "statement timeout",
			err: fmt.Errorf("reconcile: TXN_SHAPE_MISMATCH: %w", &pgconn.PgError{
				Code: "57014", Message: "canceling statement due to statement timeout",
				Where: "SELECT t.id FROM transactions t JOIN ledger_entries e", Detail: "password=" + secret}),
			wantStatus: http.StatusServiceUnavailable, wantCode: "reconciliation_timeout",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := get(apiWith(pool, func(context.Context) (*reconcile.Report, error) { return nil, tc.err }), "/dev/reconciliation")
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, tc.wantStatus, rec.Body)
			}
			var body struct {
				Error struct{ Code, Message string }
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("error body is not the standard JSON envelope: %q", rec.Body)
			}
			if body.Error.Code != tc.wantCode {
				t.Errorf("code = %q, want %q", body.Error.Code, tc.wantCode)
			}
			for _, leak := range []string{secret, "postgres://", "SELECT", "FROM", "accounts", "BALANCE_MISMATCH", "TXN_SHAPE", "dial tcp", "57014", "goroutine"} {
				if strings.Contains(rec.Body.String(), leak) {
					t.Errorf("response leaks %q: %s", leak, rec.Body)
				}
			}
		})
	}
}

// A genuine statement timeout from PostgreSQL, produced deterministically: another
// transaction holds ACCESS EXCLUSIVE on ledger_entries (in this test-only database), so the
// report's first statement waits and then hits its 150ms statement_timeout (SQLSTATE 57014).
func TestReconciliationEndpointRealStatementTimeout(t *testing.T) {
	pool := testutil.IsolatedPool(t, "reconcile_http")
	locker := rollbackOnly(t, pool, pgx.TxOptions{})
	if _, err := locker.Exec(context.Background(), `LOCK TABLE ledger_entries IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	var sawTimeout bool
	h := apiWith(pool, func(ctx context.Context) (*reconcile.Report, error) {
		rep, err := reconcile.Run(ctx, pool, reconcile.Options{StatementTimeout: 150 * time.Millisecond})
		var pgErr *pgconn.PgError
		sawTimeout = errors.As(err, &pgErr) && pgErr.Code == "57014"
		return rep, err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // guard against a hang
	defer cancel()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/dev/reconciliation", nil).WithContext(ctx))

	if !sawTimeout {
		t.Fatalf("expected PostgreSQL to cancel the statement (57014); status %d body %s", rec.Code, rec.Body)
	}
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"reconciliation_timeout"`) {
		t.Errorf("status = %d body %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "statement timeout") || strings.Contains(rec.Body.String(), "ledger_entries") {
		t.Errorf("response leaks database details: %s", rec.Body)
	}
}
