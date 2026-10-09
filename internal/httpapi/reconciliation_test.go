package httpapi_test

// HTTP tests for GET /dev/reconciliation using the real wiring (httpapi.New). They run in a
// dedicated database, "<TEST_DATABASE_URL database>_reconcile_http", which only ever holds
// valid committed data: corruption cases live in reconciliation_internal_test.go and use
// rollback-only transactions.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"ledgerflow/internal/httpapi"
	"ledgerflow/internal/reconcile"
	"ledgerflow/internal/testutil"
)

func TestReconciliationEndpointDisabledWithoutDevEndpoints(t *testing.T) {
	c := client{t, httpapi.New(testutil.IsolatedPool(t, "reconcile_http"), false)}
	for _, path := range []string{"/dev/reconciliation", "/dev/reconciliation/", "/api/reconciliation"} {
		rec := c.do("GET", path, nil, "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s with DEV_ENDPOINTS=false = %d, want 404", path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), `"accounts_checked"`) {
			t.Errorf("GET %s leaked a report", path)
		}
	}
}

func TestReconciliationEndpoint(t *testing.T) {
	c := client{t, httpapi.New(testutil.IsolatedPool(t, "reconcile_http"), true)}

	// Real data through the real API, so the report is not trivially empty.
	mkUser := func() string {
		rec := c.do("POST", "/dev/users", nil, fmt.Sprintf(`{"email":"%s@t.local","name":"T"}`, uuid.NewString()))
		var u struct{ ID string }
		c.decode(rec, &u)
		return u.ID
	}
	mkAccount := func(user string, bal int64) string {
		rec := c.do("POST", "/dev/accounts", nil, fmt.Sprintf(`{"user_id":"%s","opening_balance":%d}`, user, bal))
		if rec.Code != http.StatusCreated {
			t.Fatalf("create account: %d %s", rec.Code, rec.Body)
		}
		var a struct{ ID string }
		c.decode(rec, &a)
		return a.ID
	}
	alice, bob := mkUser(), mkUser()
	a, b := mkAccount(alice, 10_000), mkAccount(bob, 0)
	rec := c.do("POST", "/api/transfers", map[string]string{"X-User-ID": alice, "Idempotency-Key": uuid.NewString()},
		fmt.Sprintf(`{"source_account_id":"%s","destination_account_id":"%s","amount":2500,"currency":"INR"}`, a, b))
	if rec.Code != http.StatusCreated {
		t.Fatalf("transfer: %d %s", rec.Code, rec.Body)
	}

	t.Run("clean database is consistent", func(t *testing.T) {
		// No X-User-ID: the route does not consult the (development-only) identity header.
		rec := c.do("GET", "/dev/reconciliation", nil, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}
		var rep reconcile.Report
		c.decode(rec, &rep)
		if !rep.Consistent || rep.Truncated || len(rep.Discrepancies) != 0 {
			t.Errorf("want a clean report, got consistent=%v truncated=%v discrepancies=%+v", rep.Consistent, rep.Truncated, rep.Discrepancies)
		}
		if len(rep.Counts) != 7 {
			t.Errorf("counts = %v, want all 7 categories", rep.Counts)
		}
		for cat, n := range rep.Counts {
			if n != 0 {
				t.Errorf("count[%s] = %d on a clean database", cat, n)
			}
		}
		if rep.AccountsChecked < 3 || rep.TransactionsChecked < 2 || rep.EntriesChecked < 4 {
			t.Errorf("checked %d accounts / %d transactions / %d entries; expected at least the 3/2/4 created here",
				rep.AccountsChecked, rep.TransactionsChecked, rep.EntriesChecked)
		}
		if rep.Snapshot.Isolation != "repeatable read" || !rep.Snapshot.ReadOnly {
			t.Errorf("snapshot settings = %+v", rep.Snapshot)
		}

		// Empty collections serialize as [] rather than null, so clients can iterate safely.
		var raw map[string]any
		c.decode(rec, &raw)
		if d, ok := raw["discrepancies"].([]any); !ok || len(d) != 0 {
			t.Errorf(`"discrepancies" = %#v, want []`, raw["discrepancies"])
		}
		if raw["consistent"] != true {
			t.Errorf(`"consistent" = %#v`, raw["consistent"])
		}
	})

	t.Run("only GET on /dev, never under /api", func(t *testing.T) {
		if rec := c.do("POST", "/dev/reconciliation", nil, "{}"); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST /dev/reconciliation = %d, want 405", rec.Code)
		}
		hdr := map[string]string{"X-User-ID": alice}
		if rec := c.do("GET", "/api/reconciliation", hdr, ""); rec.Code != http.StatusNotFound {
			t.Errorf("GET /api/reconciliation = %d, want 404", rec.Code)
		}
	})

	t.Run("existing routes still work", func(t *testing.T) {
		if rec := c.do("GET", "/health", nil, ""); rec.Code != http.StatusOK {
			t.Errorf("health = %d", rec.Code)
		}
		if rec := c.do("GET", "/api/accounts/"+a, map[string]string{"X-User-ID": alice}, ""); rec.Code != http.StatusOK {
			t.Errorf("get account = %d", rec.Code)
		}
		if rec := c.do("GET", "/dev/outbox/failed", nil, ""); rec.Code != http.StatusOK {
			t.Errorf("failed outbox list = %d", rec.Code)
		}
	})
}

// A real execution failure through the real wiring: the pool is closed, so reconcile.Run
// cannot even begin its transaction.
func TestReconciliationEndpointExecutionFailure(t *testing.T) {
	pool := testutil.IsolatedPool(t, "reconcile_http")
	h := httpapi.New(pool, true)
	pool.Close() // safe to call twice; the helper closes it again at cleanup

	c := client{t, h}
	rec := c.do("GET", "/dev/reconciliation", nil, "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", rec.Code, rec.Body)
	}
	want := `{"error":{"code":"internal_error","message":"internal error"}}` + "\n"
	if rec.Body.String() != want {
		t.Errorf("body = %q, want the generic error %q", rec.Body.String(), want)
	}
}
