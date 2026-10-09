package httpapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"ledgerflow/internal/httpapi"
	"ledgerflow/internal/testutil"
)

type client struct {
	t *testing.T
	h http.Handler
}

func (c client) do(method, path string, headers map[string]string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, req)
	return rec
}

func (c client) decode(rec *httptest.ResponseRecorder, v any) {
	c.t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		c.t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
}

func TestTransferAPI(t *testing.T) {
	c := client{t, httpapi.New(testutil.Pool(t), true)}

	mkUser := func() string {
		rec := c.do("POST", "/dev/users", nil, fmt.Sprintf(`{"email":"%s@t.local","name":"T"}`, uuid.NewString()))
		var u struct{ ID string }
		c.decode(rec, &u)
		return u.ID
	}
	mkAccount := func(user string, bal int64) string {
		rec := c.do("POST", "/dev/accounts", nil, fmt.Sprintf(`{"user_id":"%s","opening_balance":%d}`, user, bal))
		if rec.Code != 201 {
			t.Fatalf("create account: %d %s", rec.Code, rec.Body)
		}
		var a struct{ ID string }
		c.decode(rec, &a)
		return a.ID
	}
	alice, bob := mkUser(), mkUser()
	a, b := mkAccount(alice, 10_000), mkAccount(bob, 0)

	body := func(amount int64) string {
		return fmt.Sprintf(`{"source_account_id":"%s","destination_account_id":"%s","amount":%d,"currency":"INR"}`, a, b, amount)
	}
	hdr := func(key string) map[string]string {
		return map[string]string{"X-User-ID": alice, "Idempotency-Key": key}
	}

	t.Run("health", func(t *testing.T) {
		if rec := c.do("GET", "/health", nil, ""); rec.Code != 200 {
			t.Fatalf("health = %d", rec.Code)
		}
	})
	t.Run("requires user and idempotency key", func(t *testing.T) {
		if rec := c.do("POST", "/api/transfers", map[string]string{"Idempotency-Key": "x"}, body(1)); rec.Code != 401 {
			t.Errorf("no user: %d", rec.Code)
		}
		if rec := c.do("POST", "/api/transfers", map[string]string{"X-User-ID": alice}, body(1)); rec.Code != 400 {
			t.Errorf("no key: %d", rec.Code)
		}
	})
	t.Run("account reads require the owner", func(t *testing.T) {
		if rec := c.do("GET", "/api/accounts/"+a, nil, ""); rec.Code != 401 {
			t.Errorf("no user: %d, want 401", rec.Code)
		}
		if rec := c.do("GET", "/api/accounts/"+a, map[string]string{"X-User-ID": bob}, ""); rec.Code != 404 {
			t.Errorf("other user: %d, want 404", rec.Code)
		}
		if rec := c.do("GET", "/api/accounts/"+a, map[string]string{"X-User-ID": alice}, ""); rec.Code != 200 {
			t.Errorf("owner: %d, want 200", rec.Code)
		}
	})
	t.Run("rejects invalid and malformed amounts", func(t *testing.T) {
		for _, amt := range []string{
			`0`, `-1`, `1.5`, `1.0`, `1e3`, `"100"`, `true`, `[]`, `null`,
			`9223372036854775807`, `9223372036854775808`, `99999999999999999999999`,
		} {
			raw := strings.Replace(body(1), `"amount":1`, `"amount":`+amt, 1)
			rec := c.do("POST", "/api/transfers", hdr("amt-"+amt), raw)
			if rec.Code < 400 || rec.Code >= 500 {
				t.Errorf("amount %s: status %d (%s), want a 4xx", amt, rec.Code, rec.Body)
			}
		}
		var acct struct{ Balance int64 }
		c.decode(c.do("GET", "/api/accounts/"+a, map[string]string{"X-User-ID": alice}, ""), &acct)
		if acct.Balance != 10_000 {
			t.Errorf("balance moved to %d by rejected amounts", acct.Balance)
		}
	})
	t.Run("rejects fractional and unknown fields", func(t *testing.T) {
		bad := strings.Replace(body(1), `"amount":1`, `"amount":1.5`, 1)
		if rec := c.do("POST", "/api/transfers", hdr("frac"), bad); rec.Code != 400 {
			t.Errorf("fractional amount: %d %s", rec.Code, rec.Body)
		}
	})
	t.Run("transfer, replay, key reuse, insufficient funds", func(t *testing.T) {
		first := c.do("POST", "/api/transfers", hdr("api-1"), body(2_500))
		if first.Code != 201 {
			t.Fatalf("first: %d %s", first.Code, first.Body)
		}
		replay := c.do("POST", "/api/transfers", hdr("api-1"), body(2_500))
		if replay.Code != 201 || replay.Header().Get("Idempotent-Replayed") != "true" {
			t.Fatalf("replay: %d replayed=%q", replay.Code, replay.Header().Get("Idempotent-Replayed"))
		}
		if first.Body.String() != replay.Body.String() {
			t.Errorf("replay body differs:\n%s\n%s", first.Body, replay.Body)
		}
		if rec := c.do("POST", "/api/transfers", hdr("api-1"), body(9)); rec.Code != 422 || !strings.Contains(rec.Body.String(), "idempotency_key_reuse") {
			t.Errorf("key reuse: %d %s", rec.Code, rec.Body)
		}
		if rec := c.do("POST", "/api/transfers", hdr("api-2"), body(999_999)); rec.Code != 422 || !strings.Contains(rec.Body.String(), "insufficient_funds") {
			t.Errorf("insufficient: %d %s", rec.Code, rec.Body)
		}

		var acct struct{ Balance int64 }
		c.decode(c.do("GET", "/api/accounts/"+a, map[string]string{"X-User-ID": alice}, ""), &acct)
		if acct.Balance != 7_500 {
			t.Errorf("source balance = %d, want 7500", acct.Balance)
		}
		c.decode(c.do("GET", "/api/accounts/"+b, map[string]string{"X-User-ID": bob}, ""), &acct)
		if acct.Balance != 2_500 {
			t.Errorf("destination balance = %d, want 2500", acct.Balance)
		}
	})
}
